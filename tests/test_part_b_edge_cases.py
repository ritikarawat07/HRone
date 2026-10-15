"""Comprehensive Part B Edge-Case and Boundary Suite.

Tests all 20 specific edge cases required by the compliance audit:
1. Eligible employee with no attendance logs.
2. Multiple employees tied on total late minutes.
3. Weekend attendance records.
4. An employee who joins in the middle of a month.
5. An overnight shift crossing midnight.
6. A weekday with no attendance record.
7. A record explicitly marked ABSENT.
8. A record marked LEAVE.
9. Employees marked PRESENT, WFH, and ON_DUTY.
10. Punch-in exactly at the 10-minute grace boundary.
11. Punch-in one second beyond the grace boundary.
12. Overtime just below 30 minutes and exactly at the 30-minute threshold.
13. Work hours near half-up rounding boundaries.
14. Work duration rounding to exactly 4.50 hours and just below 4.50 hours.
15. Invalid timestamps: floating-point, strings, seconds-based, malformed.
16. Missing employee and missing attendance record.
17. Invalid pagination, page size above 100, and empty filtered results.
18. Missing dates in daily trends and seven-day moving-average boundaries.
19. Date and timezone boundaries around midnight IST.
20. Repeated requests and concurrent writes.
"""

import asyncio
import os
import unittest
import uuid
from datetime import date, datetime, time, timedelta, timezone

import httpx
from pymongo import AsyncMongoClient

import app.main as main_module

TEST_MONGO_URI = os.getenv("MONGO_TEST_URI", main_module.MONGO_URI)
TEST_MONGO_DB = os.getenv("MONGO_TEST_DB") or (
    f"{main_module.MONGO_DB}_test" if main_module.MONGO_DB else None
)


@unittest.skipUnless(
    TEST_MONGO_URI and TEST_MONGO_DB,
    "Requires configured MONGO_URI / MONGO_DB (or test overrides)",
)
class PartBEdgeCasesTestSuite(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        if main_module.MONGO_DB and TEST_MONGO_DB == main_module.MONGO_DB:
            self.fail("Live MongoDB tests must use a dedicated test database")
        self.original_db = main_module.db
        self.client = AsyncMongoClient(
            TEST_MONGO_URI,
            tz_aware=True,
            serverSelectionTimeoutMS=5000,
        )
        await self.client.admin.command("ping")
        self.database = self.client[TEST_MONGO_DB]
        main_module.db = self.database
        await main_module.create_indexes()

        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main_module.app),
            base_url="http://test",
        )
        self.test_prefix = uuid.uuid4().hex[:8]
        self.employee_codes = set()
        self.addAsyncCleanup(self._cleanup)

    async def asyncTearDown(self):
        main_module.db = self.original_db

    async def _cleanup(self):
        await self.http.aclose()
        main_module.db = self.original_db
        if self.employee_codes:
            await self.database.attendance_logs.delete_many(
                {"emp_code": {"$in": list(self.employee_codes)}}
            )
            await self.database.employees.delete_many(
                {"emp_code": {"$in": list(self.employee_codes)}}
            )
        await self.client.close()

    def _make_code(self, number: int) -> str:
        # 6-digit EMP code format
        code = f"EMP{number:06d}"
        self.employee_codes.add(code)
        return code

    async def _create_test_employee(
        self,
        emp_code: str,
        department: str = "Engineering",
        joined_on: str = "2026-07-01",
        shift_start: str = "09:30",
        shift_end: str = "18:30",
    ):
        doc = {
            "emp_code": emp_code,
            "name": f"Test {emp_code}",
            "email": f"{emp_code.lower()}@example.test",
            "department": department,
            "shift_start": shift_start,
            "shift_end": shift_end,
            "joined_on": joined_on,
            "created_at": datetime.now(timezone.utc),
        }
        await self.database.employees.insert_one(doc)
        return doc

    # Case 1: Eligible employee with no attendance logs
    async def test_case_01_eligible_employee_with_no_attendance_logs(self):
        code = self._make_code(910001)
        dept = f"NoLogs-{self.test_prefix}"
        await self._create_test_employee(code, department=dept, joined_on="2026-07-01")

        # Department summary
        resp = await self.http.get(
            "/analytics/departments/summary",
            params={"month": "2026-07", "department": dept},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        items = resp.json()["items"]
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item["department"], dept)
        self.assertEqual(item["headcount"], 1)
        self.assertEqual(item["present_days"], 0)
        self.assertIsNone(item["avg_work_hours"])
        self.assertEqual(item["late_count"], 0)
        self.assertEqual(item["total_late_minutes"], 0)
        self.assertEqual(item["leave_count"], 0)
        self.assertEqual(item["on_duty_count"], 0)

        # Monthly summary
        m_resp = await self.http.get(
            f"/analytics/employees/{code}/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(m_resp.status_code, 200)
        data = m_resp.json()
        self.assertEqual(data["working_days"], 23)
        self.assertEqual(data["present_days"], 0)
        self.assertEqual(data["attendance_pct"], 0.0)

    # Case 2: Multiple employees tied on total late minutes
    async def test_case_02_multiple_employees_tied_on_total_late_minutes(self):
        dept = f"Ties-{self.test_prefix}"
        c1 = self._make_code(920001)
        c2 = self._make_code(920002)
        c3 = self._make_code(920003)
        c4 = self._make_code(920004)
        for c in (c1, c2, c3, c4):
            await self._create_test_employee(c, department=dept)

        # c1 and c2 both have 40 total late minutes (rank 1 tie)
        # c3 has 20 late minutes (rank 3 because rank 2 is skipped)
        # c4 has 10 late minutes (rank 4)
        await self.database.attendance_logs.insert_many([
            {"emp_code": c1, "date": "2026-07-06", "status": "PRESENT", "late_minutes": 40, "history": []},
            {"emp_code": c2, "date": "2026-07-06", "status": "PRESENT", "late_minutes": 40, "history": []},
            {"emp_code": c3, "date": "2026-07-06", "status": "PRESENT", "late_minutes": 20, "history": []},
            {"emp_code": c4, "date": "2026-07-06", "status": "PRESENT", "late_minutes": 10, "history": []},
        ])

        # limit = 1: Both c1 and c2 have rank 1 <= 1, so both return!
        resp1 = await self.http.get(
            "/analytics/leaderboard/late",
            params={"month": "2026-07", "department": dept, "limit": 1},
        )
        self.assertEqual(resp1.status_code, 200)
        items1 = resp1.json()["items"]
        self.assertEqual(len(items1), 2)
        self.assertEqual([i["rank"] for i in items1], [1, 1])
        self.assertEqual([i["emp_code"] for i in items1], [c1, c2])

        # limit = 2: c1 (rank 1), c2 (rank 1). Next is rank 3 (> 2). So only 2 items return!
        resp2 = await self.http.get(
            "/analytics/leaderboard/late",
            params={"month": "2026-07", "department": dept, "limit": 2},
        )
        self.assertEqual(resp2.status_code, 200)
        items2 = resp2.json()["items"]
        self.assertEqual(len(items2), 2)

        # limit = 3: c1 (1), c2 (1), c3 (3) <= 3 -> 3 items
        resp3 = await self.http.get(
            "/analytics/leaderboard/late",
            params={"month": "2026-07", "department": dept, "limit": 3},
        )
        self.assertEqual(resp3.status_code, 200)
        items3 = resp3.json()["items"]
        self.assertEqual([i["rank"] for i in items3], [1, 1, 3])

    # Case 3: Weekend attendance records
    async def test_case_03_weekend_attendance_records(self):
        code = self._make_code(930001)
        dept = f"Weekend-{self.test_prefix}"
        await self._create_test_employee(code, department=dept)

        # 2026-07-11 is Saturday, 2026-07-12 is Sunday
        # 2026-07-10 is Friday
        await self.database.attendance_logs.insert_many([
            {"emp_code": code, "date": "2026-07-10", "status": "PRESENT", "late_minutes": 10, "overtime_minutes": 30, "work_hours": 9.0, "half_day": False, "history": []},
            {"emp_code": code, "date": "2026-07-11", "status": "PRESENT", "late_minutes": 15, "overtime_minutes": 45, "work_hours": 8.0, "half_day": False, "history": []},
            {"emp_code": code, "date": "2026-07-12", "status": "WFH", "late_minutes": 0, "overtime_minutes": 60, "work_hours": 4.0, "half_day": True, "history": []},
        ])

        # R7: present_days counts only Mon-Fri records (1.0). Weekend records still add to late and overtime totals.
        resp = await self.http.get(
            f"/analytics/employees/{code}/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["present_days"], 1.0)
        self.assertEqual(data["total_late_minutes"], 25)
        self.assertEqual(data["total_overtime_minutes"], 135)
        self.assertEqual(data["late_count"], 2)

        # In trend: weekend rows have is_working_day=False, attendance_rate=None, but present_count includes them
        t_resp = await self.http.get(
            f"/analytics/departments/{dept}/trend",
            params={"from": "2026-07-10", "to": "2026-07-12"},
        )
        self.assertEqual(t_resp.status_code, 200)
        trend_items = {i["date"]: i for i in t_resp.json()["items"]}
        self.assertTrue(trend_items["2026-07-10"]["is_working_day"])
        self.assertEqual(trend_items["2026-07-10"]["present_count"], 1.0)
        self.assertEqual(trend_items["2026-07-10"]["attendance_rate"], 1.0)

        self.assertFalse(trend_items["2026-07-11"]["is_working_day"])
        self.assertEqual(trend_items["2026-07-11"]["present_count"], 1.0)
        self.assertIsNone(trend_items["2026-07-11"]["attendance_rate"])

        self.assertFalse(trend_items["2026-07-12"]["is_working_day"])
        self.assertEqual(trend_items["2026-07-12"]["present_count"], 0.5)
        self.assertIsNone(trend_items["2026-07-12"]["attendance_rate"])

    # Case 4: An employee who joins in the middle of a month
    async def test_case_04_mid_month_joiner(self):
        code = self._make_code(940001)
        dept = f"Joiner-{self.test_prefix}"
        # July 15, 2026 is Wednesday. Working days July 15 to July 31 = 13 days
        await self._create_test_employee(code, department=dept, joined_on="2026-07-15")

        resp = await self.http.get(
            f"/analytics/employees/{code}/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["working_days"], 13)

        # In trend: headcount before July 15 is 0; on July 15 it becomes 1
        t_resp = await self.http.get(
            f"/analytics/departments/{dept}/trend",
            params={"from": "2026-07-14", "to": "2026-07-16"},
        )
        self.assertEqual(t_resp.status_code, 200)
        items = {i["date"]: i for i in t_resp.json()["items"]}
        self.assertEqual(items["2026-07-14"]["headcount"], 0)
        self.assertIsNone(items["2026-07-14"]["attendance_rate"])
        self.assertEqual(items["2026-07-15"]["headcount"], 1)
        self.assertEqual(items["2026-07-15"]["attendance_rate"], 0.0)

    # Case 5: Overnight shift crossing midnight
    async def test_case_05_overnight_shift(self):
        code = self._make_code(950001)
        dept = f"Overnight-{self.test_prefix}"
        # 22:00 to 06:00
        await self._create_test_employee(
            code, department=dept, shift_start="22:00", shift_end="06:00"
        )

        # Punch in at 2026-07-07 00:30:00 IST (UTC 2026-07-06 19:00:00)
        # Because 00:30 < 06:00, belongs to shift of 2026-07-06
        # Late minutes: shift started 2026-07-06 22:00. 00:30 is 150 minutes later (> 10) -> 150
        punch_in_ms = int(datetime(2026, 7, 6, 19, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        in_resp = await self.http.post(
            "/attendance/punch-in",
            json={"emp_code": code, "punched_at": punch_in_ms, "status": "PRESENT"},
        )
        self.assertEqual(in_resp.status_code, 201)
        rec = in_resp.json()
        self.assertEqual(rec["date"], "2026-07-06")
        self.assertEqual(rec["late_minutes"], 150)

        # Punch out at 2026-07-07 07:15:00 IST (UTC 2026-07-07 01:45:00)
        # Shift end is 2026-07-07 06:00:00. Overtime = 75 minutes (>= 30).
        # Duration: 00:30 to 07:15 = 6.75 hours.
        punch_out_ms = int(datetime(2026, 7, 7, 1, 45, 0, tzinfo=timezone.utc).timestamp() * 1000)
        out_resp = await self.http.post(
            "/attendance/punch-out",
            json={"emp_code": code, "punched_at": punch_out_ms},
        )
        self.assertEqual(out_resp.status_code, 200)
        rec_out = out_resp.json()
        self.assertEqual(rec_out["date"], "2026-07-06")
        self.assertEqual(rec_out["work_hours"], 6.75)
        self.assertEqual(rec_out["overtime_minutes"], 75)
        self.assertFalse(rec_out["half_day"])

    # Case 6: Weekday with no attendance record
    async def test_case_06_weekday_with_no_attendance_record(self):
        code = self._make_code(960001)
        dept = f"WeekdayGap-{self.test_prefix}"
        await self._create_test_employee(code, department=dept, joined_on="2026-07-01")

        # Employee only punches in on Monday (2026-07-06). Tuesday (2026-07-07) has no log.
        await self.database.attendance_logs.insert_one({
            "emp_code": code,
            "date": "2026-07-06",
            "status": "PRESENT",
            "punch_in": datetime(2026, 7, 6, 4, 0, tzinfo=timezone.utc),
            "punch_out": datetime(2026, 7, 6, 13, 0, tzinfo=timezone.utc),
            "work_hours": 9.0,
            "late_minutes": 0,
            "overtime_minutes": 0,
            "half_day": False,
            "history": [],
        })

        resp = await self.http.get(
            f"/analytics/employees/{code}/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["working_days"], 23)
        self.assertEqual(data["present_days"], 1.0)
        self.assertEqual(data["leave_days"], 0)

        # Gap filled trend for department
        t_resp = await self.http.get(
            f"/analytics/departments/{dept}/trend",
            params={"from": "2026-07-06", "to": "2026-07-07"},
        )
        self.assertEqual(t_resp.status_code, 200)
        items = {i["date"]: i for i in t_resp.json()["items"]}
        self.assertEqual(items["2026-07-06"]["present_count"], 1.0)
        self.assertEqual(items["2026-07-06"]["attendance_rate"], 1.0)
        self.assertEqual(items["2026-07-07"]["present_count"], 0.0)
        self.assertEqual(items["2026-07-07"]["attendance_rate"], 0.0)

    # Case 7: Record explicitly marked ABSENT
    async def test_case_07_record_marked_absent(self):
        code = self._make_code(970001)
        dept = f"Absent-{self.test_prefix}"
        await self._create_test_employee(code, department=dept)

        await self.database.attendance_logs.insert_one({
            "emp_code": code,
            "date": "2026-07-06",
            "status": "ABSENT",
            "punch_in": None,
            "punch_out": None,
            "work_hours": None,
            "late_minutes": 0,
            "overtime_minutes": 0,
            "half_day": False,
            "history": [],
        })

        resp = await self.http.get(
            f"/analytics/employees/{code}/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["present_days"], 0.0)
        self.assertEqual(resp.json()["leave_days"], 0)

        # Regularization: attempting to supply punch_in with ABSENT is 422
        patch_resp = await self.http.patch(
            f"/attendance/{code}/2026-07-06",
            json={
                "status": "ABSENT",
                "punch_in": int(datetime(2026, 7, 6, 4, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "reason": "Invalid regularization with punch",
                "regularized_by": "admin",
            },
        )
        self.assertEqual(patch_resp.status_code, 422)

    # Case 8: Record marked LEAVE
    async def test_case_08_record_marked_leave(self):
        code = self._make_code(980001)
        dept = f"Leave-{self.test_prefix}"
        await self._create_test_employee(code, department=dept)

        await self.database.attendance_logs.insert_one({
            "emp_code": code,
            "date": "2026-07-06",
            "status": "LEAVE",
            "punch_in": None,
            "punch_out": None,
            "work_hours": None,
            "late_minutes": 0,
            "overtime_minutes": 0,
            "half_day": False,
            "history": [],
        })

        resp = await self.http.get(
            f"/analytics/employees/{code}/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["present_days"], 0.0)
        self.assertEqual(resp.json()["leave_days"], 1)

        summary = await self.http.get(
            "/analytics/departments/summary",
            params={"month": "2026-07", "department": dept},
        )
        self.assertEqual(summary.status_code, 200)
        self.assertEqual(summary.json()["items"][0]["leave_count"], 1)

    # Case 9: Employees marked PRESENT, WFH, and ON_DUTY
    async def test_case_09_presence_statuses_present_wfh_on_duty(self):
        dept = f"Presence-{self.test_prefix}"
        c_p = self._make_code(990001)
        c_w = self._make_code(990002)
        c_o = self._make_code(990003)
        for c in (c_p, c_w, c_o):
            await self._create_test_employee(c, department=dept)

        await self.database.attendance_logs.insert_many([
            {"emp_code": c_p, "date": "2026-07-06", "status": "PRESENT", "work_hours": 8.0, "half_day": False, "late_minutes": 0, "overtime_minutes": 0, "history": []},
            {"emp_code": c_w, "date": "2026-07-06", "status": "WFH", "work_hours": 8.0, "half_day": False, "late_minutes": 0, "overtime_minutes": 0, "history": []},
            {"emp_code": c_o, "date": "2026-07-06", "status": "ON_DUTY", "work_hours": 8.0, "half_day": False, "late_minutes": 0, "overtime_minutes": 0, "history": []},
        ])

        summary = await self.http.get(
            "/analytics/departments/summary",
            params={"month": "2026-07", "department": dept},
        )
        self.assertEqual(summary.status_code, 200)
        item = summary.json()["items"][0]
        self.assertEqual(item["headcount"], 3)
        self.assertEqual(item["present_days"], 3.0)
        self.assertEqual(item["on_duty_count"], 1)

    # Case 10: Punch-in exactly at the 10-minute grace boundary
    async def test_case_10_punch_in_at_exact_10_min_grace_boundary(self):
        code = self._make_code(100001)
        await self._create_test_employee(code, shift_start="09:30")

        # 09:40:00 IST on 2026-07-06 is exactly 10 min (600s). R2: late_minutes = 0
        instant_ms = int(datetime(2026, 7, 6, 4, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
        resp = await self.http.post(
            "/attendance/punch-in",
            json={"emp_code": code, "punched_at": instant_ms},
        )
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.json()["late_minutes"], 0)

    # Case 11: Punch-in one second beyond the grace boundary
    async def test_case_11_punch_in_one_second_beyond_grace_boundary(self):
        code = self._make_code(110001)
        await self._create_test_employee(code, shift_start="09:30")

        # 09:40:01 IST on 2026-07-06 is 601s (> 600s). R2: late_minutes = 10
        instant_ms = int(datetime(2026, 7, 6, 4, 10, 1, tzinfo=timezone.utc).timestamp() * 1000)
        resp = await self.http.post(
            "/attendance/punch-in",
            json={"emp_code": code, "punched_at": instant_ms},
        )
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.json()["late_minutes"], 10)

    # Case 12: Overtime just below 30 minutes and exactly at the 30-minute threshold
    async def test_case_12_overtime_threshold_boundary(self):
        code1 = self._make_code(120001)
        code2 = self._make_code(120002)
        await self._create_test_employee(code1, shift_end="18:30")
        await self._create_test_employee(code2, shift_end="18:30")

        # Punch in at 09:30 IST
        in_ms = int(datetime(2026, 7, 6, 4, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        await self.http.post("/attendance/punch-in", json={"emp_code": code1, "punched_at": in_ms})
        await self.http.post("/attendance/punch-in", json={"emp_code": code2, "punched_at": in_ms})

        # 18:59:59 IST (29 min 59 sec past 18:30) -> overtime = 0
        out1_ms = int(datetime(2026, 7, 6, 13, 29, 59, tzinfo=timezone.utc).timestamp() * 1000)
        resp1 = await self.http.post("/attendance/punch-out", json={"emp_code": code1, "punched_at": out1_ms})
        self.assertEqual(resp1.status_code, 200)
        self.assertEqual(resp1.json()["overtime_minutes"], 0)

        # 19:00:00 IST (exactly 30 min 0 sec past 18:30) -> overtime = 30
        out2_ms = int(datetime(2026, 7, 6, 13, 30, 0, tzinfo=timezone.utc).timestamp() * 1000)
        resp2 = await self.http.post("/attendance/punch-out", json={"emp_code": code2, "punched_at": out2_ms})
        self.assertEqual(resp2.status_code, 200)
        self.assertEqual(resp2.json()["overtime_minutes"], 30)

    # Case 13: Work hours near half-up rounding boundaries
    async def test_case_13_work_hours_half_up_rounding(self):
        code1 = self._make_code(130001)
        code2 = self._make_code(130002)
        await self._create_test_employee(code1)
        await self._create_test_employee(code2)

        start_dt = datetime(2026, 7, 6, 4, 0, 0, tzinfo=timezone.utc)
        in_ms = int(start_dt.timestamp() * 1000)
        await self.http.post("/attendance/punch-in", json={"emp_code": code1, "punched_at": in_ms})
        await self.http.post("/attendance/punch-in", json={"emp_code": code2, "punched_at": in_ms})

        # 54 seconds = 0.015 hours -> half-up rounds to 0.02
        out1_ms = int((start_dt + timedelta(seconds=54)).timestamp() * 1000)
        resp1 = await self.http.post("/attendance/punch-out", json={"emp_code": code1, "punched_at": out1_ms})
        self.assertEqual(resp1.status_code, 200)
        self.assertEqual(resp1.json()["work_hours"], 0.02)

        # 53 seconds = 0.01472... hours -> rounds to 0.01
        out2_ms = int((start_dt + timedelta(seconds=53)).timestamp() * 1000)
        resp2 = await self.http.post("/attendance/punch-out", json={"emp_code": code2, "punched_at": out2_ms})
        self.assertEqual(resp2.status_code, 200)
        self.assertEqual(resp2.json()["work_hours"], 0.01)

    # Case 14: Work duration rounding to exactly 4.50 hours and just below 4.50 hours
    async def test_case_14_half_day_boundary(self):
        code1 = self._make_code(140001)
        code2 = self._make_code(140002)
        await self._create_test_employee(code1)
        await self._create_test_employee(code2)

        start_dt = datetime(2026, 7, 6, 4, 0, 0, tzinfo=timezone.utc)
        in_ms = int(start_dt.timestamp() * 1000)
        await self.http.post("/attendance/punch-in", json={"emp_code": code1, "punched_at": in_ms})
        await self.http.post("/attendance/punch-in", json={"emp_code": code2, "punched_at": in_ms})

        # 16181 seconds / 3600 = 4.4947... -> rounds to 4.49 -> half_day = True
        out1_ms = int((start_dt + timedelta(seconds=16181)).timestamp() * 1000)
        resp1 = await self.http.post("/attendance/punch-out", json={"emp_code": code1, "punched_at": out1_ms})
        self.assertEqual(resp1.status_code, 200)
        self.assertEqual(resp1.json()["work_hours"], 4.49)
        self.assertTrue(resp1.json()["half_day"])

        # 16182 seconds / 3600 = 4.4950 -> rounds half-up to 4.50 -> half_day = False
        out2_ms = int((start_dt + timedelta(seconds=16182)).timestamp() * 1000)
        resp2 = await self.http.post("/attendance/punch-out", json={"emp_code": code2, "punched_at": out2_ms})
        self.assertEqual(resp2.status_code, 200)
        self.assertEqual(resp2.json()["work_hours"], 4.50)
        self.assertFalse(resp2.json()["half_day"])

    # Case 15: Invalid timestamps: floats, strings, seconds, malformed
    async def test_case_15_invalid_timestamps_rejected_422(self):
        code = self._make_code(150001)
        await self._create_test_employee(code)

        invalid_timestamps = [
            1783312500000.5,  # float
            "1783312500000",   # string
            1783312500,        # seconds (less than 100_000_000_000)
            5_000_000_000_000, # beyond max
            -1,                # negative
        ]
        for bad_ts in invalid_timestamps:
            resp = await self.http.post(
                "/attendance/punch-in",
                json={"emp_code": code, "punched_at": bad_ts},
            )
            self.assertEqual(resp.status_code, 422, f"Expected 422 for {bad_ts}, got {resp.status_code}")

    # Case 16: Missing employee and missing attendance record
    async def test_case_16_missing_resources_return_404(self):
        missing_code = "EMP999999"
        ts = int(datetime(2026, 7, 6, 4, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

        # Missing employee on punch-in
        r1 = await self.http.post("/attendance/punch-in", json={"emp_code": missing_code})
        self.assertEqual(r1.status_code, 404)

        # Missing employee on punch-out
        r2 = await self.http.post("/attendance/punch-out", json={"emp_code": missing_code})
        self.assertEqual(r2.status_code, 404)

        # Missing attendance record on punch-out for existing employee
        code = self._make_code(160001)
        await self._create_test_employee(code)
        r3 = await self.http.post("/attendance/punch-out", json={"emp_code": code})
        self.assertEqual(r3.status_code, 404)

        # Missing employee on monthly analytics
        r4 = await self.http.get(f"/analytics/employees/{missing_code}/monthly", params={"month": "2026-07"})
        self.assertEqual(r4.status_code, 404)

        # Missing employee or record on regularization
        r5 = await self.http.patch(
            f"/attendance/{code}/2026-07-06",
            json={"status": "PRESENT", "punch_in": ts, "reason": "fixing record", "regularized_by": "manager"},
        )
        self.assertEqual(r5.status_code, 404)

        # Missing department on trend
        r6 = await self.http.get("/analytics/departments/NonExistentDept/trend", params={"from": "2026-07-06", "to": "2026-07-08"})
        self.assertEqual(r6.status_code, 404)

    # Case 17: Invalid pagination, page size above 100, and empty filtered results
    async def test_case_17_pagination_validation_and_empty_filters(self):
        for bad_param in [{"page": 0}, {"page_size": 101}, {"page_size": 0}]:
            r = await self.http.get("/employees", params=bad_param)
            self.assertEqual(r.status_code, 422)
            r_att = await self.http.get("/attendance", params=bad_param)
            self.assertEqual(r_att.status_code, 422)

        # Empty filtered results return 200 with total 0
        r_empty = await self.http.get("/employees", params={"department": "EmptyDepartmentName"})
        self.assertEqual(r_empty.status_code, 200)
        self.assertEqual(r_empty.json()["items"], [])
        self.assertEqual(r_empty.json()["total"], 0)

    # Case 18: Missing dates in daily trends and 7-day moving-average boundaries
    async def test_case_18_trend_gap_filling_and_moving_average_window(self):
        dept = f"TrendWindow-{self.test_prefix}"
        code = self._make_code(180001)
        await self._create_test_employee(code, department=dept, joined_on="2026-07-01")

        # Range: 2026-07-06 (Monday) to 2026-07-12 (Sunday) - 7 days total
        # Single log on Monday: present_count = 1.0 -> rate = 1.0
        # Tue-Fri have 0 logs -> rates = 0.0
        # Sat-Sun are weekends -> rates = None
        await self.database.attendance_logs.insert_one({
            "emp_code": code,
            "date": "2026-07-06",
            "status": "PRESENT",
            "work_hours": 8.0,
            "half_day": False,
            "late_minutes": 0,
            "overtime_minutes": 0,
            "history": [],
        })

        resp = await self.http.get(
            f"/analytics/departments/{dept}/trend",
            params={"from": "2026-07-06", "to": "2026-07-12"},
        )
        self.assertEqual(resp.status_code, 200)
        items = resp.json()["items"]
        self.assertEqual(len(items), 7)
        # Day 1: 2026-07-06, window size 1: rate 1.0 -> moving_avg_7d = 1.0
        self.assertEqual(items[0]["date"], "2026-07-06")
        self.assertEqual(items[0]["attendance_rate"], 1.0)
        self.assertEqual(items[0]["moving_avg_7d"], 1.0)

        # Day 2: 2026-07-07, window size 2: rates [1.0, 0.0] -> avg = 0.50
        self.assertEqual(items[1]["date"], "2026-07-07")
        self.assertEqual(items[1]["attendance_rate"], 0.0)
        self.assertEqual(items[1]["moving_avg_7d"], 0.50)

        # Day 5: 2026-07-10 (Friday), window size 5: rates [1.0, 0.0, 0.0, 0.0, 0.0] -> avg = 0.20
        self.assertEqual(items[4]["date"], "2026-07-10")
        self.assertEqual(items[4]["attendance_rate"], 0.0)
        self.assertEqual(items[4]["moving_avg_7d"], 0.20)

        # Weekend: 2026-07-11 (Saturday): rate = None, moving_avg_7d averages non-null preceding rows [1.0, 0, 0, 0, 0] = 0.20
        self.assertEqual(items[5]["date"], "2026-07-11")
        self.assertIsNone(items[5]["attendance_rate"])
        self.assertEqual(items[5]["moving_avg_7d"], 0.20)

    # Case 19: Date and timezone boundaries around midnight IST
    async def test_case_19_midnight_ist_timezone_boundary(self):
        code = self._make_code(190001)
        await self._create_test_employee(code, shift_start="09:30", shift_end="18:30")

        # 23:59:59 IST on 2026-07-06 corresponds to 18:29:59 UTC on 2026-07-06
        dt1 = datetime(2026, 7, 6, 18, 29, 59, tzinfo=timezone.utc)
        ms1 = int(dt1.timestamp() * 1000)
        r1 = await self.http.post("/attendance/punch-in", json={"emp_code": code, "punched_at": ms1})
        self.assertEqual(r1.status_code, 201)
        self.assertEqual(r1.json()["date"], "2026-07-06")

        # 00:00:01 IST on 2026-07-07 corresponds to 18:30:01 UTC on 2026-07-06
        dt2 = datetime(2026, 7, 6, 18, 30, 1, tzinfo=timezone.utc)
        ms2 = int(dt2.timestamp() * 1000)
        r2 = await self.http.post("/attendance/punch-in", json={"emp_code": code, "punched_at": ms2})
        self.assertEqual(r2.status_code, 201)
        self.assertEqual(r2.json()["date"], "2026-07-07")

    # Case 20: Repeated requests and concurrent writes
    async def test_case_20_repeated_and_concurrent_writes(self):
        code = self._make_code(200001)
        await self._create_test_employee(code)

        # 1. Concurrent duplicate punch-ins: exactly one 201, rest 409
        punch_in_ms = int(datetime(2026, 7, 6, 4, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        barrier = asyncio.Barrier(6)

        async def concurrent_punch_in():
            await barrier.wait()
            return await self.http.post("/attendance/punch-in", json={"emp_code": code, "punched_at": punch_in_ms})

        responses = await asyncio.gather(*(concurrent_punch_in() for _ in range(6)))
        status_codes = [r.status_code for r in responses]
        self.assertEqual(status_codes.count(201), 1)
        self.assertEqual(status_codes.count(409), 5)

        # 2. Concurrent duplicate punch-outs: exactly one 200, rest 409
        punch_out_ms = int(datetime(2026, 7, 6, 13, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        barrier_out = asyncio.Barrier(6)

        async def concurrent_punch_out():
            await barrier_out.wait()
            return await self.http.post("/attendance/punch-out", json={"emp_code": code, "punched_at": punch_out_ms})

        out_responses = await asyncio.gather(*(concurrent_punch_out() for _ in range(6)))
        out_codes = [r.status_code for r in out_responses]
        self.assertEqual(out_codes.count(200), 1)
        self.assertEqual(out_codes.count(409), 5)


if __name__ == "__main__":
    unittest.main()

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
    "configure MONGO_TEST_URI and MONGO_TEST_DB (or MONGO_URI and MONGO_DB for the _test default)",
)
class MongoConcurrencyIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        if main_module.MONGO_DB and TEST_MONGO_DB == main_module.MONGO_DB:
            self.fail("live MongoDB tests must use a dedicated test database")
        self.original_db = main_module.db
        self.client = AsyncMongoClient(
            TEST_MONGO_URI,
            tz_aware=True,
            serverSelectionTimeoutMS=3000,
        )
        await self.client.admin.command("ping")
        self.database = self.client[TEST_MONGO_DB]
        main_module.db = self.database
        await main_module.create_indexes()
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main_module.app),
            base_url="http://test",
        )
        self.employee_codes = set()
        self.addAsyncCleanup(self._cleanup)
        self.emp_code = await self._unused_employee_code()
        self.attendance_date = date.today().isoformat()
        self.punch_in = datetime.combine(date.today(), time(4, tzinfo=timezone.utc))
        self.punch_out = datetime.combine(date.today(), time(13, tzinfo=timezone.utc))
        response = await self.http.post(
            "/employees",
            json={
                "emp_code": self.emp_code,
                "name": "Concurrency Test",
                "email": "concurrency@example.test",
                "department": "Integration",
                "shift_start": "09:30",
                "shift_end": "18:30",
                "joined_on": date.today().isoformat(),
            },
        )
        if response.status_code == 201:
            self.employee_codes.add(self.emp_code)
        self.assertEqual(response.status_code, 201, response.text)
        created_at_ms = response.json()["created_at"]
        stored_employee = await self.database.employees.find_one(
            {"emp_code": self.emp_code}
        )
        self.assertIs(type(created_at_ms), int)
        self.assertIsInstance(stored_employee["created_at"], datetime)
        self.assertEqual(
            main_module.epoch_ms(stored_employee["created_at"]),
            created_at_ms,
        )

    async def asyncTearDown(self):
        main_module.db = self.original_db

    async def _cleanup(self):
        await self.http.aclose()
        main_module.db = self.original_db
        await self.database.attendance_logs.delete_many(
            {"emp_code": {"$in": list(self.employee_codes)}}
        )
        await self.database.employees.delete_many(
            {"emp_code": {"$in": list(self.employee_codes)}}
        )
        await self.client.close()

    async def _unused_employee_code(self):
        while True:
            emp_code = f"EMP{uuid.uuid4().int % 1_000_000:06d}"
            if not await self.database.employees.find_one({"emp_code": emp_code}):
                return emp_code

    async def test_startup_indexes_are_present_unique_and_idempotent(self):
        await main_module.create_indexes()
        employee_indexes = {
            index["name"]: index
            async for index in await self.database.employees.list_indexes()
        }
        attendance_indexes = {
            index["name"]: index
            async for index in await self.database.attendance_logs.list_indexes()
        }

        self.assertTrue(employee_indexes["employee_code_unique"]["unique"])
        self.assertTrue(attendance_indexes["employee_date_unique"]["unique"])
        self.assertEqual(
            attendance_indexes["employee_date_unique"]["key"],
            {"emp_code": 1, "date": 1},
        )
        self.assertTrue({
            "department_employee_code",
            "department_joined",
            "employee_joined",
        }.issubset(employee_indexes))
        self.assertTrue({
            "employee_punch_in",
            "attendance_date_employee",
            "attendance_date_late",
            "attendance_status_date",
        }.issubset(attendance_indexes))

    async def test_parallel_employee_creates_enforce_unique_code(self):
        duplicate_code = await self._unused_employee_code()
        start_together = asyncio.Barrier(8)
        payloads = [
            {
                "emp_code": duplicate_code,
                "name": f"Duplicate Employee {number}",
                "email": f"duplicate-{number}@example.test",
                "department": "Integration",
                "joined_on": date.today().isoformat(),
            }
            for number in range(8)
        ]

        async def create_employee(payload):
            await start_together.wait()
            return await self.http.post("/employees", json=payload)

        try:
            responses = await asyncio.gather(
                *(create_employee(payload) for payload in payloads)
            )
            status_counts = {
                status_code: sum(response.status_code == status_code for response in responses)
                for status_code in (201, 409)
            }
            document_count = await self.database.employees.count_documents(
                {"emp_code": duplicate_code}
            )
            print(f"Concurrent employee-create HTTP status counts: {status_counts}")
            print(
                "MongoDB documents for generated emp_code after requests: "
                f"{document_count}"
            )
            self.assertEqual(status_counts, {201: 1, 409: 7})
            self.assertEqual(document_count, 1)
        finally:
            await self.database.employees.delete_many({"emp_code": duplicate_code})

    async def test_parallel_punch_ins_enforce_unique_employee_date(self):
        punched_at = int(self.punch_in.timestamp() * 1000)
        payload = {"emp_code": self.emp_code, "punched_at": punched_at}
        start_together = asyncio.Barrier(8)

        async def punch_in():
            await start_together.wait()
            return await self.http.post("/attendance/punch-in", json=payload)

        responses = await asyncio.gather(*(punch_in() for _ in range(8)))
        self.assertEqual(sum(response.status_code == 201 for response in responses), 1)
        self.assertEqual(sum(response.status_code == 409 for response in responses), 7)
        self.assertEqual(
            await self.database.attendance_logs.count_documents({"emp_code": self.emp_code}),
            1,
        )

    async def test_parallel_punch_outs_update_record_once(self):
        await self.database.attendance_logs.insert_one({
            "emp_code": self.emp_code,
            "date": self.attendance_date,
            "status": "PRESENT",
            "punch_in": self.punch_in,
            "punch_out": None,
            "work_hours": None,
            "late_minutes": 0,
            "overtime_minutes": 0,
            "half_day": False,
            "history": [],
        })
        start_together = asyncio.Barrier(8)

        async def punch_out(offset_seconds):
            await start_together.wait()
            payload = {
                "emp_code": self.emp_code,
                "punched_at": int(
                    (self.punch_out + timedelta(seconds=offset_seconds)).timestamp() * 1000
                ),
            }
            return await self.http.post("/attendance/punch-out", json=payload)

        responses = await asyncio.gather(*(punch_out(offset) for offset in range(8)))
        winners = [response for response in responses if response.status_code == 200]
        self.assertEqual(sum(response.status_code == 200 for response in responses), 1)
        self.assertEqual(sum(response.status_code == 409 for response in responses), 7)
        record = await self.database.attendance_logs.find_one({"emp_code": self.emp_code})
        self.assertIsNotNone(record["punch_out"])
        self.assertEqual(record["work_hours"], 9.0)
        self.assertEqual(
            main_module.epoch_ms(record["punch_out"]),
            winners[0].json()["punch_out"],
        )
        self.assertEqual(record["overtime_minutes"], 0)
        self.assertFalse(record["half_day"])

    async def test_parallel_regularizations_use_compare_and_set_and_preserve_history(self):
        await self.database.attendance_logs.insert_one({
            "emp_code": self.emp_code,
            "date": self.attendance_date,
            "status": "PRESENT",
            "punch_in": self.punch_in,
            "punch_out": None,
            "work_hours": None,
            "late_minutes": 0,
            "overtime_minutes": 0,
            "half_day": False,
            "history": [],
        })
        collection = self.database.attendance_logs

        class ConcurrentReadBarrier:
            def __init__(self):
                self.readers = 0
                self.lock = asyncio.Lock()
                self.both_read = asyncio.Event()

            async def find_one(self, query, *args, **kwargs):
                result = await collection.find_one(query, *args, **kwargs)
                if query.get("emp_code") == self_emp_code and query.get("date") == self_attendance_date:
                    async with self.lock:
                        self.readers += 1
                        if self.readers == 2:
                            self.both_read.set()
                    await self.both_read.wait()
                return result

            def __getattr__(self, name):
                return getattr(collection, name)

        self_emp_code = self.emp_code
        self_attendance_date = self.attendance_date
        main_module.db = type("DatabaseWithReadBarrier", (), {
            "employees": self.database.employees,
            "attendance_logs": ConcurrentReadBarrier(),
        })()
        bodies = (
            {"status": "WFH", "reason": "correct status", "regularized_by": "hr.one"},
            {"status": "ON_DUTY", "reason": "correct status", "regularized_by": "hr.two"},
        )
        responses = await asyncio.gather(*(
            self.http.patch(
                f"/attendance/{self.emp_code}/{self.attendance_date}",
                json=body,
            )
            for body in bodies
        ))
        self.assertEqual(sorted(response.status_code for response in responses), [200, 409])
        main_module.db = self.database
        record = await self.database.attendance_logs.find_one({"emp_code": self.emp_code})
        self.assertEqual(len(record["history"]), 1)
        self.assertIn(record["status"], {"WFH", "ON_DUTY"})
        self.assertEqual(record["history"][0]["changes"]["status"]["to"], record["status"])

    async def test_analytics_routes_execute_aggregations_against_mongodb(self):
        codes = []
        while len(codes) < 5:
            candidate = await self._unused_employee_code()
            if candidate not in codes:
                codes.append(candidate)
        self.employee_codes.update(codes)
        department = f"Integration-{uuid.uuid4().hex[:8]}"
        zero_log_code = await self._unused_employee_code()
        self.employee_codes.add(zero_log_code)
        zero_log_department = f"ZeroLogs-{uuid.uuid4().hex[:8]}"
        employees = [
            {
                "emp_code": code,
                "name": f"Analytics Test {number}",
                "email": f"{uuid.uuid4().hex}@example.test",
                "department": department,
                "shift_start": "09:30",
                "shift_end": "18:30",
                "joined_on": joined_on,
                "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
            }
            for number, (code, joined_on) in enumerate(
                zip(codes, (
                    "2026-07-08",
                    "2026-07-01",
                    "2026-07-01",
                    "2026-07-09",
                    "2026-08-01",
                )),
                start=1,
            )
        ]
        await self.database.employees.insert_many(employees)
        await self.database.employees.insert_one({
            "emp_code": zero_log_code,
            "name": "Zero Log Employee",
            "email": f"{uuid.uuid4().hex}@example.test",
            "department": zero_log_department,
            "shift_start": "09:30",
            "shift_end": "18:30",
            "joined_on": "2026-07-01",
            "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        })

        def attendance_record(code, day, status, late, overtime, hours, half_day=False):
            has_punch = status in {"PRESENT", "WFH", "ON_DUTY"}
            punch_in = datetime.fromisoformat(f"{day}T04:00:00+00:00") if has_punch else None
            punch_out = datetime.fromisoformat(f"{day}T13:00:00+00:00") if has_punch and hours is not None else None
            return {
                "emp_code": code,
                "date": day,
                "status": status,
                "punch_in": punch_in,
                "punch_out": punch_out,
                "work_hours": hours,
                "late_minutes": late,
                "overtime_minutes": overtime,
                "half_day": half_day,
                "history": [],
            }

        await self.database.attendance_logs.insert_many([
            attendance_record(codes[0], "2026-07-08", "PRESENT", 45, 0, 4.49, True),
            attendance_record(codes[0], "2026-07-10", "PRESENT", 0, 0, 8.50),
            attendance_record(codes[0], "2026-07-11", "WFH", 5, 30, 9.00),
            attendance_record(codes[1], "2026-07-06", "PRESENT", 40, 0, 9.00),
            attendance_record(codes[1], "2026-07-07", "LEAVE", 0, 0, None),
            attendance_record(codes[1], "2026-07-09", "ABSENT", 0, 0, None),
            attendance_record(codes[2], "2026-07-06", "ON_DUTY", 40, 0, 4.49, True),
            attendance_record(codes[3], "2026-07-09", "PRESENT", 5, 0, None),
        ])

        employee_page = await self.http.get(
            "/employees",
            params={"department": department, "page_size": 100},
        )
        self.assertEqual(employee_page.status_code, 200, employee_page.text)
        self.assertEqual(employee_page.json()["total"], 5)
        self.assertEqual(
            [item["emp_code"] for item in employee_page.json()["items"]],
            sorted(codes),
        )
        paged_codes = []
        for page in (1, 2, 3):
            page_response = await self.http.get(
                "/employees",
                params={"department": department, "page": page, "page_size": 2},
            )
            self.assertEqual(page_response.status_code, 200, page_response.text)
            self.assertEqual(page_response.json()["total"], 5)
            paged_codes.extend(
                item["emp_code"] for item in page_response.json()["items"]
            )
        self.assertEqual(paged_codes, sorted(codes))
        for params in ({"page": 0}, {"page_size": 101}):
            invalid_page = await self.http.get("/employees", params=params)
            self.assertEqual(invalid_page.status_code, 422, invalid_page.text)

        attendance_page = await self.http.get(
            "/attendance",
            params={
                "emp_code": codes[0],
                "date_from": "2026-07-08",
                "date_to": "2026-07-11",
                "page_size": 10,
            },
        )
        self.assertEqual(attendance_page.status_code, 200, attendance_page.text)
        self.assertEqual(attendance_page.json()["total"], 3)
        self.assertTrue(all("_id" not in item for item in attendance_page.json()["items"]))

        missing_code = f"EMP{uuid.uuid4().int % 1_000_000:06d}"
        while await self.database.employees.find_one({"emp_code": missing_code}):
            missing_code = f"EMP{uuid.uuid4().int % 1_000_000:06d}"
        missing_employee = await self.http.post(
            "/attendance/punch-in",
            json={"emp_code": missing_code, "status": "PRESENT"},
        )
        self.assertEqual(missing_employee.status_code, 404, missing_employee.text)

        monthly = await self.http.get(
            f"/analytics/employees/{codes[0]}/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(monthly.status_code, 200, monthly.text)
        self.assertEqual(
            {key: monthly.json()[key] for key in (
                "working_days", "present_days", "late_count",
                "total_late_minutes", "total_overtime_minutes",
            )},
            {
                "working_days": 18,
                "present_days": 1.5,
                "late_count": 2,
                "total_late_minutes": 50,
                "total_overtime_minutes": 30,
            },
        )
        employee_with_absent = await self.http.get(
            f"/analytics/employees/{codes[1]}/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(employee_with_absent.status_code, 200, employee_with_absent.text)
        self.assertEqual(employee_with_absent.json()["present_days"], 1.0)
        self.assertEqual(employee_with_absent.json()["leave_days"], 1)
        self.assertEqual(employee_with_absent.json()["working_days"], 23)

        employee_joining_after_month = await self.http.get(
            f"/analytics/employees/{codes[4]}/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(
            employee_joining_after_month.json()["working_days"],
            0,
        )
        self.assertIsNone(employee_joining_after_month.json()["attendance_pct"])

        summary = await self.http.get(
            "/analytics/departments/summary",
            params={"month": "2026-07", "department": department},
        )
        self.assertEqual(summary.status_code, 200, summary.text)
        self.assertEqual(len(summary.json()["items"]), 1)
        summary_item = summary.json()["items"][0]
        self.assertEqual(
            {key: summary_item[key] for key in (
                "headcount", "present_days", "avg_work_hours", "late_count",
                "total_late_minutes", "leave_count",
            )},
            {
                "headcount": 4,
                "present_days": 4.0,
                "avg_work_hours": 7.1,
                "late_count": 5,
                "total_late_minutes": 135,
                "leave_count": 1,
            },
        )
        self.assertIsNone(await self.database.attendance_logs.find_one({
            "emp_code": zero_log_code,
            "date": {"$gte": "2026-07-01", "$lte": "2026-07-31"},
        }))
        zero_log_summary = await self.http.get(
            "/analytics/departments/summary",
            params={"month": "2026-07", "department": zero_log_department},
        )
        self.assertEqual(zero_log_summary.status_code, 200, zero_log_summary.text)
        self.assertEqual(
            zero_log_summary.json()["items"],
            [{
                "department": zero_log_department,
                "headcount": 1,
                "present_days": 0,
                "avg_work_hours": None,
                "late_count": 0,
                "total_late_minutes": 0,
                "leave_count": 0,
                "on_duty_count": 0,
            }],
        )
        no_late_arrivals = await self.http.get(
            "/analytics/leaderboard/late",
            params={"month": "2026-07", "department": zero_log_department},
        )
        self.assertEqual(no_late_arrivals.status_code, 200, no_late_arrivals.text)
        self.assertEqual(no_late_arrivals.json()["items"], [])

        leaderboard = await self.http.get(
            "/analytics/leaderboard/late",
            params={"month": "2026-07", "department": department, "limit": 1},
        )
        self.assertEqual(leaderboard.status_code, 200, leaderboard.text)
        leaderboard_items = leaderboard.json()["items"]
        self.assertEqual([item["rank"] for item in leaderboard_items], [1])
        self.assertEqual(
            [item["emp_code"] for item in leaderboard_items],
            [codes[0]],
        )
        expected_ranked_codes = [codes[0], *sorted(codes[1:3]), codes[3]]
        for limit, expected_count, expected_ranks in (
            (2, 3, [1, 2, 2]),
            (3, 3, [1, 2, 2]),
            (4, 4, [1, 2, 2, 4]),
        ):
            tied_cutoff = await self.http.get(
                "/analytics/leaderboard/late",
                params={"month": "2026-07", "department": department, "limit": limit},
            )
            self.assertEqual(tied_cutoff.status_code, 200, tied_cutoff.text)
            self.assertEqual(len(tied_cutoff.json()["items"]), expected_count)
            self.assertEqual(
                [item["rank"] for item in tied_cutoff.json()["items"]],
                expected_ranks,
            )
            self.assertEqual(
                [item["emp_code"] for item in tied_cutoff.json()["items"]],
                expected_ranked_codes[:expected_count],
            )

        trend = await self.http.get(
            f"/analytics/departments/{department}/trend",
            params={"from": "2026-07-06", "to": "2026-07-12"},
        )
        self.assertEqual(trend.status_code, 200, trend.text)
        trend_by_date = {item["date"]: item for item in trend.json()["items"]}
        self.assertEqual(len(trend_by_date), 7)
        self.assertEqual(trend_by_date["2026-07-06"]["headcount"], 2)
        self.assertEqual(trend_by_date["2026-07-08"]["headcount"], 3)
        self.assertEqual(trend_by_date["2026-07-10"]["headcount"], 4)
        self.assertEqual(trend_by_date["2026-07-08"]["present_count"], 0.5)
        self.assertEqual(trend_by_date["2026-07-08"]["attendance_rate"], 0.1667)
        self.assertIsNone(trend_by_date["2026-07-11"]["attendance_rate"])
        self.assertEqual(trend_by_date["2026-07-11"]["present_count"], 1)
        self.assertEqual(trend_by_date["2026-07-10"]["moving_avg_7d"], 0.2833)

        explain_cases = (
            ("attendance_list", {
                "emp_code": codes[0],
                "date_from": "2026-07-06",
                "date_to": "2026-07-12",
                "status": "PRESENT",
                "page": 2,
                "page_size": 2,
            }),
            ("employee_monthly", {"emp_code": codes[0], "month": "2026-07"}),
            ("department_summary", {"department": department, "month": "2026-07"}),
            ("late_leaderboard", {
                "department": department,
                "month": "2026-07",
                "limit": 1,
            }),
            ("department_trend", {
                "department": department,
                "from": "2026-07-06",
                "to": "2026-07-12",
            }),
        )
        for endpoint, params in explain_cases:
            response = await self.http.get(
                f"/admin/explain/{endpoint}",
                params=params,
            )
            self.assertEqual(response.status_code, 200, response.text)
            explanation = response.json()["explain"]
            plan_text = response.text
            self.assertIn("IXSCAN", plan_text, f"{endpoint} explain plan has no IXSCAN")
            self.assertNotIn("COLLSCAN", plan_text, f"{endpoint} explain plan uses COLLSCAN")
            if endpoint == "department_summary":
                indexes_used = set()

                def collect_indexes(value):
                    if isinstance(value, dict):
                        indexes_used.update(value.get("indexesUsed", []))
                        for child in value.values():
                            collect_indexes(child)
                    elif isinstance(value, list):
                        for child in value:
                            collect_indexes(child)

                collect_indexes(explanation)
                self.assertIn("employee_date_unique", indexes_used)
            print(
                f"Explain {endpoint}: IXSCAN={'IXSCAN' in plan_text}, "
                f"COLLSCAN={'COLLSCAN' in plan_text}"
            )


if __name__ == "__main__":
    unittest.main()

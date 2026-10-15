import asyncio
import copy
import json
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
from bson import json_util
from bson.json_util import JSONOptions
from fastapi import HTTPException
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError
from pydantic import ValidationError

import app.main as main_module
from app.main import (
    EmployeeIn,
    PunchInIn,
    PunchOutIn,
    RegularizeIn,
    attendance_day,
    api_record,
    compute_late_minutes,
    compute_overtime,
    compute_work_hours,
    department_summary_pipeline,
    month_bounds,
    monthly_employee_pipeline,
    trend_pipeline,
)


ROOT = Path(__file__).resolve().parents[1]
PRESENCE = {"PRESENT", "WFH", "ON_DUTY"}
UTC = timezone.utc


class ImportAndHealthInitializationTests(unittest.TestCase):
    def test_module_import_does_not_connect_to_mongodb(self):
        self.assertIsNotNone(main_module.app)
        self.assertIsNone(main_module.client)
        self.assertIsNone(main_module.db)

    def test_health_returns_503_when_ping_fails(self):
        original_client, original_db = main_module.client, main_module.db
        main_module.client = SimpleNamespace(
            admin=SimpleNamespace(command=AsyncMock(side_effect=ServerSelectionTimeoutError("unavailable")))
        )
        main_module.db = object()
        try:
            with self.assertRaises(HTTPException) as error:
                asyncio.run(main_module.health())
            self.assertEqual(error.exception.status_code, 503)
        finally:
            main_module.client, main_module.db = original_client, original_db


class FakeAsyncCursor:
    def __init__(self, documents):
        self.documents = iter(documents)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.documents)
        except StopIteration:
            raise StopAsyncIteration from None


class FakeIndexCollection:
    def __init__(self, indexes=()):
        self.indexes = [copy.deepcopy(index) for index in indexes]
        self.create_calls = []

    async def list_indexes(self):
        return FakeAsyncCursor(self.indexes)

    async def create_index(self, keys, name, unique=False):
        self.create_calls.append((tuple(keys), name, unique))
        self.indexes.append({
            "name": name,
            "key": dict(keys),
            "unique": unique,
        })
        return name


class FakeIndexDatabase:
    def __init__(self, employee_indexes=()):
        self.collections = {
            "employees": FakeIndexCollection(employee_indexes),
            "attendance_logs": FakeIndexCollection(),
        }

    def __getitem__(self, name):
        return self.collections[name]


class StartupLifespanTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_pings_creates_indexes_and_closes_client(self):
        database = FakeIndexDatabase()
        employees = database["employees"]
        attendance = database["attendance_logs"]

        class FakeClient:
            def __init__(self):
                self.admin = SimpleNamespace(command=AsyncMock(return_value={"ok": 1}))
                self.close = AsyncMock()

            def __getitem__(self, name):
                self.database_name = name
                return database

        fake_client = FakeClient()
        with (
            patch.object(main_module, "AsyncMongoClient", return_value=fake_client),
            patch.object(main_module, "MONGO_URI", "mongodb://unit.test"),
            patch.object(main_module, "MONGO_DB", "unit"),
        ):
            async with main_module.lifespan(main_module.app):
                self.assertIs(main_module.db, database)
                self.assertEqual(fake_client.database_name, "unit")
                self.assertEqual(len(employees.create_calls), 4)
                self.assertEqual(len(attendance.create_calls), 5)
            fake_client.close.assert_awaited_once()
        self.assertIsNone(main_module.client)
        self.assertIsNone(main_module.db)

    async def test_startup_fails_clearly_when_mongodb_configuration_is_missing(self):
        with (
            patch.object(main_module, "AsyncMongoClient") as client_factory,
            patch.object(main_module, "MONGO_URI", None),
            patch.object(main_module, "MONGO_DB", "unit"),
        ):
            with self.assertRaisesRegex(RuntimeError, "MONGO_URI and MONGO_DB"):
                async with main_module.lifespan(main_module.app):
                    self.fail("Startup must not proceed without MongoDB configuration")
        client_factory.assert_not_called()

    async def test_startup_fails_and_closes_client_when_ping_is_unavailable(self):
        class FakeClient:
            def __init__(self):
                self.admin = SimpleNamespace(
                    command=AsyncMock(side_effect=ServerSelectionTimeoutError("unavailable"))
                )
                self.close = AsyncMock()

            def __getitem__(self, name):
                return object()

        client = FakeClient()
        with (
            patch.object(main_module, "AsyncMongoClient", return_value=client),
            patch.object(main_module, "MONGO_URI", "mongodb://unit.test"),
            patch.object(main_module, "MONGO_DB", "unit"),
        ):
            with self.assertRaises(ServerSelectionTimeoutError):
                async with main_module.lifespan(main_module.app):
                    self.fail("Startup must fail when MongoDB ping fails")
        client.close.assert_awaited_once()


class IndexMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_db = main_module.db
        self.database = FakeIndexDatabase()
        main_module.db = self.database

    async def asyncTearDown(self):
        main_module.db = self.original_db

    async def test_versioned_index_migration_is_idempotent_and_preserves_existing_indexes(self):
        await main_module.create_indexes()
        first_employee_indexes = copy.deepcopy(self.database["employees"].indexes)
        first_attendance_indexes = copy.deepcopy(self.database["attendance_logs"].indexes)
        first_calls = (
            len(self.database["employees"].create_calls),
            len(self.database["attendance_logs"].create_calls),
        )

        await main_module.create_indexes()

        self.assertEqual(first_calls, (4, 5))
        self.assertEqual(
            (
                len(self.database["employees"].create_calls),
                len(self.database["attendance_logs"].create_calls),
            ),
            first_calls,
        )
        self.assertEqual(self.database["employees"].indexes, first_employee_indexes)
        self.assertEqual(self.database["attendance_logs"].indexes, first_attendance_indexes)

    async def test_migration_retains_all_nine_index_names_keys_and_options(self):
        await main_module.create_indexes()

        expected = {
            ("employees", "employee_code_unique"): ([("emp_code", 1)], True),
            ("employees", "department_employee_code"): (
                [("department", 1), ("emp_code", 1)],
                False,
            ),
            ("employees", "department_joined"): (
                [("department", 1), ("joined_on", 1)],
                False,
            ),
            ("employees", "employee_joined"): ([("joined_on", 1)], False),
            ("attendance_logs", "employee_date_unique"): (
                [("emp_code", 1), ("date", 1)],
                True,
            ),
            ("attendance_logs", "employee_punch_in"): (
                [("emp_code", 1), ("punch_in", -1)],
                False,
            ),
            ("attendance_logs", "attendance_date_employee"): (
                [("date", -1), ("emp_code", 1)],
                False,
            ),
            ("attendance_logs", "attendance_date_late"): (
                [("date", 1), ("late_minutes", 1)],
                False,
            ),
            ("attendance_logs", "attendance_status_date"): (
                [("status", 1), ("date", -1), ("emp_code", 1)],
                False,
            ),
        }
        actual = {
            (collection_name, index["name"]): (
                list(index["key"].items()),
                bool(index.get("unique", False)),
            )
            for collection_name in ("employees", "attendance_logs")
            for index in self.database[collection_name].indexes
        }

        self.assertEqual(actual, expected)

    async def test_valid_preexisting_named_index_is_kept_without_recreation(self):
        existing_index = {
            "name": "employee_code_unique",
            "key": {"emp_code": 1},
            "unique": True,
        }
        database = FakeIndexDatabase(employee_indexes=[existing_index])
        main_module.db = database

        await main_module.create_indexes()

        self.assertEqual(database["employees"].indexes[0], existing_index)
        self.assertEqual(len(database["employees"].create_calls), 3)

    async def test_conflicting_named_index_fails_without_replacing_or_dropping_it(self):
        conflicting = {
            "name": "employee_code_unique",
            "key": {"emp_code": -1},
            "unique": True,
        }
        database = FakeIndexDatabase(employee_indexes=[conflicting])
        main_module.db = database

        with self.assertRaisesRegex(RuntimeError, "Resolve the index manually"):
            await main_module.create_indexes()

        self.assertEqual(database["employees"].indexes, [conflicting])
        self.assertEqual(database["employees"].create_calls, [])

    async def test_hidden_required_index_is_reported_as_a_conflict(self):
        hidden = {
            "name": "employee_code_unique",
            "key": {"emp_code": 1},
            "unique": True,
            "hidden": True,
        }
        database = FakeIndexDatabase(employee_indexes=[hidden])
        main_module.db = database

        with self.assertRaisesRegex(RuntimeError, "Resolve the index manually"):
            await main_module.create_indexes()

        self.assertEqual(database["employees"].indexes, [hidden])
        self.assertEqual(database["employees"].create_calls, [])

class HealthEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original = (main_module.client, main_module.db)
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main_module.app),
            base_url="http://test",
        )

    async def asyncTearDown(self):
        main_module.client, main_module.db = self.original
        await self.http.aclose()

    def _client(self, ping):
        return SimpleNamespace(admin=SimpleNamespace(command=ping))

    async def test_health_503_over_http_when_mongo_down(self):
        main_module.client = self._client(AsyncMock(side_effect=ServerSelectionTimeoutError("down")))
        main_module.db = object()
        response = await self.http.get("/health")
        self.assertEqual(response.status_code, 503)

    async def test_health_503_when_client_not_initialized(self):
        main_module.client = None
        main_module.db = None
        response = await self.http.get("/health")
        self.assertEqual(response.status_code, 503)

    async def test_health_200_when_ping_ok(self):
        main_module.client = self._client(AsyncMock(return_value={"ok": 1}))
        main_module.db = object()
        response = await self.http.get("/health")
        self.assertEqual(response.status_code, 200)


class BusinessRuleTests(unittest.TestCase):
    def test_late_grace_boundary_is_strict_and_truncated_to_minutes(self):
        shift_start = datetime(2026, 7, 6, 4, 0, tzinfo=UTC)
        self.assertEqual(compute_late_minutes(shift_start + timedelta(minutes=10), "09:30", "2026-07-06"), 0)
        self.assertEqual(compute_late_minutes(shift_start + timedelta(minutes=10, seconds=1), "09:30", "2026-07-06"), 10)
        self.assertEqual(compute_late_minutes(shift_start + timedelta(minutes=45, seconds=59), "09:30", "2026-07-06"), 45)

    def test_overtime_requires_thirty_whole_minutes(self):
        employee = {"shift_start": "09:30", "shift_end": "18:30"}
        shift_end = datetime(2026, 7, 6, 13, 0, tzinfo=UTC)
        self.assertEqual(compute_overtime(shift_end + timedelta(seconds=1799), employee, "2026-07-06"), 0)
        self.assertEqual(compute_overtime(shift_end + timedelta(seconds=1800), employee, "2026-07-06"), 30)

    def test_work_hours_use_half_up_rounding(self):
        start = datetime(2026, 7, 6, tzinfo=UTC)
        self.assertEqual(compute_work_hours(start, start + timedelta(seconds=90)), 0.03)
        self.assertEqual(compute_work_hours(start, start + timedelta(seconds=54)), 0.02)

    def test_instants_are_truncated_to_whole_seconds(self):
        value = main_module.parse_instant(1_783_312_500_999)
        self.assertEqual(value.microsecond, 0)
        self.assertEqual(main_module.epoch_ms(value), 1_783_312_500_000)

    def test_overnight_record_uses_shift_start_date(self):
        employee = {"shift_start": "22:00", "shift_end": "06:00"}
        punch_in = datetime(2026, 7, 7, 0, 0, tzinfo=UTC)
        self.assertEqual(attendance_day(punch_in, employee), "2026-07-06")
        self.assertEqual(compute_overtime(datetime(2026, 7, 7, 1, 10, tzinfo=UTC), employee, "2026-07-06"), 40)
        at_shift_end = datetime(2026, 7, 7, 0, 30, tzinfo=UTC)
        self.assertEqual(attendance_day(at_shift_end, employee), "2026-07-07")

    def test_punch_input_rejects_seconds_and_float_timestamps(self):
        with self.assertRaises(ValidationError):
            PunchInIn(emp_code="EMP0001", punched_at=1_783_312_500)
        with self.assertRaises(ValidationError):
            PunchInIn(emp_code="EMP0001", punched_at=1_783_312_500_000.0)

    def test_employee_shift_and_date_validation(self):
        with self.assertRaises(ValidationError):
            EmployeeIn(emp_code="EMP0001", name="A", email="a@example.com", department="HR",
                       shift_start="09:30", shift_end="09:30", joined_on="2026-01-01")
        with self.assertRaises(ValidationError):
            EmployeeIn(emp_code="EMP0001", name="A", email="a@example.com", department="HR",
                       joined_on="2026-02-30")
        with self.assertRaises(ValidationError):
            RegularizeIn(status=None, reason="correct status", regularized_by="hr.admin")


class SampleDataRuleTests(unittest.TestCase):
    def test_seed_documents_match_computed_rules(self):
        employees = json_util.loads((ROOT / "sample_data" / "employees.json").read_text(),
                                    json_options=JSONOptions(tz_aware=True))
        records = json_util.loads((ROOT / "sample_data" / "attendance_logs.json").read_text(),
                                  json_options=JSONOptions(tz_aware=True))
        by_code = {employee["emp_code"]: employee for employee in employees}
        self.assertEqual(len(employees), 6)
        self.assertEqual(len(records), 9)
        for record in records:
            employee = by_code[record["emp_code"]]
            if record["status"] in PRESENCE:
                self.assertEqual(attendance_day(record["punch_in"], employee), record["date"])
                self.assertEqual(compute_late_minutes(record["punch_in"], employee["shift_start"], record["date"]),
                                 record.get("late_minutes", 0))
            if record.get("punch_out") is not None:
                hours = compute_work_hours(record["punch_in"], record["punch_out"])
                self.assertEqual(hours, record["work_hours"])
                self.assertEqual(hours < 4.5, record.get("half_day", False))
                self.assertEqual(compute_overtime(record["punch_out"], employee, record["date"]),
                                 record.get("overtime_minutes", 0))


class AggregationContractTests(unittest.TestCase):
    def test_employee_monthly_uses_stored_values(self):
        pipeline = monthly_employee_pipeline("EMP0001", "2026-07")
        self.assertEqual(pipeline[0]["$match"]["date"], {"$gte": "2026-07-01", "$lt": "2026-08-01"})
        group = pipeline[-1]["$group"]
        self.assertIn("$half_day", json.dumps(group))
        self.assertNotIn("punch_in", json.dumps(group))

    def test_missing_weekday_is_not_an_absent_record(self):
        pipeline = monthly_employee_pipeline("EMP0001", "2026-07")
        pipeline_text = json.dumps(pipeline)
        self.assertNotIn("$densify", pipeline_text)
        self.assertNotIn("$documents", pipeline_text)
        self.assertIn('"LEAVE"', pipeline_text)
        self.assertNotIn('"ABSENT"', pipeline_text)

    def test_department_summary_starts_with_employee_headcount(self):
        pipeline = department_summary_pipeline("2026-07", None)
        self.assertIn("joined_on", pipeline[0]["$match"])
        self.assertEqual(pipeline[1]["$lookup"]["from"], "attendance_logs")
        self.assertIn('"headcount"', json.dumps(pipeline))

    def test_trend_is_gap_filled_and_calculated_in_mongodb(self):
        pipeline = trend_pipeline("Sales", date(2026, 7, 6), date(2026, 7, 8))
        self.assertEqual(pipeline[0]["$match"]["date"], {"$gte": "2026-07-06", "$lte": "2026-07-08"})
        self.assertTrue(any("$documents" in stage.get("$unionWith", {}).get("pipeline", [{}])[0]
                            for stage in pipeline if "$unionWith" in stage))
        self.assertTrue(any("$densify" in stage for stage in pipeline))
        self.assertTrue(any("$setWindowFields" in stage for stage in pipeline))
        pipeline_text = json_util.dumps(pipeline)
        self.assertIn("attendance_rate", pipeline_text)
        self.assertIn("headcount_result", pipeline_text)
        self.assertNotIn("$round", pipeline_text)

    def test_month_end_and_leap_day_bounds(self):
        self.assertEqual(month_bounds("2024-02")[:2], ("2024-02-01", "2024-03-01"))
        self.assertEqual(month_bounds("2026-12")[:2], ("2026-12-01", "2027-01-01"))

    def test_legacy_null_fields_are_serialized_as_defaults(self):
        record = api_record({
            "emp_code": "EMP0001",
            "date": "2026-07-06",
            "status": "PRESENT",
            "punch_in": datetime(2026, 7, 6, 4, 0, 0, tzinfo=UTC),
            "punch_out": None,
            "history": [{
                "at": datetime(2026, 7, 6, 4, 1, tzinfo=UTC),
                "by": "hr.admin",
                "reason": "correct status",
                "internal_note": "must not be returned",
                "changes": {
                    "late_minutes": {"from": 5, "to": 0, "internal": True},
                    "internal_flag": {"from": False, "to": True},
                },
            }],
            "internal_note": "must not be returned",
            "_id": ObjectId(),
        })
        self.assertEqual(
            set(record),
            {
                "emp_code", "date", "status", "punch_in", "punch_in_ist",
                "punch_out", "punch_out_ist", "work_hours", "late_minutes",
                "overtime_minutes", "half_day", "history",
            },
        )
        self.assertNotIn("internal_note", record)
        self.assertFalse(record["half_day"])
        self.assertEqual(record["punch_in_ist"], "2026-07-06T09:30:00+05:30")
        self.assertIsNone(record["punch_out_ist"])
        self.assertNotIn("_id", record)
        self.assertEqual(
            record["history"][0]["changes"],
            {"late_minutes": {"from": 5, "to": 0}},
        )
        self.assertNotIn("internal_note", record["history"][0])
        missing = api_record({"emp_code": "EMP0001", "date": "2026-07-07", "status": "PRESENT"})
        self.assertEqual(missing["history"], [])
        self.assertFalse(missing["half_day"])
        self.assertIsNone(missing["punch_in_ist"])
        self.assertIsNone(missing["punch_out_ist"])


class RemainingEndpointHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_db = main_module.db
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main_module.app),
            base_url="http://test",
        )

    async def asyncTearDown(self):
        main_module.db = self.original_db
        await self.http.aclose()

    @staticmethod
    def _employee():
        return {
            "emp_code": "EMP0001",
            "department": "Engineering",
            "joined_on": "2026-01-01",
            "shift_start": "09:30",
            "shift_end": "18:30",
        }

    @staticmethod
    def _attendance(punch_in=None, punch_out=None):
        return {
            "_id": ObjectId(),
            "emp_code": "EMP0001",
            "date": "2026-07-06",
            "status": "PRESENT",
            "punch_in": punch_in or datetime(2026, 7, 6, 4, 35, tzinfo=UTC),
            "punch_out": punch_out,
            "work_hours": None,
            "late_minutes": 35,
            "overtime_minutes": 0,
            "half_day": False,
            "history": [],
        }

    def _set_database(self, employee=None, attendance=None):
        main_module.db = SimpleNamespace(
            employees=employee or SimpleNamespace(find_one=AsyncMock(return_value=self._employee())),
            attendance_logs=attendance or SimpleNamespace(),
        )

    async def test_punch_out_computes_exact_half_day_boundary_and_rejects_invalid_elapsed_time(self):
        punch_in = datetime(2026, 7, 6, 4, 0, tzinfo=UTC)
        record = self._attendance(punch_in=punch_in)
        logs = SimpleNamespace(
            find_one=AsyncMock(side_effect=lambda *args, **kwargs: copy.deepcopy(record)),
            update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
        )
        self._set_database(attendance=logs)
        exact_half_day = int(datetime(2026, 7, 6, 8, 30, tzinfo=UTC).timestamp() * 1000)

        response = await self.http.post(
            "/attendance/punch-out",
            json={"emp_code": "EMP0001", "punched_at": exact_half_day},
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["work_hours"], 4.5)
        self.assertFalse(response.json()["half_day"])
        self.assertEqual(response.json()["punch_out"], exact_half_day)
        update = logs.update_one.await_args.args[1]["$set"]
        self.assertEqual(update["work_hours"], 4.5)
        self.assertFalse(update["half_day"])

        just_under_half_day = await self.http.post(
            "/attendance/punch-out",
            json={
                "emp_code": "EMP0001",
                "punched_at": int(datetime(2026, 7, 6, 8, 29, tzinfo=UTC).timestamp() * 1000),
            },
        )
        self.assertEqual(just_under_half_day.status_code, 200, just_under_half_day.text)
        self.assertEqual(just_under_half_day.json()["work_hours"], 4.48)
        self.assertTrue(just_under_half_day.json()["half_day"])

        for instant in (
            int(punch_in.timestamp() * 1000),
            int((punch_in + timedelta(days=1, seconds=1)).timestamp() * 1000),
        ):
            invalid = await self.http.post(
                "/attendance/punch-out",
                json={"emp_code": "EMP0001", "punched_at": instant},
            )
            self.assertEqual(invalid.status_code, 422, invalid.text)
        self.assertEqual(logs.update_one.await_count, 2)

    async def test_overnight_punch_out_computes_overtime_from_next_day_shift_end(self):
        employee = self._employee()
        employee.update({"shift_start": "22:00", "shift_end": "06:00"})
        punch_in = datetime(2026, 7, 6, 16, 25, tzinfo=UTC)
        record = self._attendance(punch_in=punch_in)
        logs = SimpleNamespace(
            find_one=AsyncMock(side_effect=lambda *args, **kwargs: copy.deepcopy(record)),
            update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
        )
        self._set_database(
            employee=SimpleNamespace(find_one=AsyncMock(return_value=employee)),
            attendance=logs,
        )
        punch_out = int(datetime(2026, 7, 7, 1, 10, tzinfo=UTC).timestamp() * 1000)

        response = await self.http.post(
            "/attendance/punch-out",
            json={"emp_code": "EMP0001", "punched_at": punch_out},
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["date"], "2026-07-06")
        self.assertEqual(response.json()["overtime_minutes"], 40)
        self.assertEqual(response.json()["punch_out_ist"], "2026-07-07T06:40:00+05:30")

    async def test_punch_out_distinguishes_missing_employee_and_punch_in(self):
        employees = SimpleNamespace(find_one=AsyncMock(return_value=None))
        logs = SimpleNamespace(find_one=AsyncMock(return_value=None))
        self._set_database(employee=employees, attendance=logs)
        payload = {"emp_code": "EMP9999"}

        unknown_employee = await self.http.post("/attendance/punch-out", json=payload)
        self.assertEqual(unknown_employee.status_code, 404)

        employees.find_one.return_value = self._employee()
        no_punch_in = await self.http.post("/attendance/punch-out", json=payload)
        self.assertEqual(no_punch_in.status_code, 404)

        invalid_timestamp = await self.http.post(
            "/attendance/punch-out",
            json={"emp_code": "EMP0001", "punched_at": 1_783_312_500.0},
        )
        self.assertEqual(invalid_timestamp.status_code, 422)

    async def test_regularization_recomputes_fields_and_records_only_changed_values(self):
        record = self._attendance(
            punch_in=datetime(2026, 7, 6, 4, 35, tzinfo=UTC),
            punch_out=datetime(2026, 7, 6, 13, 0, tzinfo=UTC),
        )
        old_punch_in = int(record["punch_in"].timestamp() * 1000)
        old_punch_out = int(record["punch_out"].timestamp() * 1000)
        record.update({"work_hours": 8.42, "late_minutes": 35})
        logs = SimpleNamespace(
            find_one=AsyncMock(return_value=record),
            update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
        )
        self._set_database(attendance=logs)
        new_punch_in = int(datetime(2026, 7, 6, 4, 0, tzinfo=UTC).timestamp() * 1000)
        new_punch_out = int(datetime(2026, 7, 6, 12, 30, tzinfo=UTC).timestamp() * 1000)

        response = await self.http.patch(
            "/attendance/EMP0001/2026-07-06",
            json={
                "punch_in": new_punch_in,
                "punch_out": new_punch_out,
                "reason": "biometric correction",
                "regularized_by": "hr.admin",
            },
        )

        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        self.assertEqual(result["late_minutes"], 0)
        self.assertEqual(result["work_hours"], 8.5)
        self.assertFalse(result["half_day"])
        changes = result["history"][0]["changes"]
        self.assertEqual(changes["punch_in"], {"from": old_punch_in, "to": new_punch_in})
        self.assertEqual(changes["punch_out"], {"from": old_punch_out, "to": new_punch_out})
        self.assertEqual(changes["late_minutes"], {"from": 35, "to": 0})
        self.assertEqual(changes["work_hours"], {"from": 8.42, "to": 8.5})
        self.assertNotIn("overtime_minutes", changes)
        self.assertNotIn("half_day", changes)
        logs.update_one.assert_awaited_once()

    async def test_regularization_rejects_noop_invalid_combinations_and_missing_records(self):
        record = self._attendance()
        logs = SimpleNamespace(
            find_one=AsyncMock(return_value=record),
            update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
        )
        self._set_database(attendance=logs)

        noop = await self.http.patch(
            "/attendance/EMP0001/2026-07-06",
            json={"reason": "no change", "regularized_by": "hr.admin"},
        )
        self.assertEqual(noop.status_code, 422, noop.text)

        conflicting_status_and_time = await self.http.patch(
            "/attendance/EMP0001/2026-07-06",
            json={
                "status": "LEAVE",
                "punch_in": int(record["punch_in"].timestamp() * 1000),
                "reason": "mark leave",
                "regularized_by": "hr.admin",
            },
        )
        self.assertEqual(conflicting_status_and_time.status_code, 422)

        invalid_date = await self.http.patch(
            "/attendance/EMP0001/2026-02-30",
            json={"status": "WFH", "reason": "correct status", "regularized_by": "hr.admin"},
        )
        self.assertEqual(invalid_date.status_code, 422)

        logs.find_one.return_value = None
        missing_record = await self.http.patch(
            "/attendance/EMP0001/2026-07-06",
            json={"status": "WFH", "reason": "correct status", "regularized_by": "hr.admin"},
        )
        self.assertEqual(missing_record.status_code, 404)
        logs.update_one.assert_not_awaited()

    async def test_employee_listing_uses_filtered_count_pagination_and_response_fields(self):
        employee_document = {
            "_id": ObjectId(),
            "emp_code": "EMP0002",
            "name": "Asha Rao",
            "email": "asha@example.com",
            "department": "Engineering",
            "shift_start": "09:30",
            "shift_end": "18:30",
            "joined_on": "2026-01-05",
            "created_at": datetime(2026, 1, 5, 4, 30, tzinfo=UTC),
            "internal_note": "must not be returned",
        }
        cursor = SimpleNamespace()
        cursor.sort = Mock(return_value=cursor)
        cursor.skip = Mock(return_value=cursor)
        cursor.limit = Mock(return_value=cursor)
        cursor.to_list = AsyncMock(return_value=[copy.deepcopy(employee_document)])
        employees = SimpleNamespace(
            count_documents=AsyncMock(return_value=3),
            find=Mock(return_value=cursor),
        )
        self._set_database(employee=employees)

        response = await self.http.get(
            "/employees",
            params={"department": "Engineering", "page": 2, "page_size": 1},
        )

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(
            {key: body[key] for key in ("total", "page", "page_size")},
            {"total": 3, "page": 2, "page_size": 1},
        )
        self.assertEqual(body["items"][0]["created_at"], 1_767_587_400_000)
        self.assertEqual(
            set(body["items"][0]),
            set(main_module.EMPLOYEE_RESPONSE_FIELDS),
        )
        self.assertNotIn("_id", body["items"][0])
        self.assertNotIn("internal_note", body["items"][0])
        employees.count_documents.assert_awaited_once_with(
            {"department": "Engineering"}
        )
        employees.find.assert_called_once()
        projection = employees.find.call_args.args[1]
        self.assertEqual(projection["_id"], 0)
        self.assertTrue(all(
            projection[field] == 1
            for field in main_module.EMPLOYEE_RESPONSE_FIELDS
        ))
        cursor.sort.assert_called_once_with("emp_code", main_module.ASCENDING)
        cursor.skip.assert_called_once_with(1)
        cursor.limit.assert_called_once_with(1)
        cursor.to_list.assert_awaited_once_with(length=1)

    async def test_analytics_and_explain_reject_invalid_ranges_and_missing_inputs(self):
        employees = SimpleNamespace(find_one=AsyncMock(return_value=None), count_documents=AsyncMock(return_value=0))
        logs = SimpleNamespace()
        self._set_database(employee=employees, attendance=logs)

        unknown_employee = await self.http.get(
            "/analytics/employees/EMP9999/monthly",
            params={"month": "2026-07"},
        )
        self.assertEqual(unknown_employee.status_code, 404)

        invalid_month = await self.http.get(
            "/analytics/employees/EMP9999/monthly",
            params={"month": "2026-13"},
        )
        self.assertEqual(invalid_month.status_code, 422)

        bad_summary_month = await self.http.get(
            "/analytics/departments/summary",
            params={"month": "2026-00"},
        )
        self.assertEqual(bad_summary_month.status_code, 422)

        bad_leaderboard_limit = await self.http.get(
            "/analytics/leaderboard/late",
            params={"month": "2026-07", "limit": 51},
        )
        self.assertEqual(bad_leaderboard_limit.status_code, 422)

        reversed_trend = await self.http.get(
            "/analytics/departments/Engineering/trend",
            params={"from": "2026-07-07", "to": "2026-07-06"},
        )
        self.assertEqual(reversed_trend.status_code, 422)

        unknown_department = await self.http.get(
            "/analytics/departments/Unknown/trend",
            params={"from": "2026-07-06", "to": "2026-07-07"},
        )
        self.assertEqual(unknown_department.status_code, 404)

        missing_explain_parameters = await self.http.get(
            "/admin/explain/employee_monthly",
        )
        self.assertEqual(missing_explain_parameters.status_code, 422)

        unknown_explain_endpoint = await self.http.get(
            "/admin/explain/unknown",
        )
        self.assertEqual(unknown_explain_endpoint.status_code, 422)


class AsyncCollection:
    def __init__(self, documents=()):
        self.documents = [copy.deepcopy(document) for document in documents]
        self.lock = asyncio.Lock()

    async def find_one(self, query, projection=None, sort=None):
        matches = [document for document in self.documents if self._matches(document, query)]
        if sort:
            field, direction = sort[0]
            matches.sort(key=lambda document: document.get(field), reverse=direction == -1)
        if not matches:
            return None
        result = copy.deepcopy(matches[0])
        await asyncio.sleep(0)
        if projection:
            included = {field for field, enabled in projection.items() if enabled}
            if included:
                result = {field: value for field, value in result.items() if field in included or field == "_id"}
            else:
                for field, enabled in projection.items():
                    if not enabled:
                        result.pop(field, None)
        return result

    async def insert_one(self, document):
        async with self.lock:
            key = (document["emp_code"], document["date"])
            if any((item["emp_code"], item["date"]) == key for item in self.documents):
                raise DuplicateKeyError("duplicate attendance key")
            stored = copy.deepcopy(document)
            stored["_id"] = ObjectId()
            self.documents.append(stored)
            return SimpleNamespace(inserted_id=stored["_id"])

    async def update_one(self, query, update):
        async with self.lock:
            for document in self.documents:
                if not self._matches(document, query):
                    continue
                document.update(copy.deepcopy(update.get("$set", {})))
                for field, value in update.get("$push", {}).items():
                    document.setdefault(field, []).append(copy.deepcopy(value))
                return SimpleNamespace(modified_count=1)
        return SimpleNamespace(modified_count=0)

    @staticmethod
    def _matches(document, query):
        for key, expected in query.items():
            value = document.get(key)
            if isinstance(expected, dict) and "$lte" in expected:
                if value is None or value > expected["$lte"]:
                    return False
            elif isinstance(expected, dict) and "$exists" in expected:
                if (key in document) != expected["$exists"]:
                    return False
            elif key not in document or value != expected:
                return False
        return True


class AttendanceConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_db = main_module.db
        employee = {
            "emp_code": "EMP0001",
            "shift_start": "09:30",
            "shift_end": "18:30",
        }
        self.employees = AsyncCollection([employee])
        self.attendance = AsyncCollection()
        main_module.db = SimpleNamespace(employees=self.employees, attendance_logs=self.attendance)

    async def asyncTearDown(self):
        main_module.db = self.original_db

    async def test_punch_in_truncates_instant_before_ist_date_and_late_calculation(self):
        boundary = datetime(2026, 7, 6, 4, 10, 0, 999000, tzinfo=UTC)
        response = await main_module.punch_in(PunchInIn(
            emp_code="EMP0001",
            punched_at=int(boundary.timestamp() * 1000),
        ))

        stored = self.attendance.documents[0]
        expected = boundary.replace(microsecond=0)
        self.assertEqual(stored["date"], "2026-07-06")
        self.assertEqual(stored["punch_in"], expected)
        self.assertEqual(response["punch_in"], int(expected.timestamp() * 1000))
        self.assertEqual(stored["late_minutes"], 0)

    async def test_punch_in_late_minutes_follow_strict_grace_rule(self):
        after_grace = datetime(2026, 7, 6, 4, 10, 1, 999000, tzinfo=UTC)
        response = await main_module.punch_in(PunchInIn(
            emp_code="EMP0001",
            punched_at=int(after_grace.timestamp() * 1000),
        ))

        self.assertEqual(response["late_minutes"], 10)

    async def test_overnight_punch_in_before_shift_end_uses_previous_shift_date(self):
        self.employees.documents[0].update({
            "shift_start": "22:00",
            "shift_end": "06:00",
        })
        early_morning = datetime(2026, 7, 7, 0, 29, 59, 999000, tzinfo=UTC)
        response = await main_module.punch_in(PunchInIn(
            emp_code="EMP0001",
            punched_at=int(early_morning.timestamp() * 1000),
        ))

        self.assertEqual(response["date"], "2026-07-06")
        self.assertEqual(response["punch_in"], int(early_morning.replace(microsecond=0).timestamp() * 1000))
        self.assertEqual(response["late_minutes"], 479)

    async def test_concurrent_duplicate_punch_ins_create_one_record(self):
        instant = int(datetime(2026, 7, 6, 4, 0, tzinfo=UTC).timestamp() * 1000)
        requests = [main_module.punch_in(PunchInIn(emp_code="EMP0001", punched_at=instant)) for _ in range(2)]
        results = await asyncio.gather(*requests, return_exceptions=True)
        self.assertEqual(sum(not isinstance(result, Exception) for result in results), 1)
        self.assertEqual(sum(isinstance(result, HTTPException) and result.status_code == 409 for result in results), 1)
        self.assertEqual(len(self.attendance.documents), 1)

    async def test_concurrent_punch_outs_close_record_once(self):
        punch_in = datetime(2026, 7, 6, 4, 0, tzinfo=UTC)
        self.attendance.documents.append({
            "_id": ObjectId(),
            "emp_code": "EMP0001",
            "date": "2026-07-06",
            "status": "PRESENT",
            "punch_in": punch_in,
            "punch_out": None,
            "work_hours": None,
            "late_minutes": 0,
            "overtime_minutes": 0,
            "half_day": False,
            "history": [],
        })
        punched_at = int(datetime(2026, 7, 6, 13, 0, tzinfo=UTC).timestamp() * 1000)
        results = await asyncio.gather(
            *(main_module.punch_out(PunchOutIn(emp_code="EMP0001", punched_at=punched_at)) for _ in range(2)),
            return_exceptions=True,
        )
        self.assertEqual(sum(not isinstance(result, Exception) for result in results), 1)
        self.assertEqual(sum(isinstance(result, HTTPException) and result.status_code == 409 for result in results), 1)

    async def test_regularization_normalizes_null_history_and_appends_atomically(self):
        self.attendance.documents.append({
            "_id": ObjectId(),
            "emp_code": "EMP0001",
            "date": "2026-07-06",
            "status": "PRESENT",
            "punch_in": datetime(2026, 7, 6, 4, 0, tzinfo=UTC),
            "punch_out": None,
            "work_hours": None,
            "late_minutes": 0,
            "overtime_minutes": 0,
            "half_day": False,
            "history": None,
        })
        body = RegularizeIn(status="WFH", reason="correct status", regularized_by="hr.admin")
        result = await main_module.regularize_attendance("EMP0001", "2026-07-06", body)
        self.assertEqual(result["status"], "WFH")
        self.assertEqual(len(result["history"]), 1)
        self.assertEqual(result["history"][0]["changes"], {"status": {"from": "PRESENT", "to": "WFH"}})

    async def test_concurrent_regularizations_do_not_lose_history(self):
        self.attendance.documents.append({
            "_id": ObjectId(),
            "emp_code": "EMP0001",
            "date": "2026-07-06",
            "status": "PRESENT",
            "punch_in": datetime(2026, 7, 6, 4, 0, tzinfo=UTC),
            "punch_out": None,
            "work_hours": None,
            "late_minutes": 0,
            "overtime_minutes": 0,
            "half_day": False,
            "history": [],
        })
        requests = [
            main_module.regularize_attendance(
                "EMP0001",
                "2026-07-06",
                RegularizeIn(status=status, reason="correct status", regularized_by="hr.admin"),
            )
            for status in ("WFH", "ON_DUTY")
        ]
        results = await asyncio.gather(*requests, return_exceptions=True)
        self.assertEqual(sum(not isinstance(result, Exception) for result in results), 1)
        self.assertEqual(sum(isinstance(result, HTTPException) and result.status_code == 409 for result in results), 1)
        self.assertEqual(len(self.attendance.documents[0]["history"]), 1)


if __name__ == "__main__":
    unittest.main()

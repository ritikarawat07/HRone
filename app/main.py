import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Annotated, Literal, Optional

from bson import Timestamp, json_util
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.errors import ConfigurationError, DuplicateKeyError, OperationFailure, PyMongoError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=False)

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
client: Optional[AsyncMongoClient] = None
db = None

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
STATUSES = (*PRESENCE, "ABSENT", "LEAVE")
EMPLOYEE_RESPONSE_FIELDS = (
    "emp_code",
    "name",
    "email",
    "department",
    "shift_start",
    "shift_end",
    "joined_on",
    "created_at",
)
ATTENDANCE_RESPONSE_FIELDS = (
    "emp_code",
    "date",
    "status",
    "punch_in",
    "punch_out",
    "work_hours",
    "late_minutes",
    "overtime_minutes",
    "half_day",
    "history",
)
HISTORY_ENTRY_FIELDS = ("at", "by", "reason", "changes")
HISTORY_CHANGE_FIELDS = (
    "status",
    "punch_in",
    "punch_out",
    "work_hours",
    "late_minutes",
    "overtime_minutes",
    "half_day",
)
EPOCH_MIN = 100_000_000_000
EPOCH_MAX = 4_102_444_800_000


@dataclass(frozen=True)
class IndexDefinition:
    collection: str
    name: str
    keys: tuple[tuple[str, int], ...]
    unique: bool = False


@dataclass(frozen=True)
class IndexMigration:
    version: int
    indexes: tuple[IndexDefinition, ...]


INDEX_MIGRATIONS = (
    IndexMigration(
        version=1,
        indexes=(
            IndexDefinition("employees", "employee_code_unique", (("emp_code", ASCENDING),), unique=True),
            IndexDefinition("employees", "department_employee_code", (("department", ASCENDING), ("emp_code", ASCENDING))),
            IndexDefinition("employees", "department_joined", (("department", ASCENDING), ("joined_on", ASCENDING))),
            IndexDefinition("employees", "employee_joined", (("joined_on", ASCENDING),)),
            IndexDefinition("attendance_logs", "employee_date_unique", (("emp_code", ASCENDING), ("date", ASCENDING)), unique=True),
            IndexDefinition("attendance_logs", "employee_punch_in", (("emp_code", ASCENDING), ("punch_in", DESCENDING))),
            IndexDefinition("attendance_logs", "attendance_date_employee", (("date", DESCENDING), ("emp_code", ASCENDING))),
            IndexDefinition("attendance_logs", "attendance_date_late", (("date", ASCENDING), ("late_minutes", ASCENDING))),
            IndexDefinition("attendance_logs", "attendance_status_date", (("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING))),
        ),
    ),
)


def get_db():
    if db is None:
        raise RuntimeError("Database is not initialized")
    return db


def index_matches(actual: dict, expected: IndexDefinition) -> bool:
    if list(actual["key"].items()) != list(expected.keys):
        return False
    if bool(actual.get("unique", False)) != expected.unique:
        return False
    if bool(actual.get("hidden", False)):
        return False
    for option in ("partialFilterExpression", "sparse", "expireAfterSeconds", "collation"):
        if actual.get(option) is not None:
            return False
    return True


async def ensure_index_definitions(database, definitions: tuple[IndexDefinition, ...]) -> None:
    indexes_by_collection = {}
    for definition in definitions:
        if definition.collection not in indexes_by_collection:
            collection = database[definition.collection]
            cursor = await collection.list_indexes()
            indexes_by_collection[definition.collection] = {
                index["name"]: index async for index in cursor
            }
        indexes = indexes_by_collection[definition.collection]
        existing = indexes.get(definition.name)
        if existing is not None:
            if not index_matches(existing, definition):
                expected_keys = list(definition.keys)
                actual_keys = list(existing["key"].items())
                raise RuntimeError(
                    f"Conflicting MongoDB index '{definition.name}' on "
                    f"'{definition.collection}': expected keys {expected_keys} "
                    f"and unique={definition.unique}, found keys {actual_keys} "
                    f"and unique={bool(existing.get('unique', False))}. Resolve "
                    "the index manually; startup will not drop or replace it."
                )
            continue
        collection = database[definition.collection]
        try:
            await collection.create_index(
                list(definition.keys),
                name=definition.name,
                unique=definition.unique,
            )
        except (DuplicateKeyError, OperationFailure) as error:
            raise RuntimeError(
                f"Could not create MongoDB index '{definition.name}' on "
                f"'{definition.collection}'. Check for duplicate existing data "
                "and verify the database user can create indexes; no data or "
                "existing index was removed."
            ) from error
        indexes[definition.name] = {
            "name": definition.name,
            "key": dict(definition.keys),
            "unique": definition.unique,
        }


async def create_indexes() -> None:
    database = get_db()
    versions = [migration.version for migration in INDEX_MIGRATIONS]
    if any(version < 1 for version in versions) or len(versions) != len(set(versions)):
        raise RuntimeError("MongoDB index migration versions must be unique positive integers")
    for migration in sorted(INDEX_MIGRATIONS, key=lambda item: item.version):
        await ensure_index_definitions(database, migration.indexes)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global client, db
    client = None
    db = None
    if not MONGO_URI or not MONGO_DB:
        raise RuntimeError("MONGO_URI and MONGO_DB must be configured")
    client = AsyncMongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=8000)
    db = client[MONGO_DB]
    try:
        await client.admin.command("ping")
        await create_indexes()
        yield
    finally:
        if client is not None:
            await client.close()
        client = None
        db = None


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


def valid_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError("date must be YYYY-MM-DD")
    if parsed.isoformat() != value:
        raise ValueError("date must be YYYY-MM-DD")
    return value


def valid_month(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise ValueError("month must be YYYY-MM")
    try:
        date.fromisoformat(f"{value}-01")
    except ValueError:
        raise ValueError("month must be YYYY-MM")
    return value


def request_date(value: str) -> str:
    try:
        return valid_date(value)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error


def request_month(value: str) -> str:
    try:
        return valid_month(value)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error


def parse_instant(value: int) -> datetime:
    if type(value) is not int or value < EPOCH_MIN or value > EPOCH_MAX:
        raise ValueError("instant must be an integer epoch-millisecond value in range")
    return datetime.fromtimestamp(value // 1000, UTC)


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def epoch_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.timestamp() * 1000)


def format_ist(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(IST).replace(microsecond=0).isoformat(timespec="seconds")


def api_record(source: dict) -> dict:
    doc = {
        key: value
        for key, value in source.items()
        if key in ATTENDANCE_RESPONSE_FIELDS
    }
    for key in ("punch_in", "punch_out"):
        instant = doc.get(key)
        doc[f"{key}_ist"] = format_ist(instant)
        doc[key] = epoch_ms(instant)
    doc.setdefault("work_hours", None)
    doc.setdefault("late_minutes", 0)
    doc.setdefault("overtime_minutes", 0)
    doc.setdefault("half_day", False)
    doc["history"] = [
        {key: value for key, value in entry.items() if key in HISTORY_ENTRY_FIELDS}
        for entry in (doc.get("history") or [])
    ]
    for entry in doc["history"]:
        entry["at"] = epoch_ms(entry.get("at"))
        entry["changes"] = {
            field: {
                key: value
                for key, value in change.items()
                if key in ("from", "to")
            }
            for field, change in entry.get("changes", {}).items()
            if field in HISTORY_CHANGE_FIELDS
        }
        for field, change in entry["changes"].items():
            if field in ("punch_in", "punch_out"):
                change["from"] = epoch_ms(change.get("from"))
                change["to"] = epoch_ms(change.get("to"))
    return doc


def shift_datetime(day: str, shift_time: str, next_day: bool = False) -> datetime:
    shift_day = date.fromisoformat(day) + (timedelta(days=1) if next_day else timedelta())
    return datetime.combine(shift_day, time.fromisoformat(shift_time), IST).astimezone(UTC)


def attendance_day(punch_in: datetime, emp: dict) -> str:
    local = punch_in.astimezone(IST)
    start_hm = emp["shift_start"]
    end_hm = emp["shift_end"]
    day = local.date()
    if end_hm <= start_hm and local.strftime("%H:%M") < end_hm:
        day -= timedelta(days=1)
    return day.isoformat()


def compute_late_minutes(punch_in: datetime, shift_start: str, day: str) -> int:
    start = shift_datetime(day, shift_start)
    seconds = int((punch_in - start).total_seconds())
    return seconds // 60 if seconds > 600 else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out: datetime, emp: dict, day: str) -> int:
    overnight = emp["shift_end"] <= emp["shift_start"]
    end = shift_datetime(day, emp["shift_end"], overnight)
    minutes = int((punch_out - end).total_seconds()) // 60
    return minutes if minutes >= 30 else 0


def working_days(start: date, end: date) -> int:
    if start > end:
        return 0
    return sum(1 for offset in range((end - start).days + 1) if (start + timedelta(days=offset)).weekday() < 5)


def rounded(value, places=2):
    if value is None:
        return None
    quantum = Decimal("1").scaleb(-places)
    return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP))


def bson_doc_to_json(value):
    if isinstance(value, Timestamp):
        return {"t": value.time, "i": value.inc}
    if isinstance(value, Decimal128):
        return str(value.to_decimal())
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: bson_doc_to_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [bson_doc_to_json(item) for item in value]
    return value


def mongo_round_half_up(value, places: int) -> dict:
    scale = Decimal128(str(10**places))
    half = Decimal128("0.5")
    return {"$divide": [
        {"$floor": {"$add": [{"$multiply": [{"$toDecimal": value}, scale]}, half]}},
        scale,
    ]}


def month_bounds(month: str) -> tuple[str, str, date, date]:
    valid_month(month)
    year, number = map(int, month.split("-"))
    first = date(year, number, 1)
    after = date(year + (number == 12), 1 if number == 12 else number + 1, 1)
    last = after - timedelta(days=1)
    return first.isoformat(), after.isoformat(), first, last


class APIModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class EmployeeIn(APIModel):
    emp_code: str = Field(pattern=r"^EMP\d{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    shift_end: str = Field(default="18:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    joined_on: str

    @field_validator("joined_on")
    @classmethod 
    def check_joined_on(cls, value):
        return valid_date(value)

    @model_validator(mode="after")
    def shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start and shift_end must differ")
        return self


class PunchInIn(APIModel):
    emp_code: str
    punched_at: Optional[StrictInt] = None
    status: str = "PRESENT"

    @field_validator("punched_at")
    @classmethod
    def check_punched_at(cls, value):
        if value is not None:
            parse_instant(value)
        return value

    @field_validator("status")
    @classmethod
    def check_status(cls, value):
        if value not in PRESENCE:
            raise ValueError("status must be PRESENT, WFH or ON_DUTY")
        return value


class PunchOutIn(APIModel):
    emp_code: str
    punched_at: Optional[StrictInt] = None

    @field_validator("punched_at")
    @classmethod
    def check_punched_at(cls, value):
        if value is not None:
            parse_instant(value)
        return value


class RegularizeIn(APIModel):
    status: Optional[str] = None
    punch_in: Optional[StrictInt] = None
    punch_out: Optional[StrictInt] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @field_validator("status")
    @classmethod
    def check_status(cls, value):
        if value is None or value not in STATUSES:
            raise ValueError("invalid status")
        return value

    @field_validator("punch_in", "punch_out")
    @classmethod
    def check_times(cls, value):
        if value is not None:
            parse_instant(value)
        return value

    @model_validator(mode="after")
    def disallow_null_times(self):
        for key in ("punch_in", "punch_out"):
            if key in self.model_fields_set and getattr(self, key) is None:
                raise ValueError(f"{key} must be an epoch-millisecond integer")
        return self


def attendance_filter(emp_code=None, date_from=None, date_to=None, status=None):
    query = {}
    if status is not None and status not in STATUSES:
        raise HTTPException(422, "invalid status")
    if emp_code:
        query["emp_code"] = emp_code
    if date_from or date_to:
        query["date"] = {}
        if date_from:
            query["date"]["$gte"] = request_date(date_from)
        if date_to:
            query["date"]["$lte"] = request_date(date_to)
        if date_from and date_to and date_from > date_to:
            raise HTTPException(422, "date_from must be on or before date_to")
    if status:
        query["status"] = status
    return query


def date_match(start: str, after: str) -> dict:
    return {"date": {"$gte": start, "$lt": after}}


async def aggregate_to_list(collection, pipeline: list, length: Optional[int] = None) -> list:
    cursor = await collection.aggregate(pipeline)
    return await cursor.to_list(length=length)


def monthly_employee_pipeline(emp_code: str, month: str) -> list:
    start, after, _, _ = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, **date_match(start, after)}},
        {"$set": {"weekday": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}}}},
        {"$group": {"_id": None,
            "present_days": {"$sum": {"$cond": [{"$and": [{"$in": ["$status", list(PRESENCE)]}, {"$in": ["$weekday", [2, 3, 4, 5, 6]]}]}, {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1]}, 0]}},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}},
            "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}}}},
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> list:
    start, after, _, last = month_bounds(month)
    match = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        match["department"] = department
    return [
        {"$match": match},
        {"$lookup": {"from": "attendance_logs", "let": {"code": "$emp_code"},
            "pipeline": [{"$match": {"$expr": {"$and": [{"$eq": ["$emp_code", "$$code"]}, {"$gte": ["$date", start]}, {"$lt": ["$date", after]}]}}},
                {"$set": {"weekday": {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}}}},
                {"$group": {"_id": None,
                    "present_days": {"$sum": {"$cond": [{"$and": [{"$in": ["$status", list(PRESENCE)]}, {"$in": ["$weekday", [2, 3, 4, 5, 6]]}]}, {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1]}, 0]}},
                    "hours_sum": {"$sum": {"$cond": [{"$and": [{"$in": ["$status", list(PRESENCE)]}, {"$ne": [{"$ifNull": ["$work_hours", None]}, None]}]}, "$work_hours", 0]}},
                    "hours_count": {"$sum": {"$cond": [{"$and": [{"$in": ["$status", list(PRESENCE)]}, {"$ne": [{"$ifNull": ["$work_hours", None]}, None]}]}, 1, 0]}},
                    "late_count": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}},
                    "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                    "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                    "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}}}}],
            "as": "stats"}},
        {"$set": {"stats": {"$ifNull": [{"$arrayElemAt": ["$stats", 0]}, {}]}}},
        {"$group": {"_id": "$department", "headcount": {"$sum": 1},
            "present_days": {"$sum": {"$ifNull": ["$stats.present_days", 0]}},
            "hours_sum": {"$sum": {"$ifNull": ["$stats.hours_sum", 0]}},
            "hours_count": {"$sum": {"$ifNull": ["$stats.hours_count", 0]}},
            "late_count": {"$sum": {"$ifNull": ["$stats.late_count", 0]}},
            "total_late_minutes": {"$sum": {"$ifNull": ["$stats.total_late_minutes", 0]}},
            "leave_count": {"$sum": {"$ifNull": ["$stats.leave_count", 0]}},
            "on_duty_count": {"$sum": {"$ifNull": ["$stats.on_duty_count", 0]}}}},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "department": "$_id", "headcount": 1, "present_days": 1,
            "avg_work_hours": {"$cond": [{"$gt": ["$hours_count", 0]}, {"$divide": ["$hours_sum", "$hours_count"]}, None]},
            "late_count": 1, "total_late_minutes": 1, "leave_count": 1, "on_duty_count": 1}},
    ]


def trend_pipeline(department: str, start: date, end: date) -> list:
    count = (end - start).days + 1
    return [
        {"$match": {"date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}},
        {"$lookup": {"from": "employees", "localField": "emp_code", "foreignField": "emp_code", "as": "employee"}},
        {"$unwind": "$employee"},
        {"$match": {"employee.department": department}},
        {"$group": {"_id": {"$dateFromString": {"dateString": "$date"}},
            "present_count": {"$sum": {"$cond": [{"$in": ["$status", list(PRESENCE)]}, {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1]}, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}}}},
        {"$project": {"_id": 0, "date": "$_id", "present_count": 1, "late_count": 1}},
        {"$unionWith": {"pipeline": [
            {"$documents": [{"start": datetime.combine(start, time.min, UTC), "count": count}]},
            {"$project": {
                "date": {"$map": {
                    "input": {"$range": [0, "$count"]},
                    "as": "offset",
                    "in": {"$dateAdd": {"startDate": "$start", "unit": "day", "amount": "$$offset"}},
                }},
            }},
            {"$unwind": "$date"},
            {"$project": {"_id": 0, "date": 1, "present_count": {"$literal": 0}, "late_count": {"$literal": 0}}},
        ]}},
        {"$group": {"_id": "$date", "present_count": {"$max": "$present_count"}, "late_count": {"$max": "$late_count"}}},
        {"$project": {"_id": 0, "date": "$_id", "present_count": 1, "late_count": 1}},
        {"$densify": {"field": "date", "range": {"step": 1, "unit": "day",
            "bounds": [datetime.combine(start, time.min, UTC), datetime.combine(end, time.min, UTC)]}}},
        {"$set": {
            "date_string": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date", "timezone": "UTC"}},
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
            "is_working_day": {"$not": [{"$in": [{"$dayOfWeek": "$date"}, [1, 7]]}]},
        }},
        {"$lookup": {
            "from": "employees",
            "let": {"day": "$date_string"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$department", department]},
                    {"$lte": ["$joined_on", "$$day"]},
                ]}}},
                {"$count": "headcount"},
            ],
            "as": "headcount_result",
        }},
        {"$set": {
            "headcount": {"$ifNull": [{"$arrayElemAt": ["$headcount_result.headcount", 0]}, 0]},
        }},
        {"$set": {
            "attendance_rate": {"$cond": [
                {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                mongo_round_half_up({"$divide": [{"$toDecimal": "$present_count"}, {"$toDecimal": "$headcount"}]}, 4),
                None,
            ]},
        }},
        {"$sort": {"date": 1}},
        {"$setWindowFields": {
            "sortBy": {"date": 1},
            "output": {"moving_avg_7d": {
                "$avg": "$attendance_rate",
                "window": {"documents": [-6, 0]},
            }},
        }},
        {"$set": {"moving_avg_7d": mongo_round_half_up("$moving_avg_7d", 4)}},
        {"$project": {
            "_id": 0,
            "date": "$date_string",
            "is_working_day": 1,
            "headcount": 1,
            "present_count": 1,
            "late_count": 1,
            "attendance_rate": {"$toDouble": "$attendance_rate"},
            "moving_avg_7d": {"$toDouble": "$moving_avg_7d"},
        }},
    ]


@app.get("/health")
async def health():
    if client is None or db is None:
        raise HTTPException(503, "MongoDB is unavailable")
    try:
        await client.admin.command("ping")
    except (ConfigurationError, PyMongoError, OSError, ValueError):
        raise HTTPException(503, "MongoDB is unavailable") from None
    return {"status": "ok"}


@app.post("/employees", status_code=201)
async def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        await get_db().employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    doc.pop("_id", None)
    doc["created_at"] = epoch_ms(doc["created_at"])
    return doc


@app.get("/employees")
async def list_employees(department: Optional[str] = None,
                   page: Annotated[int, Query(ge=1)] = 1,
                   page_size: Annotated[int, Query(ge=1, le=100)] = 20):
    database = get_db()
    query = {"department": department} if department is not None else {}
    total = await database.employees.count_documents(query)
    projection = {"_id": 0, **dict.fromkeys(EMPLOYEE_RESPONSE_FIELDS, 1)}
    items = await database.employees.find(query, projection).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size).to_list(length=page_size)
    for item in items:
        for field in tuple(item):
            if field not in EMPLOYEE_RESPONSE_FIELDS:
                del item[field]
        item["created_at"] = epoch_ms(item.get("created_at"))
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
async def punch_in(body: PunchInIn):
    database = get_db()
    emp = await database.employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    instant = parse_instant(body.punched_at) if body.punched_at is not None else now_utc()
    day = attendance_day(instant, emp)
    doc = {"emp_code": body.emp_code, "date": day, "status": body.status, "punch_in": instant, "punch_out": None,
           "work_hours": None, "late_minutes": compute_late_minutes(instant, emp["shift_start"], day),
           "overtime_minutes": 0, "half_day": False, "history": []}
    try:
        await database.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return api_record(doc)

@app.post("/attendance/punch-out")
async def punch_out(body: PunchOutIn):
    database = get_db()
    emp = await database.employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    instant = parse_instant(body.punched_at) if body.punched_at is not None else now_utc()
    candidate = await database.attendance_logs.find_one({"emp_code": body.emp_code, "punch_in": {"$lte": instant}}, sort=[("punch_in", DESCENDING)])
    if candidate is None:
        raise HTTPException(404, "no punch-in found")
    if candidate.get("punch_out") is not None:
        raise HTTPException(409, "record is already punched out")
    start = candidate["punch_in"]
    elapsed = (instant - start).total_seconds()
    if elapsed <= 0 or elapsed > 24 * 3600:
        raise HTTPException(422, "punch-out must be after punch-in and within 24 hours")
    hours = compute_work_hours(start, instant)
    changes = {"punch_out": instant, "work_hours": hours,
               "overtime_minutes": compute_overtime(instant, emp, candidate["date"]),
               "half_day": hours < 4.5}
    result = await database.attendance_logs.update_one({"_id": candidate["_id"], "punch_out": None}, {"$set": changes})
    if result.modified_count != 1:
        raise HTTPException(409, "record is already punched out")
    candidate.update(changes)
    return api_record(candidate)


@app.get("/attendance")
async def list_attendance(emp_code: Optional[str] = None, date_from: Optional[str] = None, date_to: Optional[str] = None,
                    status: Optional[Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]] = None,
                    page: Annotated[int, Query(ge=1)] = 1,
                    page_size: Annotated[int, Query(ge=1, le=100)] = 20):
    database = get_db()
    query = attendance_filter(emp_code, date_from, date_to, status)
    total = await database.attendance_logs.count_documents(query)
    docs = await database.attendance_logs.find(query, {"_id": 0}).sort([("date", DESCENDING), ("emp_code", ASCENDING)]).skip((page - 1) * page_size).limit(page_size).to_list(length=page_size)
    return {"items": [api_record(doc) for doc in docs], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
async def regularize_attendance(emp_code: str, date: str, body: RegularizeIn):
    database = get_db()
    request_date(date)
    emp = await database.employees.find_one({"emp_code": emp_code})
    current = await database.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if emp is None or current is None:
        raise HTTPException(404, "employee or attendance record not found")
    fields = body.model_fields_set
    updated = dict(current)
    if "status" in fields:
        updated["status"] = body.status
    if updated["status"] in ("ABSENT", "LEAVE") and ({"punch_in", "punch_out"} & fields):
        raise HTTPException(422, "ABSENT and LEAVE cannot be supplied with punch times")
    for name in ("punch_in", "punch_out"):
        if name in fields:
            updated[name] = parse_instant(getattr(body, name))
    if updated["status"] in ("ABSENT", "LEAVE"):
        updated["punch_in"] = None
        updated["punch_out"] = None
    elif updated.get("punch_in") is None:
        raise HTTPException(422, "presence statuses require punch_in")
    if updated.get("punch_in") is not None:
        if attendance_day(updated["punch_in"], emp) != date:
            raise HTTPException(422, "punch_in must belong to the attendance date")
        updated["late_minutes"] = compute_late_minutes(updated["punch_in"], emp["shift_start"], date)
    else:
        updated["late_minutes"] = 0
    if updated.get("punch_out") is not None:
        if updated.get("punch_in") is None:
            raise HTTPException(422, "punch_out requires punch_in")
        elapsed = (updated["punch_out"] - updated["punch_in"]).total_seconds()
        if elapsed <= 0 or elapsed > 24 * 3600:
            raise HTTPException(422, "punch_out must be after punch_in and within 24 hours")
        updated["work_hours"] = compute_work_hours(updated["punch_in"], updated["punch_out"])
        updated["overtime_minutes"] = compute_overtime(updated["punch_out"], emp, date)
        updated["half_day"] = updated["work_hours"] < 4.5
    else:
        updated["work_hours"] = None
        updated["overtime_minutes"] = 0
        updated["half_day"] = False
    changed = {}
    defaults = {"punch_in": None, "punch_out": None, "work_hours": None, "late_minutes": 0,
                "overtime_minutes": 0, "half_day": False}
    for key in ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day"):
        before, after = current.get(key, defaults.get(key)), updated.get(key, defaults.get(key))
        if before != after:
            changed[key] = {"from": before, "to": after}
    if not changed:
        raise HTTPException(422, "request does not change the record")
    history_value = current.get("history")
    history = history_value or []
    if "history" not in current:
        history_filter = {"history": {"$exists": False}}
    elif history_value is None:
        history_filter = {"history": None}
    else:
        history_filter = {"history": history_value}
    update_filter = {"_id": current["_id"], **history_filter}
    for key in ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day"):
        update_filter[key] = current[key] if key in current else {"$exists": False}
    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changed}
    values = {key: updated[key] for key in changed}
    update = {"$set": values}
    if history_value is None:
        update["$set"]["history"] = [*history, entry]
    else:
        update["$push"] = {"history": entry}
    result = await database.attendance_logs.update_one(update_filter, update)
    if result.modified_count != 1:
        raise HTTPException(409, "record changed concurrently; retry")
    current.update(values)
    current["history"] = [*history, entry]
    return api_record(current)


@app.get("/analytics/employees/{emp_code}/monthly")
async def employee_monthly(emp_code: str, month: str):
    database = get_db()
    request_month(month)
    emp = await database.employees.find_one({"emp_code": emp_code}, {"joined_on": 1})
    if emp is None:
        raise HTTPException(404, "employee not found")
    _, _, first, last = month_bounds(month)
    joined = max(first, date.fromisoformat(emp["joined_on"]))
    totals = await aggregate_to_list(
        database.attendance_logs,
        monthly_employee_pipeline(emp_code, month),
        length=1,
    )
    totals = totals[0] if totals else {}
    days = working_days(joined, last)
    present = rounded(totals.get("present_days", 0))
    return {"emp_code": emp_code, "month": month, "working_days": days, "present_days": present,
            "leave_days": totals.get("leave_days", 0), "late_count": totals.get("late_count", 0),
            "total_late_minutes": totals.get("total_late_minutes", 0),
            "total_overtime_minutes": totals.get("total_overtime_minutes", 0),
            "attendance_pct": rounded(present * 100 / days, 4) if days else None}


@app.get("/analytics/departments/summary")
async def department_summary(month: str, department: Optional[str] = None):
    request_month(month)
    items = await aggregate_to_list(
        get_db().employees,
        department_summary_pipeline(month, department),
    )
    for item in items:
        item["present_days"] = rounded(item["present_days"])
        item["avg_work_hours"] = rounded(item["avg_work_hours"])
    return {"month": month, "items": items}


def leaderboard_pipeline(month: str, department: Optional[str], limit: int):
    start, after, _, _ = month_bounds(month)
    pipeline = [
        {"$match": {**date_match(start, after), "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "employee"}},
        {"$unwind": "$employee"},
    ]
    if department is not None:
        pipeline.append({"$match": {"employee.department": department}})
    pipeline.extend([
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$employee.name", "department": "$employee.department", "total_late_minutes": 1, "late_count": 1}},
    ])
    return pipeline


@app.get("/analytics/leaderboard/late")
async def late_leaderboard(month: str, limit: Annotated[int, Query(ge=1, le=50)] = 10, department: Optional[str] = None):
    request_month(month)
    pipeline = leaderboard_pipeline(month, department, limit)
    items = await aggregate_to_list(get_db().attendance_logs, pipeline)
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
async def department_trend(department: str, from_date: Annotated[str, Query(alias="from")], to_date: Annotated[str, Query(alias="to")]):
    request_date(from_date)
    request_date(to_date)
    start, end = date.fromisoformat(from_date), date.fromisoformat(to_date)
    if end < start or (end - start).days + 1 > 92:
        raise HTTPException(422, "date range must be ordered and no longer than 92 days")
    database = get_db()
    if await database.employees.count_documents({"department": department}) == 0:
        raise HTTPException(404, "department not found")
    pipeline = trend_pipeline(department, start, end)
    items = await aggregate_to_list(database.attendance_logs, pipeline)
    return {"department": department, "items": items}


@app.get("/admin/explain/{endpoint}")
async def explain_endpoint(endpoint: str, emp_code: Optional[str] = None, month: Optional[str] = None,
                     department: Optional[str] = None, limit: Annotated[int, Query(ge=1, le=50)] = 10,
                     date_from: Optional[str] = None, date_to: Optional[str] = None, status: Optional[str] = None,
                     from_date: Optional[str] = Query(default=None, alias="from"),
                     to_date: Optional[str] = Query(default=None, alias="to"),
                     page: Annotated[int, Query(ge=1)] = 1, page_size: Annotated[int, Query(ge=1, le=100)] = 20):
    if endpoint == "attendance_list":
        query = attendance_filter(emp_code, date_from, date_to, status)
        operation = {"find": "attendance_logs", "filter": query, "projection": {"_id": 0}, "sort": {"date": -1, "emp_code": 1},
                     "skip": (page - 1) * page_size, "limit": page_size}
        coll = "attendance_logs"
    elif endpoint == "employee_monthly":
        if not emp_code or not month:
            raise HTTPException(422, "emp_code and month are required")
        request_month(month)
        month_bounds(month)
        operation = {"aggregate": "attendance_logs", "pipeline": monthly_employee_pipeline(emp_code, month), "cursor": {}}
        coll = "attendance_logs"
    elif endpoint == "department_summary":
        if not month:
            raise HTTPException(422, "month is required")
        request_month(month)
        operation = {"aggregate": "employees", "pipeline": department_summary_pipeline(month, department), "cursor": {}}
        coll = "employees"
    elif endpoint == "late_leaderboard":
        if not month:
            raise HTTPException(422, "month is required")
        request_month(month)
        pipeline = leaderboard_pipeline(month, department, limit)
        operation = {"aggregate": "attendance_logs", "pipeline": pipeline, "cursor": {}}
        coll = "attendance_logs"
    elif endpoint == "department_trend":
        if not department or not from_date or not to_date:
            raise HTTPException(422, "department, from and to are required")
        request_date(from_date)
        request_date(to_date)
        start, end = date.fromisoformat(from_date), date.fromisoformat(to_date)
        if end < start or (end - start).days + 1 > 92:
            raise HTTPException(422, "date range must be ordered and no longer than 92 days")
        operation = {"aggregate": "attendance_logs", "pipeline": trend_pipeline(department, start, end), "cursor": {}}
        coll = "attendance_logs"
    else:
        raise HTTPException(422, "unknown endpoint")
    explain_raw = await get_db().command({"explain": operation, "verbosity": "executionStats"})
    explain = bson_doc_to_json(json_util.loads(json_util.dumps(explain_raw)))
    return {"endpoint": endpoint, "collection": coll, "explain": explain}

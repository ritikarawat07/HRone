# HROne Employee Attendance & Analytics API

A FastAPI service for employee records, daily attendance, manual corrections, and attendance analytics. The application
uses asynchronous PyMongo and MongoDB; the HTTP and business contract is defined in [`openapi.yaml`](./openapi.yaml),
and stored collection shapes are documented in [`DATA_MODEL.md`](./DATA_MODEL.md).

## Technology and requirements

- Python 3.11 or newer (the suite was run with Python 3.14.5).
- MongoDB 6.0 or newer for application and integration-test use.
- FastAPI, Pydantic 2, PyMongo Async, Uvicorn, and `python-dotenv`.

## Project layout

```text
app/main.py                 All API routes, validation, business logic, and MongoDB queries
requirements.txt            Runtime and test dependencies
README.md                   Setup, API, and verification guide
REVIEW.md                   Assignment defect review
DECISIONS.md                Five engineering decision answers
openapi.yaml                Authoritative API contract and rules R1–R10
DATA_MODEL.md               MongoDB collection contract
tests/test_main.py          Mock-backed unit and HTTP tests
tests/test_mongo_concurrency.py
                            Live MongoDB integration, explain, and concurrency tests
test_concurrent.py          Runner for the live employee-create concurrency test
sample_data/                Optional sample documents (not required by the API)
sample_seed.py              Optional, additive sample-data seeder
```

The required submission files are `app/main.py`, `requirements.txt`, `README.md`, `REVIEW.md`, and `DECISIONS.md`.
The tests and provided `sample_data/`/`sample_seed.py` are retained as useful verification resources; they are not needed
to start the API. The assignment prohibits database dumps, not these small supplied fixtures. Keep `.env`, credentials,
virtual environments, caches, database exports, and other secrets or local artifacts out of a public submission.

## Installation and configuration

From the repository root in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Set the required `MONGO_URI` and `MONGO_DB` values in the process environment or a local `.env` file. Environment
variables take precedence over `.env` (via `load_dotenv(..., override=False)`). Do not commit `.env`; `.env.example` is
provided as a template with placeholder values. Use a MongoDB account that can connect to the server and create indexes
in the configured database.

### Setting up MongoDB Atlas from scratch

Follow this workflow to configure a cloud MongoDB Atlas database for the service:

1. **Sign in or register:** Open [MongoDB Atlas](https://www.mongodb.com/cloud/atlas) and sign in or create an account.
2. **Project selection:** Create a new project or select an existing project from the Atlas console.
3. **Cluster deployment:** Click **Create** to deploy a database cluster. The free-tier shared option (e.g., M0) is fully suitable.
4. **Cloud provider and region:** Choose a cloud provider (such as AWS, GCP, or Azure) and select a region closest to your host to minimize network latency.
5. **Database user credentials:** Open **Database Access** under Security in the left navigation panel. Click **Add New Database User**, choose **Password** authentication, enter an appropriate username (`<username>`), and generate or supply a secure password (`<password>`). Grant the user read and write privileges (`Read and write to any database` or custom read/write privileges including index management on the target database).
6. **Network access configuration:** Open **Network Access** under Security. Click **Add IP Address** and select **Add Current IP Address** to whitelist your development machine's current public IP address. *Security note:* Specifying `0.0.0.0/0` permits connections from any IPv4 address globally and exposes the database to arbitrary external traffic; it should not be the default security recommendation.
7. **Connection menu:** Return to the **Database Deployments** overview and click **Connect** for your cluster.
8. **Select driver:** Choose **Drivers** under application connection methods. Select **Python** as the driver. The application relies on asynchronous PyMongo (`pymongo.AsyncMongoClient` from `pymongo[srv]>=4.13`) and uses standard MongoDB SRV connection strings. If the Atlas interface lists the driver simply as Python, select the latest version compatible with your Python runtime; the connection string format (`mongodb+srv://...`) is natively supported.
9. **Copy connection string:** Copy the connection string template provided by Atlas, which has the format:
   ```text
   mongodb+srv://<username>:<password>@<cluster-address>/?retryWrites=true&w=majority
   ```
10. **URL-encoding and database name:** Replace `<username>` and `<password>` placeholders with your actual database user credentials. If your password contains special characters (e.g. `@`, `:`, `/`, `?`, `#`, `[`), URL-encode them (e.g., using Python's `urllib.parse.quote_plus`) so they do not disrupt URI parsing. Set the database name in `MONGO_DB`.
11. **Configure environment:** Set `MONGO_URI` and `MONGO_DB` in your process environment or in a local `.env` file at the repository root:
    ```bash
    MONGO_URI="mongodb+srv://<username>:<password>@<cluster-address>/?retryWrites=true&w=majority"
    MONGO_DB="attendance_db"
    ```
12. **Environment variable precedence:** Application configuration in `app/main.py` calls `load_dotenv(override=False)`. Real environment variables defined in the operating system or shell session strictly override any values defined in `.env`.
13. **Start the application:** Launch the API service using Uvicorn:
    ```powershell
    uvicorn app.main:app --port 8000
    ```
14. **Verify health endpoint:** Check service readiness by requesting `GET /health`:
    ```powershell
    Invoke-RestMethod http://127.0.0.1:8000/health
    ```
    The endpoint returns HTTP **200** with `{"status":"ok"}` when MongoDB is reachable and answers a ping. If MongoDB is unreachable, ping fails, or the client is not initialized, it returns HTTP **503** (`{"detail":"MongoDB is unavailable"}`).
15. **Automatic index creation:** At startup, FastAPI's lifespan context automatically verifies connectivity and creates all nine required indexes idempotently across `employees` and `attendance_logs`. No manual index creation in MongoDB Atlas is required.

## Start and check readiness

From the repository root:

```powershell
uvicorn app.main:app --port 8000
```

FastAPI startup pings MongoDB, then verifies and creates the required indexes. Startup fails if configuration, database
connectivity, or required index initialization fails; a required index with a conflicting definition or a hidden index
is not silently accepted. The Mongo client is closed when the app shuts down.

In another PowerShell window:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

Expected ready response:

```json
{"status":"ok"}
```

`GET /health` returns **200** only when MongoDB answers a ping; it returns **503** when no client is initialized or the
ping fails. Interactive API documentation is available at <http://127.0.0.1:8000/docs>.

## API routes

All errors follow FastAPI's `{"detail": ...}` body. Request validation failures use **422**. MongoDB `_id` is internal
and is not part of any API response.

| Method and path | Parameters / request | Result |
|---|---|---|
| `GET /health` | None | Readiness status; 200 or 503 |
| `POST /employees` | Employee JSON; `emp_code`, `name`, `email`, `department`, `joined_on` required; shifts default to `09:30`–`18:30` | 201 created, 409 duplicate code, 422 invalid body |
| `GET /employees` | Optional exact-match `department`; `page` (default 1); `page_size` (default 20, maximum 100) | Employees sorted by `emp_code` ascending; `total` reflects the filter |
| `POST /attendance/punch-in` | `emp_code` required; optional `punched_at` epoch ms and `status` (`PRESENT`, `WFH`, or `ON_DUTY`, default `PRESENT`) | 201 created, 404 unknown employee, 409 already punched in, 422 invalid body |
| `POST /attendance/punch-out` | `emp_code` required; optional `punched_at` epoch ms | 200 updated, 404 unknown employee/no eligible punch-in, 409 already punched out, 422 invalid elapsed time |
| `GET /attendance` | Optional `emp_code`, inclusive `date_from`/`date_to`, `status`, `page`, `page_size` | Filtered records sorted by date descending then employee code ascending |
| `PATCH /attendance/{emp_code}/{date}` | Optional `status`, `punch_in`, `punch_out`; required `reason` and `regularized_by` | 200 corrected record, 404 missing employee/record, 409 concurrent edit, 422 invalid/no-op correction |
| `GET /analytics/employees/{emp_code}/monthly` | Required `month=YYYY-MM` | Employee monthly attendance metrics; 404 unknown employee, 422 invalid month |
| `GET /analytics/departments/summary` | Required `month`; optional exact-match `department` | Monthly metrics grouped by department |
| `GET /analytics/leaderboard/late` | Required `month`; optional `limit` (default 10, range 1–50) and `department` | Monthly late leaderboard, ranked after aggregation |
| `GET /analytics/departments/{department}/trend` | Required `from` and `to` dates; range at most 92 days | One trend row per calendar day; 404 unknown department, 422 invalid range |
| `GET /admin/explain/{endpoint}` | Endpoint is one of `attendance_list`, `employee_monthly`, `department_summary`, `late_leaderboard`, `department_trend`; pass that query's parameters | MongoDB explain document at `executionStats` verbosity |

Dates use `YYYY-MM`, `YYYY-MM-DD`, and shift times use 24-hour `HH:MM`. Instants in request and response bodies are
integer Unix epoch **milliseconds**, not seconds. Values that look like seconds, strings, floats, and out-of-range values
are rejected. Employee create example:

```json
{
  "emp_code": "EMP0001",
  "name": "Asha Rao",
  "email": "asha@example.com",
  "department": "Engineering",
  "shift_start": "09:30",
  "shift_end": "18:30",
  "joined_on": "2026-01-05"
}
```

Employee response includes the server-set `created_at` as an integer epoch-millisecond value. MongoDB stores instants as
BSON dates in UTC; the API does not apply an IST offset when converting to epoch milliseconds.

Illustrative employee response shape (timestamp value follows the sample document in `DATA_MODEL.md`):

```json
{
  "emp_code": "EMP0001",
  "name": "Asha Rao",
  "email": "asha@example.com",
  "department": "Engineering",
  "shift_start": "09:30",
  "shift_end": "18:30",
  "joined_on": "2026-01-05",
  "created_at": 1767587400000
}
```

Paged employee and attendance results use the same envelope:

```json
{"items": [], "total": 0, "page": 1, "page_size": 20}
```

Punch-in example:

```json
{"emp_code":"EMP0001","punched_at":1783312500000,"status":"PRESENT"}
```

Punch-out example:

```json
{"emp_code":"EMP0001","punched_at":1783344600000}
```

Attendance responses include `punch_in`/`punch_out` as epoch milliseconds, `punch_in_ist`/`punch_out_ist` as
human-readable IST values, computed fields, and `history`. The exact response schemas and required fields are in
[`openapi.yaml`](./openapi.yaml).

Illustrative attendance response based on the sample normal-day document:

```json
{
  "emp_code": "EMP0001",
  "date": "2026-07-06",
  "status": "PRESENT",
  "punch_in": 1783310280000,
  "punch_in_ist": "2026-07-06T09:28:00+05:30",
  "punch_out": 1783343100000,
  "punch_out_ist": "2026-07-06T18:35:00+05:30",
  "work_hours": 9.12,
  "late_minutes": 0,
  "overtime_minutes": 0,
  "half_day": false,
  "history": []
}
```

Regularization example:

```json
{
  "status": "WFH",
  "reason": "Corrected attendance status",
  "regularized_by": "hr.admin"
}
```

Omitted fields remain unchanged. Each successful correction appends one history item containing only fields that
actually changed. A correction that changes nothing is rejected.

Analytics response shapes follow their endpoint schemas. For example, a monthly result includes the employee and month,
weekday totals, stored late/overtime totals, and a nullable attendance percentage:

```json
{
  "emp_code": "EMP0001",
  "month": "2026-07",
  "working_days": 18,
  "present_days": 1.5,
  "leave_days": 0,
  "late_count": 2,
  "total_late_minutes": 50,
  "total_overtime_minutes": 30,
  "attendance_pct": 8.3333
}
```

Department summary items contain `department`, `headcount`, `present_days`, nullable `avg_work_hours`, `late_count`,
`total_late_minutes`, `leave_count`, and `on_duty_count`. Leaderboard items contain `rank`, `emp_code`, `name`,
`department`, `total_late_minutes`, and `late_count`. Trend items contain `date`, `is_working_day`, `headcount`,
`present_count`, `late_count`, nullable `attendance_rate`, and nullable `moving_avg_7d`. The explain response is shaped
as `{"endpoint": "...", "collection": "...", "explain": {}}`. These are schema examples, not claims about a fixed
response for every dataset.

## Attendance rules and analytics

- Shift times and attendance dates use IST (UTC+05:30). A record date is based on `punch_in`; an overnight shift ends the
  next day, and an early-morning punch before its `shift_end` belongs to the previous shift-start date.
- Instants are truncated to whole seconds before calculations and storage. Late arrivals use a strict 10-minute grace
  period; late minutes are floored from shift start. Overtime is floored and counted only from 30 minutes after shift end.
- Work hours are elapsed seconds divided by 3600 and rounded to two decimals, half-up. `half_day` is true when rounded
  work hours are below 4.50. Work hours remain null before punch-out.
- `PRESENT`, `WFH`, and `ON_DUTY` are presence statuses. Weekdays are Monday–Friday with no holiday calendar.
  Mid-month joiners' working days start at `joined_on`; weekday presence is counted, but weekend records still contribute
  to late/overtime totals.
- Analytics read stored computed attendance fields. Missing weekdays are not implicitly marked absent. Missing/null
  legacy `history` is returned as an empty array; missing/null `half_day` is treated as false.
- Department headcount includes employees who joined by month-end even if they have no attendance logs. Trend headcount
  is computed for each day using employees who joined by that date.
- Reported numeric values use two-decimal half-up rounding; rates use four decimals half-up. Pagination starts at page 1,
  defaults to 20 records, is capped at 100, and reports totals after filtering.
- The late leaderboard uses standard competition ranking. `limit` is a rank cutoff, not a strict row count, so ties at
  the cutoff are all included. Rows are ordered by total late minutes descending, then employee code ascending.
- Trend output includes every requested calendar date, including weekends and no-log dates; the seven-day average uses
  non-null rates only and is limited to the requested range.

## MongoDB indexes and concurrency

Startup creates or validates these nine named indexes, without dropping or replacing existing indexes:

| Collection | Index name | Key sequence | Constraint / query purpose |
|---|---|---|---|
| `employees` | `employee_code_unique` | `emp_code ASC` | Unique employee identity and lookup |
| `employees` | `department_employee_code` | `department ASC`, `emp_code ASC` | Department-filtered employee listing/order |
| `employees` | `department_joined` | `department ASC`, `joined_on ASC` | Department and join-date filtering |
| `employees` | `employee_joined` | `joined_on ASC` | Eligible employee date filtering |
| `attendance_logs` | `employee_date_unique` | `emp_code ASC`, `date ASC` | Unique daily attendance record and natural-key lookup |
| `attendance_logs` | `employee_punch_in` | `emp_code ASC`, `punch_in DESC` | Employee's most recent eligible punch-in |
| `attendance_logs` | `attendance_date_employee` | `date DESC`, `emp_code ASC` | Attendance date ordering/listing |
| `attendance_logs` | `attendance_date_late` | `date ASC`, `late_minutes ASC` | Date-range and late-minute analytics |
| `attendance_logs` | `attendance_status_date` | `status ASC`, `date DESC`, `emp_code ASC` | Status-filtered attendance date queries |

The unique attendance index is the final protection against duplicate daily punch-ins. Punch-out uses a conditional update
that matches only an open record, so concurrent requests cannot both close it. Regularization compares the loaded record
values (and history state) during its update; the losing concurrent correction receives 409 rather than silently
overwriting the winner's history.

No additional partial index is installed. A live explain of the tested department-summary aggregation reported
`employee_date_unique` in the `$lookup` stage's `indexesUsed`; this is evidence for that observed query plan only, not a
benchmark or a guarantee for other datasets.

## Tests and verification

Install the dependencies above. The test categories are:

| Command | Coverage | Database |
|---|---|---|
| `python -m unittest tests.test_main -v` | Calculations, validation, mocked HTTP behavior, startup/index behavior, and in-memory race handling | Mocked |
| `python -m unittest discover -s tests -p test_mongo_concurrency.py -v` | Persisted indexes, analytics, explain plans, and simultaneous API requests | Real MongoDB |
| `python test_concurrent.py` | Standalone employee-create concurrency scenario | Real MongoDB |
| `python -m unittest discover -s tests -v` | Complete suite | Mocked unit tests plus real MongoDB integration tests |

The live integration tests require `MONGO_URI`/`MONGO_DB` or `MONGO_TEST_URI`/`MONGO_TEST_DB`. They run against
`MONGO_TEST_DB` or, by default, `<MONGO_DB>_test`, and must not use the application's database. They create unique test
employees, send concurrent requests using `asyncio` barriers, and remove only documents created by each test. If the
MongoDB server cannot be reached, live test setup fails; that is a blocked infrastructure check, not a passing mock test.
The unit suite can run without MongoDB.

To verify plans manually while the application is running, use `/admin/explain/{endpoint}` with the same filters as the
target query. The response includes MongoDB execution statistics. `IXSCAN` means an index scan appears in the plan;
`COLLSCAN` means a collection scan appears. Explain output reflects the queried data and MongoDB planner; it is not a
performance benchmark.

### Latest recorded verification

Command: `python -m unittest discover -s tests -v` (Python 3.14.5). **47 tests passed**: 41 mock/unit tests and 6
real-MongoDB integration tests. The standalone `python test_concurrent.py` runner also passed its one live test. The live
scenarios observed:

- 8 concurrent employee creates: 1 returned 201, 7 returned 409, and the document count before cleanup was 1.
- 8 simultaneous punch-ins: 1 returned 201, 7 returned 409, and one attendance record remained.
- 8 simultaneous punch-outs: 1 returned 200 and 7 returned 409; the persisted punch-out matched the winning response.
- 2 simultaneous regularizations: 1 returned 200 and 1 returned 409; one history entry remained.
- Five explain patterns returned an `IXSCAN` and no `COLLSCAN` in the test output. The department-summary lookup
  specifically reported use of `employee_date_unique`.

These results are from the current test dataset; no 100,000-record benchmark was run. A fresh external virtual
environment installed `requirements.txt` successfully. A temporary source-only copy without `.env`, started with
environment-based MongoDB configuration on port 8001, initialized indexes automatically and returned HTTP 200 from
`/health`. With MongoDB deliberately unavailable, startup exited with `ServerSelectionTimeoutError`; the unit tests also
verify that `/health` returns HTTP 503 when its ping fails.

The service was started and verified directly on port 8000 with `uvicorn app.main:app --port 8000`. The lifespan
handler connected to MongoDB Atlas, verified and created all nine required indexes idempotently, and `GET /health`
returned HTTP 200 `{"status":"ok"}`.

## Public-repository preparation

Before submission, verify the required files and inspect all staged changes. Do not publish `.env`, credentials, test
database contents, database dumps, `.venv`, caches, or unrelated local artifacts. The repository must be public and its
URL submitted by the deadline using the method in the assignment email. Do not commit or push until you have reviewed the
final status and diff:

```powershell
git status --short
git diff --check
git diff
```

This workspace copy does not include Git metadata, so Git status, diff, commit, and push commands must be run after
placing the permitted submission files in a Git repository. No commit or push was performed as part of this work.

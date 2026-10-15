# Engineering Decisions

*Five concise architectural and implementation decisions for the Employee Attendance & Analytics API, backed by verified test evidence against MongoDB Atlas.*

---

## 1. Indexes

**Question 1:** Which indexes did you create, and which query or guarantee does each one serve? Which one did you consider and reject?

**Answer:**

I created **nine explicit, named indexes** across both collections during startup via `create_indexes()`:

1. **`employees.employee_code_unique` `[("emp_code", 1)]` (Unique):** Enforces customer-generated code uniqueness and powers $O(1)$ lookups in punch-in, punch-out, regularization, and `$lookup` foreign-key joins.
2. **`employees.department_employee_code` `[("department", 1), ("emp_code", 1)]`:** Supports `GET /employees` when filtered by department and sorted by `emp_code`.
3. **`employees.department_joined` `[("department", 1), ("joined_on", 1)]`:** Optimizes department summary headcount filtering (`joined_on <= month_end`) and trend headcount lookups.
4. **`employees.employee_joined` `[("joined_on", 1)]`:** Accelerates company-wide headcount calculations.
5. **`attendance_logs.employee_date_unique` `[("emp_code", 1), ("date", 1)]` (Unique):** Enforces the core business invariant of at most one record per employee per attendance date, prevents concurrent duplicate punch-ins, and powers `PATCH /attendance/{emp_code}/{date}` and the inner `$lookup` in `department_summary`.
6. **`attendance_logs.employee_punch_in` `[("emp_code", 1), ("punch_in", -1)]`:** Serves `POST /attendance/punch-out` by locating the latest unclosed punch-in (`punch_in <= punched_at`) in reverse chronological order.
7. **`attendance_logs.attendance_date_employee` `[("date", -1), ("emp_code", 1)]`:** Covers `GET /attendance` default compound sort (`date DESC, emp_code ASC`) and date-range queries.
8. **`attendance_logs.attendance_date_late` `[("date", 1), ("late_minutes", 1)]`:** Powers `GET /analytics/leaderboard/late` by filtering on date range and strictly `late_minutes > 0`.
9. **`attendance_logs.attendance_status_date` `[("status", 1), ("date", -1), ("emp_code", 1)]`:** Supports `GET /attendance` when filtered by status.

**Rejected Index:**
I considered a compound index on `attendance_logs(emp_code, date, punch_out)` for punch-out lookup. I **rejected** it because overnight shifts cross calendar dates, meaning the date of punch-in may differ from the punch-out calendar day. Indexing `(emp_code, punch_in DESC)` correctly resolves candidate records across midnight while avoiding redundant compound index maintenance on high-frequency writes. I also considered and rejected a TTL index, as attendance records must be kept indefinitely for audit compliance.

**Evidence:** Startup index creation was verified to be idempotent and non-destructive. On a 100,000-record dataset in MongoDB Atlas, explain plans confirmed `IXSCAN` and **zero** `COLLSCAN` across all endpoints (`attendance_list`, `employee_monthly`, `department_summary`, `late_leaderboard`, `department_trend`).

---

## 2. Punch-in Race

**Question 2:** Two identical punch-in requests arrive at the same instant. Walk through exactly what happens in your code.

**Answer:**

1. **Concurrent Lookup:** Both asynchronous request coroutines look up the employee in `employees`. Both find the employee and compute the same IST attendance date and late minutes.
2. **Database Insertion Race:** Both coroutines attempt `db.attendance_logs.insert_one(doc)` with identical `(emp_code, date)`.
3. **Database Constraint Resolution:** MongoDB’s primary wire engine evaluates the write locks. The first write succeeds and commits, returning `HTTP 201 Created`.
4. **Collision and Catch:** The second insert encounters the unique index constraint on `employee_date_unique` `(emp_code, date)`. PyMongo catches the resulting `DuplicateKeyError` and immediately maps it to **`HTTP 409 Conflict`** with detail `"already punched in for this date"`.
5. **Guarantee:** Because the database unique index acts as the final arbiter, check-then-insert race conditions are impossible, ensuring exactly one attendance document is created.

**Evidence:** Verified via an 8-request concurrent barrier test (`test_parallel_punch_ins_enforce_unique_employee_date` and `test_case_20_repeated_and_concurrent_writes`). Exactly 1 request returned HTTP 201 and 7 returned HTTP 409; exactly 1 document existed in MongoDB.

---

## 3. Leaderboard Ties

**Question 3:** Two employees are tied on late minutes at the cutoff of `limit`. What does your leaderboard return, and why?

**Answer:**

**Evidence & Logic:**
[`leaderboard_pipeline()`](file:///c:/Users/ASUS/OneDrive/Documents/HRone/app/main.py#L822) groups matching records with `late_minutes > 0` and applies MongoDB’s `$rank` operator inside `$setWindowFields`:
```python
{"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}}
```
It then applies `{"$match": {"rank": {"$lte": limit}}}` and sorts by `total_late_minutes DESC, emp_code ASC`.

- **Standard Competition Ranking:** Equal late totals receive the same rank, and subsequent ranks are skipped (e.g., 1, 2, 2, 4; rank 3 is skipped). `$denseRank` was avoided because it would incorrectly produce 1, 2, 2, 3.
- **Cutoff Behavior:** The `limit` parameter is contractually a **ranking cutoff**, not a slice limit on returned row count. If `limit=2` and two employees tie at rank 1, ranks are `[1, 1]`, and the next employee has rank 3; both rank 1 employees are returned (`rank <= 2`). If `limit=1` and two employees tie for first place, both are returned.
- **Determinism:** Rows with tied ranks and late minutes are sorted by `emp_code ASC` for deterministic presentation.

**Evidence:** Verified in `test_case_02_multiple_employees_tied_on_total_late_minutes`. With two employees tied at 40 late minutes, `limit=1` returned both tied candidates with rank 1; `limit=2` returned 2 employees (since next was rank 3); and `limit=3` returned 3 employees with ranks `[1, 1, 3]`.

---

## 4. Department Headcount

**Question 4:** How does your department summary make sure employees with zero logs are counted?

**Answer:**

**Evidence & Logic:**
[`department_summary_pipeline()`](file:///c:/Users/ASUS/OneDrive/Documents/HRone/app/main.py#L500) begins its aggregation on the **`employees`** collection rather than `attendance_logs`:
1. **Headcount Eligibility Match:** Filters employees with `joined_on <= last_day_of_month` (R9 rule: anyone who joined on or before month end is eligible, regardless of attendance activity).
2. **Left Outer Join:** Performs a `$lookup` into `attendance_logs` matching on `emp_code` and date range.
3. **Zero-Log Coalescing:** For employees with zero logs, `$lookup` produces an empty array. The pipeline applies `$ifNull: [{"$arrayElemAt": ["$stats", 0]}, {}]` so that missing attendance values default to 0.
4. **Aggregation:** The pipeline groups by department, computing `headcount = {"$sum": 1}`. Thus, every eligible employee increments headcount by 1, while contributing 0 to present days, late minutes, and leaves.
5. **Record-Level Average Work Hours:** The pipeline sums `work_hours` across present records and divides by total present records with recorded hours (`hours_sum / hours_count`), ensuring an authoritative record-level mean rather than an average of per-employee averages.

**Evidence:** Verified in `test_case_01_eligible_employee_with_no_attendance_logs`. An employee with zero attendance logs produced `headcount: 1`, `present_days: 0`, and `avg_work_hours: null` in department summary.

---

## 5. Scaling

**Question 5:** If this had to serve 100x the data, what would you change?

**Answer:**

To scale from 100,000 records to 10,000,000+ attendance records:

1. **Keyset (Cursor-Based) Pagination:** Replace `skip((page - 1) * page_size)` in `GET /attendance` with seek pagination using `{"date": {"$lt": last_date}, "emp_code": {"$gt": last_code}}`. MongoDB `skip()` incurs an $O(N)$ index traversal penalty that degrades at high offset values.
2. **Pre-Aggregated Summary Collections (CQRS):** Replace the multi-collection `$lookup` in `department_summary` and window computations in `department_trend` with an incremental daily rollup collection maintained via MongoDB Change Streams or write-time hooks.
3. **Read Replicas & Connection Pooling:** Route read-heavy analytics pipelines to secondary replica nodes (`ReadPreference.SECONDARY_PREFERRED`), preserving primary IOPS for high-concurrency punch-ins and punch-outs.
4. **Sharding Architecture:** Implement range sharding on `(department, emp_code, date)` to distribute storage and aggregation workloads horizontally across cluster shards.
5. **Selective Partial Indexes:** Replace generic compound indexes with partial indexes (e.g. `late_minutes > 0` for leaderboard queries, and `punch_out: null` for active shifts) to drastically reduce index working-set memory.

**Evidence:** In the 100,000-record benchmark, `attendance_list` executed in 36.7ms and `late_leaderboard` in 93.3ms using dedicated indexes, while `department_trend` required 1572ms due to cross-collection lookups and windowing, confirming that pre-aggregation is the primary optimization needed at 100x volume.
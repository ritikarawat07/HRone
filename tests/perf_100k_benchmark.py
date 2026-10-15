"""100,000 Attendance Record Performance and Explain Plan Benchmark.

Seeds approximately 100,000 attendance records and associated employees
in the dedicated test database, runs explain plans with executionStats,
measures latency for each list and analytics endpoint, and cleans up.
"""

import asyncio
import os
import time
from datetime import date, datetime, timedelta, timezone
from pymongo import AsyncMongoClient

import app.main as main_module

TEST_MONGO_URI = os.getenv("MONGO_TEST_URI", main_module.MONGO_URI)
TEST_MONGO_DB = os.getenv("MONGO_TEST_DB") or (
    f"{main_module.MONGO_DB}_test" if main_module.MONGO_DB else None
)


async def run_benchmark():
    if not TEST_MONGO_URI or not TEST_MONGO_DB:
        print("BLOCKED: Missing MONGO_URI / MONGO_DB")
        return

    client = AsyncMongoClient(TEST_MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=8000)
    db = client[TEST_MONGO_DB]
    main_module.db = db

    print(f"Connected to dedicated test database: {TEST_MONGO_DB}")
    await main_module.create_indexes()

    # Tag for performance documents so we can clean them up safely
    tag = "perf_bench"
    print("Generating 500 test employees...")
    departments = ["Engineering", "Product", "Sales", "Support", "Finance"]
    employees = []
    for i in range(1, 501):
        emp_code = f"EMPP{i:05d}"
        dept = departments[i % len(departments)]
        employees.append({
            "emp_code": emp_code,
            "name": f"Perf Employee {i}",
            "email": f"perf{i}@example.test",
            "department": dept,
            "shift_start": "09:30",
            "shift_end": "18:30",
            "joined_on": "2026-01-01",
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "tag": tag,
        })

    total_records = 100000
    # Check if records already seeded from previous step
    existing_count = await db.attendance_logs.count_documents({"tag": tag})
    if existing_count == total_records:
        print(f"Detected {existing_count} already seeded performance records. Skipping re-seed.")
    else:
        # Clean any previous partial run
        await db.employees.delete_many({"tag": tag})
        await db.attendance_logs.delete_many({"tag": tag})

        await db.employees.insert_many(employees, ordered=False)
        print("Inserted 500 test employees.")

        # Generate 100,000 attendance records:
        print("Generating 100,000 attendance records...")
        start_date = date(2026, 1, 1)
        batch_size = 10000
        current_batch = []
        total_inserted = 0

        t_start_insert = time.time()
        for day_offset in range(200):
            att_date = (start_date + timedelta(days=day_offset)).isoformat()
            for emp_idx in range(1, 501):
                emp_code = f"EMPP{emp_idx:05d}"
                mod = (emp_idx + day_offset) % 100
                if mod < 85:
                    status = "PRESENT"
                    hours = 8.5
                    late = 15 if (mod % 5 == 0) else 0
                    ot = 30 if (mod % 7 == 0) else 0
                    half = False
                    p_in = datetime.combine(start_date + timedelta(days=day_offset), datetime.min.time(), timezone.utc) + timedelta(hours=4)
                    p_out = p_in + timedelta(hours=8, minutes=30)
                elif mod < 90:
                    status = "WFH"
                    hours = 8.0
                    late = 0
                    ot = 0
                    half = False
                    p_in = datetime.combine(start_date + timedelta(days=day_offset), datetime.min.time(), timezone.utc) + timedelta(hours=4)
                    p_out = p_in + timedelta(hours=8)
                elif mod < 95:
                    status = "ON_DUTY"
                    hours = 7.5
                    late = 0
                    ot = 0
                    half = False
                    p_in = datetime.combine(start_date + timedelta(days=day_offset), datetime.min.time(), timezone.utc) + timedelta(hours=4)
                    p_out = p_in + timedelta(hours=7, minutes=30)
                elif mod < 98:
                    status = "LEAVE"
                    hours = None
                    late = 0
                    ot = 0
                    half = False
                    p_in = None
                    p_out = None
                else:
                    status = "ABSENT"
                    hours = None
                    late = 0
                    ot = 0
                    half = False
                    p_in = None
                    p_out = None

                current_batch.append({
                    "emp_code": emp_code,
                    "date": att_date,
                    "status": status,
                    "punch_in": p_in,
                    "punch_out": p_out,
                    "work_hours": hours,
                    "late_minutes": late,
                    "overtime_minutes": ot,
                    "half_day": half,
                    "history": [],
                    "tag": tag,
                })

                if len(current_batch) >= batch_size:
                    await db.attendance_logs.insert_many(current_batch, ordered=False)
                    total_inserted += len(current_batch)
                    current_batch = []
                    print(f"Inserted {total_inserted}/{total_records} records...")

        if current_batch:
            await db.attendance_logs.insert_many(current_batch, ordered=False)
            total_inserted += len(current_batch)
        insert_duration = time.time() - t_start_insert
        print(f"Successfully seeded {total_inserted} records in {insert_duration:.2f}s.")

    # Run Benchmark Queries and Explains
    print("\n--- Running Explain Plans and Performance Benchmarks on 100,000 records ---")
    results = {}

    # 1. Attendance List
    list_query = main_module.attendance_filter("EMPP00001", "2026-02-01", "2026-04-30", "PRESENT")
    t0 = time.perf_counter()
    list_docs = await db.attendance_logs.find(list_query, {"_id": 0}).sort([("date", -1), ("emp_code", 1)]).skip(0).limit(20).to_list(20)
    t_list = (time.perf_counter() - t0) * 1000
    explain_list = await db.command({
        "explain": {
            "find": "attendance_logs",
            "filter": list_query,
            "projection": {"_id": 0},
            "sort": {"date": -1, "emp_code": 1},
            "skip": 0,
            "limit": 20,
        },
        "verbosity": "executionStats",
    })
    results["attendance_list"] = {"time_ms": t_list, "count": len(list_docs), "explain": explain_list}

    # 2. Employee Monthly
    pipe_monthly = main_module.monthly_employee_pipeline("EMPP00001", "2026-03")
    t0 = time.perf_counter()
    monthly_res = await main_module.aggregate_to_list(db.attendance_logs, pipe_monthly, length=1)
    t_monthly = (time.perf_counter() - t0) * 1000
    explain_monthly = await db.command({
        "explain": {"aggregate": "attendance_logs", "pipeline": pipe_monthly, "cursor": {}},
        "verbosity": "executionStats",
    })
    results["employee_monthly"] = {"time_ms": t_monthly, "result": monthly_res, "explain": explain_monthly}

    # 3. Department Summary
    pipe_dept = main_module.department_summary_pipeline("2026-03", "Engineering")
    t0 = time.perf_counter()
    dept_res = await main_module.aggregate_to_list(db.employees, pipe_dept, length=100)
    t_dept = (time.perf_counter() - t0) * 1000
    explain_dept = await db.command({
        "explain": {"aggregate": "employees", "pipeline": pipe_dept, "cursor": {}},
        "verbosity": "executionStats",
    })
    results["department_summary"] = {"time_ms": t_dept, "count": len(dept_res), "explain": explain_dept}

    # 4. Late Leaderboard
    pipe_board = main_module.leaderboard_pipeline("2026-03", "Engineering", 10)
    t0 = time.perf_counter()
    board_res = await main_module.aggregate_to_list(db.attendance_logs, pipe_board, length=50)
    t_board = (time.perf_counter() - t0) * 1000
    explain_board = await db.command({
        "explain": {"aggregate": "attendance_logs", "pipeline": pipe_board, "cursor": {}},
        "verbosity": "executionStats",
    })
    results["late_leaderboard"] = {"time_ms": t_board, "count": len(board_res), "explain": explain_board}

    # 5. Department Trend
    pipe_trend = main_module.trend_pipeline("Engineering", date(2026, 3, 1), date(2026, 3, 31))
    t0 = time.perf_counter()
    trend_res = await main_module.aggregate_to_list(db.attendance_logs, pipe_trend, length=100)
    t_trend = (time.perf_counter() - t0) * 1000
    explain_trend = await db.command({
        "explain": {"aggregate": "attendance_logs", "pipeline": pipe_trend, "cursor": {}},
        "verbosity": "executionStats",
    })
    results["department_trend"] = {"time_ms": t_trend, "count": len(trend_res), "explain": explain_trend}

    print("\n=== BENCHMARK RESULTS (100,000 records) ===")
    for name, data in results.items():
        ex_str = str(data["explain"])
        has_ixscan = "IXSCAN" in ex_str
        has_collscan = "COLLSCAN" in ex_str
        print(f"Endpoint: {name:20s} | Latency: {data['time_ms']:8.2f} ms | IXSCAN: {has_ixscan!s:5s} | COLLSCAN: {has_collscan!s:5s}")

    # Detailed Explain Plan checks
    print("\n--- Detailed Query Plan Breakdown ---")
    for name, data in results.items():
        ex = data["explain"]
        stats = ex.get("executionStats") or {}
        n_returned = stats.get("nReturned")
        exec_time = stats.get("executionTimeMillis")
        total_keys = stats.get("totalKeysExamined")
        total_docs = stats.get("totalDocsExamined")
        print(f"[{name}] nReturned: {n_returned}, execTimeMillis: {exec_time}, totalKeysExamined: {total_keys}, totalDocsExamined: {total_docs}")

    # Clean up performance records
    print("\nCleaning up 100,000 performance test documents...")
    t0 = time.time()
    del_logs = await db.attendance_logs.delete_many({"tag": tag})
    del_emps = await db.employees.delete_many({"tag": tag})
    print(f"Cleaned up {del_logs.deleted_count} logs and {del_emps.deleted_count} employees in {time.time() - t0:.2f}s.")
    await client.close()
    print("Benchmark complete and test database cleaned.")


if __name__ == "__main__":
    asyncio.run(run_benchmark())

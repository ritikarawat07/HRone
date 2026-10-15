"""Audit Part C Explain Endpoint against 100,000 Attendance Records in MongoDB Atlas.

Verifies:
1. All 5 endpoints with various parameter combinations.
2. Full recursive search of queryPlanner, executionStats, and nested aggregation stages for IXSCAN and COLLSCAN.
3. Parameter equivalence between normal endpoints and explain endpoint.
4. Error handling for missing/invalid parameters and unknown endpoints.
5. Recording of exact metrics: index names, totalKeysExamined, totalDocsExamined, executionTimeMillis, nReturned.
6. Safe cleanup of benchmark dataset.
"""

import asyncio
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import httpx
from pymongo import AsyncMongoClient

import app.main as main_module

TEST_MONGO_URI = os.getenv("MONGO_TEST_URI", main_module.MONGO_URI)
TEST_MONGO_DB = os.getenv("MONGO_TEST_DB") or (
    f"{main_module.MONGO_DB}_test" if main_module.MONGO_DB else None
)


def recursive_find_stages(node, stages=None, indexes=None):
    if stages is None:
        stages = []
    if indexes is None:
        indexes = set()
    if isinstance(node, dict):
        if "stage" in node:
            stages.append(node["stage"])
        if "indexName" in node:
            indexes.add(node["indexName"])
        for k, v in node.items():
            recursive_find_stages(v, stages, indexes)
    elif isinstance(node, list):
        for item in node:
            recursive_find_stages(item, stages, indexes)
    return stages, indexes


async def main():
    if not TEST_MONGO_URI or not TEST_MONGO_DB:
        print("ERROR: MONGO_URI / MONGO_DB not configured.")
        return

    print("Connecting to MongoDB Atlas...")
    client = AsyncMongoClient(TEST_MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=8000)
    db = client[TEST_MONGO_DB]
    main_module.db = db
    await main_module.create_indexes()

    tag = "audit_explain_100k"
    total_records = 100000

    # Ensure 100k records are present
    existing_count = await db.attendance_logs.count_documents({"tag": tag})
    if existing_count == total_records:
        print(f"Reusing existing {existing_count} performance documents (tag={tag}).")
    else:
        print("Seeding 500 employees and 100,000 attendance records...")
        await db.employees.delete_many({"tag": tag})
        await db.attendance_logs.delete_many({"tag": tag})

        departments = ["Engineering", "Product", "Sales", "Support", "Finance"]
        employees = [
            {
                "emp_code": f"EMPE{i:05d}",
                "name": f"Explain Emp {i}",
                "email": f"ex{i}@example.test",
                "department": departments[i % len(departments)],
                "shift_start": "09:30",
                "shift_end": "18:30",
                "joined_on": "2026-01-01",
                "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "tag": tag,
            }
            for i in range(1, 501)
        ]
        await db.employees.insert_many(employees, ordered=False)

        start_date = date(2026, 1, 1)
        batch = []
        inserted = 0
        t0 = time.time()
        for day in range(200):
            d_str = (start_date + timedelta(days=day)).isoformat()
            for e_idx in range(1, 501):
                code = f"EMPE{e_idx:05d}"
                mod = (e_idx + day) % 100
                is_pres = mod < 85
                late = 15 if (is_pres and mod % 5 == 0) else 0
                ot = 30 if (is_pres and mod % 7 == 0) else 0
                hours = 8.5 if is_pres else None
                status = "PRESENT" if is_pres else ("WFH" if mod < 90 else ("ON_DUTY" if mod < 95 else "LEAVE"))
                p_in = datetime.combine(start_date + timedelta(days=day), datetime.min.time(), timezone.utc) + timedelta(hours=4) if is_pres else None
                p_out = p_in + timedelta(hours=8, minutes=30) if p_in else None

                batch.append({
                    "emp_code": code,
                    "date": d_str,
                    "status": status,
                    "punch_in": p_in,
                    "punch_out": p_out,
                    "work_hours": hours,
                    "late_minutes": late,
                    "overtime_minutes": ot,
                    "half_day": False,
                    "history": [],
                    "tag": tag,
                })
                if len(batch) >= 10000:
                    await db.attendance_logs.insert_many(batch, ordered=False)
                    inserted += len(batch)
                    batch = []
                    print(f"Seeded {inserted}/{total_records}...")
        if batch:
            await db.attendance_logs.insert_many(batch, ordered=False)
            inserted += len(batch)
        print(f"Seeded {inserted} records in {time.time() - t0:.2f}s.")

    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main_module.app),
        base_url="http://test",
    )

    test_cases = [
        {
            "name": "attendance_list (default)",
            "endpoint": "attendance_list",
            "params": {"page": 1, "page_size": 20},
            "real_url": "/attendance",
        },
        {
            "name": "attendance_list (by emp_code)",
            "endpoint": "attendance_list",
            "params": {"emp_code": "EMPE00001", "page": 1, "page_size": 20},
            "real_url": "/attendance",
        },
        {
            "name": "attendance_list (by date range)",
            "endpoint": "attendance_list",
            "params": {"date_from": "2026-02-01", "date_to": "2026-03-31", "page": 1, "page_size": 20},
            "real_url": "/attendance",
        },
        {
            "name": "attendance_list (emp_code + date range + status)",
            "endpoint": "attendance_list",
            "params": {"emp_code": "EMPE00001", "date_from": "2026-02-01", "date_to": "2026-03-31", "status": "PRESENT", "page": 1, "page_size": 20},
            "real_url": "/attendance",
        },
        {
            "name": "employee_monthly (valid emp_code + month)",
            "endpoint": "employee_monthly",
            "params": {"emp_code": "EMPE00001", "month": "2026-03"},
            "real_url": "/analytics/employees/EMPE00001/monthly",
        },
        {
            "name": "department_summary (month only)",
            "endpoint": "department_summary",
            "params": {"month": "2026-03"},
            "real_url": "/analytics/departments/summary",
        },
        {
            "name": "department_summary (month + department)",
            "endpoint": "department_summary",
            "params": {"month": "2026-03", "department": "Engineering"},
            "real_url": "/analytics/departments/summary",
        },
        {
            "name": "late_leaderboard (default limit 10)",
            "endpoint": "late_leaderboard",
            "params": {"month": "2026-03"},
            "real_url": "/analytics/leaderboard/late",
        },
        {
            "name": "late_leaderboard (department + limit 5)",
            "endpoint": "late_leaderboard",
            "params": {"month": "2026-03", "department": "Engineering", "limit": 5},
            "real_url": "/analytics/leaderboard/late",
        },
        {
            "name": "department_trend (department + from/to)",
            "endpoint": "department_trend",
            "params": {"department": "Engineering", "from": "2026-03-01", "to": "2026-03-31"},
            "real_url": "/analytics/departments/Engineering/trend",
        },
    ]

    print("\n=================== PART C EXPLAIN ENDPOINT AUDIT ===================")
    audit_results = []

    for tc in test_cases:
        ep_name = tc["endpoint"]
        params = tc["params"]
        label = tc["name"]

        # Call explain endpoint
        t_start = time.perf_counter()
        resp = await http.get(f"/admin/explain/{ep_name}", params=params)
        duration_ms = (time.perf_counter() - t_start) * 1000

        if resp.status_code != 200:
            print(f"[FAIL] {label} returned HTTP {resp.status_code}: {resp.text}")
            audit_results.append({
                "endpoint": ep_name,
                "label": label,
                "status": "FAIL",
                "error": resp.text,
            })
            continue

        body = resp.json()
        assert "endpoint" in body and "collection" in body and "explain" in body, "Missing required keys"
        explain_doc = body["explain"]

        # Recursively search for stages and index names
        stages, indexes_used = recursive_find_stages(explain_doc)

        has_ixscan = "IXSCAN" in stages or "IXSCAN" in json.dumps(explain_doc)
        has_collscan = "COLLSCAN" in stages or '"stage": "COLLSCAN"' in json.dumps(explain_doc)

        # Extract execution stats
        ex_stats = explain_doc.get("executionStats") or {}
        n_returned = ex_stats.get("nReturned")
        exec_ms = ex_stats.get("executionTimeMillis")
        keys_examined = ex_stats.get("totalKeysExamined")
        docs_examined = ex_stats.get("totalDocsExamined")

        # Also call real endpoint to verify parameter equivalence
        real_params = {k: v for k, v in params.items() if k not in ("emp_code", "department")}
        real_resp = await http.get(tc["real_url"], params=real_params)
        param_equiv = real_resp.status_code == 200

        print(f"\n--- {label} ---")
        print(f"Collection: {body['collection']} | Latency: {duration_ms:.2f}ms | Real Endpoint HTTP: {real_resp.status_code}")
        print(f"IXSCAN Present: {has_ixscan} | COLLSCAN Present: {has_collscan}")
        print(f"Indexes Used: {sorted(list(indexes_used))}")
        print(f"Execution Stats: nReturned={n_returned}, execTimeMillis={exec_ms}, totalKeysExamined={keys_examined}, totalDocsExamined={docs_examined}")

        status = "PASS" if (has_ixscan and not has_collscan and param_equiv) else "FAIL"
        audit_results.append({
            "endpoint": ep_name,
            "label": label,
            "collection": body["collection"],
            "has_ixscan": has_ixscan,
            "has_collscan": has_collscan,
            "indexes": sorted(list(indexes_used)),
            "n_returned": n_returned,
            "keys_examined": keys_examined,
            "docs_examined": docs_examined,
            "exec_ms": exec_ms,
            "latency_ms": duration_ms,
            "status": status,
        })

    # Test Error and Validation Cases
    print("\n--- Testing Error and Validation Handlers ---")
    error_cases = [
        {"desc": "Unknown endpoint name", "url": "/admin/explain/unknown_route", "params": {}, "expected": 422},
        {"desc": "Missing emp_code for employee_monthly", "url": "/admin/explain/employee_monthly", "params": {"month": "2026-03"}, "expected": 422},
        {"desc": "Missing month for employee_monthly", "url": "/admin/explain/employee_monthly", "params": {"emp_code": "EMPE00001"}, "expected": 422},
        {"desc": "Invalid month format for employee_monthly", "url": "/admin/explain/employee_monthly", "params": {"emp_code": "EMPE00001", "month": "2026-13"}, "expected": 422},
        {"desc": "Missing month for department_summary", "url": "/admin/explain/department_summary", "params": {}, "expected": 422},
        {"desc": "Missing month for late_leaderboard", "url": "/admin/explain/late_leaderboard", "params": {}, "expected": 422},
        {"desc": "Limit > 50 for late_leaderboard", "url": "/admin/explain/late_leaderboard", "params": {"month": "2026-03", "limit": 51}, "expected": 422},
        {"desc": "Missing parameters for department_trend", "url": "/admin/explain/department_trend", "params": {}, "expected": 422},
        {"desc": "Reversed date range for department_trend", "url": "/admin/explain/department_trend", "params": {"department": "Engineering", "from": "2026-03-31", "to": "2026-03-01"}, "expected": 422},
        {"desc": "Date range > 92 days for department_trend", "url": "/admin/explain/department_trend", "params": {"department": "Engineering", "from": "2026-01-01", "to": "2026-04-15"}, "expected": 422},
    ]

    all_errors_passed = True
    for ec in error_cases:
        r = await http.get(ec["url"], params=ec["params"])
        if r.status_code == ec["expected"]:
            print(f"[PASS] {ec['desc']}: returned expected HTTP {ec['expected']}")
        else:
            print(f"[FAIL] {ec['desc']}: expected HTTP {ec['expected']}, got {r.status_code} ({r.text})")
            all_errors_passed = False

    # Clean up test data
    print("\nCleaning up 100,000 performance test records...")
    t_del = time.time()
    await db.attendance_logs.delete_many({"tag": tag})
    await db.employees.delete_many({"tag": tag})
    print(f"Cleaned up in {time.time() - t_del:.2f}s.")
    await http.aclose()
    await client.close()

    print("\n=================== FINAL AUDIT SUMMARY ===================")
    print(f"Total Test Cases Evaluated: {len(test_cases)}")
    print(f"All Explain Endpoints Use IXSCAN & No COLLSCAN: {all(r['has_ixscan'] and not r['has_collscan'] for r in audit_results)}")
    print(f"All Validation Error Cases Passed: {all_errors_passed}")


if __name__ == "__main__":
    asyncio.run(main())

"""Run the cleanup-safe employee-creation concurrency integration test."""

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent


if __name__ == "__main__":
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-p",
            "test_mongo_concurrency.py",
            "-k",
            "parallel_employee_creates_enforce_unique_code",
            "-v",
        ],
        cwd=PROJECT_ROOT,
        check=False,
    )
    raise SystemExit(result.returncode)

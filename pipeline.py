"""
Recon prototype orchestrator: generate mock data, load DuckDB, run dbt, export dashboard JSON.

Usage (from repository root):
    python pipeline.py
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from generate.constants import MERCHANTS
from generate.data_generator import run_generation
from generate.db_loader import DUCKDB_PATH, load_raw_tables

PROJECT_ROOT = Path(__file__).resolve().parent
DBT_DIR = PROJECT_ROOT / "recon_dbt"
RESULTS_PATH = PROJECT_ROOT / "data" / "results.json"


def _log(message: str) -> None:
    print(message, flush=True)


def _run_dbt(*args: str, check: bool = False) -> subprocess.CompletedProcess[str]:
    cmd = ["dbt", *args, "--profiles-dir", "."]
    _log(f"  $ {' '.join(cmd)}")
    proc = subprocess.run(
        cmd,
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    if proc.stderr:
        print(proc.stderr, end="" if proc.stderr.endswith("\n") else "\n", file=sys.stderr)
    if check and proc.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} failed (exit {proc.returncode})")
    return proc


def _expected_test_names() -> list[str]:
    tests_dir = DBT_DIR / "tests"
    if not tests_dir.is_dir():
        return []
    return sorted(p.stem for p in tests_dir.glob("assert_*.sql"))


def _parse_dbt_test_results(test_proc: subprocess.CompletedProcess[str]) -> list[dict]:
    """Build test_name / status rows from dbt run_results.json (fallback: stdout)."""
    results_path = DBT_DIR / "target" / "run_results.json"
    parsed: dict[str, str] = {}

    if results_path.exists():
        payload = json.loads(results_path.read_text(encoding="utf-8"))
        for row in payload.get("results", []):
            unique_id = row.get("unique_id", "")
            if not unique_id.startswith("test."):
                continue
            test_name = unique_id.split(".")[-1]
            raw_status = str(row.get("status", "")).lower()
            if raw_status in ("pass", "passed", "success"):
                parsed[test_name] = "passed"
            else:
                parsed[test_name] = "failed"

    if not parsed and test_proc.stdout:
        for line in test_proc.stdout.splitlines():
            stripped = line.strip()
            if "PASS" in stripped and "assert_" in stripped:
                for name in _expected_test_names():
                    if name in stripped:
                        parsed[name] = "passed"
            elif ("FAIL" in stripped or "ERROR" in stripped) and "assert_" in stripped:
                for name in _expected_test_names():
                    if name in stripped:
                        parsed[name] = "failed"

    out: list[dict] = []
    for name in _expected_test_names():
        out.append({"test_name": name, "status": parsed.get(name, "failed")})
    return out


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _read_mart_table(con: duckdb.DuckDBPyConnection, table_name: str) -> pd.DataFrame:
    for qualified in (table_name, f"main.{table_name}"):
        try:
            return con.execute(f"select * from {qualified}").df()
        except duckdb.CatalogException:
            continue
    raise RuntimeError(f"Mart table not found: {table_name}")


def _export_results(dbt_test_results: list[dict]) -> None:
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        data_issues = _read_mart_table(con, "fct_data_issues")
        nexus_exposure = _read_mart_table(con, "fct_nexus_exposure")
        txn_counts = con.execute("""
            select merchant_id, count(*) as total_transactions
            from int_unified_transactions
            where is_refund = false
            group by merchant_id
        """).df()
    finally:
        con.close()

    payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "merchants": [{"id": m["id"], "name": m["name"]} for m in MERCHANTS],
    "data_issues": _df_to_records(data_issues),
    "nexus_exposure": _df_to_records(nexus_exposure),
    "transaction_counts": _df_to_records(txn_counts),
    "dbt_test_results": dbt_test_results,
    }

    RESULTS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    _log(f"Wrote {RESULTS_PATH}")


def main() -> None:
    _log("Step 1/5: Generating mock Stripe and Shopify data...")
    run_generation()

    _log("Step 2/5: Loading generated JSON into DuckDB...")
    load_raw_tables()

    _log("Step 3/5: Installing dbt packages (dbt deps)...")
    _run_dbt("deps", check=True)

    _log("Step 4/5: Seeding reference data and running dbt models...")
    _run_dbt("seed", check=True)
    _run_dbt("run", check=True)

    _log("Step 5/5: Running dbt tests and exporting results.json...")
    test_proc = _run_dbt("test")
    dbt_test_results = _parse_dbt_test_results(test_proc)
    _export_results(dbt_test_results)

    passed = sum(1 for t in dbt_test_results if t["status"] == "passed")
    failed = len(dbt_test_results) - passed
    _log(f"dbt tests: {passed} passed, {failed} failed (expected with seeded dirty data).")
    _log("Done. Open dashboard.html in your browser after this run.")


if __name__ == "__main__":
    main()

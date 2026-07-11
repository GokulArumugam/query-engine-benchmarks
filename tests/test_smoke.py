"""Small end-to-end checks for the Q6 correctness gate and execution paths."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from bench.runner import benchmark_one


@pytest.fixture
def sf01_data(tmp_path: Path) -> Path:
    """A minimal shared-Parquet SF0.1-shaped fixture for the Q6 smoke path.

    The full benchmark generates real TPC-H with the DuckDB extension. Keeping
    this fixture tiny lets CI exercise the three adapters and correctness gate
    without downloading that extension during a unit-test run.
    """
    duckdb = pytest.importorskip("duckdb")
    root = tmp_path / "data"
    destination = root / "sf0.1"
    destination.mkdir(parents=True)
    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE TABLE lineitem AS
            SELECT DATE '1994-06-01' AS l_shipdate, 0.06::DOUBLE AS l_discount,
                   10.0::DOUBLE AS l_quantity, 100.0::DOUBLE AS l_extendedprice
            UNION ALL
            SELECT DATE '1995-01-01', 0.06, 10.0, 100.0
            """
        )
        con.execute(f"COPY lineitem TO '{destination / 'lineitem.parquet'}' (FORMAT PARQUET)")
        for table in ("customer", "nation", "orders", "part", "partsupp", "region", "supplier"):
            con.execute(f"CREATE TABLE {table} AS SELECT 1 AS placeholder WHERE FALSE")
            con.execute(f"COPY {table} TO '{destination / f'{table}.parquet'}' (FORMAT PARQUET)")
    finally:
        con.close()
    return root


@pytest.mark.parametrize("engine", ["duckdb", "polars"])
def test_q6_correctness_gate(engine: str, sf01_data: Path) -> None:
    pytest.importorskip(engine)
    result = benchmark_one(engine, 0.1, "q6", sf01_data, rows=1)
    assert result["status"] == "OK", result.get("error")


def test_q6_spark_correctness_gate(sf01_data: Path) -> None:
    if shutil.which("java") is None or subprocess.run(["java", "-version"], capture_output=True).returncode != 0:
        pytest.skip("Spark smoke test requires a working JVM; benchmark reports this gracefully")
    pytest.importorskip("pyspark")
    result = benchmark_one("spark", 0.1, "q6", sf01_data, rows=1)
    assert result["status"] == "OK", result.get("error")

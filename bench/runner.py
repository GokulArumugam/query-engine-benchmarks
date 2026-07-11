#!/usr/bin/env python3
"""Run the reproducible cold/warm TPC-H benchmark matrix.

The runner deliberately keeps data in Parquet and implementations in ``queries/``.
It never prepares an engine-native store: every adapter creates ephemeral views or
lazy scans directly over the same generated files.
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import platform
import subprocess
import sys
import time
import traceback
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from bench.datagen import TABLES, sf_label

ROOT = Path(__file__).resolve().parents[1]
QUERY_IDS = ("q1", "q3", "q5", "q6", "q9", "q18")
ENGINE_IDS = ("duckdb", "polars", "spark")
WORKER_PREFIX = "BENCH_WORKER_JSON="


class EngineUnavailable(RuntimeError):
    """An optional engine cannot be launched in the current environment."""


class DuckDBEngine:
    def __init__(self, data_dir: Path) -> None:
        import duckdb

        self.con = duckdb.connect()
        for table in TABLES:
            path = str(data_dir / f"{table}.parquet").replace("'", "''")
            self.con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")

    def run(self, query_id: str):
        sql = (ROOT / "queries" / "duckdb_sql" / f"{query_id}.sql").read_text()
        return self.con.execute(sql).to_arrow_table()

    def close(self) -> None:
        self.con.close()


class PolarsContext:
    def __init__(self, data_dir: Path) -> None:
        import polars as pl

        self.pl = pl
        self.data_dir = data_dir

    def scan(self, table: str):
        return self.pl.scan_parquet(self.data_dir / f"{table}.parquet")


class PolarsEngine:
    def __init__(self, data_dir: Path) -> None:
        # Import here, rather than at module import time, to preserve cold timing.
        import polars  # noqa: F401

        self.ctx = PolarsContext(data_dir)

    def run(self, query_id: str):
        module = importlib.import_module(f"queries.polars.{query_id}")
        return module.run(self.ctx).collect()

    def close(self) -> None:
        pass


class SparkEngine:
    def __init__(self, data_dir: Path) -> None:
        java = subprocess.run(["java", "-version"], capture_output=True, text=True)
        if java.returncode != 0:
            raise EngineUnavailable(
                "Spark requires a JVM, but `java -version` failed. Install/configure Java and retry; "
                "the benchmark does not install Java automatically."
            )
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise EngineUnavailable("pyspark is not installed; install requirements.txt and retry.") from exc

        cores = os.cpu_count() or 1
        self.spark = (
            SparkSession.builder.master("local[*]")
            .appName("query-engine-benchmarks")
            .config("spark.sql.shuffle.partitions", str(cores))
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        for table in TABLES:
            self.spark.read.parquet(str(data_dir / f"{table}.parquet")).createOrReplaceTempView(table)

    def run(self, query_id: str):
        sql = (ROOT / "queries" / "spark_sql" / f"{query_id}.sql").read_text()
        return self.spark.sql(sql).collect()

    def close(self) -> None:
        self.spark.stop()


def build_engine(engine_id: str, data_dir: Path):
    if engine_id == "duckdb":
        return DuckDBEngine(data_dir)
    if engine_id == "polars":
        return PolarsEngine(data_dir)
    if engine_id == "spark":
        return SparkEngine(data_dir)
    raise ValueError(f"unknown engine {engine_id!r}")


def _records(result: Any) -> tuple[list[str], list[dict[str, Any]]]:
    """Normalise the small final result sets from all adapters for checking."""
    if hasattr(result, "to_pylist") and hasattr(result, "column_names"):
        return list(result.column_names), result.to_pylist()
    if hasattr(result, "to_dicts") and hasattr(result, "columns"):
        return list(result.columns), result.to_dicts()
    if isinstance(result, list):  # Spark Rows
        if not result:
            return [], []
        rows = [row.asDict(recursive=True) for row in result]
        return list(rows[0]), rows
    raise TypeError(f"cannot fingerprint result type {type(result)!r}")


def _numeric(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def fingerprint(result: Any) -> dict[str, Any]:
    """Return the contract's row count plus sum of the first numeric column."""
    columns, rows = _records(result)
    numeric_column = next(
        (column for column in columns if any(_numeric(row.get(column)) for row in rows)), None
    )
    numeric_sum = 0.0
    if numeric_column is not None:
        numeric_sum = float(sum((row[numeric_column] or 0) for row in rows))
    return {"row_count": len(rows), "numeric_column": numeric_column, "numeric_sum": numeric_sum}


def fingerprints_match(reference: dict[str, Any], candidate: dict[str, Any]) -> tuple[bool, str]:
    if reference["row_count"] != candidate["row_count"]:
        return False, f"row count {candidate['row_count']} != reference {reference['row_count']}"
    if reference["numeric_column"] is None and candidate["numeric_column"] is None:
        return True, ""
    if reference["numeric_column"] is None or candidate["numeric_column"] is None:
        return False, "one result has no numeric checksum column"
    if not math.isclose(reference["numeric_sum"], candidate["numeric_sum"], rel_tol=1e-6, abs_tol=1e-9):
        return (
            False,
            f"numeric sum {candidate['numeric_sum']} != reference {reference['numeric_sum']} "
            f"(rtol=1e-6)",
        )
    return True, ""


def _run_once(engine_id: str, sf: float, query_id: str, data_root: Path) -> dict[str, Any]:
    data_dir = data_root / f"sf{sf_label(sf)}"
    start = time.perf_counter()
    engine = build_engine(engine_id, data_dir)
    init_seconds = time.perf_counter() - start
    try:
        result = engine.run(query_id)
        return {"engine_init_seconds": init_seconds, "fingerprint": fingerprint(result)}
    finally:
        engine.close()


def worker(engine_id: str, sf: float, query_id: str, data_root: Path) -> int:
    """Fresh-process worker used only for a single cold query execution."""
    try:
        payload = _run_once(engine_id, sf, query_id, data_root)
        payload["status"] = "OK"
        print(WORKER_PREFIX + json.dumps(payload, default=str))
        return 0
    except Exception as exc:  # error must be machine-readable to the parent
        payload = {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}
        print(WORKER_PREFIX + json.dumps(payload))
        traceback.print_exc(file=sys.stderr)
        return 1


def cold_run(engine_id: str, sf: float, query_id: str, data_root: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "bench.runner",
        "--worker",
        "--engine",
        engine_id,
        "--sf",
        sf_label(sf),
        "--query",
        query_id,
        "--data-dir",
        str(data_root),
    ]
    start = time.perf_counter()
    completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    wall_seconds = time.perf_counter() - start
    payload = None
    for line in reversed(completed.stdout.splitlines()):
        if line.startswith(WORKER_PREFIX):
            payload = json.loads(line.removeprefix(WORKER_PREFIX))
            break
    if completed.returncode != 0 or payload is None or payload["status"] != "OK":
        detail = (payload or {}).get("error", completed.stderr.strip() or "worker returned no payload")
        raise EngineUnavailable(detail)
    payload["wall_seconds"] = wall_seconds
    return payload


def warm_runs(engine_id: str, sf: float, query_id: str, data_root: Path) -> list[float]:
    """One discarded warm-up followed by exactly five timed in-process repeats."""
    engine = build_engine(engine_id, data_root / f"sf{sf_label(sf)}")
    try:
        engine.run(query_id)  # discarded warm-up
        samples = []
        for _ in range(5):
            start = time.perf_counter()
            engine.run(query_id)
            samples.append(time.perf_counter() - start)
        return samples
    finally:
        engine.close()


def reference_fingerprint(sf: float, query_id: str, data_root: Path) -> dict[str, Any]:
    return _run_once("duckdb", sf, query_id, data_root)["fingerprint"]


def input_row_count(sf: float, data_root: Path) -> int:
    engine = DuckDBEngine(data_root / f"sf{sf_label(sf)}")
    try:
        return sum(engine.con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in TABLES)
    finally:
        engine.close()


def engine_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for name in ("duckdb", "polars", "pyspark", "pyarrow", "psutil"):
        try:
            module = importlib.import_module(name)
            versions[name] = getattr(module, "__version__", "installed")
        except Exception:
            versions[name] = "unavailable"
    return versions


def load_sample() -> dict[str, float | None]:
    try:
        one, five, fifteen = os.getloadavg()
        return {"load1": one, "load5": five, "load15": fifteen}
    except (AttributeError, OSError):
        return {"load1": None, "load5": None, "load15": None}


def total_ram_bytes() -> int | None:
    try:
        import psutil

        return psutil.virtual_memory().total
    except Exception:
        try:
            return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        except (AttributeError, ValueError, OSError):
            return None


def environment_stamp(start_load: dict[str, float | None], end_load: dict[str, float | None]) -> dict[str, Any]:
    cores = os.cpu_count() or 1
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": cores,
        "total_ram_bytes": total_ram_bytes(),
        "python_version": sys.version,
        "engine_versions": engine_versions(),
        "thread_configuration": {
            "duckdb": "engine default parallelism",
            "polars": "engine default parallelism",
            "spark": f"local[*]; spark.sql.shuffle.partitions={cores}",
        },
        "load_before": start_load,
        "load_after": end_load,
        "high_start_load": start_load["load1"] is not None and start_load["load1"] > cores / 2,
    }


def benchmark_one(engine_id: str, sf: float, query_id: str, data_root: Path, rows: int) -> dict[str, Any]:
    result: dict[str, Any] = {
        "engine": engine_id,
        "sf": sf,
        "query": query_id,
        "input_rows": rows,
        "status": "PENDING",
    }
    reference = reference_fingerprint(sf, query_id, data_root)
    try:
        candidate = _run_once(engine_id, sf, query_id, data_root)["fingerprint"]
        matched, message = fingerprints_match(reference, candidate)
    except EngineUnavailable as exc:
        result.update(status="FAILED_ENVIRONMENT", error=str(exc), reference=reference)
        return result
    except Exception as exc:
        result.update(status="FAILED_ENVIRONMENT", error=f"{type(exc).__name__}: {exc}", reference=reference)
        return result

    result.update(reference=reference, candidate=candidate)
    if not matched:
        result.update(status="FAILED_CORRECTNESS", error=message)
        return result

    try:
        cold = cold_run(engine_id, sf, query_id, data_root)
        warm = warm_runs(engine_id, sf, query_id, data_root)
    except Exception as exc:
        result.update(status="FAILED_ENVIRONMENT", error=f"{type(exc).__name__}: {exc}")
        return result
    result.update(
        status="OK",
        cold_seconds=cold["wall_seconds"],
        engine_init_seconds=cold["engine_init_seconds"],
        cold_fingerprint=cold["fingerprint"],
        warm_seconds=warm,
        warm_median_seconds=sorted(warm)[len(warm) // 2],
    )
    return result


def run_benchmark(
    engines: list[str], sfs: list[float], queries: list[str], data_root: Path, results_path: Path
) -> dict[str, Any]:
    missing = [
        data_root / f"sf{sf_label(sf)}" / f"{table}.parquet"
        for sf in sfs
        for table in TABLES
        if not (data_root / f"sf{sf_label(sf)}" / f"{table}.parquet").exists()
    ]
    if missing:
        raise FileNotFoundError(f"missing shared Parquet input: {missing[0]}; run `make data` first")

    before = load_sample()
    results = []
    row_counts = {sf_label(sf): input_row_count(sf, data_root) for sf in sfs}
    for sf in sfs:
        for query_id in queries:
            for engine_id in engines:
                print(f"running engine={engine_id} sf={sf_label(sf)} query={query_id}", flush=True)
                results.append(benchmark_one(engine_id, sf, query_id, data_root, row_counts[sf_label(sf)]))
    after = load_sample()
    payload = {
        "methodology": "docs/METHODOLOGY.md",
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "environment": environment_stamp(before, after),
        "input_rows_by_sf": row_counts,
        "results": results,
    }
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    return payload


def parse_csv(value: str, permitted: tuple[str, ...]) -> list[str]:
    values = [item.strip().lower() for item in value.split(",") if item.strip()]
    unknown = sorted(set(values) - set(permitted))
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown values: {', '.join(unknown)}")
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", default=",".join(ENGINE_IDS), help="comma-separated engines")
    parser.add_argument("--sf", default="0.1,1", help="comma-separated scale factors")
    parser.add_argument("--query", default=",".join(QUERY_IDS), help="comma-separated query identifiers")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--results", type=Path, default=ROOT / "results" / "results.json")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    engines = parse_csv(args.engine, ENGINE_IDS)
    queries = parse_csv(args.query, QUERY_IDS)
    sfs = [float(value) for value in args.sf.split(",")]
    if args.worker:
        if len(engines) != 1 or len(sfs) != 1 or len(queries) != 1:
            parser.error("--worker accepts exactly one engine, scale factor, and query")
        raise SystemExit(worker(engines[0], sfs[0], queries[0], args.data_dir))
    run_benchmark(engines, sfs, queries, args.data_dir, args.results)


if __name__ == "__main__":
    main()

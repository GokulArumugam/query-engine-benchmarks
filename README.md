# Query-engine benchmarks: DuckDB, Polars, and Spark

This repository benchmarks a deliberately small, reproducible TPC-H slice at SF 0.1 and SF 1. It is about separating fixed startup cost from per-row work on one machine—not claiming that a laptop represents a Spark cluster.

```text
DuckDB tpch extension → shared Parquet → DuckDB SQL / Polars lazy / Spark SQL
                                      ↓
                         correctness gate → cold + warm timings → JSON + Markdown
```

## Quickstart

```sh
python3 -m pip install -r requirements.txt
make data
make bench
```

`make bench ENGINES=duckdb,polars SF=0.1` limits a local run. Spark needs a working JVM (`java -version`); the harness does not install one and records a clear environment failure when it is unavailable.

## Methodology

The binding [methodology contract](docs/METHODOLOGY.md) mechanically enforces shared Parquet bytes, side-by-side audited query variants, fresh-process cold runs, five-repeat warm medians, a DuckDB correctness gate, environment/load stamping, all-core defaults, and a one-command workflow.

The query implementations are intentionally adjacent in [queries](queries): SQL for DuckDB and Spark (dialect changes only), and lazy, idiomatic Polars expressions.

## Results

Filled per run — see blog post. Local output is written to `results/results.json` and `results/summary.md`.

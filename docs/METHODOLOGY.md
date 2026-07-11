# Methodology

Benchmarks are easy to run and easy to lie with. This document is the contract every published number follows; the harness enforces it mechanically.

## What this series measures — and deliberately doesn't

Small TPC-H scale factors (SF 0.1 ≈ 100 MB, SF 1 ≈ 1 GB) on a laptop. The point is **not** "which engine wins at 100 MB" — at that size they all finish before you blink. The point is decomposing runtime into:

- **Fixed overhead** — startup, JVM spin-up, planner, session init: independent of data size
- **Per-row cost** — scan/join/aggregate work that scales with data

Measuring both at two scale factors lets us fit a simple linear model per engine per query and *reason in writing* about where curves cross — at what data size Spark's fixed tax amortizes, when a laptop stops being enough — instead of pretending a MacBook benchmark proves anything about a cluster. Every extrapolation is labeled as a model, not a measurement.

## Rules (enforced by the harness)

1. **Same bytes for everyone.** TPC-H data generated once (DuckDB `tpch` extension), written as Parquet, read by all engines from the same files. No engine-native pre-loaded formats.
2. **Same queries.** A fixed subset of TPC-H queries (Q1 aggregation-heavy, Q3/Q5 joins, Q6 scan-filter, Q9 many-join, Q18 large aggregation) expressed idiomatically per engine — SQL for DuckDB and Spark SQL, Polars expressions for Polars — reviewed side-by-side in `queries/` so anyone can audit fairness.
3. **Cold vs warm split.** Cold = fresh process, includes startup and first-read (reported separately as fixed overhead). Warm = median of N=5 in-process repeats after 1 discarded warm-up run.
4. **Correctness gate before timing.** Each engine's result is checksummed against DuckDB's reference output (row count + numeric aggregate within tolerance). A benchmark that returns wrong answers is disqualified, not just slow.
5. **Environment stamped into results.** Hardware, OS, engine versions, thread counts, memory ceilings recorded in every `results.json`. Runs with other heavy processes are flagged (the harness samples system load).
6. **All engines get all cores** — default parallel configuration, no per-engine hand-tuning. The one Spark concession: `local[*]` with default shuffle partitions reduced to core count (documented; leaving 200 shuffle partitions on MB data would be a strawman).
7. **Reproducible in one command:** `make bench` regenerates data, runs everything, emits `results/results.json` + a markdown summary table.

## Honest limitations (stated in every post)

Laptop thermals are noisy; medians over 5 runs mitigate, not eliminate. Single-node only — nothing here measures distributed shuffle, the thing Spark actually exists for; the extrapolation model marks the boundary where that omission matters. Small data over-weights fixed costs — which is exactly the phenomenon under study, but numbers must not be quoted without that context.

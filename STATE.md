# STATE — handoff file

**Last updated:** 2026-07-11 — harness implementation complete (Codex)

Project 3 of Gokul's DE portfolio (series repo). Site: portfolio-site-eosin-eta.vercel.app (blog stub "duckdb-spark-polars-benchmark" exists — post #1 replaces it). Workflow: orchestrator designs/reviews; Codex implements; live verification before acceptance. Sizing decision (Gokul): MB-scale data on purpose — measure fixed-vs-per-row costs, extrapolate scale in writing.

## Status
- ✅ docs/METHODOLOGY.md — binding contract for the harness and all posts.
- ✅ Harness code complete pending a live benchmark run: data generation (DuckDB tpch → shared Parquet), auditable DuckDB/Spark SQL + lazy Polars query variants, cold/warm runner, DuckDB correctness gate, environment/load stamp, JSON/Markdown summary, Make targets, and Q6 smoke tests.
- 🔧 Harness backlog: `ENGINES=` filtered runs REPLACE results.json instead of merging — fine for the methodology (publish single-run numbers only) but surprising; fix or document. Polars Q18 join-coalescing bug fixed (group on left-name keys, rename after).
- ⬜ NEXT (orchestrator): stop Colima, run `make bench` on this machine, inspect `results/summary.md`, then publish blog post #1 and replace the site stub.

## Env note
Host has colima VM (8GB) running the wiki pipeline — stop it during timed runs (`colima stop` / restart after) or flag the interference; harness load-sampling should catch it.

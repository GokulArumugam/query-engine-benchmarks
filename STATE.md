# STATE — handoff file

**Last updated:** 2026-07-11 — methodology phase (Claude)

Project 3 of Gokul's DE portfolio (series repo). Site: portfolio-site-eosin-eta.vercel.app (blog stub "duckdb-spark-polars-benchmark" exists — post #1 replaces it). Workflow: orchestrator designs/reviews; Codex implements; live verification before acceptance. Sizing decision (Gokul): MB-scale data on purpose — measure fixed-vs-per-row costs, extrapolate scale in writing.

## Status
- ✅ docs/METHODOLOGY.md — binding contract for the harness and all posts.
- ⬜ NEXT (Codex): harness per methodology — datagen (DuckDB tpch ext → parquet SF0.1 + SF1), runners (duckdb, polars, spark local), cold/warm split, correctness gate vs DuckDB reference, env stamping, results/results.json + markdown table, `make bench`, pytest smoke tests. Python 3.11; pip has duckdb, pyarrow; needs polars + pyspark in requirements.
- ⬜ Then: real run on this machine (orchestrator), post #1 on the blog, replace stub.

## Env note
Host has colima VM (8GB) running the wiki pipeline — stop it during timed runs (`colima stop` / restart after) or flag the interference; harness load-sampling should catch it.

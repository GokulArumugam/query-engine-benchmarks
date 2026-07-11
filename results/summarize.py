#!/usr/bin/env python3
"""Render the benchmark JSON into the checked-in human-readable summary."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

QUERY_IDS = ("q1", "q3", "q5", "q6", "q9", "q18")
ENGINE_IDS = ("duckdb", "polars", "spark")


def seconds(value: float | None) -> str:
    return "—" if value is None else f"{value:.4f}"


def render(payload: dict) -> str:
    entries = payload.get("results", [])
    by_sf: dict[float, dict[tuple[str, str], dict]] = defaultdict(dict)
    for entry in entries:
        by_sf[float(entry["sf"])][(entry["engine"], entry["query"])] = entry

    lines = [
        "# Benchmark summary",
        "",
        "Generated from `results/results.json`. Every cell is `cold s / warm-median s`; "
        "only entries that passed the DuckDB correctness gate appear as timings.",
        "",
    ]
    for sf in sorted(by_sf):
        lines.extend([f"## SF {sf:g}", "", "| Engine | " + " | ".join(query.upper() for query in QUERY_IDS) + " |", "|---|" + "|".join("---" for _ in QUERY_IDS) + "|"])
        for engine in ENGINE_IDS:
            cells = []
            for query in QUERY_IDS:
                entry = by_sf[sf].get((engine, query))
                if not entry:
                    cells.append("not run")
                elif entry["status"] == "OK":
                    cells.append(f"{seconds(entry['cold_seconds'])} / {seconds(entry['warm_median_seconds'])}")
                else:
                    cells.append(entry["status"])
            lines.append("| " + engine + " | " + " | ".join(cells) + " |")
        successful = [entry for entry in by_sf[sf].values() if entry["status"] == "OK"]
        lines.extend(["", "Fixed-overhead estimate (mean of cold minus warm median):", "", "| Engine | Seconds |", "|---|---:|"])
        for engine in ENGINE_IDS:
            values = [
                item["cold_seconds"] - item["warm_median_seconds"]
                for item in successful
                if item["engine"] == engine
            ]
            estimate = sum(values) / len(values) if values else None
            lines.append(f"| {engine} | {seconds(estimate)} |")
        lines.append("")

    lines.extend([
        "## Incremental per-row cost, SF 0.1 → SF 1",
        "",
        "`(warm SF1 − warm SF0.1) / (all TPC-H input rows SF1 − SF0.1)`. This is a simple "
        "two-point model estimate, not a measured cluster throughput claim.",
        "",
        "| Engine | Query | Seconds / input row |",
        "|---|---|---:|",
    ])
    low, high = by_sf.get(0.1, {}), by_sf.get(1.0, {})
    rows = payload.get("input_rows_by_sf", {})
    delta_rows = rows.get("1", rows.get("1.0", 0)) - rows.get("0.1", 0)
    for engine in ENGINE_IDS:
        for query in QUERY_IDS:
            left, right = low.get((engine, query)), high.get((engine, query))
            if not left or not right or left["status"] != "OK" or right["status"] != "OK" or delta_rows <= 0:
                value = "—"
            else:
                value = f"{(right['warm_median_seconds'] - left['warm_median_seconds']) / delta_rows:.3e}"
            lines.append(f"| {engine} | {query.upper()} | {value} |")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("results/results.json"))
    parser.add_argument("--output", type=Path, default=Path("results/summary.md"))
    args = parser.parse_args()
    payload = json.loads(args.input.read_text())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(payload))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Generate the shared Parquet inputs required by the benchmark contract."""

from __future__ import annotations

import argparse
from pathlib import Path

TABLES = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
)


def sf_label(sf: float) -> str:
    """Return the stable directory spelling used by the repository."""
    return f"{sf:g}"


def generate(sf: float, root: Path, force: bool = False) -> Path:
    """Generate a scale factor once and export the canonical Parquet files."""
    import duckdb

    output = root / f"sf{sf_label(sf)}"
    expected = [output / f"{table}.parquet" for table in TABLES]
    if not force and all(path.exists() for path in expected):
        print(f"SF {sf_label(sf)} already exists at {output}; skipping")
        return output

    output.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        # Keep extension state inside the repository data directory. This avoids
        # relying on a writable home directory in CI/container environments.
        extension_dir = str(root / ".duckdb_extensions").replace("'", "''")
        con.execute(f"SET extension_directory = '{extension_dir}'")
        # INSTALL is intentionally retained: it makes a clean machine reproducible.
        con.execute("INSTALL tpch")
        con.execute("LOAD tpch")
        con.execute("CALL dbgen(sf = ?)", [sf])
        for table, destination in zip(TABLES, expected):
            if destination.exists() and not force:
                print(f"exists {destination}; skipping")
                continue
            if force and destination.exists():
                destination.unlink()
            escaped = str(destination).replace("'", "''")
            con.execute(f"COPY {table} TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            print(f"wrote {destination}")
    finally:
        con.close()
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sf", type=float, nargs="+", default=[0.1, 1.0])
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--force", action="store_true", help="Regenerate existing Parquet files")
    args = parser.parse_args()
    for sf in args.sf:
        generate(sf, args.data_dir, args.force)


if __name__ == "__main__":
    main()

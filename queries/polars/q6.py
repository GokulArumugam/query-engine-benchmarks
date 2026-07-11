"""TPC-H Q6: a pure lazy scan/filter/aggregate."""
from datetime import date

import polars as pl


def run(ctx):
    return (
        ctx.scan("lineitem")
        .filter(
            (pl.col("l_shipdate") >= pl.lit(date(1994, 1, 1)))
            & (pl.col("l_shipdate") < pl.lit(date(1995, 1, 1)))
            & pl.col("l_discount").is_between(0.05, 0.07)
            & (pl.col("l_quantity") < 24)
        )
        .select((pl.col("l_extendedprice") * pl.col("l_discount")).sum().alias("revenue"))
    )

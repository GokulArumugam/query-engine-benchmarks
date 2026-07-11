"""TPC-H Q1, expressed as a lazy Polars aggregation."""
from datetime import date

import polars as pl


def run(ctx):
    lineitem = ctx.scan("lineitem")
    discounted = pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
    return (
        lineitem.filter(pl.col("l_shipdate") <= pl.lit(date(1998, 9, 2)))
        .group_by(["l_returnflag", "l_linestatus"])
        .agg(
            pl.col("l_quantity").sum().alias("sum_qty"),
            pl.col("l_extendedprice").sum().alias("sum_base_price"),
            discounted.sum().alias("sum_disc_price"),
            (discounted * (1 + pl.col("l_tax"))).sum().alias("sum_charge"),
            pl.col("l_quantity").mean().alias("avg_qty"),
            pl.col("l_extendedprice").mean().alias("avg_price"),
            pl.col("l_discount").mean().alias("avg_disc"),
            pl.len().alias("count_order"),
        )
        .sort(["l_returnflag", "l_linestatus"])
    )

"""TPC-H Q3, using lazy filters, joins, and aggregation."""
from datetime import date

import polars as pl


def run(ctx):
    customers = ctx.scan("customer").filter(pl.col("c_mktsegment") == "BUILDING")
    orders = (
        ctx.scan("orders")
        .filter(pl.col("o_orderdate") < pl.lit(date(1995, 3, 15)))
        .join(customers.select("c_custkey"), left_on="o_custkey", right_on="c_custkey")
    )
    return (
        ctx.scan("lineitem")
        .filter(pl.col("l_shipdate") > pl.lit(date(1995, 3, 15)))
        .join(orders, left_on="l_orderkey", right_on="o_orderkey")
        .group_by(["l_orderkey", "o_orderdate", "o_shippriority"])
        .agg((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum().alias("revenue"))
        .sort(["revenue", "o_orderdate"], descending=[True, False])
        .limit(10)
    )

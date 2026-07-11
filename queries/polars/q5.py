"""TPC-H Q5, retaining a lazy join graph rather than materialising inputs."""
from datetime import date

import polars as pl


def run(ctx):
    region = ctx.scan("region").filter(pl.col("r_name") == "ASIA")
    nation = ctx.scan("nation").join(region, left_on="n_regionkey", right_on="r_regionkey")
    suppliers = ctx.scan("supplier").join(nation, left_on="s_nationkey", right_on="n_nationkey")
    customers = ctx.scan("customer")
    orders = (
        ctx.scan("orders")
        .filter(
            (pl.col("o_orderdate") >= pl.lit(date(1994, 1, 1)))
            & (pl.col("o_orderdate") < pl.lit(date(1995, 1, 1)))
        )
        .join(customers, left_on="o_custkey", right_on="c_custkey")
    )
    return (
        ctx.scan("lineitem")
        .join(orders, left_on="l_orderkey", right_on="o_orderkey")
        .join(suppliers, left_on="l_suppkey", right_on="s_suppkey")
        .filter(pl.col("c_nationkey") == pl.col("s_nationkey"))
        .group_by("n_name")
        .agg((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum().alias("revenue"))
        .sort("revenue", descending=True)
    )

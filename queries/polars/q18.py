"""TPC-H Q18: qualifying orders are calculated lazily before the final join."""

import polars as pl


def run(ctx):
    lineitem = ctx.scan("lineitem")
    large_orders = (
        lineitem.group_by("l_orderkey")
        .agg(pl.col("l_quantity").sum().alias("order_quantity"))
        .filter(pl.col("order_quantity") > 300)
        .select("l_orderkey")
    )
    orders = ctx.scan("orders").join(large_orders, left_on="o_orderkey", right_on="l_orderkey")
    return (
        lineitem.join(orders, left_on="l_orderkey", right_on="o_orderkey")
        .join(ctx.scan("customer"), left_on="o_custkey", right_on="c_custkey")
        # Polars coalesces differently named join keys to the LEFT name — this
        # applies to the custkey join too, so group on the coalesced names and
        # rename afterwards to retain SQL's output schema.
        .group_by(["c_name", "o_custkey", "l_orderkey", "o_orderdate", "o_totalprice"])
        .agg(pl.col("l_quantity").sum().alias("sum_quantity"))
        .rename({"l_orderkey": "o_orderkey", "o_custkey": "c_custkey"})
        .sort(["o_totalprice", "o_orderdate"], descending=[True, False])
        .limit(100)
    )

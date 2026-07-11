"""TPC-H Q9, the many-join query, expressed as a lazy Polars plan."""

import polars as pl


def run(ctx):
    parts = ctx.scan("part").filter(pl.col("p_name").str.contains("green"))
    partsupp = ctx.scan("partsupp")
    suppliers = ctx.scan("supplier")
    nations = ctx.scan("nation")
    orders = ctx.scan("orders")
    profit = (
        ctx.scan("lineitem")
        .join(partsupp, left_on=["l_suppkey", "l_partkey"], right_on=["ps_suppkey", "ps_partkey"])
        .join(parts, left_on="l_partkey", right_on="p_partkey")
        .join(suppliers, left_on="l_suppkey", right_on="s_suppkey")
        .join(nations, left_on="s_nationkey", right_on="n_nationkey")
        .join(orders, left_on="l_orderkey", right_on="o_orderkey")
        .with_columns(
            pl.col("o_orderdate").dt.year().alias("o_year"),
            (
                pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
                - pl.col("ps_supplycost") * pl.col("l_quantity")
            ).alias("amount"),
        )
    )
    return profit.group_by(["n_name", "o_year"]).agg(pl.col("amount").sum().alias("sum_profit")).rename({"n_name": "nation"}).sort(["nation", "o_year"], descending=[False, True])

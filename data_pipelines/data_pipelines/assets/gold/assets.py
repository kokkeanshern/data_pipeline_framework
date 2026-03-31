import os
import json
import polars as pl
from dagster import asset, AssetKey
from data_pipelines.assets.config import date_partition_start_date
from datetime import datetime, timezone, timedelta


@asset(
    partitions_def=date_partition_start_date,
)
def gold_performance_metrics(context, silver_massive_prices, silver_coingecko_prices):
    exec_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    now = datetime.now(timezone.utc)

    df_cg = pl.read_parquet(silver_massive_prices)
    df_massive = pl.read_parquet(silver_coingecko_prices)
    df_final = pl.concat([df_cg, df_massive])

    output_dir = f"data/gold/gold_performance_metrics/year={exec_date.year}/month={exec_date.month:02d}/day={exec_date.day:02d}"
    os.makedirs(output_dir, exist_ok=True)
    df_final.write_parquet(f"{output_dir}/{now.strftime('%H%M%S')}.parquet")

import os
import json
import polars as pl
from dagster import asset, AssetKey
from datetime import datetime, timezone
from data_pipelines.assets.config import (
    date_partition_start_date,
    MASSIVE_TICKERS,
    SILVER_SCHEMA,
)


@asset(
    partitions_def=date_partition_start_date,
    deps=[
        AssetKey(f"bronze_massive_{t.replace(':', '_').lower()}")
        for t in MASSIVE_TICKERS
    ],
)
def silver_massive_prices(context):
    exec_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    now = datetime.now(timezone.utc)

    all_records = []
    for ticker in MASSIVE_TICKERS:
        safe_name = ticker.replace(":", "_").lower()
        ticker_dir = f"data/bronze/massive/{safe_name}/year={exec_date.year}/month={exec_date.month:02d}/day={exec_date.day:02d}"

        if not os.path.exists(ticker_dir):
            continue

        for file in os.listdir(ticker_dir):
            if file.endswith(".json"):
                with open(os.path.join(ticker_dir, file)) as f:
                    data = json.load(f)
                for row in data:
                    all_records.append(
                        {
                            "date": exec_date,
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            "volume": row["volume"],
                            "market_cap": None,
                            "ticker": ticker,
                            "source": "massive",
                        }
                    )

    df = pl.DataFrame(all_records, schema=SILVER_SCHEMA)
    output_dir = f"data/silver/massive_prices/year={exec_date.year}/month={exec_date.month:02d}/day={exec_date.day:02d}"
    os.makedirs(output_dir, exist_ok=True)
    df.write_parquet(f"{output_dir}/{now.strftime('%H%M%S')}.parquet")

    return f"{output_dir}/{now.strftime('%H%M%S')}.parquet"


@asset(partitions_def=date_partition_start_date)
def silver_coingecko_prices(context, bronze_bitcoin):
    path = bronze_bitcoin

    with open(path) as f:
        data = json.load(f)

    exec_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    now = datetime.now(timezone.utc)

    prices = {p[0]: p[1] for p in data["prices"]}
    market_caps = {m[0]: m[1] for m in data["market_caps"]}
    volumes = {v[0]: v[1] for v in data["total_volumes"]}

    all_records = []
    for ts in prices:
        all_records.append(
            {
                "date": exec_date,
                "open": None,
                "high": None,
                "low": None,
                "close": prices[ts],
                "volume": volumes.get(ts),
                "market_cap": market_caps.get(ts),
                "ticker": "BTC",
                "source": "coingecko",
            }
        )

    df = pl.DataFrame(all_records, schema=SILVER_SCHEMA)
    output_dir = f"data/silver/coingecko_prices/year={exec_date.year}/month={exec_date.month:02d}/day={exec_date.day:02d}"
    os.makedirs(output_dir, exist_ok=True)
    df.write_parquet(f"{output_dir}/{now.strftime('%H%M%S')}.parquet")

    return f"{output_dir}/{now.strftime('%H%M%S')}.parquet"

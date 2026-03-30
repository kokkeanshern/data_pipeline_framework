from dagster import asset, AssetKey
from data_pipelines.assets.config import date_partition_start_date, MASSIVE_TICKERS
import json
import os
import pandas as pd
from datetime import datetime, timezone


@asset(
    partitions_def=date_partition_start_date,
    deps=[
        AssetKey(f"bronze_massive_{t.replace(':', '_').lower()}")
        for t in MASSIVE_TICKERS
    ],
)
def silver_massive_prices(context):
    exec_date = datetime.strptime(context.partition_key, "%Y-%m-%d")

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

    df = pd.DataFrame(all_records)
    output_dir = f"data/silver/massive_prices/year={exec_date.year}/month={exec_date.month:02d}/day={exec_date.day:02d}"
    os.makedirs(output_dir, exist_ok=True)
    df.to_parquet(f"{output_dir}/data.parquet", index=False)

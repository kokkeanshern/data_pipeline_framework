import os
import json
import pandas as pd
from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext
from datetime import datetime, timezone, timedelta
from data_pipelines.resources.coingecko import CoinGeckoResource
from data_pipelines.resources.massive import MassiveResource
from data_pipelines.assets.config import date_partition_start_date, MASSIVE_TICKERS


@asset(partitions_def=date_partition_start_date)
def bronze_bitcoin(context: AssetExecutionContext, coingecko: CoinGeckoResource):
    exec_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    now = datetime.now(timezone.utc)
    from_ts = int(datetime.combine(exec_date, datetime.min.time(), timezone.utc).timestamp())
    to_ts = from_ts + 86400  # end of day

    data = coingecko.get_market_chart("bitcoin", from_ts=from_ts, to_ts=to_ts)

    # Create output directory
    output_dir = f"data/bronze/coingecko/bitcoin/year={exec_date.year}/month={exec_date.month:02d}/day={exec_date.day:02d}"
    os.makedirs(output_dir, exist_ok=True)

    # Write raw response as JSON
    output_path = f"{output_dir}/{now.strftime('%H%M%S')}.json"

    with open(output_path, "w") as f:
        json.dump(data.model_dump(), f)

    return output_path

def build_bronze_massive_asset(ticker: str):
    safe_name = ticker.replace(":", "_").lower()

    @asset(partitions_def=date_partition_start_date, name=f"bronze_massive_{safe_name}")
    def _asset(context: AssetExecutionContext, massive: MassiveResource):
        exec_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
        now = datetime.now(timezone.utc)
        from_ts = int(datetime.combine(exec_date, datetime.min.time(), timezone.utc).timestamp())
        to_ts = from_ts + 86400  # end of day

        data = massive.get_price_info(ticker=ticker, from_=from_ts, to=to_ts)

        output_dir = f"data/bronze/massive/{safe_name}/year={exec_date.year}/month={exec_date.month:02d}/day={exec_date.day:02d}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/{now.strftime('%H%M%S')}.json"

        serializable = [vars(d) for d in data]
        with open(output_path, "w") as f:
            json.dump(serializable, f)
        
        return output_path

    return _asset


bronze_massive_assets = [build_bronze_massive_asset(t) for t in MASSIVE_TICKERS]

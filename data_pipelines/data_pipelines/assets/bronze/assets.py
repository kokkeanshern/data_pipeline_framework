import os
import json
from dagster import asset, multi_asset, AssetSpec
from datetime import datetime, timedelta, timezone
from data_pipelines.resources.coingecko import CoinGeckoResource
from data_pipelines.resources.massive import MassiveResource


@asset
def bronze_bitcoin(coingecko: CoinGeckoResource):
    data = coingecko.get_market_chart("bitcoin")

    """
    The code below writes the raw response from the CoinGecko API to the local filesystem.
    ToDo: Implement minIO to simulate the experience of writing to S3.
    """
    # Create output directory
    output_dir = "data/bronze/coingecko/bitcoin"
    os.makedirs(output_dir, exist_ok=True)

    # Write raw response as JSON
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = f"{output_dir}/{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(data.model_dump(), f)

    return output_path


MASSIVE_TICKERS = ["AAPL", "GOOGL", "MSFT", "SPY", "C:EURUSD", "C:GBPUSD"]


def build_bronze_massive_asset(ticker: str):
    safe_name = ticker.replace(":", "_").lower()

    @asset(name=f"bronze_massive_{safe_name}")
    def _asset(massive: MassiveResource):
        data = massive.get_price_info(ticker=ticker)
        output_dir = f"data/bronze/massive/{ticker}"
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        output_path = f"{output_dir}/{timestamp}.json"
        serializable = [vars(d) for d in data]
        with open(output_path, "w") as f:
            json.dump(serializable, f)
        return output_path

    return _asset


bronze_massive_assets = [build_bronze_massive_asset(t) for t in MASSIVE_TICKERS]

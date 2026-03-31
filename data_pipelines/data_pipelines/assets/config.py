import polars as pl
from dagster import DailyPartitionsDefinition
from datetime import datetime, timezone, timedelta

date_partition_start_date = DailyPartitionsDefinition(
    start_date=(datetime.now(timezone.utc).date() - timedelta(days=3)).isoformat()
)

MASSIVE_TICKERS = ["AAPL", "GOOGL", "MSFT", "SPY", "C:EURUSD", "C:GBPUSD"]

import polars as pl

SILVER_SCHEMA = {
    "date": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "market_cap": pl.Float64,
    "ticker": pl.Utf8,
    "source": pl.Utf8,
}

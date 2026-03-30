from dagster import DailyPartitionsDefinition
from datetime import datetime, timezone, timedelta

date_partition_start_date = DailyPartitionsDefinition(
    start_date=(datetime.now(timezone.utc).date() - timedelta(days=3)).isoformat()
)

MASSIVE_TICKERS = ["AAPL", "GOOGL", "MSFT", "SPY", "C:EURUSD", "C:GBPUSD"]

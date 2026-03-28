from coingecko_sdk import Coingecko
from dagster import ConfigurableResource
from datetime import datetime, timedelta, timezone


class CoinGeckoResource(ConfigurableResource):
    api_key: str

    def _get_client(self):
        return Coingecko(
            demo_api_key=self.api_key,
            environment="demo",
        )

    def get_market_chart(
        self,
        coin_id: str,
        vs_currency: str = "usd",
        from_ts: str = None,
        to_ts: str = None,
        interval: str = "daily",
        precision: str = "full",
    ):
        # if no time range is provided, default to yesterday's date
        if from_ts is None or to_ts is None:
            yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
            from_ts = from_ts or int(
                datetime.combine(
                    yesterday, datetime.min.time(), timezone.utc
                ).timestamp()
            )
            to_ts = to_ts or int(
                datetime.combine(
                    yesterday, datetime.min.time(), timezone.utc
                ).timestamp()
            )
        try:
            client = self._get_client()
            return client.coins.market_chart.get_range(
                id=coin_id,
                vs_currency=vs_currency,
                from_=str(from_ts),
                to=str(to_ts),
                interval=interval,
                precision=precision,
            )
        except Exception as e:
            raise RuntimeError(f"Error fetching market chart data: {e}")

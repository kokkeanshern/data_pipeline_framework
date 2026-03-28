from massive import RESTClient
from dagster import ConfigurableResource
from datetime import datetime, timedelta, timezone


class MassiveResource(ConfigurableResource):
    api_key: str

    def _get_client(self):
        return RESTClient(api_key=self.api_key)

    def get_price_info(
        self,
        ticker: str,
        multiplier: int = 1,
        timespan: str = "day",
        from_: str = None,
        to: str = None,
        limit: int = 50000,
    ):
        if from_ is None or to is None:
            yesterday = (
                datetime.now(timezone.utc).date() - timedelta(days=1)
            ).isoformat()
            from_ = from_ or yesterday
            to = to or yesterday

        try:
            client = self._get_client()
            aggs = []
            for resp in client.list_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=from_,
                to=to,
                limit=limit,
            ):
                aggs.append(resp)
            return aggs
        except Exception as e:
            raise RuntimeError(f"Error fetching price data for {ticker}: {e}")

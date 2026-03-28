from dagster import Definitions, EnvVar
from data_pipelines.resources.coingecko import CoinGeckoResource
from data_pipelines.resources.massive import MassiveResource
from data_pipelines.assets.bronze.assets import bronze_bitcoin, bronze_massive_assets

defs = Definitions(
    assets=[bronze_bitcoin, *bronze_massive_assets],
    resources={
        "coingecko": CoinGeckoResource(
            api_key=EnvVar("COINGECKO_API_KEY"),
        ),
        "massive": MassiveResource(
            api_key=EnvVar("MASSIVE_API_KEY"),
        ),
    },
)

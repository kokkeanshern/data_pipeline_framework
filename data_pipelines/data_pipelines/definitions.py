from dagster import Definitions, EnvVar
from data_pipelines.resources.coingecko import CoinGeckoResource
from data_pipelines.resources.massive import MassiveResource
from data_pipelines.assets.bronze.assets import *
from data_pipelines.assets.silver.assets import *
from data_pipelines.assets.gold.assets import *

defs = Definitions(
    assets=[
        bronze_bitcoin,
        *bronze_massive_assets,
        silver_massive_prices,
        silver_coingecko_prices,
        gold_performance_metrics,
    ],
    resources={
        "coingecko": CoinGeckoResource(
            api_key=EnvVar("COINGECKO_API_KEY"),
        ),
        "massive": MassiveResource(
            api_key=EnvVar("MASSIVE_API_KEY"),
        ),
    },
)

from dagster import asset


@asset
def bronze_bitcoin():
    return {"price": 69000, "date": "2026-03-28"}


@asset
def silver_prices(bronze_bitcoin):
    raw = bronze_bitcoin
    return {"close": raw["price"], "date": raw["date"]}

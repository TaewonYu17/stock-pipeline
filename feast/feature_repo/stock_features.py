from datetime import timedelta
from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.snowflake_source import SnowflakeSource
from feast.types import Float64

ticker = Entity(
    name="ticker",
    description="Stock ticker symbol e.g. AAPL, MSFT",
    join_keys=["ticker"],
)

price_features_source = SnowflakeSource(
    database="STOCK_DB",
    schema="FEATURES",
    table="PRICE_FEATURES",
    timestamp_field="DATE",
    created_timestamp_column=None,
)

price_features_view = FeatureView(
    name="price_features",
    entities=[ticker],
    ttl=timedelta(days=1),
    schema=[
        Field(name="ma_5",              dtype=Float64),
        Field(name="ma_20",             dtype=Float64),
        Field(name="ma_50",             dtype=Float64),
        Field(name="rsi_14",            dtype=Float64),
        Field(name="volatility_20d",    dtype=Float64),
        Field(name="volume_ratio",      dtype=Float64),
        Field(name="bb_upper",          dtype=Float64),
        Field(name="bb_lower",          dtype=Float64),
        Field(name="macd",              dtype=Float64),
        Field(name="macd_signal",       dtype=Float64),
        Field(name="price_vs_52w_high", dtype=Float64),
        Field(name="sector_return_1w",  dtype=Float64),
        Field(name="vix_close",         dtype=Float64),
        Field(name="return_1d",         dtype=Float64),
    ],
    source=price_features_source,
)

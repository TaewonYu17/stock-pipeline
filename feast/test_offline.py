import pandas as pd
from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo/")

# Entity dataframe: the list of ticker + date combinations
# you want features for. This is what you'd build for training.
entity_df = pd.DataFrame({
    "ticker": ["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA"],
    "event_timestamp": pd.to_datetime([
        "2024-01-15",
        "2024-01-15",
        "2024-01-15",
        "2024-03-01",
        "2024-06-01",
    ])
})

print("Fetching historical features from Snowflake...")
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "price_features:ma_5",
        "price_features:ma_20",
        "price_features:rsi_14",
        "price_features:volatility_20d",
        "price_features:vix_close",
        "price_features:return_1d",
    ],
).to_df()

print(f"\nRetrieved {len(training_df)} rows")
print(f"\nColumns: {list(training_df.columns)}")
print(f"\nSample data:")
print(training_df.head(10).to_string())

print(f"\nRSI range: {training_df['price_features__rsi_14'].min():.1f} - {training_df['price_features__rsi_14'].max():.1f}")
print(f"Null count: {training_df.isnull().sum().sum()} total nulls")

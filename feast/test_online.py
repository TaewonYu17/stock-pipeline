from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo/")

# Fetch latest features for a few tickers from the online store
feature_vector = store.get_online_features(
    features=[
        "price_features:ma_5",
        "price_features:rsi_14",
        "price_features:volatility_20d",
        "price_features:vix_close",
    ],
    entity_rows=[
        {"ticker": "AAPL"},
        {"ticker": "MSFT"},
        {"ticker": "NVDA"},
    ],
).to_dict()

for i, ticker in enumerate(["AAPL", "MSFT", "NVDA"]):
    print(f"\n{ticker}:")
    print(f"  MA5:         {feature_vector['price_features__ma_5'][i]:.2f}")
    print(f"  RSI14:       {feature_vector['price_features__rsi_14'][i]:.2f}")
    print(f"  Volatility:  {feature_vector['price_features__volatility_20d'][i]:.4f}")
    print(f"  VIX:         {feature_vector['price_features__vix_close'][i]:.2f}")

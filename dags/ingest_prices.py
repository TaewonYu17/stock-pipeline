# airflow/dags/ingest_prices.py

from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "JPM", "V", "UNH"
]


@dag(
    dag_id="ingest_prices",
    description="Fetch daily OHLCV prices from yfinance → Snowflake raw layer",
    schedule="0 18 * * 1-5",   # 6pm daily, weekdays only (market closes at 4pm ET)
    start_date=datetime(2024, 1, 1),
    catchup=False,              # don't backfill all past dates on first run
    default_args={
        "retries": 3,                           # retry 3 times if a task fails
        "retry_delay": timedelta(minutes=5),    # wait 5 min between retries
        "owner": "data-team",
    },
    tags=["ingestion", "prices"],
)
def ingest_prices_dag():

    @task()
    def fetch_prices() -> list[dict]:
        all_rows = []

        for ticker in TICKERS:
            try:
                df = yf.download(
                    ticker,
                    period="2d",
                    interval="1d",
                    auto_adjust=True,
                    progress=False,
                )

                if df.empty:
                    print(f"WARNING: No data returned for {ticker}")
                    continue

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [col[0].lower() for col in df.columns]
                else:
                    df.columns = [col.lower() for col in df.columns]

                df = df.reset_index()
                df.columns = [c.lower() if isinstance(c, str) else c[0].lower() for c in df.columns]
                df["ticker"] = ticker

                rows = df[["ticker", "date", "open", "high", "low", "close", "volume"]].copy()
                rows["date"] = rows["date"].dt.strftime("%Y-%m-%d")  # Timestamp → plain string
                all_rows.extend(rows.to_dict(orient="records"))

            except Exception as e:
                print(f"ERROR fetching {ticker}: {e}")

        print(f"Fetched {len(all_rows)} rows across {len(TICKERS)} tickers")
        return all_rows



    @task()
    def load_to_snowflake(rows: list[dict]) -> None:
        """
        Load the fetched rows into raw.stock_prices using MERGE,
        so re-running the DAG on the same day doesn't create duplicates.
        """
        if not rows:
            print("No rows to load — skipping")
            return

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("USE WAREHOUSE STOCK_WH")   # ← add this line
        cursor.execute("USE DATABASE STOCK_DB")

        insert_sql = """
            INSERT INTO raw.stock_prices_stage
            (ticker, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_sql, [
            (r["ticker"], str(r["date"])[:10],
             r["open"], r["high"], r["low"], r["close"], int(r["volume"]))
            for r in rows
        ])

        cursor.execute("""
            MERGE INTO raw.stock_prices AS target
            USING raw.stock_prices_stage AS source
                ON target.ticker = source.ticker
                AND target.date   = source.date
            WHEN MATCHED THEN UPDATE SET
                open   = source.open,
                high   = source.high,
                low    = source.low,
                close  = source.close,
                volume = source.volume
            WHEN NOT MATCHED THEN INSERT
                (ticker, date, open, high, low, close, volume)
            VALUES
                (source.ticker, source.date, source.open, source.high,
                 source.low, source.close, source.volume)
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully loaded {len(rows)} rows into raw.stock_prices")

    rows = fetch_prices()
    load_to_snowflake(rows)

ingest_prices_dag()
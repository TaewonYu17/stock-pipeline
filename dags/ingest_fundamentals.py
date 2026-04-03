from datetime import datetime, timedelta
import requests
import os
import time
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dag(
    dag_id="ingest_fundamentals",
    description="Fetch company fundamentals from Alpha Vantage → Snowflake",
    schedule="0 20 * * 0",   
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=10)},
    tags=["ingestion", "fundamentals"],
)
def ingest_fundamentals_dag():

    TICKERS = ["AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","JPM","V","UNH"]
    API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

    @task()
    def fetch_fundamentals() -> list[dict]:
        api_key = os.getenv("ALPHA_VANTAGE_KEY")
        
        if not api_key:
            raise ValueError("ALPHA_VANTAGE_KEY environment variable is not set")

        rows = []
        for ticker in TICKERS:
            url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}"
            resp = requests.get(url, timeout=30)
            data = resp.json()

            # Alpha Vantage returns this key when you're being throttled
            if "Note" in data or "Information" in data:
                msg = data.get("Note") or data.get("Information")
                print(f"WARNING: Rate limit hit on {ticker}: {msg}")
                print("Waiting 60 seconds before continuing...")
                time.sleep(60)
                # Retry once after waiting
                resp = requests.get(url, timeout=30)
                data = resp.json()

            if "Symbol" not in data:
                print(f"WARNING: No data for {ticker} — response keys: {list(data.keys())}")
                continue

            print(f"OK: fetched {ticker} ({data.get('Sector')})")
            rows.append({
                "ticker":      data.get("Symbol"),
                "sector":      data.get("Sector"),
                "industry":    data.get("Industry"),
                "market_cap":  data.get("MarketCapitalization"),
                "pe_ratio":    data.get("PERatio"),
                "description": data.get("Description", "")[:500],
            })

            # Wait 15 seconds between every request — stays well under 5/min
            time.sleep(15)

        print(f"Total tickers fetched: {len(rows)}")
        return rows


    @task()
    def load_to_snowflake(rows: list[dict]):
        if not rows:
            return
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        cursor = conn.cursor()
        for r in rows:
            cursor.execute("""
                MERGE INTO raw.company_info AS t USING (
                    SELECT %s AS ticker, %s AS sector, %s AS industry,
                           %s AS market_cap, %s AS pe_ratio, %s AS description
                ) AS s ON t.ticker = s.ticker
                WHEN MATCHED THEN UPDATE SET
                    sector=s.sector, industry=s.industry, market_cap=s.market_cap,
                    pe_ratio=s.pe_ratio, description=s.description
                WHEN NOT MATCHED THEN INSERT
                    (ticker,sector,industry,market_cap,pe_ratio,description)
                VALUES (s.ticker,s.sector,s.industry,s.market_cap,s.pe_ratio,s.description)
            """, (r["ticker"],r["sector"],r["industry"],r["market_cap"],r["pe_ratio"],r["description"]))
        conn.commit()
        cursor.close()
        conn.close()

    rows = fetch_fundamentals()
    load_to_snowflake(rows)

ingest_fundamentals_dag()
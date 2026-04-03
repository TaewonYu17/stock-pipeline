from datetime import datetime, timedelta
import requests
import os
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

FRED_SERIES = {
    "VIXCLS":  "vix",          # CBOE Volatility Index
    "DGS10":   "treasury_10y", # 10-Year Treasury Yield
    "FEDFUNDS": "fed_rate",    # Federal Funds Rate
}

@dag(
    dag_id="ingest_macro",
    description="Fetch macro indicators from FRED → Snowflake",
    schedule="0 19 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion", "macro"],
)
def ingest_macro_dag():

    @task()
    def fetch_macro() -> list[dict]:
        api_key = os.getenv("FRED_API_KEY")
        
        # Add this — if the key is missing, fail loudly instead of silently
        if not api_key:
            raise ValueError("FRED_API_KEY environment variable is not set")
        
        rows = []
        for series_id, name in FRED_SERIES.items():
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={series_id}&api_key={api_key}"
                f"&file_type=json&limit=10&sort_order=desc"
            )
            response = requests.get(url, timeout=30)
            data = response.json()
            
            # Add this — log exactly what FRED sent back
            print(f"{series_id} response keys: {list(data.keys())}")
            print(f"{series_id} raw response: {data}")

            for obs in data.get("observations", []):
                if obs["value"] == ".":
                    print(f"{series_id} skipping missing value on {obs['date']}")
                    continue
                rows.append({
                    "series_id": series_id,
                    "indicator": name,
                    "date":      obs["date"],
                    "value":     float(obs["value"]),
                })

        print(f"Total rows fetched: {len(rows)}")
        return rows

    @task()
    def load_to_snowflake(rows: list[dict]):
        if not rows:
            return
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.economic_indicators (
                series_id   VARCHAR(20),
                indicator   VARCHAR(50),
                date        DATE,
                value       FLOAT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (series_id, date)
            )
        """)
        cursor.executemany("""
            MERGE INTO raw.economic_indicators AS t USING (
                SELECT %s AS series_id, %s AS indicator, %s AS date, %s AS value
            ) AS s ON t.series_id = s.series_id AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET value = s.value
            WHEN NOT MATCHED THEN INSERT (series_id, indicator, date, value)
            VALUES (s.series_id, s.indicator, s.date, s.value)
        """, [(r["series_id"], r["indicator"], r["date"], r["value"]) for r in rows])
        conn.commit()
        cursor.close()
        conn.close()

    rows = fetch_macro()
    load_to_snowflake(rows)

ingest_macro_dag()
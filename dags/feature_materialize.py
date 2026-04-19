from datetime import datetime, timedelta
from airflow.decorators import dag, task
import subprocess

@dag(
    dag_id="feature_materialize",
    description="Materialize latest features from Snowflake into Feast online store",
    schedule="0 21 * * 1-5",   # 9pm weekdays — after prices (6pm) and dbt (~8pm)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["feast", "features"],
)
def feature_materialize_dag():

    @task()
    def materialize():
        result = subprocess.run(
            ["feast", "materialize-incremental",
             datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")],
            cwd="/opt/airflow/feast/feature_repo",
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"Feast materialization failed: {result.stderr}")
        print("Materialization complete")

    materialize()

feature_materialize_dag()

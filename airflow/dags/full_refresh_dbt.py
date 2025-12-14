from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_CONTAINER = "dbt_spark"
PROJECT_DIR = "/workspace"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="dbt_full_refresh",
    description="Full refresh of all dbt models (recreate all tables)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "spark", "full_refresh", "youtube_trends"],
):

    full_refresh = BashOperator(
        task_id="dbt_full_refresh",
        bash_command=f"""
            (
                echo '============================';
                echo ' RUNNING DBT FULL REFRESH';
                echo '============================';

                docker exec {DBT_CONTAINER} \
                    dbt build --project-dir {PROJECT_DIR} --full-refresh
            ) || true
        """
    )
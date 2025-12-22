from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_CONTAINER = "dbt_spark_trino"
PROJECT_DIR = "/workspace"

with DAG(
    dag_id="dbt_clean",
    description="Clean dbt artifacts (target, dbt_packages, manifests)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "maintenance", "cleanup"],
):

    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command=(
            f"docker exec {DBT_CONTAINER} "
            f"bash -c '"
            f"cd {PROJECT_DIR} && "
            f"dbt clean"
            f"'"
        ),
    )
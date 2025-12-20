from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_CONTAINER = "dbt_spark"
PROJECT_DIR = "/workspace"

with DAG(
    dag_id="dbt_compile",
    description="Compile dbt models without running them",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "maintenance", "compile"],
):

    dbt_compile = BashOperator(
        task_id="dbt_compile",
        bash_command=(
            f"docker exec {DBT_CONTAINER} "
            f"bash -c '"
            f"cd {PROJECT_DIR} && "
            f"dbt compile"
            f"'"
        ),
    )
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_CONTAINER = "dbt_spark_trino"
PROJECT_DIR = "/workspace"

with DAG(
    dag_id="dbt_compile",
    description="Compile dbt models without running them",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "maintenance", "compile"],
):

    dbt_compile_spark = BashOperator(
        task_id="dbt_compile_spark",
        bash_command=(
            f"docker exec {DBT_CONTAINER} "
            f"bash -c '"
            f"cd {PROJECT_DIR} && "
            f"dbt compile --target spark --select tag:spark"
            f"'"
        ),
    )

    dbt_compile_trino = BashOperator(
        task_id="dbt_compile_trino",
        bash_command=(
            f"docker exec {DBT_CONTAINER} "
            f"bash -c '"
            f"cd {PROJECT_DIR} && "
            f"dbt compile --target trino --select tag:trino"
            f"'"
        ),
    )

    dbt_compile_spark >> dbt_compile_trino
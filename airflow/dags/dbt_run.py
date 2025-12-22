from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_CONTAINER = "dbt_spark_trino"
PROJECT_DIR = "/workspace"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="dbt_run",
    description="Run dbt models on Spark and Trino",
    start_date=datetime(2025, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "youtube_trends"],
):

    dbt_run_spark = BashOperator(
        task_id="dbt_run_spark",
        bash_command=(
            f"docker exec {DBT_CONTAINER} "
            f"dbt run "
            f"--project-dir {PROJECT_DIR} "
            f"--target spark "
            f"--select tag:spark"
        ),
    )

    trino_stage_videos_refresh = BashOperator(
        task_id="trino_stage_videos_refresh",
        bash_command="""
        docker exec trino trino \
          --server http://localhost:8080 \
          --catalog youtube_trends \
          --schema gold \
          --file /opt/trino/sql/stg_videos_refresh.sql
        """
    )

    dbt_run_postgres = BashOperator(
        task_id="dbt_run_postgres",
        bash_command=(
            f"docker exec {DBT_CONTAINER} "
            f"dbt run "
            f"--project-dir {PROJECT_DIR} "
            f"--target postgres "
            f"--select tag:postgres"
        ),
    )

    generate_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=(
            f"docker exec {DBT_CONTAINER} "
            f"dbt docs generate --project-dir {PROJECT_DIR}"
        ),
    )

    fix_docs = BashOperator(
        task_id="fix_docs_database",
        bash_command=f"""
        docker exec {DBT_CONTAINER} \
        bash /scripts/fix_dbt_docs_database.sh
        """,
    )

    dbt_run_spark >> trino_stage_videos_refresh >> dbt_run_postgres >> generate_docs >> fix_docs
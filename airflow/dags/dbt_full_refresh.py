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

    drop_seeds = BashOperator(
        task_id="drop_dim_regions_geo",
        bash_command=f"""
            docker exec {DBT_CONTAINER} \
            dbt run-operation drop_table_if_exists \
            --args '{{"relation": "silver.dim_regions_geo"}}'
        """
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"""
            docker exec {DBT_CONTAINER} \
              dbt seed --project-dir {PROJECT_DIR}
        """
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
            docker exec {DBT_CONTAINER} \
              dbt run --project-dir {PROJECT_DIR} --full-refresh
        """
    )

    generate_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=f"""
            docker exec {DBT_CONTAINER} \
            dbt docs generate --project-dir {PROJECT_DIR}
        """
    )

    fix_docs = BashOperator(
        task_id="fix_docs_database",
        bash_command="""
            docker exec dbt_spark /scripts/fix_dbt_docs_database.sh
        """,
    )

    drop_seeds >> dbt_seed >> dbt_run >> generate_docs >> fix_docs
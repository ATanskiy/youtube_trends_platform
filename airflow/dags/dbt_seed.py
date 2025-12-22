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
    dag_id="dbt_seed",
    description="Run dbt models on Spark every 5 minutes",
    start_date=datetime(2025, 1, 1),
    schedule=None, #"*/5 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "spark", "youtube_trends", "seeds"],
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
            (
                echo '--- Running dbt ---';
                docker exec {DBT_CONTAINER} \
                    dbt seed --project-dir {PROJECT_DIR}
            )
        """
    )

    drop_seeds >> dbt_seed
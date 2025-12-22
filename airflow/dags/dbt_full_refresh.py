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
    dag_id="dbt_full_refresh",
    description="Full refresh of Spark and Trino dbt models",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "full_refresh", "youtube_trends"],
):

    drop_dim_regions_geo = BashOperator(
        task_id="drop_dim_regions_geo",
        bash_command=f"""
            docker exec {DBT_CONTAINER} \
            dbt run-operation drop_table_if_exists \
            --args '{{"relation": "silver.dim_regions_geo"}}' \
            --target spark
        """
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"""
            docker exec {DBT_CONTAINER} \
            dbt seed \
              --project-dir {PROJECT_DIR} \
              --target spark
        """
    )

    dbt_full_refresh_spark = BashOperator(
        task_id="dbt_full_refresh_spark",
        bash_command=f"""
            docker exec {DBT_CONTAINER} \
            dbt run \
              --project-dir {PROJECT_DIR} \
              --target spark \
              --select tag:spark \
              --full-refresh
        """
    )

    trino_stage_videos_full_refresh = BashOperator(
        task_id="trino_stage_videos_refresh",
        bash_command="""
        docker exec trino trino \
          --server http://localhost:8080 \
          --catalog youtube_trends \
          --schema gold \
          --file /opt/trino/sql/stg_videos_full_refresh.sql
        """
    )

    dbt_full_refresh_postgres = BashOperator(
        task_id="dbt_full_refresh_postgres",
        bash_command=f"""
            docker exec {DBT_CONTAINER} \
            dbt run \
              --project-dir {PROJECT_DIR} \
              --target postgres \
              --select tag:postgres \
              --full-refresh
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
        bash_command=f"""
            docker exec {DBT_CONTAINER} \
            /scripts/fix_dbt_docs_database.sh
        """,
    )

    (    drop_dim_regions_geo >> 
         dbt_seed >> 
         dbt_full_refresh_spark >> 
         trino_stage_videos_full_refresh >> 
         dbt_full_refresh_postgres >> 
         generate_docs >> fix_docs
    )
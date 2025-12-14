from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "spark_streaming"

with DAG(
    dag_id="spark_run_batch_languages",
    description="Run Spark Batch job safely to load languages data",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "spark", "youtube_trends", "languages"],
):

    run_streaming = BashOperator(
        task_id="spark_run_batch_languages",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} "
            "/opt/spark/bin/spark-submit "
            "/opt/streaming/jobs/main.py --name languages "
        )
    )
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "spark_streaming"

with DAG(
    dag_id="spark_run_streaming_videos",
    description="Run Spark Streaming job safely to load categories data",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["streaming", "spark", "youtube_trends", "videos"],
):

    run_streaming = BashOperator(
        task_id="spark_run_streaming_videos",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} "
            "/opt/spark/bin/spark-submit "
            "/opt/streaming/jobs/main.py --name videos "
            "|| [ $? -eq 143 ] || [ $? -eq 130 ] || exit $?"
        )
    )
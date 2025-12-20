from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "spark_streaming"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
JOB_MAIN = "/opt/streaming/jobs/main.py"

with DAG(
    dag_id="spark_upsert_regions_categories_languages",
    description="Run Spark batch jobs for YouTube reference tables",
    start_date=datetime(2025, 1, 1),
    schedule="0 1,13 * * *",
    catchup=False,
    tags=["spark", "batch", "youtube_trends", "reference_data"],
):

    def spark_task(name: str):
        return BashOperator(
            task_id=f"spark_{name}",
            bash_command=(
                f"docker exec {SPARK_CONTAINER} "
                f"{SPARK_SUBMIT} "
                f"{JOB_MAIN} --name {name} "
                "|| [ $? -eq 143 ] || [ $? -eq 130 ] || exit $?"
            ),
            retries=1,
            retry_delay=timedelta(minutes=2),
        )

    spark_regions = spark_task("regions")
    spark_categories = spark_task("categories")
    spark_languages = spark_task("languages")

    spark_regions >> spark_categories >> spark_languages
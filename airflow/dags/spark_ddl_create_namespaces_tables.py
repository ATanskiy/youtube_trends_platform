from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "spark_streaming"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
JOB_MAIN = "/opt/streaming/jobs/main.py"

with DAG(
    dag_id="spark_ddl_create_namespaces_tables",
    description="create namespaces and tables in iceberg for youtube trends platform",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "ddl", "youtube_trends", "create_namespaces_tables"],
):

    def spark_ddl(name: str):
        return BashOperator(
            task_id=f"spark_{name}",
            bash_command=(
                f"docker exec {SPARK_CONTAINER} "
                f"{SPARK_SUBMIT} "
                f"{JOB_MAIN} --name {name} "
            ),
            retries=1,
            retry_delay=timedelta(minutes=2),
        )

    spark_ddl("ddl")
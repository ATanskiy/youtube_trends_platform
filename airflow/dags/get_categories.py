from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

CONTAINER_NAME = "python_youtube_trends"

with DAG(
    dag_id="get_categories",
    description="Conduct an API call to get YouTube video categories",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["youtube_trends", "get_categories"],
):

    run_generator = BashOperator(
        task_id="get_categories",
        bash_command=f"""
            docker exec {CONTAINER_NAME} \
            python -u /app/youtube_trends_project/main.py --name categories \
            || true
        """
    )
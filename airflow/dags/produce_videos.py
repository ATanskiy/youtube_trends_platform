from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

CONTAINER_NAME = "python_youtube_trends"
APP_MAIN = "/app/youtube_trends_project/main.py"

with DAG(
    dag_id="produce_videos",
    description="Conduct an API call to get YouTube videos data",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["youtube_trends", "produce_videos"],
):

    run_generator = BashOperator(
        task_id="get_videos",
        bash_command=f"""
            docker exec {CONTAINER_NAME} \
            python -u {APP_MAIN} --name videos 
        """
    )
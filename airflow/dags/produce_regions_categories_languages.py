from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

CONTAINER_NAME = "python_youtube_trends"
APP_MAIN = "/app/youtube_trends_project/main.py"

def api_task(name: str) -> BashOperator:
    return BashOperator(
        task_id=f"get_{name}",
        bash_command=(
            f"docker exec {CONTAINER_NAME} "
            f"python -u {APP_MAIN} --name {name} "
        ),
    )

with DAG(
    dag_id="produce_regions_categories_languages",
    description="Fetch YouTube reference data via API",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["youtube_trends", "produce_regions", "produce_categories", "produce_languages"],
):

    produce_regions = api_task("regions")
    produce_categories = api_task("categories")
    produce_languages = api_task("languages")

    produce_regions >> produce_categories >> produce_languages
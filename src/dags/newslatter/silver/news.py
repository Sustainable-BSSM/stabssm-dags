import os

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

NEWSLATTER_BRONZE_NEWS = Dataset("newslatter/bronze/news")
NEWSLATTER_SILVER_NEWS = Dataset("newslatter/silver/news")

S3_ENV = {
    "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
    "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
    "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
    "S3_REGION": os.environ.get("S3_REGION"),
    "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY"),
    "AWS_DEFAULT_REGION": os.environ.get("S3_REGION"),
}

with DAG(
        dag_id="silver__transform_newslatter_news",
        start_date=datetime(2024, 1, 1, tz="Asia/Seoul"),
        schedule=[NEWSLATTER_BRONZE_NEWS],
        catchup=False,
        max_active_runs=1,
        tags=["newslatter"],
):
    def _get_week(inlet_events):
        events = list(inlet_events[NEWSLATTER_BRONZE_NEWS])
        for event in reversed(events):
            if "week" in event.extra:
                return event.extra["week"]
            if "crawled_week" in event.extra:
                return event.extra["crawled_week"]
        raise ValueError(f"No 'week' in inlet events. extras={[e.extra for e in events]}")

    get_week = PythonOperator(
        task_id="get_week",
        python_callable=_get_week,
        inlets=[NEWSLATTER_BRONZE_NEWS],
    )

    transform_and_upload = DockerOperator(
        task_id="transform_and_upload",
        image="stabssm-jobs:latest",
        command=[
            "src.jobs.newslatter.silver.transform_news_parquet",
            "--week", "{{ ti.xcom_pull(task_ids='get_week') }}",
        ],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment=S3_ENV,
    )

    def _emit_silver_event(outlet_events, ti):
        week = ti.xcom_pull(task_ids="get_week")
        outlet_events[NEWSLATTER_SILVER_NEWS].extra = {"week": week}

    emit_silver_event = PythonOperator(
        task_id="emit_silver_event",
        python_callable=_emit_silver_event,
        outlets=[NEWSLATTER_SILVER_NEWS],
    )

    get_week >> transform_and_upload >> emit_silver_event

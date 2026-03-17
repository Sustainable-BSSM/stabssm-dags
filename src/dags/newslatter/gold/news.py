import os

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

NEWSLATTER_SILVER_NEWS = Dataset("newslatter/silver/news")
NEWSLATTER_GOLD_NEWS = Dataset("newslatter/gold/news")

ENV = {
    "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
    "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
    "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
    "S3_REGION": os.environ.get("S3_REGION"),
    "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY"),
    "AWS_DEFAULT_REGION": os.environ.get("S3_REGION"),
    "OPENROUTER_API_KEY": os.environ.get("OPENROUTER_API_KEY"),
}

with DAG(
        dag_id="gold__curate_newslatter_news",
        start_date=datetime(2024, 1, 1, tz="Asia/Seoul"),
        schedule=[NEWSLATTER_SILVER_NEWS],
        catchup=False,
        max_active_runs=1,
        tags=["newslatter"],
):
    def _get_week(inlet_events):
        events = inlet_events[NEWSLATTER_SILVER_NEWS]
        for event in events:
            if "week" in event.extra:
                return event.extra["week"]
        raise ValueError(f"No 'week' in inlet events. extras={[e.extra for e in events]}")

    get_week = PythonOperator(
        task_id="get_week",
        python_callable=_get_week,
        inlets=[NEWSLATTER_SILVER_NEWS],
    )

    curate_news = DockerOperator(
        task_id="curate_news",
        image="stabssm-jobs:latest",
        command=[
            "src.jobs.newslatter.gold.curate_news",
            "--week", "{{ ti.xcom_pull(task_ids='get_week') }}",
        ],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment=ENV,
    )

    def _emit_gold_event(outlet_events, ti):
        week = ti.xcom_pull(task_ids="get_week")
        outlet_events[NEWSLATTER_GOLD_NEWS].extra = {"week": week}

    emit_gold_event = PythonOperator(
        task_id="emit_gold_event",
        python_callable=_emit_gold_event,
        outlets=[NEWSLATTER_GOLD_NEWS],
    )

    get_week >> curate_news >> emit_gold_event

import os

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

NAVER_BRONZE_NEWS = Dataset("newslatter/bronze/news")

S3_ENV = {
    "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
    "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
    "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
    "S3_REGION": os.environ.get("S3_REGION"),
    "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY"),
    "AWS_DEFAULT_REGION": os.environ.get("S3_REGION"),
    "NAVER_NEWS_CLIENT_ID": os.environ.get("NAVER_NEWS_CLIENT_ID"),
    "NAVER_NEWS_SECRET": os.environ.get("NAVER_NEWS_SECRET"),
}

with DAG(
        dag_id="bronze__collect_naver_news",
        start_date=datetime(2024, 1, 1, tz="Asia/Seoul"),
        schedule="@weekly",
        catchup=False,
        max_active_runs=1,
) as dag:

    collect_news = DockerOperator(
        task_id="collect_news",
        image="stabssm-jobs:latest",
        command=["src.jobs.newslatter.bronze.collect_news_upload_storage"],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment=S3_ENV,
    )

    def _emit_bronze_event(outlet_events, data_interval_start):
        week = data_interval_start.in_timezone("Asia/Seoul").strftime("%G-%V")
        outlet_events[NAVER_BRONZE_NEWS].extra = {"crawled_week": week}

    emit_bronze_event = PythonOperator(
        task_id="emit_bronze_event",
        python_callable=_emit_bronze_event,
        outlets=[NAVER_BRONZE_NEWS],
    )

    collect_news >> emit_bronze_event

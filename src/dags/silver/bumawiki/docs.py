import os

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime

BUMAWIKI_BRONZE_DOCS = Dataset("bumawiki/bronze/docs")
BUMAWIKI_SILVER_DOCS = Dataset("bumawiki/silver/docs")

with DAG(
        dag_id="silver__transform_bumawiki_docs",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule=[BUMAWIKI_BRONZE_DOCS],
        catchup=False,
        max_active_runs=1,
):
    transform_and_upload = DockerOperator(
        task_id="transform_and_upload",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.silver.transform_docs_detail_parquet --ds {{ ds }}",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        outlets=[BUMAWIKI_SILVER_DOCS],
        environment={
            "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
            "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
            "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
            "S3_REGION": os.environ.get("S3_REGION"),
        },
    )

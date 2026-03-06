import os

from airflow import DAG
from pendulum import datetime
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id="bronze__collect_bumawiki_accident",
    schedule="@monthly",
    start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
):
    crawl_and_upload = DockerOperator(
        task_id="crawl_and_upload",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.bronze.collect_accident_upload_storage --ds {{ ds }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment={
            "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
            "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
            "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
            "S3_REGION": os.environ.get("S3_REGION"),
        },
    )

import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime

with DAG(
        dag_id="silver__transform_bumawiki_docs",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule="@monthly",
        catchup=False,
        max_active_runs=1,
):
    transform_and_upload = DockerOperator(
        task_id="transform_and_upload",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.silver.transform_docs_detail_parquet --ds {{ ds }}",
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

import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime

with DAG(
        dag_id="maintenance__iceberg_gc",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule="@weekly",
        catchup=False,
        max_active_runs=1,
        tags=["maintenance"],
        params={"older_than_days": 7},
):
    iceberg_gc = DockerOperator(
        task_id="iceberg_gc",
        image="stabssm-jobs:latest",
        command=[
            "src.jobs.maintenance.iceberg_gc",
            "--older-than-days", "{{ params.older_than_days }}",
        ],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment={
            "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY"),
            "AWS_DEFAULT_REGION": os.environ.get("S3_REGION"),
            "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
            "GLUE_SILVER_DATABASE": os.environ.get("GLUE_SILVER_DATABASE"),
            "GLUE_GOLD_DATABASE": os.environ.get("GLUE_GOLD_DATABASE"),
            "ICEBERG_WAREHOUSE": os.environ.get("ICEBERG_WAREHOUSE"),
        },
    )

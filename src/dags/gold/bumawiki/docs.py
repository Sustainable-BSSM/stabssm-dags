import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from pendulum import datetime

with DAG(
        dag_id="gold__build_bumawiki_docs_graph",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule="@monthly",
        catchup=False,
        max_active_runs=1,
):
    # wait_for_silver = ExternalTaskSensor(
    #     task_id="wait_for_silver",
    #     external_dag_id="silver__transform_bumawiki_docs",
    #     external_task_id=None,
    #     mode="reschedule",
    # )

    build_graph = DockerOperator(
        task_id="build_graph",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.gold.build_docs_graph --ds {{ ds }}",
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

    # wait_for_silver >> build_graph

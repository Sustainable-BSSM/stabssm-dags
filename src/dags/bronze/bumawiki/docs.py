import os
import shlex
from airflow import DAG
from airflow.decorators import task
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime


with DAG(
    dag_id="bronze__collect_bumawiki_docs",
    start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
):
    list_docs_titles = DockerOperator(
        task_id="list_docs_titles",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.bronze.get_docs_titles --ds {{ ds }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        do_xcom_push=True,
        xcom_all=True,
        environment={
            "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
            "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
            "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
            "S3_REGION": os.environ.get("S3_REGION"),
        },
    )

    @task
    def make_commands(titles: list, ds: str = None) -> list:
        return [
            f"src.jobs.bumawiki.bronze.collect_docs_upload_storage --ds {ds} --title {shlex.quote(t)}"
            for t in titles
        ]

    commands = make_commands(list_docs_titles.output)

    crawl_and_upload = DockerOperator.partial(
        task_id="crawl_and_write",
        image="stabssm-jobs:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment={
            "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
            "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
            "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
            "S3_REGION": os.environ.get("S3_REGION"),
        },
    ).expand(command=commands)

    list_docs_titles >> commands >> crawl_and_upload

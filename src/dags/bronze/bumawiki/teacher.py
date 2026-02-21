from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime

with DAG(
    dag_id="bronze__collect_bumawiki_teacher",
    start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
):
    crawl_and_upload = DockerOperator(
        task_id="crawl_and_upload",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.collect_teacher_upload_storage --ds {{ ds }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
    )
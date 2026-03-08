import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime

with DAG(
        dag_id="bronze__collect_bumawiki_student",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule="@weekly",
        catchup=False,
        max_active_runs=1,
) as dag:
    # TODO: 부마위키 학생 정보 크롤링 후 s3 업로드
    crawl_and_upload = DockerOperator(
        task_id="crawl_and_upload",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.bronze.collect_student_upload_storage --ds {{ ds }}",
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

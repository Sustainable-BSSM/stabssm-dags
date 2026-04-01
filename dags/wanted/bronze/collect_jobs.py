from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime

with DAG(
        dag_id="bronze__collect_wanted_jobs",
        start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,
        tags=["wanted"],
) as dag:

    collect_jobs = DockerOperator(
        task_id="collect_jobs",
        image="stabssm-jobs:latest",
        command=[
            "src.jobs.wanted.bronze.collect_jobs_upload_storage",
            "--ds", "{{ ds }}",
        ],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment={
            "S3_ACCESS_KEY": "{{ var.value.S3_ACCESS_KEY }}",
            "S3_SECRET_KEY": "{{ var.value.S3_SECRET_KEY }}",
            "S3_BUCKET_NAME": "{{ var.value.S3_BUCKET_NAME }}",
            "S3_REGION": "{{ var.value.S3_REGION }}",
            "AWS_ACCESS_KEY_ID": "{{ var.value.S3_ACCESS_KEY }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.S3_SECRET_KEY }}",
            "AWS_DEFAULT_REGION": "{{ var.value.S3_REGION }}",
        },
    )

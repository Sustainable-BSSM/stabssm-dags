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
            "AWS_ACCESS_KEY_ID": "{{ var.value.S3_ACCESS_KEY }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.S3_SECRET_KEY }}",
            "AWS_DEFAULT_REGION": "{{ var.value.S3_REGION }}",
            "S3_ACCESS_KEY": "{{ var.value.S3_ACCESS_KEY }}",
            "S3_SECRET_KEY": "{{ var.value.S3_SECRET_KEY }}",
            "S3_BUCKET_NAME": "{{ var.value.S3_BUCKET_NAME }}",
            "S3_REGION": "{{ var.value.S3_REGION }}",
            "GLUE_SILVER_DATABASE": "{{ var.value.GLUE_SILVER_DATABASE }}",
            "GLUE_GOLD_DATABASE": "{{ var.value.GLUE_GOLD_DATABASE }}",
            "ICEBERG_WAREHOUSE": "{{ var.value.ICEBERG_WAREHOUSE }}",
        },
    )

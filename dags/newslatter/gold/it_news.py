import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

ENV = {
    "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
    "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
    "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
    "S3_REGION": os.environ.get("S3_REGION"),
    "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY"),
    "AWS_DEFAULT_REGION": os.environ.get("S3_REGION"),
    "OPENROUTER_API_KEY": os.environ.get("OPENROUTER_API_KEY"),
}

with DAG(
        dag_id="gold__curate_newslatter_it_news",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule="@weekly",
        catchup=False,
        max_active_runs=1,
        tags=["newslatter"],
        params={"week": ""},
):
    def _compute_week(data_interval_start, dag_run):
        if dag_run.conf and (week := dag_run.conf.get("week")):
            return week
        dt = data_interval_start.in_timezone("Asia/Seoul")
        week_of_month = (dt.day - 1) // 7 + 1
        return f"{dt.year}-{dt.month:02d}-{week_of_month:02d}"

    compute_week = PythonOperator(
        task_id="compute_week",
        python_callable=_compute_week,
    )

    curate_news = DockerOperator(
        task_id="curate_news",
        image="stabssm-jobs:latest",
        command=[
            "src.jobs.newslatter.gold.curate_it_news",
            "--week", "{{ ti.xcom_pull(task_ids='compute_week') }}",
        ],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment=ENV,
    )

    compute_week >> curate_news

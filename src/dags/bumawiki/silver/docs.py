import os

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

BUMAWIKI_BRONZE_DOCS = Dataset("bumawiki/bronze/docs")
BUMAWIKI_SILVER_DOCS = Dataset("bumawiki/silver/docs")

with DAG(
        dag_id="silver__transform_bumawiki_docs",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule=[BUMAWIKI_BRONZE_DOCS],
        tags=["bumawiki"],
        catchup=False,
        max_active_runs=1,
):
    def _get_ds(inlet_events):
        events = inlet_events[BUMAWIKI_BRONZE_DOCS]
        for event in events:
            if "ds" in event.extra:
                return event.extra["ds"]
        raise ValueError(f"No 'ds' found in inlet events. extras={[e.extra for e in events]}")

    get_ds = PythonOperator(
        task_id="get_ds",
        python_callable=_get_ds,
        inlets=[BUMAWIKI_BRONZE_DOCS],
    )

    transform_and_upload = DockerOperator(
        task_id="transform_and_upload",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.silver.transform_docs_detail_parquet --ds {{ ti.xcom_pull(task_ids='get_ds') }}",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment={
            "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
            "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
            "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
            "S3_REGION": os.environ.get("S3_REGION"),
            "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY"),
            "AWS_DEFAULT_REGION": os.environ.get("S3_REGION"),
        },
    )

    def _emit_silver_event(outlet_events, ti):
        outlet_events[BUMAWIKI_SILVER_DOCS].extra = {"ds": ti.xcom_pull(task_ids="get_ds")}

    emit_silver_event = PythonOperator(
        task_id="emit_silver_event",
        python_callable=_emit_silver_event,
        outlets=[BUMAWIKI_SILVER_DOCS],
    )

    get_ds >> transform_and_upload >> emit_silver_event

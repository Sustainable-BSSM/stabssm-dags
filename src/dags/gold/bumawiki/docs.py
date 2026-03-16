import os

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

BUMAWIKI_SILVER_DOCS = Dataset("bumawiki/silver/docs")
BUMAWIKI_GOLD_DOCS_GRAPH = Dataset("bumawiki/gold/docs-graph")

with DAG(
        dag_id="gold__build_bumawiki_docs_graph",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule=[BUMAWIKI_SILVER_DOCS],
        catchup=False,
        max_active_runs=1,
):
    def _get_ds(inlet_events):
        events = inlet_events[BUMAWIKI_SILVER_DOCS]
        for event in events:
            if "ds" in event.extra:
                return event.extra["ds"]
        raise ValueError(f"No 'ds' found in inlet events. extras={[e.extra for e in events]}")

    get_ds = PythonOperator(
        task_id="get_ds",
        python_callable=_get_ds,
        inlets=[BUMAWIKI_SILVER_DOCS],
    )

    build_graph = DockerOperator(
        task_id="build_graph",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.gold.build_docs_graph --ds {{ ti.xcom_pull(task_ids='get_ds') }}",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        outlets=[BUMAWIKI_GOLD_DOCS_GRAPH],
        environment={
            "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
            "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
            "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
            "S3_REGION": os.environ.get("S3_REGION"),
            "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY"),
            "AWS_DEFAULT_REGION": os.environ.get("S3_REGION"),
            "OPENROUTER_API_KEY": os.environ.get("OPENROUTER_API_KEY"),
        },
    )

    get_ds >> build_graph

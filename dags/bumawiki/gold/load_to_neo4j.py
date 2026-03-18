import os

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

BUMAWIKI_GOLD_DOCS_GRAPH = Dataset("bumawiki/gold/docs-graph")

with DAG(
        dag_id="gold__load_bumawiki_graph_to_neo4j",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule=[BUMAWIKI_GOLD_DOCS_GRAPH],
        catchup=False,
        max_active_runs=1,
        tags=["bumawiki"],
        params={"ds": ""},
):
    def _get_ds(inlet_events, params):
        events = inlet_events[BUMAWIKI_GOLD_DOCS_GRAPH]
        for event in events:
            if "ds" in event.extra:
                return event.extra["ds"]
        if params.get("ds"):
            return params["ds"]
        raise ValueError("No 'ds' found in inlet events or params")

    get_ds = PythonOperator(
        task_id="get_ds",
        python_callable=_get_ds,
        inlets=[BUMAWIKI_GOLD_DOCS_GRAPH],
    )

    load_to_neo4j = DockerOperator(
        task_id="load_to_neo4j",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.gold.load_graph_to_neo4j --ds {{ ti.xcom_pull(task_ids='get_ds') }}",
        docker_url="unix:///var/run/docker.sock",
        network_mode="stabssm-dags_stabssm-net",
        mount_tmp_dir=False,
        environment={
            "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
            "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
            "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
            "S3_REGION": os.environ.get("S3_REGION"),
            "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY"),
            "AWS_DEFAULT_REGION": os.environ.get("S3_REGION"),
            "NEO4J_URI": os.environ.get("NEO4J_URI"),
            "NEO4J_USER": os.environ.get("NEO4J_USER"),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD"),
        },
    )

    get_ds >> load_to_neo4j

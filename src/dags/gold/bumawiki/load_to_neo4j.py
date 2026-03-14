import os

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime

BUMAWIKI_GOLD_DOCS_GRAPH = Dataset("bumawiki/gold/docs-graph")

with DAG(
        dag_id="gold__load_bumawiki_graph_to_neo4j",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule=[BUMAWIKI_GOLD_DOCS_GRAPH],
        catchup=False,
        max_active_runs=1,
):
    load_to_neo4j = DockerOperator(
        task_id="load_to_neo4j",
        image="stabssm-jobs:latest",
        command="src.jobs.bumawiki.gold.load_graph_to_neo4j --ds {{ ds }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="stabssm-dags_stabssm-net",
        mount_tmp_dir=False,
        environment={
            "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY"),
            "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY"),
            "S3_BUCKET_NAME": os.environ.get("S3_BUCKET_NAME"),
            "S3_REGION": os.environ.get("S3_REGION"),
            "NEO4J_URI": os.environ.get("NEO4J_URI"),
            "NEO4J_USER": os.environ.get("NEO4J_USER"),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD"),
        },
    )

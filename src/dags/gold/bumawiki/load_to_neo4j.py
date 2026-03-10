import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from pendulum import datetime

with DAG(
        dag_id="gold__load_bumawiki_graph_to_neo4j",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule="@monthly",
        catchup=False,
        max_active_runs=1,
):
    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_gold",
        external_dag_id="gold__build_bumawiki_docs_graph",
        external_task_id=None,
        mode="reschedule",
    )

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

    wait_for_gold >> load_to_neo4j

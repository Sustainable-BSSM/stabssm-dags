import json

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

BUMAWIKI_BRONZE_DOCS = Dataset("bumawiki/bronze/docs")

S3_ENV = {
    "S3_ACCESS_KEY": "{{ var.value.S3_ACCESS_KEY }}",
    "S3_SECRET_KEY": "{{ var.value.S3_SECRET_KEY }}",
    "S3_BUCKET_NAME": "{{ var.value.S3_BUCKET_NAME }}",
    "S3_REGION": "{{ var.value.S3_REGION }}",
    "AWS_ACCESS_KEY_ID": "{{ var.value.S3_ACCESS_KEY }}",
    "AWS_SECRET_ACCESS_KEY": "{{ var.value.S3_SECRET_KEY }}",
    "AWS_DEFAULT_REGION": "{{ var.value.S3_REGION }}",
}

with DAG(
        dag_id="bronze__collect_bumawiki",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule="@monthly",
        tags=["bumawiki"],
        catchup=False,
        max_active_runs=1,
        params={"ds": ""},
) as dag:

    def _compute_ds(data_interval_start, params):
        if params.get("ds"):
            return params["ds"]
        return data_interval_start.in_timezone("Asia/Seoul").strftime("%Y-%m-%d")

    compute_ds = PythonOperator(
        task_id="compute_ds",
        python_callable=_compute_ds,
    )

    start = EmptyOperator(task_id="start")

    collect_student = DockerOperator(
        task_id="collect_student",
        image="stabssm-jobs:latest",
        command="src.jobs.schoolwiki.bronze.get_student_docs",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        do_xcom_push=True,
        environment=S3_ENV,
    )

    collect_teacher = DockerOperator(
        task_id="collect_teacher",
        image="stabssm-jobs:latest",
        command="src.jobs.schoolwiki.bronze.get_teacher_docs",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        do_xcom_push=True,
        environment=S3_ENV,
    )

    collect_club = DockerOperator(
        task_id="collect_club",
        image="stabssm-jobs:latest",
        command="src.jobs.schoolwiki.bronze.get_club_docs",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        do_xcom_push=True,
        environment=S3_ENV,
    )

    collect_accident = DockerOperator(
        task_id="collect_accident",
        image="stabssm-jobs:latest",
        command="src.jobs.schoolwiki.bronze.get_incident_docs",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        do_xcom_push=True,
        environment=S3_ENV,
    )

    def _merge_titles(ti):
        all_titles = []
        for task_id in ["collect_student", "collect_teacher", "collect_club", "collect_accident"]:
            raw = ti.xcom_pull(task_ids=task_id)
            if raw:
                all_titles.extend(json.loads(raw))
        return json.dumps(all_titles)

    merge_titles = PythonOperator(
        task_id="merge_titles",
        python_callable=_merge_titles,
    )

    collect_docs = DockerOperator(
        task_id="collect_docs",
        image="stabssm-jobs:latest",
        command=[
            "src.jobs.schoolwiki.bronze.collect_docs_upload_storage",
            "--ds", "{{ ti.xcom_pull(task_ids='compute_ds') }}",
            "--docs", "{{ ti.xcom_pull(task_ids='merge_titles') }}",
        ],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment=S3_ENV,
    )

    def _emit_bronze_event(outlet_events, ti):
        ds = ti.xcom_pull(task_ids="compute_ds")
        outlet_events[BUMAWIKI_BRONZE_DOCS].extra = {"ds": ds}

    emit_bronze_event = PythonOperator(
        task_id="emit_bronze_event",
        python_callable=_emit_bronze_event,
        outlets=[BUMAWIKI_BRONZE_DOCS],
    )

    start >> compute_ds
    start >> [collect_student, collect_teacher, collect_club, collect_accident] >> merge_titles
    [compute_ds, merge_titles] >> collect_docs >> emit_bronze_event

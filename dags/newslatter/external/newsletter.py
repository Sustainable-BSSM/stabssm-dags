from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

with DAG(
        dag_id="external__generate_newslatter_pdf",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule="@weekly",
        catchup=False,
        max_active_runs=1,
        tags=["newslatter", "external"],
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

    generate_pdf = DockerOperator(
        task_id="generate_pdf",
        image="stabssm-jobs:latest",
        command=[
            "src.jobs.newslatter.external.generate_newsletter",
            "--week", "{{ ti.xcom_pull(task_ids='compute_week') }}",
        ],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment={
            "AWS_ACCESS_KEY_ID": "{{ var.value.S3_ACCESS_KEY }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.S3_SECRET_KEY }}",
            "AWS_DEFAULT_REGION": "{{ var.value.S3_REGION }}",
            "S3_BUCKET_NAME": "{{ var.value.S3_BUCKET_NAME }}",
            "OPENROUTER_API_KEY": "{{ var.value.OPENROUTER_API_KEY }}",
            "GOOGLE_SERVICE_ACCOUNT_JSON": "{{ var.value.GOOGLE_SERVICE_ACCOUNT_JSON }}",
        },
    )

    compute_week >> generate_pdf

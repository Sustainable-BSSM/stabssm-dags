from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

with DAG(
        dag_id="bronze__collect_naver_school_news",
        start_date=datetime(2020, 1, 1, tz="Asia/Seoul"),
        schedule="@weekly",
        catchup=False,
        max_active_runs=1,
        tags=["newslatter"],
) as dag:

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

    collect_news = DockerOperator(
        task_id="collect_news",
        image="stabssm-jobs:latest",
        command=[
            "src.jobs.newslatter.bronze.collect_news_upload_storage",
            "--week", "{{ ti.xcom_pull(task_ids='compute_week') }}",
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
            "NAVER_NEWS_CLIENT_ID": "{{ var.value.NAVER_NEWS_CLIENT_ID }}",
            "NAVER_NEWS_SECRET": "{{ var.value.NAVER_NEWS_SECRET }}",
        },
    )

    compute_week >> collect_news

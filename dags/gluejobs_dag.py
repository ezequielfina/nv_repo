from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow import DAG
from datetime import datetime, timedelta

import sys
sys.path.insert(0, "/opt/airflow")

from dags.infra.slack_alert import send_slack_alert

default_args = {
"owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": send_slack_alert,
}

with DAG(
    dag_id="glue_job_dag",
    description="Escucha en bucket S3 y dispara glue job",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    ejecutar_job_glue = GlueJobOperator(
        task_id="correr_gj",
        job_name="gj_nv",
        aws_conn_id="aws_default",
        region_name='us-east-2',
        wait_for_completion=True,
        verbose=False,
        script_args={
            "--fecha": "2026-06-25",
            "--ambiente": "dev"
        }
    )


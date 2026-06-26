from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import DAG
from datetime import datetime, timedelta


default_args = {
"owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False
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
        verbose=True,
        script_args={
            "--fecha": "2026-06-25",
            "--ambiente": "dev"
        }
    )


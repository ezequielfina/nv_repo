"""
dags/sales_pipeline.py
DAG principal del pipeline de ventas.

Flujo:
    wait_for_file (S3KeySensor) → transform → load → quality_check

El pipeline se dispara cuando detecta un archivo nuevo en:
    s3://<bucket>/raw/sales/<fecha>/sales_<fecha>.csv

Para probarlo: subí manualmente un CSV a esa ruta en S3.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

import sys
sys.path.insert(0, "/opt/airflow")

from tasks.transform import run as transform_run
from tasks.load import run as load_run
from tasks.quality_check import run as quality_check_run
from dags.infra.slack_alert import send_slack_alert


S3_BUCKET = os.environ.get("S3_BUCKET", "")
RAW_KEY_TEMPLATE = "raw/sales/{{ ds }}/sales_{{ ds }}.csv"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": send_slack_alert,
}

with DAG(
    dag_id="sales_pipeline",
    description="Pipeline ETL de ventas — se activa cuando llega un archivo a S3 raw/",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",  # chequea todos los días a las 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sales", "etl", "monitored"],
) as dag:

    wait_for_file = S3KeySensor(
        task_id="wait_for_raw_file",
        bucket_name=S3_BUCKET,
        bucket_key=RAW_KEY_TEMPLATE,
        aws_conn_id="aws_default",
        poke_interval=60,       # chequea S3 cada 60 segundos
        timeout=3600,           # falla si después de 1 hora no llegó el archivo
        mode="poke",            # se queda ocupando un worker mientras espera
        soft_fail=False,        # si timeout → FAILED (no skipped)
        doc_md="""
        ### Wait for raw file
        Espera que aparezca el archivo CSV en la capa raw de S3:
        `raw/sales/<fecha>/sales_<fecha>.csv`

        - Chequea cada 60 segundos
        - Timeout: 1 hora
        - Para probar: subí un CSV manualmente al bucket en esa ruta
        """,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_run,
        doc_md="""
        ### Transform
        Lee el CSV crudo de S3, valida y limpia los datos.
        Las filas rechazadas van a `quarantine/` en S3 como evidencia.
        **Output XCom:** `processed_key`, `valid_count`, `rejected_count`
        """,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_run,
        doc_md="""
        ### Load
        Inserta los datos procesados en Postgres (idempotente via ON CONFLICT DO NOTHING).
        **Input XCom:** `processed_key` de transform
        **Output XCom:** `inserted_count`
        """,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check_run,
        doc_md="""
        ### Quality Check
        Valida los datos ya cargados en Postgres:
        - Sin clientes nulos
        - Sin cantidades negativas
        - Mínimo 50 registros cargados
        - Sin order_ids duplicados

        Si algún check falla, el DAG queda en FAILED y el reporte
        de evidencia queda en `quality-reports/` en S3.
        """,
    )

    wait_for_file >> transform >> load >> quality_check
"""
tasks/load.py
Lee el CSV procesado de S3 e inserta los registros en Postgres via SalesDbHook.
Usa INSERT ... ON CONFLICT DO NOTHING para idempotencia.
"""

import os
import csv
import boto3
import sys

from io import StringIO
from datetime import date

sys.path.insert(0, "/opt/airflow")
from infra.cloudwatch_logger import get_logger
from infra.db_hook import SalesDbHook

TASK_NAME = "load"


def read_processed(bucket: str, s3_key: str) -> list[dict]:
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    content = response["Body"].read().decode("utf-8")
    return list(csv.DictReader(StringIO(content)))


def run(**context):
    log = get_logger(TASK_NAME)
    run_date = context.get("ds", date.today().isoformat())
    bucket = os.environ["S3_BUCKET"]

    processed_key = context["ti"].xcom_pull(task_ids="transform", key="processed_key")
    log.info(f"Leyendo procesados desde s3://{bucket}/{processed_key}")

    rows = read_processed(bucket, processed_key)
    log.info(f"Filas a cargar: {len(rows)}")

    try:
        hook = SalesDbHook()
        inserted = hook.insert_rows_idempotent(rows)
    except Exception as e:
        log.error(f"Error al cargar en Postgres: {e}")
        raise

    log.info(f"Filas insertadas: {inserted} / {len(rows)} (el resto ya existían)")
    context["ti"].xcom_push(key="inserted_count", value=inserted)

    return inserted

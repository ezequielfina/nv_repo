"""
tasks/transform.py
Lee el CSV crudo de S3, aplica transformaciones y validaciones básicas,
y sube el resultado limpio a S3 (capa processed).

Errores simulados: con FORCE_TRANSFORM_ERROR=true falla intencionalmente
para demostrar el flujo de triage.
"""

import os
import csv
import boto3
import sys

from io import StringIO
from datetime import date

sys.path.insert(0, "/opt/airflow")
from infra.cloudwatch_logger import get_logger

TASK_NAME = "transform"


def read_from_s3(bucket: str, s3_key: str) -> list[dict]:
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    content = response["Body"].read().decode("latin-1")
    reader = csv.DictReader(StringIO(content))
    return list(reader)


def transform_rows(rows: list[dict], log) -> tuple[list[dict], list[dict]]:
    """
    Aplica transformaciones y separa filas válidas de inválidas.
    Retorna (valid_rows, rejected_rows).
    """
    valid, rejected = [], []

    for row in rows:
        issues = []

        # Validaciones
        if not row.get("customer"):
            issues.append("customer es null")
        if not row.get("order_id"):
            issues.append("order_id es null")
        try:
            qty = int(row.get("quantity", 0))
            if qty <= 0:
                issues.append(f"quantity inválida: {qty}")
            else:
                row["quantity"] = qty
        except (ValueError, TypeError):
            issues.append(f"quantity no es número: {row.get('quantity')}")

        try:
            row["unit_price"] = float(row.get("unit_price", 0))
        except (ValueError, TypeError):
            issues.append(f"unit_price inválido: {row.get('unit_price')}")

        # Normalización
        if row.get("region"):
            row["region"] = row["region"].strip().upper()

        if issues:
            row["_issues"] = "; ".join(issues)
            rejected.append(row)
            log.warning(f"Fila rechazada order_id={row.get('order_id')}: {row['_issues']}")
        else:
            valid.append(row)

    return valid, rejected


def upload_csv(rows: list[dict], bucket: str, s3_key: str):
    if not rows:
        return
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue().encode("utf-8"))


def run(**context):
    log = get_logger(TASK_NAME)
    run_date = context.get("ds", date.today().isoformat())
    bucket = os.environ["S3_BUCKET"]

    # Simula error forzado para demo de triage
    if os.environ.get("FORCE_TRANSFORM_ERROR", "false").lower() == "true":
        log.error("FORCE_TRANSFORM_ERROR activado — simulando falla de transformación")
        raise ValueError("Schema inesperado: columna 'unit_price' no encontrada en el CSV fuente.")

    s3_key_raw = f"raw/sales/{run_date}/sales_{run_date}.csv"
    log.info(f"Leyendo raw desde s3://{bucket}/{s3_key_raw}")

    rows = read_from_s3(bucket, s3_key_raw)
    log.info(f"Filas leídas: {len(rows)}")

    valid, rejected = transform_rows(rows, log)
    log.info(f"Filas válidas: {len(valid)} | Rechazadas: {len(rejected)}")

    # Subir válidas a processed/
    processed_key = f"processed/sales/{run_date}/sales_{run_date}_clean.csv"
    upload_csv(valid, bucket, processed_key)
    log.info(f"Procesados subidos a s3://{bucket}/{processed_key}")

    # Subir rechazadas a quarantine/ como evidencia (clave para el triage)
    if rejected:
        quarantine_key = f"quarantine/sales/{run_date}/rejected_{run_date}.csv"
        upload_csv(rejected, bucket, quarantine_key)
        log.warning(f"Filas en cuarentena subidas a s3://{bucket}/{quarantine_key}")
        context["ti"].xcom_push(key="quarantine_key", value=quarantine_key)

    context["ti"].xcom_push(key="processed_key", value=processed_key)
    context["ti"].xcom_push(key="valid_count", value=len(valid))
    context["ti"].xcom_push(key="rejected_count", value=len(rejected))

    return processed_key

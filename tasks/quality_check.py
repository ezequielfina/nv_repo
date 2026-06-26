"""
tasks/quality_check.py
Ejecuta controles de calidad sobre los datos ya cargados en Postgres via SalesDbHook.
Si algún check falla, levanta una excepción y el DAG queda en estado FAILED.
Sube el reporte de calidad a S3 como evidencia de triage.
"""

import os
import json
import boto3
import sys

from datetime import date, datetime

sys.path.insert(0, "/opt/airflow")
from dags.infra.cloudwatch_logger import get_logger
from dags.infra.db_hook import SalesDbHook

TASK_NAME = "quality_check"

QUALITY_CHECKS = [
    {
        "name": "no_null_customers",
        "description": "No debe haber registros con customer NULL",
        "query": "SELECT COUNT(*) FROM sales WHERE customer IS NULL AND sale_date = %s",
        "threshold": 0,
        "operator": "eq",
    },
    {
        "name": "no_negative_quantity",
        "description": "Todas las cantidades deben ser positivas",
        "query": "SELECT COUNT(*) FROM sales WHERE quantity <= 0 AND sale_date = %s",
        "threshold": 0,
        "operator": "eq",
    },
    {
        "name": "minimum_records",
        "description": "Se deben haber cargado al menos 50 registros hoy",
        "query": "SELECT COUNT(*) FROM sales WHERE sale_date = %s",
        "threshold": 50,
        "operator": "gte",
    },
    {
        "name": "no_duplicate_orders",
        "description": "No debe haber order_ids duplicados en el día",
        "query": """
            SELECT COUNT(*) FROM (
                SELECT order_id FROM sales WHERE sale_date = %s
                GROUP BY order_id HAVING COUNT(*) > 1
            ) dupes
        """,
        "threshold": 0,
        "operator": "eq",
    },
]


def evaluate_check(check: dict, count: int) -> bool:
    op = check["operator"]
    threshold = check["threshold"]
    if op == "eq":
        return count == threshold
    elif op == "gte":
        return count >= threshold
    elif op == "lte":
        return count <= threshold
    return False


def upload_report(report: dict, bucket: str, run_date: str) -> str:
    s3 = boto3.client("s3")
    key = f"quality-reports/sales/{run_date}/report_{run_date}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def run(**context):
    log = get_logger(TASK_NAME)
    run_date = context.get("ds", date.today().isoformat())
    bucket = os.environ["S3_BUCKET"]

    log.info(f"Iniciando quality checks para fecha {run_date}")

    hook = SalesDbHook()
    results = []
    failed_checks = []

    for check in QUALITY_CHECKS:
        count = hook.run_quality_check(check["query"], (run_date,))
        passed = evaluate_check(check, count)

        result = {
            "check": check["name"],
            "description": check["description"],
            "value": count,
            "threshold": check["threshold"],
            "operator": check["operator"],
            "passed": passed,
            "evaluated_at": datetime.utcnow().isoformat(),
        }
        results.append(result)

        if passed:
            log.info(f"✅ {check['name']}: {count} (threshold={check['threshold']})")
        else:
            log.error(f"❌ {check['name']} FALLÓ: {count} (threshold={check['threshold']})")
            failed_checks.append(check["name"])

    # Subir reporte a S3 como evidencia para triage
    report = {
        "run_date": run_date,
        "total_checks": len(results),
        "passed": len(results) - len(failed_checks),
        "failed": len(failed_checks),
        "checks": results,
    }
    report_key = upload_report(report, bucket, run_date)
    log.info(f"Reporte de calidad subido a s3://{bucket}/{report_key}")
    context["ti"].xcom_push(key="quality_report_key", value=report_key)

    if failed_checks:
        raise ValueError(
            f"Quality check FALLÓ en: {', '.join(failed_checks)}. "
            f"Ver reporte en s3://{bucket}/{report_key}"
        )

    log.info(f"Todos los quality checks pasaron ✅ ({len(results)}/{len(results)})")

"""
tasks/extract.py
Genera datos de ventas falsos y los sube a S3 como CSV (capa raw).
"""

import os
import csv
import uuid
import random
import boto3
import sys

from io import StringIO
from datetime import date
from faker import Faker

sys.path.insert(0, "/opt/airflow")
from infra.cloudwatch_logger import get_logger

TASK_NAME = "extract"
fake = Faker("es_ES")

PRODUCTS = [
    ("Laptop Pro 15", 1200.00),
    ("Monitor 4K", 450.00),
    ("Teclado Mecánico", 89.99),
    ("Mouse Inalámbrico", 45.00),
    ("Auriculares BT", 120.00),
    ("Webcam HD", 75.00),
    ("SSD 1TB", 95.00),
    ("Hub USB-C", 35.00),
]

REGIONS = ["AMBA", "NOA", "NEA", "Cuyo", "Patagonia", "Centro"]


def generate_sales(n: int = 100) -> list[dict]:
    """Genera n registros de ventas. Introduce ~5% de filas con datos sucios."""
    rows = []
    for _ in range(n):
        product, price = random.choice(PRODUCTS)
        dirty = random.random() < 0.05  # 5% filas sucias para que quality_check las detecte

        rows.append({
            "order_id": str(uuid.uuid4()),
            "customer": fake.name() if not dirty else None,
            "product": product,
            "quantity": random.randint(1, 10) if not dirty else -1,
            "unit_price": price,
            "region": random.choice(REGIONS),
            "sale_date": date.today().isoformat(),
        })
    return rows


def upload_to_s3(rows: list[dict], bucket: str, run_date: str) -> str:
    """Serializa a CSV y sube a S3. Retorna el s3_key."""
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

    s3_key = f"raw/sales/{run_date}/sales_{run_date}.csv"

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    return s3_key


def run(**context):
    log = get_logger(TASK_NAME)
    run_date = context.get("ds", date.today().isoformat())
    bucket = os.environ["S3_BUCKET"]

    log.info(f"Iniciando extracción para fecha {run_date}")

    rows = generate_sales(n=100)
    log.info(f"Generados {len(rows)} registros de ventas")

    s3_key = upload_to_s3(rows, bucket, run_date)
    log.info(f"Archivo subido a s3://{bucket}/{s3_key}")

    # Pushear el s3_key para que la siguiente task lo lea
    context["ti"].xcom_push(key="s3_key", value=s3_key)
    context["ti"].xcom_push(key="row_count", value=len(rows))

    return s3_key

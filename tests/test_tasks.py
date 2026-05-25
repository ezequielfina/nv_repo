"""
tests/test_tasks.py
Unit tests para las tasks del pipeline ETL.
Usan mocks para no depender de AWS, Postgres ni Airflow reales.
"""

import sys
from unittest.mock import MagicMock, patch

# Mockear airflow y sus providers ANTES de cualquier import de tasks
# Necesario porque Airflow no corre nativamente en Windows
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.providers"] = MagicMock()
sys.modules["airflow.providers.postgres"] = MagicMock()
sys.modules["airflow.providers.postgres.hooks"] = MagicMock()
sys.modules["airflow.providers.postgres.hooks.postgres"] = MagicMock()
sys.modules["watchtower"] = MagicMock()

sys.path.insert(0, ".")


# ─── Extract ──────────────────────────────────────────────────────────────────

from tasks.extract import generate_sales


def test_generate_sales_returns_correct_count():
    rows = generate_sales(n=50)
    assert len(rows) == 50


def test_generate_sales_has_required_fields():
    rows = generate_sales(n=10)
    required = {"order_id", "customer", "product", "quantity", "unit_price", "region", "sale_date"}
    for row in rows:
        assert required.issubset(row.keys()), f"Faltan campos: {required - row.keys()}"


def test_generate_sales_order_ids_are_unique():
    rows = generate_sales(n=100)
    ids = [r["order_id"] for r in rows if r["order_id"]]
    assert len(ids) == len(set(ids)), "Hay order_ids duplicados"


# ─── Transform ────────────────────────────────────────────────────────────────

from tasks.transform import transform_rows, read_from_s3


def test_read_from_s3_handles_latin1():
    # simular un archivo con caracteres latin-1
    content = "order_id,customer\n123,José\n".encode("latin-1")

    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=content))
    }

    with patch("boto3.client", return_value=mock_s3):
        rows = read_from_s3("bucket", "key")

    assert rows[0]["customer"] == "José"

def _mock_logger():
    return MagicMock()


def test_transform_rejects_null_customer():
    rows = [
        {"order_id": "123", "customer": None, "product": "X",
         "quantity": "2", "unit_price": "10.0", "region": "AMBA", "sale_date": "2024-01-01"},
    ]
    valid, rejected = transform_rows(rows, _mock_logger())
    assert len(valid) == 0
    assert len(rejected) == 1
    assert "customer es null" in rejected[0]["_issues"]


def test_transform_rejects_negative_quantity():
    rows = [
        {"order_id": "456", "customer": "Juan", "product": "X",
         "quantity": "-3", "unit_price": "10.0", "region": "NOA", "sale_date": "2024-01-01"},
    ]
    valid, rejected = transform_rows(rows, _mock_logger())
    assert len(rejected) == 1
    assert "quantity" in rejected[0]["_issues"]


def test_transform_accepts_valid_row():
    rows = [
        {"order_id": "789", "customer": "Ana", "product": "Laptop",
         "quantity": "2", "unit_price": "1200.00", "region": "centro", "sale_date": "2024-01-01"},
    ]
    valid, rejected = transform_rows(rows, _mock_logger())
    assert len(valid) == 1
    assert len(rejected) == 0


def test_transform_normalizes_region():
    rows = [
        {"order_id": "abc", "customer": "María", "product": "Mouse",
         "quantity": "1", "unit_price": "45.00", "region": "  amba  ", "sale_date": "2024-01-01"},
    ]
    valid, _ = transform_rows(rows, _mock_logger())
    assert valid[0]["region"] == "AMBA"


# ─── Quality Check ────────────────────────────────────────────────────────────

from tasks.quality_check import evaluate_check


def test_quality_check_eq_passes():
    check = {"operator": "eq", "threshold": 0}
    assert evaluate_check(check, 0) is True
    assert evaluate_check(check, 1) is False


def test_quality_check_gte_passes():
    check = {"operator": "gte", "threshold": 50}
    assert evaluate_check(check, 50) is True
    assert evaluate_check(check, 100) is True
    assert evaluate_check(check, 49) is False

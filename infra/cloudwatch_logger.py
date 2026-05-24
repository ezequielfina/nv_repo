"""
infra/cloudwatch_logger.py
Wrapper sobre watchtower para loggear a CloudWatch desde cualquier task.
Uso:
    from infra.cloudwatch_logger import get_logger
    log = get_logger("mi_task")
    log.info("mensaje")
    log.error("algo falló", extra={"details": "..."})
"""

import logging
import os
import watchtower
import boto3
from datetime import datetime

LOG_GROUP = "etl-monitoring-pipeline"


def get_logger(task_name: str, dag_id: str = "sales_pipeline") -> logging.Logger:
    """
    Retorna un logger que escribe en CloudWatch y también en stdout.
    El log stream se organiza como: dag_id/task_name/YYYY-MM-DD
    """
    logger = logging.getLogger(f"{dag_id}.{task_name}")

    if logger.handlers:
        return logger  # ya inicializado, evita duplicar handlers

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Handler: stdout (visible en logs de Airflow y docker)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Handler: CloudWatch (solo si las credenciales están configuradas)
    try:
        boto_session = boto3.Session(
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        )
        cw_handler = watchtower.CloudWatchLogHandler(
            log_group=LOG_GROUP,
            stream_name=f"{dag_id}/{task_name}/{datetime.utcnow().strftime('%Y-%m-%d')}",
            boto3_session=boto_session,
            create_log_group=True,
        )
        cw_handler.setFormatter(formatter)
        logger.addHandler(cw_handler)
    except Exception as e:
        logger.warning(f"CloudWatch handler no disponible: {e}. Loggeando solo a stdout.")

    return logger

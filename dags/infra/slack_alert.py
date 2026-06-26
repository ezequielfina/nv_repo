"""
infra/slack_alert.py
Callback para notificar a Slack cuando una task falla.
Se usa en el DAG como on_failure_callback.
"""

import os
import requests


def send_slack_alert(context):
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]
    execution_date = context["ds"]
    exception = context.get("exception", "Sin detalle")
    log_url = context.get("task_instance").log_url

    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "❌ Task fallida en Airflow",
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                    {"type": "mrkdwn", "text": f"*Fecha:*\n`{execution_date}`"},
                    {"type": "mrkdwn", "text": f"*Run ID:*\n`{run_id}`"},
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{str(exception)[:300]}```"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Ver log en Airflow"},
                        "url": log_url,
                        "style": "danger"
                    }
                ]
            }
        ]
    }

    response = requests.post(webhook_url, json=message)
    if response.status_code != 200:
        raise ValueError(f"Error enviando alerta a Slack: {response.text}")
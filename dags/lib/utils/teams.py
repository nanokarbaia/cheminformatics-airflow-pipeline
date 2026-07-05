"""MS Teams alerting through an Airflow connection."""

import http
import logging
from urllib.parse import urlencode

import requests
from airflow.hooks.base import BaseHook

WEBHOOK_CONN_ID = 'msteams_webhook'


def get_webhook_url(conn_id=WEBHOOK_CONN_ID):
    conn = BaseHook.get_connection(conn_id)

    scheme = conn.conn_type or 'https'
    netloc = conn.host

    if conn.port:
        netloc = f'{netloc}:{conn.port}'

    url = f'{scheme}://{netloc}/{conn.schema or ""}'

    extra = conn.extra_dejson
    if extra:
        url = f'{url}?{urlencode(extra)}'

    return url


def send_teams_alert(context):
    webhook_url = get_webhook_url()

    dag_id = context['task_instance'].dag_id
    task_id = context['task_instance'].task_id

    message = {
        'type': 'message',
        'attachments': [
            {
                'contentType': 'application/vnd.microsoft.card.adaptive',
                'content': {
                    '$schema': 'http://adaptivecards.io/schemas/adaptive-card.json',
                    'type': 'AdaptiveCard',
                    'version': '1.4',
                    'body': [
                        {
                            'type': 'TextBlock',
                            'text': '🚨 Airflow task failed',
                            'wrap': True,
                            'weight': 'Bolder',
                            'color': 'Attention',
                            'size': 'Medium',
                        },
                        {
                            'type': 'FactSet',
                            'facts': [
                                {'title': 'DAG', 'value': dag_id},
                                {'title': 'Task', 'value': task_id},
                            ],
                        },
                    ],
                },
            },
        ],
    }

    response = requests.post(webhook_url, json=message, timeout=30)

    if response.status_code == http.HTTPStatus.OK:
        logging.info('Teams alert sent successfully')
    else:
        logging.error(
            'Failed to send Teams alert: %s %s',
            response.status_code,
            response.text,
        )

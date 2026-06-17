"""
Prod SOS DAG - PLACEHOLDER / DUMMY (not wired to real logic yet)

This is a starting skeleton for the prodSOS DAG. It is standalone, paused-safe, and
does nothing but log a heartbeat so we can confirm it parses and shows up in Airflow.
Real query + Slack rendering will be filled in next.

Schedule: placeholder (daily 09:00 IST) - change when the real cadence is decided.
Channel:  placeholder via PROD_SOS_SLACK_CHANNEL env (defaults to the test channel).
"""

from datetime import timedelta
import logging
import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ==================== CONFIG (placeholder) ====================
SLACK_CHANNEL_ID = os.getenv('PROD_SOS_SLACK_CHANNEL', 'C0B4J9RBWDC')  # test channel for now


# ==================== MAIN TASK (dummy) ====================

def run_prod_sos(**context):
    logger.info('=' * 60)
    logger.info('PROD SOS DAG - DUMMY RUN (no-op placeholder)')
    logger.info('target channel (placeholder): %s', SLACK_CHANNEL_ID)
    logger.info('TODO: add BigQuery query + Slack rendering here')
    logger.info('=' * 60)


# ==================== DAG DEFINITION ====================

default_args = {
    'owner': 'cs_team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'prod_sos_slack',
    default_args=default_args,
    description='[PLACEHOLDER] Prod SOS DAG - dummy skeleton, no real logic yet',
    schedule_interval='0 9 * * *',  # placeholder: daily 09:00 Asia/Kolkata
    catchup=False,
    is_paused_upon_creation=True,  # keep paused until the real logic lands
    tags=['slack', 'prod_sos', 'cs_team', 'placeholder'],
)

run_prod_sos_task = PythonOperator(
    task_id='run_prod_sos',
    python_callable=run_prod_sos,
    dag=dag,
)

"""
Local preview runner for ticket_stats_hourly_slack_dag.py.

Stubs out airflow / google.cloud / requests / utils.slack so the *real* DAG
module imports cleanly in a plain Python env, then calls the actual
build_slack_message() against real rows saved in preview_rows.json.

This guarantees the preview tests the exact formatter that will ship — no copy
that can drift from the DAG file.

Usage:
    python3 render_preview.py            # print the rendered message
    PREVIEW_DATE_STR="Sunday, 08 Jun 2026" python3 render_preview.py
"""

import importlib.util
import json
import os
import sys
import types


def _stub(name):
    m = types.ModuleType(name)
    sys.modules[name] = m
    return m


# --- airflow ---
airflow = _stub('airflow')


class _DAG:
    def __init__(self, *a, **k):
        pass


airflow.DAG = _DAG
_stub('airflow.operators')
_op_python = _stub('airflow.operators.python')


class _PythonOperator:
    def __init__(self, *a, **k):
        pass


_op_python.PythonOperator = _PythonOperator

# --- google.cloud.bigquery ---
_stub('google')
_google_cloud = _stub('google.cloud')
_bq = _stub('google.cloud.bigquery')


class _Client:
    def __init__(self, *a, **k):
        pass


_bq.Client = _Client
_google_cloud.bigquery = _bq

# --- pendulum (only datetime() is used, for the tz-aware start_date) ---
import datetime as _dt
_pendulum = _stub('pendulum')
_pendulum.datetime = lambda *a, **k: _dt.datetime(*[x for x in a if isinstance(x, int)])

# --- requests ---
_stub('requests')

# --- utils.slack.slack_config ---
_stub('utils')
_stub('utils.slack')
_slack_config = _stub('utils.slack.slack_config')
_slack_config.SLACK_BOT_TOKEN_ALERTS = 'preview-token'


def load_dag_module():
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, 'cs_ticket_count_slack_daily_cs_metrics.py')
    spec = importlib.util.spec_from_file_location('dagmod', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    dagmod = load_dag_module()

    with open(os.path.join(here, 'preview_rows.json')) as f:
        rows = json.load(f)

    date_str = os.getenv('PREVIEW_DATE_STR', 'Sunday, 08 Jun 2026')
    message = dagmod.build_slack_message(rows, date_str)
    print(message)
    return message


if __name__ == '__main__':
    main()

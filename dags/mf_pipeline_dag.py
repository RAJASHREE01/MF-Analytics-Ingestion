import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
import json
import sys

sys.path.insert(0, '/usr/local/airflow/include')
from upsert_nav_daily import run

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    'owner': 'rajashree',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id='mf_analytics_pipeline',
    default_args=default_args,
    description='Daily MF NAV ingestion + dbt refresh',
    schedule_interval='30 18 * * 1-5',  # 11:30 PM IST = 6:30 PM UTC, Mon-Fri
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['mf-analytics', 'ingestion', 'dbt'],
) as dag:

    # Task 1 — fetch latest NAV and upsert into Snowflake
    ingest_nav = PythonOperator(
        task_id='ingest_nav_daily',
        python_callable=run,
    )

    # Task 2 — trigger dbt Cloud job via API
    trigger_dbt = SimpleHttpOperator(
        task_id='trigger_dbt_cloud_job',
        method='POST',
        http_conn_id='dbt_cloud',
        endpoint='/api/v2/accounts/{{ var.value.dbt_account_id }}/jobs/{{ var.value.dbt_job_id }}/run/',
        headers={
            "Content-Type": "application/json",
            "Authorization": "Token {{ var.value.dbt_api_token }}"
        },
        data=json.dumps({"cause": "Triggered by Airflow"}),
        response_check=lambda response: response.json()['status']['is_complete'] is not None,
        log_response=True,
    )

    # Task dependency — ingest first, then dbt
    ingest_nav >> trigger_dbt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import subprocess
import requests

def notify_discord_success(context):
    webhook_url = os.environ.get("DISCORD_WEBHOOK")
    if webhook_url:
        requests.post(webhook_url, json={"content": f"DAG run success: {context['dag'].dag_id}"})

def notify_discord_failure(context):
    webhook_url = os.environ.get("DISCORD_WEBHOOK")
    if webhook_url:
        requests.post(webhook_url, json={"content": f"DAG run failed: {context['dag'].dag_id}"})

def download_data():
    print("Download from Kaggle...")
    subprocess.run(["python", "/opt/airflow/scripts/download_kaggle.py"], check=True)

def load_to_bigquery():
    print("Load to BigQuery...")
    subprocess.run(["python", "/opt/airflow/scripts/load_to_bq.py"], check=True)

def run_dbt():
    print("DBT run...")
    result = subprocess.run([
        "dbt", "run",
        "--profiles-dir", "/opt/airflow/dbt/lego_dbt/profiles",
        "--project-dir", "/opt/airflow/dbt/lego_dbt"
    ], capture_output=True, text=True)

    print("✅ STDOUT:\n", result.stdout)
    print("⚠️ STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise Exception("DBT run failed")

def test_dbt_model():
    print("Run DBT test at schema model...")
    result = subprocess.run([
        "dbt", "test",
        "--select", "models/model/",
        "--profiles-dir", "/opt/airflow/dbt/lego_dbt/profiles",
        "--project-dir", "/opt/airflow/dbt/lego_dbt"
    ], capture_output=True, text=True)

    print("✅ STDOUT:\n", result.stdout)
    print("⚠️ STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise Exception("DBT test failed")

with DAG(
    "lego_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="download_data", python_callable=download_data)
    t2 = PythonOperator(task_id="load_to_bigquery", python_callable=load_to_bigquery)
    t3 = PythonOperator(task_id="transform_with_dbt", python_callable=run_dbt)
    
    t4 = PythonOperator(
        task_id="test_dbt_model",
        python_callable=test_dbt_model,
        on_success_callback=notify_discord_success,
        on_failure_callback=notify_discord_failure,
    )

    t1 >> t2 >> t3 >> t4

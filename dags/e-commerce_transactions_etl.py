"""
Airflow DAG: e-commerce_transactions_etl.py
Orchestrates the daily incremental transaction pipeline:
1. setup_db_task: Initializes schema, tables, partitions, and seeds products
2. generate_source_data_task: Simulates daily transaction CSV data (with incremental updates & anomalies)
3. ingest_data_task: Filters incremental records and loads into staging.transactions
4. run_data_quality_task: Runs 5 configurable quality gates, separating valid and invalid records
5. quarantine_errors_task: Persists invalid records to quarantine.transactions_errors
6. load_fact_table_task: Performs atomic UPSERT of valid records into partitioned fact table

Configured with failure/success alert callbacks and robust logging.
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure root project directory is on sys.path for importing scripts module
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.db_utils import execute_sql_file, get_connection
from scripts.generate_data import generate_transactions
from scripts.ingest import ingest
from scripts.data_quality import run_data_quality
from scripts.load_fact import load_quarantine_errors, upsert_fact_transactions

logger = logging.getLogger(__name__)

# Alerting Callbacks
def notify_failure(context):
    """
    Native callback triggered upon task/DAG failure.
    Logs comprehensive error context and writes to the alert audit log.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "Unknown Task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "ecommerce_transactions_etl"
    execution_date = context.get("execution_date")
    exception = context.get("exception", "No exception detail provided")
    log_url = ti.log_url if ti else "N/A"

    alert_msg = (
        f"[ALERT - PIPELINE FAILURE] DAG: {dag_id} | Task: {task_id} | "
        f"Execution Date: {execution_date} | Error: {exception} | Log: {log_url}\n"
    )
    logger.error(alert_msg)

    # Persist alert to logs directory
    alert_log_path = os.path.join(PROJECT_ROOT, "logs", "pipeline_alerts.log")
    try:
        os.makedirs(os.path.dirname(alert_log_path), exist_ok=True)
        with open(alert_log_path, "a", encoding="utf-8") as f:
            f.write(f"{datetime.utcnow().isoformat()} {alert_msg}")
    except Exception as e:
        logger.warning("Could not write to alert log file: %s", e)


def notify_success(context):
    """
    Native callback triggered upon task/DAG success.
    Logs pipeline completion and metrics.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "Unknown Task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "ecommerce_transactions_etl"
    execution_date = context.get("execution_date")

    success_msg = (
        f"[PIPELINE SUCCESS] DAG: {dag_id} | Task: {task_id} | Execution Date: {execution_date}\n"
    )
    logger.info(success_msg)

    alert_log_path = os.path.join(PROJECT_ROOT, "logs", "pipeline_alerts.log")
    try:
        os.makedirs(os.path.dirname(alert_log_path), exist_ok=True)
        with open(alert_log_path, "a", encoding="utf-8") as f:
            f.write(f"{datetime.utcnow().isoformat()} {success_msg}")
    except Exception as e:
        logger.warning("Could not write to alert log file: %s", e)


# Task Callables
def run_setup_db(**kwargs):
    """Initializes schemas, tables, and product seeds in the analytical warehouse."""
    logger.info("Initializing database schemas and product catalog...")
    init_sql = os.path.join(PROJECT_ROOT, "sql", "init_db.sql")
    create_sql = os.path.join(PROJECT_ROOT, "sql", "create_tables.sql")

    execute_sql_file(init_sql)
    execute_sql_file(create_sql)
    logger.info("Database schemas and tables initialized successfully.")


def run_generate_data(ds, **kwargs):
    """Simulates daily source transaction CSV."""
    logger.info("Generating simulated daily transaction data for date: %s", ds)
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    csv_file = generate_transactions(
        target_date=ds,
        output_dir=raw_dir,
        num_records=100,
        update_ratio=0.15,
        anomaly_ratio=0.05,
    )
    logger.info("Source data generated: %s", csv_file)
    return csv_file


def run_ingest_data(ds, **kwargs):
    """Ingests source data, runs incremental filtering, and loads into staging."""
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    target_file = os.path.join(raw_dir, f"daily_transactions_{ds}.csv")
    logger.info("Starting incremental ingestion for file: %s", target_file)
    stats = ingest(file_path=target_file)
    logger.info("Ingestion completed with statistics: %s", stats)
    return stats


def run_quality_gates(**kwargs):
    """Applies the 5 data quality gates and splits staging records."""
    logger.info("Running Data Quality validation gates...")
    config_file = os.path.join(PROJECT_ROOT, "config", "data_quality_rules.yaml")
    metrics = run_data_quality(config_path=config_file)
    logger.info("Data Quality checks completed: %s", metrics)
    return metrics


def run_quarantine(**kwargs):
    """Loads invalid records into quarantine.transactions_errors."""
    logger.info("Quarantining invalid records...")
    res = load_quarantine_errors()
    logger.info("Quarantine load finished: %s", res)
    return res


def run_load_fact(**kwargs):
    """Performs UPSERT of valid records into public.fact_transactions."""
    logger.info("Upserting valid records into public.fact_transactions...")
    res = upsert_fact_transactions()
    logger.info("Fact table UPSERT completed: %s", res)
    return res


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="ecommerce_transactions_etl",
    default_args=default_args,
    description="Automated incremental sync of e-commerce transactions with robust DQ gates",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "incremental", "data-quality", "etl"],
) as dag:

    setup_db_task = PythonOperator(
        task_id="setup_db_task",
        python_callable=run_setup_db,
    )

    generate_source_data_task = PythonOperator(
        task_id="generate_source_data_task",
        python_callable=run_generate_data,
        op_kwargs={"ds": "{{ ds }}"},
    )

    ingest_data_task = PythonOperator(
        task_id="ingest_data_task",
        python_callable=run_ingest_data,
        op_kwargs={"ds": "{{ ds }}"},
    )

    run_data_quality_task = PythonOperator(
        task_id="run_data_quality_task",
        python_callable=run_quality_gates,
    )

    quarantine_errors_task = PythonOperator(
        task_id="quarantine_errors_task",
        python_callable=run_quarantine,
        on_success_callback=notify_success,
    )

    load_fact_table_task = PythonOperator(
        task_id="load_fact_table_task",
        python_callable=run_load_fact,
        on_success_callback=notify_success,
    )

    # Define task dependencies
    (
        setup_db_task
        >> generate_source_data_task
        >> ingest_data_task
        >> run_data_quality_task
        >> [quarantine_errors_task, load_fact_table_task]
    )

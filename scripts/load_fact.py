"""
Data Loading Script: load_fact.py
Handles loading of validated records into public.fact_transactions via UPSERT,
and routing of failed records into quarantine.transactions_errors.
"""

import os
import sys
import argparse
import logging
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values
import pandas as pd

# Ensure parent directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
try:
    from scripts.db_utils import get_connection
except ModuleNotFoundError:
    from db_utils import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_quarantine_errors(
    conn: Optional[PgConnection] = None,
    source_table: str = "staging.invalid_transactions"
) -> Dict[str, int]:
    """
    Transfers records that failed data quality gates from staging.invalid_transactions
    into quarantine.transactions_errors for investigation and audit.
    """
    logger.info("=== Loading Invalid Records into Quarantine ===")
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        with conn.cursor() as cur:
            # Check how many invalid records exist
            cur.execute(f"SELECT COUNT(*) FROM {source_table};")
            count = cur.fetchone()[0]

            if count == 0:
                logger.info("No invalid records found in %s to quarantine.", source_table)
                conn.commit()
                return {"quarantined_count": 0}

            insert_sql = f"""
                INSERT INTO quarantine.transactions_errors (
                    transaction_id, customer_id, product_id, amount,
                    transaction_date, status, updated_at, error_message, error_timestamp
                )
                SELECT
                    transaction_id, customer_id, product_id, amount,
                    transaction_date, status, updated_at, error_message, error_timestamp
                FROM {source_table};
            """
            cur.execute(insert_sql)
            quarantined_count = cur.rowcount
            conn.commit()
            logger.info("Successfully transferred %d records to quarantine.transactions_errors.", quarantined_count)
            return {"quarantined_count": quarantined_count}

    except Exception as e:
        conn.rollback()
        logger.error("Failed to load records into quarantine table: %s", e)
        raise
    finally:
        if close_conn:
            conn.close()


def upsert_fact_transactions(
    conn: Optional[PgConnection] = None,
    source_table: str = "staging.valid_transactions"
) -> Dict[str, int]:
    """
    Loads verified records from staging.valid_transactions into the partitioned
    public.fact_transactions table using an atomic PostgreSQL UPSERT strategy:
    INSERT ... ON CONFLICT (transaction_id, transaction_date) DO UPDATE SET ...
    """
    logger.info("=== Loading Valid Records into public.fact_transactions (UPSERT) ===")
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {source_table};")
            count = cur.fetchone()[0]

            if count == 0:
                logger.info("No valid records found in %s to upsert.", source_table)
                conn.commit()
                return {"upserted_count": 0}

            # Upsert SQL query: handles both new records (INSERT) and modifications (UPDATE)
            # Idempotent: EXCLUDED.updated_at >= public.fact_transactions.updated_at
            upsert_sql = f"""
                INSERT INTO public.fact_transactions (
                    transaction_id, customer_id, product_id, amount, transaction_date, status, updated_at
                )
                SELECT
                    transaction_id, customer_id, product_id, amount, transaction_date, status, updated_at
                FROM {source_table}
                ON CONFLICT (transaction_id, transaction_date)
                DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    product_id = EXCLUDED.product_id,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at
                WHERE EXCLUDED.updated_at >= public.fact_transactions.updated_at;
            """
            cur.execute(upsert_sql)
            affected_rows = cur.rowcount
            conn.commit()
            logger.info("Successfully executed UPSERT on public.fact_transactions. Rows affected: %d", affected_rows)
            return {"upserted_count": affected_rows}

    except Exception as e:
        conn.rollback()
        logger.error("Failed to upsert records into public.fact_transactions: %s", e)
        raise
    finally:
        if close_conn:
            conn.close()


def upsert_dataframe_to_fact(df: pd.DataFrame, conn: Optional[PgConnection] = None) -> int:
    """
    Helper function to directly upsert a pandas DataFrame of valid records
    into public.fact_transactions (useful for programmatic or test invocation).
    """
    if df.empty:
        return 0

    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        with conn.cursor() as cur:
            upsert_query = """
                INSERT INTO public.fact_transactions (
                    transaction_id, customer_id, product_id, amount, transaction_date, status, updated_at
                ) VALUES %s
                ON CONFLICT (transaction_id, transaction_date)
                DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    product_id = EXCLUDED.product_id,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at
                WHERE EXCLUDED.updated_at >= public.fact_transactions.updated_at;
            """
            records = [
                (
                    str(row["transaction_id"]),
                    str(row["customer_id"]),
                    str(row["product_id"]),
                    float(row["amount"]),
                    str(row["transaction_date"]),
                    str(row["status"]),
                    pd.to_datetime(row["updated_at"]).isoformat(),
                )
                for _, row in df.iterrows()
            ]
            execute_values(cur, upsert_query, records)
            conn.commit()
            return len(records)
    finally:
        if close_conn and conn is not None:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data to analytical fact table or quarantine.")
    parser.add_argument("--action", choices=["fact", "quarantine", "all"], default="all",
                        help="Action to perform: 'fact', 'quarantine', or 'all'")
    args = parser.parse_args()

    if args.action in ["quarantine", "all"]:
        load_quarantine_errors()
    if args.action in ["fact", "all"]:
        upsert_fact_transactions()

"""
Data Ingestion Script: ingest.py
Reads daily transaction CSV files, performs initial data parsing and cleaning,
implements incremental delta detection against public.fact_transactions,
and loads new/updated records into staging.transactions.
Gracefully handles empty files and ensures run idempotency.
"""

import os
import sys
import argparse
import logging
from typing import Optional, Dict, Any, Tuple
import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

# Ensure parent directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
try:
    from scripts.db_utils import get_connection
except ModuleNotFoundError:
    from db_utils import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
    "transaction_id",
    "customer_id",
    "product_id",
    "amount",
    "transaction_date",
    "status",
    "updated_at",
]


def read_source_csv(file_path: str) -> pd.DataFrame:
    """
    Reads source CSV file. Gracefully handles empty files, missing files,
    or headers-only files without crashing.
    """
    if not os.path.exists(file_path):
        logger.warning("Source file not found at: %s. Returning empty DataFrame.", file_path)
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    if os.path.getsize(file_path) == 0:
        logger.warning("Source file %s is empty (0 bytes). Returning empty DataFrame.", file_path)
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    try:
        df = pd.read_csv(file_path, dtype=str)
    except pd.errors.EmptyDataError:
        logger.warning("Source file %s contains no data (EmptyDataError).", file_path)
        return pd.DataFrame(columns=EXPECTED_COLUMNS)
    except Exception as e:
        logger.error("Error reading CSV file %s: %s", file_path, e)
        raise

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Ensure all expected columns are present (fill missing ones with None)
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    logger.info("Successfully read %d raw records from %s", len(df), file_path)
    return df[EXPECTED_COLUMNS]


def parse_and_clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Initial type coercion and cleaning using Pandas while preserving
    raw values for downstream Data Quality checks and quarantine.
    """
    if df.empty:
        return df.copy()

    cleaned_df = df.copy()
    # Strip whitespace from string columns
    for col in ["transaction_id", "customer_id", "product_id", "status"]:
        if col in cleaned_df.columns:
            cleaned_df[col] = cleaned_df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    return cleaned_df


def identify_incremental_delta(
    df: pd.DataFrame,
    conn: Optional[PgConnection] = None,
    existing_lookup: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Compares incoming batch against target table state (public.fact_transactions).
    Identifies:
      - New records (transaction_id not present in target)
      - Updated records (transaction_id in target, but source.updated_at > target.updated_at)
      - Unchanged/stale records (source.updated_at <= target.updated_at -> discarded)
      - Records with missing or malformed transaction_ids are preserved for Data Quality quarantine!
    """
    stats = {"total_read": len(df), "new": 0, "updated": 0, "stale": 0, "preserved_dirty": 0}
    if df.empty:
        return df.copy(), stats

    # Build target state lookup: {transaction_id: target_updated_at}
    target_lookup = existing_lookup if existing_lookup is not None else {}
    if existing_lookup is None and conn is not None:
        valid_ids = df["transaction_id"].dropna().unique().tolist()
        if valid_ids:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT transaction_id, updated_at FROM public.fact_transactions WHERE transaction_id = ANY(%s);",
                        (valid_ids,),
                    )
                    rows = cur.fetchall()
                    target_lookup = {r[0]: pd.to_datetime(r[1], utc=True) for r in rows}
            except Exception as e:
                logger.warning("Could not fetch target state for incremental comparison: %s. Processing all records.", e)
                conn.rollback()
                target_lookup = {}

    delta_indices = []
    for idx, row in df.iterrows():
        tx_id = row.get("transaction_id")
        raw_source_ts = row.get("updated_at")

        # Preserve records with missing transaction_id so Data Quality can flag them
        if pd.isna(tx_id) or str(tx_id).strip() == "":
            delta_indices.append(idx)
            stats["preserved_dirty"] += 1
            continue

        tx_id_str = str(tx_id).strip()
        if tx_id_str not in target_lookup:
            # Brand new record
            delta_indices.append(idx)
            stats["new"] += 1
        else:
            # Transaction exists in target - compare timestamps
            target_ts = target_lookup[tx_id_str]
            try:
                source_ts = pd.to_datetime(raw_source_ts, utc=True)
                if pd.notna(source_ts) and pd.notna(target_ts) and source_ts > target_ts:
                    # True update
                    delta_indices.append(idx)
                    stats["updated"] += 1
                else:
                    # Stale or unchanged
                    stats["stale"] += 1
            except Exception:
                # If timestamp parsing fails, preserve for Data Quality evaluation
                delta_indices.append(idx)
                stats["preserved_dirty"] += 1

    delta_df = df.loc[delta_indices].copy()
    logger.info(
        "Incremental Delta Analysis: Total=%d, New=%d, Updated=%d, Stale/Skipped=%d, Dirty Preserved=%d",
        stats["total_read"], stats["new"], stats["updated"], stats["stale"], stats["preserved_dirty"]
    )
    return delta_df, stats


def load_staging_table(df: pd.DataFrame, conn: Optional[PgConnection] = None) -> int:
    """
    Loads incremental records into staging.transactions.
    Truncates staging table first to guarantee run idempotency.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        with conn.cursor() as cur:
            # Relax staging constraints to allow full data quality quarantine evaluation
            try:
                cur.execute("ALTER TABLE staging.transactions DROP CONSTRAINT IF EXISTS transactions_pkey;")
                cur.execute("ALTER TABLE staging.transactions ALTER COLUMN customer_id DROP NOT NULL;")
                cur.execute("ALTER TABLE staging.transactions ALTER COLUMN product_id DROP NOT NULL;")
                cur.execute("ALTER TABLE staging.transactions ALTER COLUMN amount DROP NOT NULL;")
                cur.execute("ALTER TABLE staging.transactions ALTER COLUMN transaction_date DROP NOT NULL;")
                cur.execute("ALTER TABLE staging.transactions ALTER COLUMN status DROP NOT NULL;")
                cur.execute("ALTER TABLE staging.transactions ALTER COLUMN updated_at DROP NOT NULL;")
            except Exception as e:
                logger.debug("Could not alter staging constraints (continuing): %s", e)

            # 1. Truncate staging table for idempotency
            logger.info("Truncating staging.transactions table for idempotent load...")
            cur.execute("TRUNCATE TABLE staging.transactions;")

            if df.empty:
                conn.commit()
                logger.info("No incremental records to load into staging. Staging is empty.")
                return 0

            # 2. Prepare records for insertion
            insert_query = """
                INSERT INTO staging.transactions (
                    transaction_id, customer_id, product_id, amount, transaction_date, status, updated_at
                ) VALUES %s;
            """
            
            # Convert NaN to None for SQL NULL compatibility
            records = []
            for _, row in df.iterrows():
                tx_id = str(row["transaction_id"]).strip() if pd.notna(row["transaction_id"]) else None
                cust_id = str(row["customer_id"]).strip() if pd.notna(row["customer_id"]) else None
                prod_id = str(row["product_id"]).strip() if pd.notna(row["product_id"]) else None
                
                try:
                    amount = float(row["amount"]) if pd.notna(row["amount"]) else None
                except (ValueError, TypeError):
                    amount = None

                tx_date = str(row["transaction_date"]).strip() if pd.notna(row["transaction_date"]) else None
                status = str(row["status"]).strip() if pd.notna(row["status"]) else None
                
                try:
                    updated_at = pd.to_datetime(row["updated_at"]).isoformat() if pd.notna(row["updated_at"]) else None
                except Exception:
                    updated_at = None

                records.append((tx_id, cust_id, prod_id, amount, tx_date, status, updated_at))

            execute_values(cur, insert_query, records)
            conn.commit()
            logger.info("Successfully loaded %d records into staging.transactions.", len(records))
            return len(records)

    except Exception as e:
        conn.rollback()
        logger.error("Failed to load staging.transactions: %s", e)
        raise
    finally:
        if close_conn:
            conn.close()


def ingest(file_path: str, conn: Optional[PgConnection] = None) -> Dict[str, Any]:
    """
    Main orchestration function for data ingestion step.
    """
    logger.info("=== Starting Data Ingestion from: %s ===", file_path)
    raw_df = read_source_csv(file_path)
    cleaned_df = parse_and_clean_dataframe(raw_df)

    close_conn = False
    if conn is None:
        try:
            conn = get_connection()
            close_conn = True
        except Exception as e:
            logger.warning("Could not establish DB connection in ingest(): %s", e)
            conn = None

    try:
        delta_df, stats = identify_incremental_delta(cleaned_df, conn=conn)
        loaded_count = 0
        if conn is not None:
            loaded_count = load_staging_table(delta_df, conn=conn)
        stats["staging_loaded"] = loaded_count
        logger.info("=== Ingestion Completed: %s ===", stats)
        return stats
    finally:
        if close_conn and conn is not None:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest daily transaction data incrementally.")
    parser.add_argument("--file", type=str, required=False, help="Path to raw transaction CSV file")
    parser.add_argument("--date", type=str, required=False, help="Transaction date YYYY-MM-DD")
    parser.add_argument("--raw-dir", type=str, default="data/raw", help="Directory containing raw CSVs")
    args = parser.parse_args()

    target_file = args.file
    if not target_file:
        if args.date:
            target_file = os.path.join(args.raw_dir, f"daily_transactions_{args.date}.csv")
        else:
            today_str = pd.Timestamp.now().strftime("%Y-%m-%d")
            target_file = os.path.join(args.raw_dir, f"daily_transactions_{today_str}.csv")

    ingest(target_file)

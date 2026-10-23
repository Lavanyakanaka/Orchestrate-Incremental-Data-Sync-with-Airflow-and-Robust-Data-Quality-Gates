"""
Data Quality Validation Module: data_quality.py
Implements a 5-pillar data quality framework against staging.transactions:
1. Schema Validation
2. Uniqueness Check
3. Completeness Check
4. Validity Check (amount > 0)
5. Referential Integrity Check (product_id in public.products)

Configurable via YAML or dictionary. Splits records into valid and invalid sets
with detailed audit error messages for quarantine routing.
"""

import os
import sys
import argparse
import logging
from typing import Optional, Dict, Any, Tuple, Set, List
import pandas as pd
import yaml
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

DEFAULT_CONFIG: Dict[str, Any] = {
    "rules": {
        "schema_validation": {
            "enabled": True,
            "expected_columns": [
                "transaction_id", "customer_id", "product_id",
                "amount", "transaction_date", "status", "updated_at"
            ],
            "allowed_statuses": ["completed", "pending", "cancelled", "refunded", "failed"],
        },
        "uniqueness_check": {
            "enabled": True,
            "columns": ["transaction_id"],
        },
        "completeness_check": {
            "enabled": True,
            "not_null_columns": [
                "transaction_id", "customer_id", "product_id",
                "amount", "transaction_date"
            ],
        },
        "validity_check": {
            "enabled": True,
            "min_amount": 0.01,
        },
        "referential_integrity_check": {
            "enabled": True,
            "foreign_key_column": "product_id",
        },
    }
}


def load_rules_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load data quality rules configuration from YAML or return default configuration.
    """
    if config_path and os.path.exists(config_path):
        logger.info("Loading Data Quality configuration from: %s", config_path)
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    # Check default path
    default_yaml_path = os.path.join(os.path.dirname(__file__), "..", "config", "data_quality_rules.yaml")
    if os.path.exists(default_yaml_path):
        logger.info("Loading Data Quality configuration from default path: %s", default_yaml_path)
        with open(default_yaml_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    logger.info("Using embedded default Data Quality configuration.")
    return DEFAULT_CONFIG


def check_schema_conformity(df: pd.DataFrame, expected_cols: List[str]) -> Tuple[pd.Series, pd.Series]:
    """
    Check 1: Schema validation - verifies expected columns exist and basic format.
    Returns (pass_series, error_series).
    """
    missing_cols = [col for col in expected_cols if col not in df.columns]
    if missing_cols:
        err_msg = f"Schema Error: Missing required columns: {missing_cols}"
        return pd.Series(False, index=df.index), pd.Series(err_msg, index=df.index)

    # Check for placeholder keys generated during ingestion
    is_placeholder = df["transaction_id"].astype(str).str.startswith("INVALID-PK-TEMP-")
    passes = ~is_placeholder
    errors = pd.Series("", index=df.index)
    errors[is_placeholder] = "Schema Error: Missing or invalid transaction_id PK in source"
    return passes, errors


def check_uniqueness(df: pd.DataFrame, key_column: str = "transaction_id") -> Tuple[pd.Series, pd.Series]:
    """
    Check 2: Uniqueness check - ensures key_column is strictly unique across the batch.
    Returns (pass_series, error_series).
    """
    if df.empty or key_column not in df.columns:
        return pd.Series(True, index=df.index), pd.Series("", index=df.index)

    # Identify duplicates (ignoring nulls, which are handled by completeness)
    not_na_series = df[key_column].dropna()
    dup_mask = df[key_column].isin(not_na_series[not_na_series.duplicated()])

    passes = ~dup_mask
    errors = pd.Series("", index=df.index)
    for idx in df[dup_mask].index:
        val = df.loc[idx, key_column]
        errors.loc[idx] = f"Uniqueness Error: Duplicate {key_column} '{val}' detected in staging batch"
    return passes, errors


def check_completeness(df: pd.DataFrame, not_null_columns: List[str]) -> Tuple[pd.Series, pd.Series]:
    """
    Check 3: Completeness check - verifies required columns do not contain NULL or empty values.
    Returns (pass_series, error_series).
    """
    passes = pd.Series(True, index=df.index)
    errors = pd.Series("", index=df.index)

    if df.empty:
        return passes, errors

    for col in not_null_columns:
        if col not in df.columns:
            continue
        # Check for NaN, None, or blank strings
        null_mask = df[col].isna() | (df[col].astype(str).str.strip().isin(["", "None", "nan", "NULL"]))
        for idx in df[null_mask].index:
            passes.loc[idx] = False
            curr_err = errors.loc[idx]
            prefix = "; " if curr_err else ""
            errors.loc[idx] = f"{curr_err}{prefix}Completeness Error: '{col}' is NULL or empty"

    return passes, errors


def check_validity(df: pd.DataFrame, min_amount: float = 0.01) -> Tuple[pd.Series, pd.Series]:
    """
    Check 4: Validity check - verifies amount is positive (greater than 0).
    Returns (pass_series, error_series).
    """
    passes = pd.Series(True, index=df.index)
    errors = pd.Series("", index=df.index)

    if df.empty or "amount" not in df.columns:
        return passes, errors

    for idx, val in df["amount"].items():
        try:
            num_val = float(val)
            if pd.isna(num_val) or num_val < min_amount:
                passes.loc[idx] = False
                errors.loc[idx] = f"Validity Error: amount must be >= {min_amount} (received: {val})"
        except (ValueError, TypeError):
            passes.loc[idx] = False
            errors.loc[idx] = f"Validity Error: amount is non-numeric (received: '{val}')"

    return passes, errors


def check_referential_integrity(
    df: pd.DataFrame,
    valid_product_ids: Set[str],
    fk_column: str = "product_id"
) -> Tuple[pd.Series, pd.Series]:
    """
    Check 5: Referential Integrity check - validates product_id exists in public.products.
    Returns (pass_series, error_series).
    """
    passes = pd.Series(True, index=df.index)
    errors = pd.Series("", index=df.index)

    if df.empty or fk_column not in df.columns:
        return passes, errors

    for idx, val in df[fk_column].items():
        if pd.isna(val) or str(val).strip() == "":
            continue  # Completeness handles missing FKs
        val_str = str(val).strip()
        if val_str not in valid_product_ids:
            passes.loc[idx] = False
            errors.loc[idx] = f"Referential Integrity Error: {fk_column} '{val_str}' does not exist in public.products catalog"

    return passes, errors


def run_data_quality_checks(
    df: pd.DataFrame,
    valid_products: Optional[Set[str]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Executes all configured data quality gates against the input DataFrame.
    Splits into valid and invalid records, populating error messages for invalid rows.
    """
    if config is None:
        config = load_rules_config()

    rules = config.get("rules", {})
    if df.empty:
        logger.info("Empty DataFrame passed to Data Quality checks. 0 valid, 0 invalid.")
        return df.copy(), pd.DataFrame(), {"total": 0, "valid": 0, "invalid": 0}

    # Initialize error tracker for each row
    row_errors = {idx: [] for idx in df.index}
    metrics: Dict[str, Any] = {
        "total": len(df),
        "schema_failures": 0,
        "uniqueness_failures": 0,
        "completeness_failures": 0,
        "validity_failures": 0,
        "referential_failures": 0,
    }

    # 1. Schema Validation
    schema_cfg = rules.get("schema_validation", {})
    if schema_cfg.get("enabled", True):
        exp_cols = schema_cfg.get("expected_columns", [
            "transaction_id", "customer_id", "product_id", "amount", "transaction_date", "status", "updated_at"
        ])
        if isinstance(exp_cols, dict):
            exp_cols = list(exp_cols.keys())
        p, e = check_schema_conformity(df, exp_cols)
        for idx in df.index[~p]:
            row_errors[idx].append(e.loc[idx])
            metrics["schema_failures"] += 1

    # 2. Uniqueness Check
    uniq_cfg = rules.get("uniqueness_check", {})
    if uniq_cfg.get("enabled", True):
        p, e = check_uniqueness(df, "transaction_id")
        for idx in df.index[~p]:
            row_errors[idx].append(e.loc[idx])
            metrics["uniqueness_failures"] += 1

    # 3. Completeness Check
    comp_cfg = rules.get("completeness_check", {})
    if comp_cfg.get("enabled", True):
        cols = comp_cfg.get("not_null_columns", [
            "transaction_id", "customer_id", "product_id", "amount", "transaction_date"
        ])
        p, e = check_completeness(df, cols)
        for idx in df.index[~p]:
            row_errors[idx].append(e.loc[idx])
            metrics["completeness_failures"] += 1

    # 4. Validity Check (amount > 0)
    val_cfg = rules.get("validity_check", {})
    if val_cfg.get("enabled", True):
        min_amt = val_cfg.get("min_amount", 0.01)
        p, e = check_validity(df, min_amt)
        for idx in df.index[~p]:
            row_errors[idx].append(e.loc[idx])
            metrics["validity_failures"] += 1

    # 5. Referential Integrity Check
    ref_cfg = rules.get("referential_integrity_check", {})
    if ref_cfg.get("enabled", True) and valid_products is not None:
        p, e = check_referential_integrity(df, valid_products, "product_id")
        for idx in df.index[~p]:
            row_errors[idx].append(e.loc[idx])
            metrics["referential_failures"] += 1

    # Split into valid and invalid sets
    invalid_indices = [idx for idx, errs in row_errors.items() if len(errs) > 0]
    valid_indices = [idx for idx, errs in row_errors.items() if len(errs) == 0]

    valid_df = df.loc[valid_indices].copy()
    invalid_df = df.loc[invalid_indices].copy()

    # Add error details to invalid records
    if not invalid_df.empty:
        invalid_df["error_message"] = [
            " | ".join(row_errors[idx]) for idx in invalid_indices
        ]
        invalid_df["error_timestamp"] = pd.Timestamp.now(tz="UTC").isoformat()

    metrics["valid"] = len(valid_df)
    metrics["invalid"] = len(invalid_df)

    logger.info(
        "Data Quality Results: Total=%d | Valid=%d | Invalid=%d (Schema=%d, Uniq=%d, Comp=%d, Valid=%d, Ref=%d)",
        metrics["total"], metrics["valid"], metrics["invalid"],
        metrics["schema_failures"], metrics["uniqueness_failures"],
        metrics["completeness_failures"], metrics["validity_failures"],
        metrics["referential_failures"],
    )
    return valid_df, invalid_df, metrics


def fetch_staging_records(conn: PgConnection) -> pd.DataFrame:
    """
    Fetch all records currently residing in staging.transactions.
    """
    cols = ["transaction_id", "customer_id", "product_id", "amount", "transaction_date", "status", "updated_at"]
    query = f"SELECT {', '.join(cols)} FROM staging.transactions;"
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame(rows, columns=cols)


def fetch_dimension_products(conn: PgConnection) -> Set[str]:
    """
    Fetch the set of valid product IDs from public.products.
    """
    query = "SELECT product_id FROM public.products;"
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return {r[0] for r in rows}


def persist_staging_results(valid_df: pd.DataFrame, invalid_df: pd.DataFrame, conn: PgConnection) -> None:
    """
    Saves validation results into staging holding tables (staging.valid_transactions
    and staging.invalid_transactions) for decoupled downstream consumption.
    """
    with conn.cursor() as cur:
        # Clear holding tables for idempotency
        cur.execute("TRUNCATE TABLE staging.valid_transactions;")
        cur.execute("TRUNCATE TABLE staging.invalid_transactions;")

        # 1. Insert valid records
        if not valid_df.empty:
            valid_insert = """
                INSERT INTO staging.valid_transactions (
                    transaction_id, customer_id, product_id, amount, transaction_date, status, updated_at
                ) VALUES %s;
            """
            valid_data = [
                (
                    row.get("transaction_id"),
                    row.get("customer_id"),
                    row.get("product_id"),
                    float(row["amount"]) if pd.notna(row.get("amount")) else None,
                    str(row.get("transaction_date")),
                    row.get("status"),
                    pd.to_datetime(row.get("updated_at")).isoformat() if pd.notna(row.get("updated_at")) else None,
                )
                for _, row in valid_df.iterrows()
            ]
            execute_values(cur, valid_insert, valid_data)
            logger.info("Persisted %d valid records to staging.valid_transactions", len(valid_data))

        # 2. Insert invalid records
        if not invalid_df.empty:
            invalid_insert = """
                INSERT INTO staging.invalid_transactions (
                    transaction_id, customer_id, product_id, amount, transaction_date, status, updated_at,
                    error_message, error_timestamp
                ) VALUES %s;
            """
            invalid_data = []
            for _, row in invalid_df.iterrows():
                try:
                    amt = float(row.get("amount")) if pd.notna(row.get("amount")) else None
                except Exception:
                    amt = None
                invalid_data.append((
                    str(row.get("transaction_id")) if pd.notna(row.get("transaction_id")) else None,
                    str(row.get("customer_id")) if pd.notna(row.get("customer_id")) else None,
                    str(row.get("product_id")) if pd.notna(row.get("product_id")) else None,
                    amt,
                    str(row.get("transaction_date")) if pd.notna(row.get("transaction_date")) else None,
                    str(row.get("status")) if pd.notna(row.get("status")) else None,
                    pd.to_datetime(row.get("updated_at")).isoformat() if pd.notna(row.get("updated_at")) else None,
                    str(row.get("error_message")),
                    str(row.get("error_timestamp")),
                ))
            execute_values(cur, invalid_insert, invalid_data)
            logger.info("Persisted %d invalid records to staging.invalid_transactions", len(invalid_data))

    conn.commit()


def run_data_quality(conn: Optional[PgConnection] = None, config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Main orchestration function for Data Quality validation step.
    """
    logger.info("=== Starting Data Quality Gate Execution ===")
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        staging_df = fetch_staging_records(conn)
        valid_products = fetch_dimension_products(conn)
        config = load_rules_config(config_path)

        valid_df, invalid_df, metrics = run_data_quality_checks(
            staging_df, valid_products=valid_products, config=config
        )

        persist_staging_results(valid_df, invalid_df, conn)
        logger.info("=== Data Quality Gate Completed: %s ===", metrics)
        return metrics
    except Exception as e:
        conn.rollback()
        logger.error("Error executing data quality checks: %s", e)
        raise
    finally:
        if close_conn and conn is not None:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute Data Quality gates on staging.transactions.")
    parser.add_argument("--config", type=str, default=None, help="Path to DQ rules YAML")
    args = parser.parse_args()

    run_data_quality(config_path=args.config)

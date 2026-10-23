"""
Data Generation Script: generate_data.py
Simulates daily source transactions in CSV format: data/raw/daily_transactions_YYYY-MM-DD.csv.
Supports new transactions, incremental updates to existing transactions with updated timestamps,
and controlled data quality anomalies to validate quality gates.
"""

import os
import random
import argparse
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Predefined valid products matching sql/init_db.sql
VALID_PRODUCTS = [
    "PROD-001", "PROD-002", "PROD-003", "PROD-004", "PROD-005",
    "PROD-006", "PROD-007", "PROD-008", "PROD-009", "PROD-010",
    "PROD-011", "PROD-012", "PROD-013", "PROD-014", "PROD-015",
]

VALID_STATUSES = ["completed", "pending", "cancelled", "refunded"]


def find_existing_transactions(raw_dir: str) -> List[Dict[str, Any]]:
    """
    Scans previously generated raw CSV files in data/raw to find prior transactions
    that can be updated in subsequent incremental runs.
    """
    existing = []
    if not os.path.exists(raw_dir):
        return existing

    csv_files = sorted([f for f in os.listdir(raw_dir) if f.startswith("daily_transactions_") and f.endswith(".csv")])
    for fname in csv_files:
        fpath = os.path.join(raw_dir, fname)
        try:
            df = pd.read_csv(fpath)
            if not df.empty and "transaction_id" in df.columns:
                records = df.to_dict(orient="records")
                existing.extend(records)
        except Exception as e:
            logger.warning("Could not read prior file %s: %s", fname, e)
    return existing


def generate_transactions(
    target_date: str,
    output_dir: str = "data/raw",
    num_records: int = 100,
    update_ratio: float = 0.15,
    anomaly_ratio: float = 0.05,
    empty_file: bool = False,
) -> str:
    """
    Generate daily CSV data file for target_date.
    
    Args:
        target_date: Date string 'YYYY-MM-DD'
        output_dir: Directory where CSV will be stored
        num_records: Total records to generate
        update_ratio: Ratio of records updating previous transactions
        anomaly_ratio: Ratio of records injecting quality gate violations
        empty_file: If True, writes an empty CSV (headers only) for edge-case testing
    """
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"daily_transactions_{target_date}.csv")

    columns = [
        "transaction_id",
        "customer_id",
        "product_id",
        "amount",
        "transaction_date",
        "status",
        "updated_at",
    ]

    if empty_file:
        logger.info("Generating EMPTY file (headers only) at: %s", file_path)
        empty_df = pd.DataFrame(columns=columns)
        empty_df.to_csv(file_path, index=False)
        return file_path

    # Base timestamp for the day's batch
    batch_base_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(
        hour=random.randint(8, 12), minute=random.randint(0, 59), second=random.randint(0, 59)
    )

    records: List[Dict[str, Any]] = []

    # 1. Check for prior transactions to simulate updates
    prior_transactions = find_existing_transactions(output_dir)
    num_updates = int(num_records * update_ratio) if prior_transactions else 0
    num_new = num_records - num_updates

    # Simulate incremental updates to existing transactions
    if num_updates > 0:
        sampled_prior = random.sample(prior_transactions, min(num_updates, len(prior_transactions)))
        for prior in sampled_prior:
            # Modify amount slightly or change status (e.g. pending -> completed)
            new_status = "completed" if prior.get("status") == "pending" else random.choice(VALID_STATUSES)
            updated_ts = (batch_base_dt + timedelta(minutes=random.randint(1, 300))).isoformat()
            
            # 80% update amount slightly, 20% keep same amount
            orig_amount = float(prior.get("amount", 50.0))
            new_amount = round(orig_amount + random.choice([-5.0, 0.0, 10.0, 15.5]), 2)
            if new_amount <= 0:
                new_amount = orig_amount

            records.append({
                "transaction_id": str(prior["transaction_id"]),
                "customer_id": str(prior["customer_id"]),
                "product_id": str(prior.get("product_id", random.choice(VALID_PRODUCTS))),
                "amount": new_amount,
                "transaction_date": str(prior.get("transaction_date", target_date)),
                "status": new_status,
                "updated_at": updated_ts,
            })
        logger.info("Generated %d updated transactions from prior runs.", len(records))

    # 2. Generate brand new transactions
    date_int = target_date.replace("-", "")
    for i in range(num_new):
        tx_id = f"TXN-{date_int}-{i + 1:05d}"
        cust_id = f"CUST-{random.randint(100, 999):04d}"
        prod_id = random.choice(VALID_PRODUCTS)
        amount = round(random.uniform(5.0, 500.0), 2)
        status = random.choice(VALID_STATUSES)
        tx_time = batch_base_dt + timedelta(seconds=random.randint(0, 28800))
        updated_at = tx_time.isoformat()

        records.append({
            "transaction_id": tx_id,
            "customer_id": cust_id,
            "product_id": prod_id,
            "amount": amount,
            "transaction_date": target_date,
            "status": status,
            "updated_at": updated_at,
        })

    # 3. Inject controlled anomalies to test data quality gates (if anomaly_ratio > 0)
    num_anomalies = int(len(records) * anomaly_ratio)
    if num_anomalies > 0 and len(records) > 5:
        anomaly_indices = random.sample(range(len(records)), num_anomalies)
        anomaly_types = [
            "missing_customer",       # Completeness check violation
            "negative_amount",        # Validity check violation (amount <= 0)
            "foreign_key_violation",  # Referential integrity check violation
            "duplicate_txn_id",       # Uniqueness check violation
            "missing_date",           # Completeness check violation
        ]

        for idx, anom_type in zip(anomaly_indices, random.choices(anomaly_types, k=len(anomaly_indices))):
            rec = records[idx]
            if anom_type == "missing_customer":
                rec["customer_id"] = None
            elif anom_type == "negative_amount":
                rec["amount"] = -round(random.uniform(10.0, 99.0), 2)
            elif anom_type == "foreign_key_violation":
                rec["product_id"] = "PROD-NONEXISTENT-999"
            elif anom_type == "duplicate_txn_id" and idx > 0:
                rec["transaction_id"] = records[idx - 1]["transaction_id"]
            elif anom_type == "missing_date":
                rec["transaction_date"] = None

        logger.info("Injected %d controlled anomalies for data quality testing.", num_anomalies)

    df = pd.DataFrame(records)
    df.to_csv(file_path, index=False)
    logger.info("Successfully generated %d records in %s", len(df), file_path)
    return file_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate daily transaction CSV data.")
    parser.add_argument("--date", type=str, default=date.today().isoformat(),
                        help="Target date in YYYY-MM-DD format (default: today)")
    parser.add_argument("--output-dir", type=str, default="data/raw",
                        help="Directory to store raw CSV files")
    parser.add_argument("--records", type=int, default=100,
                        help="Total records to generate")
    parser.add_argument("--update-ratio", type=float, default=0.15,
                        help="Ratio of incremental update records")
    parser.add_argument("--anomaly-ratio", type=float, default=0.05,
                        help="Ratio of data quality anomaly records")
    parser.add_argument("--empty", action="store_true",
                        help="Generate an empty CSV file (headers only)")
    args = parser.parse_args()

    generate_transactions(
        target_date=args.date,
        output_dir=args.output_dir,
        num_records=args.records,
        update_ratio=args.update_ratio,
        anomaly_ratio=args.anomaly_ratio,
        empty_file=args.empty,
    )

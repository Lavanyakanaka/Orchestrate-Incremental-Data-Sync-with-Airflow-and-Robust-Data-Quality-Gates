"""
Unit tests for scripts/data_quality.py
Covers the 5 distinct data quality gates:
1. Schema Validation
2. Uniqueness Check
3. Completeness Check
4. Validity Check (amount > 0)
5. Referential Integrity Check (product_id in products catalog)
Also tests composite multi-rule failures and YAML rule configuration.
"""

import tempfile
import pandas as pd
import pytest
import yaml

from scripts.data_quality import (
    run_data_quality_checks,
    check_schema_conformity,
    check_uniqueness,
    check_completeness,
    check_validity,
    check_referential_integrity,
    load_rules_config,
)

SAMPLE_VALID_PRODUCTS = {"PROD-001", "PROD-002", "PROD-003", "PROD-004"}


@pytest.fixture
def clean_batch():
    """Provides a pristine batch of valid transaction records."""
    return pd.DataFrame([
        {
            "transaction_id": "TXN-101",
            "customer_id": "CUST-001",
            "product_id": "PROD-001",
            "amount": 49.99,
            "transaction_date": "2026-10-01",
            "status": "completed",
            "updated_at": "2026-10-01T10:00:00Z",
        },
        {
            "transaction_id": "TXN-102",
            "customer_id": "CUST-002",
            "product_id": "PROD-002",
            "amount": 120.00,
            "transaction_date": "2026-10-01",
            "status": "pending",
            "updated_at": "2026-10-01T11:00:00Z",
        },
    ])


def test_clean_batch_passes_all_checks(clean_batch):
    """A pristine batch should produce 100% valid records and 0 invalid."""
    valid_df, invalid_df, metrics = run_data_quality_checks(
        clean_batch, valid_products=SAMPLE_VALID_PRODUCTS
    )
    assert len(valid_df) == 2
    assert len(invalid_df) == 0
    assert metrics["valid"] == 2
    assert metrics["invalid"] == 0


def test_schema_validation_missing_columns():
    """Schema check must fail when required columns are absent."""
    df_missing_cols = pd.DataFrame([{"transaction_id": "TXN-1", "amount": 10.0}])
    passes, errors = check_schema_conformity(
        df_missing_cols, expected_cols=["transaction_id", "customer_id", "product_id"]
    )
    assert not passes.all()
    assert "Missing required columns" in errors.iloc[0]


def test_uniqueness_check_detects_duplicates(clean_batch):
    """Uniqueness check must identify duplicate transaction_ids."""
    dup_row = clean_batch.iloc[0:1].copy()
    batch_with_dup = pd.concat([clean_batch, dup_row], ignore_index=True)

    valid_df, invalid_df, metrics = run_data_quality_checks(
        batch_with_dup, valid_products=SAMPLE_VALID_PRODUCTS
    )
    assert metrics["uniqueness_failures"] >= 1
    # Both rows with duplicate transaction_id should be flagged
    assert len(invalid_df) >= 1
    assert any("Uniqueness Error" in str(msg) for msg in invalid_df["error_message"])


def test_completeness_check_flags_nulls():
    """Completeness check must flag NULL, NaN, or empty string values in required columns."""
    df = pd.DataFrame([
        {
            "transaction_id": "TXN-201",
            "customer_id": None,  # Missing customer_id
            "product_id": "PROD-001",
            "amount": 25.00,
            "transaction_date": "2026-10-01",
            "status": "completed",
            "updated_at": "2026-10-01T10:00:00Z",
        },
        {
            "transaction_id": "TXN-202",
            "customer_id": "CUST-002",
            "product_id": "PROD-001",
            "amount": 35.00,
            "transaction_date": "  ",  # Blank date
            "status": "completed",
            "updated_at": "2026-10-01T10:00:00Z",
        },
    ])
    valid_df, invalid_df, metrics = run_data_quality_checks(
        df, valid_products=SAMPLE_VALID_PRODUCTS
    )
    assert len(valid_df) == 0
    assert len(invalid_df) == 2
    assert metrics["completeness_failures"] == 2
    assert "customer_id" in invalid_df.loc[0, "error_message"]
    assert "transaction_date" in invalid_df.loc[1, "error_message"]


def test_validity_check_flags_non_positive_amounts():
    """Validity check must reject amounts that are <= 0 or non-numeric."""
    df = pd.DataFrame([
        {
            "transaction_id": "TXN-301",
            "customer_id": "CUST-001",
            "product_id": "PROD-001",
            "amount": -50.00,  # Negative
            "transaction_date": "2026-10-01",
            "status": "completed",
            "updated_at": "2026-10-01T10:00:00Z",
        },
        {
            "transaction_id": "TXN-302",
            "customer_id": "CUST-002",
            "product_id": "PROD-002",
            "amount": 0.00,  # Zero
            "transaction_date": "2026-10-01",
            "status": "completed",
            "updated_at": "2026-10-01T10:00:00Z",
        },
    ])
    valid_df, invalid_df, metrics = run_data_quality_checks(
        df, valid_products=SAMPLE_VALID_PRODUCTS
    )
    assert len(valid_df) == 0
    assert len(invalid_df) == 2
    assert metrics["validity_failures"] == 2
    assert any("Validity Error" in str(msg) for msg in invalid_df["error_message"])


def test_referential_integrity_check():
    """Referential integrity check must flag product_ids that do not exist in catalog."""
    df = pd.DataFrame([
        {
            "transaction_id": "TXN-401",
            "customer_id": "CUST-001",
            "product_id": "PROD-NONEXISTENT-999",  # Foreign key violation
            "amount": 99.99,
            "transaction_date": "2026-10-01",
            "status": "completed",
            "updated_at": "2026-10-01T10:00:00Z",
        }
    ])
    valid_df, invalid_df, metrics = run_data_quality_checks(
        df, valid_products=SAMPLE_VALID_PRODUCTS
    )
    assert len(valid_df) == 0
    assert len(invalid_df) == 1
    assert metrics["referential_failures"] == 1
    assert "Referential Integrity Error" in invalid_df.loc[0, "error_message"]


def test_composite_multi_rule_failure():
    """A record violating multiple rules must report all failure reasons in error_message."""
    df = pd.DataFrame([
        {
            "transaction_id": "TXN-501",
            "customer_id": None,  # Completeness failure
            "product_id": "PROD-GHOST",  # Referential integrity failure
            "amount": -100.0,  # Validity failure
            "transaction_date": "2026-10-01",
            "status": "completed",
            "updated_at": "2026-10-01T10:00:00Z",
        }
    ])
    valid_df, invalid_df, metrics = run_data_quality_checks(
        df, valid_products=SAMPLE_VALID_PRODUCTS
    )
    assert len(invalid_df) == 1
    err_msg = invalid_df.loc[0, "error_message"]
    assert "Completeness Error" in err_msg
    assert "Validity Error" in err_msg
    assert "Referential Integrity Error" in err_msg


def test_load_rules_config_from_yaml():
    """Verify that custom YAML rule configuration can be loaded and applied."""
    custom_cfg = {
        "rules": {
            "validity_check": {"enabled": True, "min_amount": 10.0},
            "completeness_check": {"enabled": True, "not_null_columns": ["transaction_id"]},
        }
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(custom_cfg, f)
        temp_path = f.name

    try:
        loaded = load_rules_config(temp_path)
        assert loaded["rules"]["validity_check"]["min_amount"] == 10.0
    finally:
        import os
        if os.path.exists(temp_path):
            os.remove(temp_path)

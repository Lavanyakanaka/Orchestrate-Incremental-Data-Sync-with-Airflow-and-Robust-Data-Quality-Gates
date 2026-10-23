"""
Unit tests for scripts/ingest.py
Tests reading of valid CSVs, empty files, headers-only files,
and incremental delta identification (new, updated, and stale records).
"""

import os
import tempfile
import pandas as pd
import pytest

from scripts.ingest import (
    read_source_csv,
    parse_and_clean_dataframe,
    identify_incremental_delta,
    EXPECTED_COLUMNS,
)


@pytest.fixture
def sample_raw_dataframe():
    """Provides a sample DataFrame representing incoming source records."""
    return pd.DataFrame([
        {
            "transaction_id": "TXN-001",
            "customer_id": "CUST-1001",
            "product_id": "PROD-001",
            "amount": "149.99",
            "transaction_date": "2026-10-01",
            "status": "completed",
            "updated_at": "2026-10-01T12:00:00Z",
        },
        {
            "transaction_id": "TXN-002",
            "customer_id": "CUST-1002",
            "product_id": "PROD-002",
            "amount": "79.50",
            "transaction_date": "2026-10-01",
            "status": "pending",
            "updated_at": "2026-10-01T14:30:00Z",
        },
        {
            "transaction_id": "TXN-003",
            "customer_id": "CUST-1003",
            "product_id": "PROD-003",
            "amount": "25.00",
            "transaction_date": "2026-10-01",
            "status": "completed",
            "updated_at": "2026-10-01T10:00:00Z",
        },
    ])


def test_read_source_csv_valid(sample_raw_dataframe):
    """Test reading a standard valid CSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        sample_raw_dataframe.to_csv(f.name, index=False)
        temp_path = f.name

    try:
        df = read_source_csv(temp_path)
        assert not df.empty
        assert len(df) == 3
        assert list(df.columns) == EXPECTED_COLUMNS
        assert df.loc[0, "transaction_id"] == "TXN-001"
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def test_read_source_csv_empty_zero_bytes():
    """Test that reading an empty (0 bytes) file returns an empty DataFrame gracefully."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        temp_path = f.name

    try:
        df = read_source_csv(temp_path)
        assert df.empty
        assert list(df.columns) == EXPECTED_COLUMNS
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def test_read_source_csv_headers_only():
    """Test reading a CSV with only header columns."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(",".join(EXPECTED_COLUMNS) + "\n")
        temp_path = f.name

    try:
        df = read_source_csv(temp_path)
        assert df.empty
        assert len(df) == 0
        assert list(df.columns) == EXPECTED_COLUMNS
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def test_read_source_csv_missing_file():
    """Test that a non-existent file path returns an empty DataFrame instead of raising."""
    non_existent = "non_existent_dir/never_existed_transactions.csv"
    df = read_source_csv(non_existent)
    assert df.empty
    assert list(df.columns) == EXPECTED_COLUMNS


def test_parse_and_clean_dataframe():
    """Test string whitespace stripping during parsing."""
    raw = pd.DataFrame([{
        "transaction_id": "  TXN-999  ",
        "customer_id": " CUST-888 ",
        "product_id": "PROD-001",
        "amount": "100.00",
        "transaction_date": "2026-10-01",
        "status": " completed ",
        "updated_at": "2026-10-01T12:00:00Z",
    }])
    cleaned = parse_and_clean_dataframe(raw)
    assert cleaned.loc[0, "transaction_id"] == "TXN-999"
    assert cleaned.loc[0, "customer_id"] == "CUST-888"
    assert cleaned.loc[0, "status"] == "completed"


def test_identify_incremental_delta_all_new(sample_raw_dataframe):
    """When target fact table is empty, all source records are classified as NEW."""
    target_state = {}  # Empty target
    delta_df, stats = identify_incremental_delta(sample_raw_dataframe, existing_lookup=target_state)

    assert len(delta_df) == 3
    assert stats["new"] == 3
    assert stats["updated"] == 0
    assert stats["stale"] == 0


def test_identify_incremental_delta_with_updates_and_stale(sample_raw_dataframe):
    """
    Test mixed batch:
    - TXN-001: not in target -> NEW
    - TXN-002: in target with earlier timestamp (10:00Z) vs source (14:30Z) -> UPDATED
    - TXN-003: in target with later timestamp (11:00Z) vs source (10:00Z) -> STALE (skipped)
    """
    target_state = {
        "TXN-002": pd.to_datetime("2026-10-01T10:00:00Z", utc=True),
        "TXN-003": pd.to_datetime("2026-10-01T11:00:00Z", utc=True),
    }

    delta_df, stats = identify_incremental_delta(sample_raw_dataframe, existing_lookup=target_state)

    assert stats["new"] == 1
    assert stats["updated"] == 1
    assert stats["stale"] == 1
    assert len(delta_df) == 2

    delta_ids = set(delta_df["transaction_id"])
    assert "TXN-001" in delta_ids
    assert "TXN-002" in delta_ids
    assert "TXN-003" not in delta_ids


def test_identify_incremental_preserves_dirty_records():
    """Verify that records with missing transaction_ids are kept for Data Quality quarantine."""
    dirty_data = pd.DataFrame([
        {
            "transaction_id": None,
            "customer_id": "CUST-100",
            "product_id": "PROD-001",
            "amount": "50.00",
            "transaction_date": "2026-10-01",
            "status": "completed",
            "updated_at": "2026-10-01T12:00:00Z",
        },
        {
            "transaction_id": "",
            "customer_id": "CUST-101",
            "product_id": "PROD-002",
            "amount": "60.00",
            "transaction_date": "2026-10-01",
            "status": "pending",
            "updated_at": "2026-10-01T12:00:00Z",
        },
    ])
    delta_df, stats = identify_incremental_delta(dirty_data, existing_lookup={})
    assert len(delta_df) == 2
    assert stats["preserved_dirty"] == 2

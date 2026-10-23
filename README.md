# Orchestrated Incremental Data Sync with Airflow and Robust Data Quality Gates

[![Apache Airflow](https://img.shields.io/badge/Orchestrator-Apache%20Airflow%202.8-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/Data%20Warehouse-PostgreSQL%2015-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Pandas](https://img.shields.io/badge/Processing-Pandas-150458?logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![Docker Compose](https://img.shields.io/badge/Containerization-Docker%20Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Testing](https://img.shields.io/badge/Tests-PyTest%20Passed-brightgreen?logo=pytest&logoColor=white)](https://docs.pytest.org/)

An enterprise-grade, automated data engineering solution designed to ingest, validate, and synchronize e-commerce transaction data incrementally into an analytical PostgreSQL data warehouse. Built with Apache Airflow (CeleryExecutor), native PostgreSQL declarative range partitioning, an extensible 5-pillar data quality gate framework with dead-letter quarantine routing, and end-to-end idempotency.

---

## Table of Contents

1. [Pipeline Architecture & Data Flow](#pipeline-architecture--data-flow)
2. [Key Engineering Decisions](#key-engineering-decisions)
   - [Incremental Loading Strategy](#1-incremental-loading-strategy)
   - [5-Pillar Data Quality Framework](#2-5-pillar-data-quality-framework)
   - [Quarantine Mechanism & Dead-Letter Isolation](#3-quarantine-mechanism--dead-letter-isolation)
   - [Declarative Partitioning & Indexing](#4-declarative-partitioning--indexing)
   - [Idempotency & Fault Tolerance](#5-idempotency--fault-tolerance)
3. [Database Schemas](#database-schemas)
   - [`staging.transactions`](#1-stagingtransactions)
   - [`public.fact_transactions`](#2-publicfact_transactions)
   - [`quarantine.transactions_errors`](#3-quarantinetransactions_errors)
   - [`public.products`](#4-publicproducts)
4. [Project Structure](#project-structure)
5. [Prerequisites & Environment Setup](#prerequisites--environment-setup)
6. [Quickstart Guide (Docker Compose)](#quickstart-guide-docker-compose)
7. [DAG Execution & Monitoring](#dag-execution--monitoring)
   - [Accessing Airflow Web UI](#accessing-airflow-web-ui)
   - [Triggering the DAG via UI or CLI](#triggering-the-dag-via-ui-or-cli)
   - [Inspecting the Analytical Warehouse](#inspecting-the-analytical-warehouse)
8. [Testing & Verification Evidence](#testing--verification-evidence)
9. [Troubleshooting & FAQ](#troubleshooting--faq)

---

## Pipeline Architecture & Data Flow

```
                      +-----------------------------+
                      |   scripts/generate_data.py  |
                      |   (Simulates daily CSVs)    |
                      +-----------------------------+
                                     |
                                     v
                      +-----------------------------+
                      |       data/raw/*.csv        |
                      +-----------------------------+
                                     |
                                     v
+-----------------------------------------------------------------------+
|  ingest_data_task (scripts/ingest.py)                                 |
|  1. Ingests CSV & handles empty files gracefully                      |
|  2. Queries public.fact_transactions for existing IDs                 |
|  3. Classifies Delta: New vs Updated (source > target updated_at)     |
|  4. Idempotently loads current delta into staging.transactions        |
+-----------------------------------------------------------------------+
                                     |
                                     v
+-----------------------------------------------------------------------+
|  run_data_quality_task (scripts/data_quality.py)                      |
|  Evaluates 5 Configurable Quality Gates (YAML/Dict):                  |
|  [Schema] -> [Uniqueness] -> [Completeness] -> [Validity] -> [FK Ref] |
+-----------------------------------------------------------------------+
                 /                                       \
          (Valid Records)                         (Invalid Records)
               /                                           \
              v                                             v
+-----------------------------------+     +-----------------------------------+
|  load_fact_table_task             |     |  quarantine_errors_task           |
|  (scripts/load_fact.py)           |     |  (scripts/load_fact.py)           |
|  Atomic PostgreSQL UPSERT         |     |  Appends to:                      |
|  into public.fact_transactions    |     |  quarantine.transactions_errors   |
|  (Declarative Range Partitions)   |     |  with audit error reasons & time  |
+-----------------------------------+     +-----------------------------------+
```

---

## Key Engineering Decisions

### 1. Incremental Loading Strategy
Rather than reading and re-inserting entire historical datasets every day, `scripts/ingest.py` implements an efficient high-water mark / state comparison mechanism:
- **New Records**: Records whose `transaction_id` does not exist in `public.fact_transactions` are captured for ingestion.
- **Modified Records**: For existing `transaction_id`s, `source.updated_at` is evaluated against `target.updated_at`. If `source.updated_at > target.updated_at`, the record is flagged as an update.
- **Stale / Duplicate Records**: If `source.updated_at <= target.updated_at`, the record is bypassed, preventing unnecessary write pressure on downstream systems.
- **Dirty Records**: Records lacking IDs or with unparseable fields are deliberately retained in the staging batch to allow Data Quality gates to quarantine and record their error reasons.

### 2. 5-Pillar Data Quality Framework
The data validation layer is decoupled, configurable (`config/data_quality_rules.yaml`), and executes 5 critical checks:
1. **Schema Validation**: Ensures all expected columns (`transaction_id`, `customer_id`, `product_id`, `amount`, `transaction_date`, `status`, `updated_at`) exist with compatible datatypes.
2. **Uniqueness Check**: Ensures no duplicate `transaction_id` entries exist within the current batch.
3. **Completeness Check**: Asserts `NOT NULL` constraints on business-critical fields: `transaction_id`, `customer_id`, `product_id`, `amount`, and `transaction_date`.
4. **Validity Check**: Validates that transaction `amount > 0` (flags 0, negative values, or malformed numbers).
5. **Referential Integrity Check**: Confirms that `product_id` matches an active SKU in the dimensional product catalog (`public.products`).

Composite failures on single records are concatenated (e.g. `Completeness Error: 'customer_id' is NULL | Validity Error: amount must be >= 0.01`).

### 3. Quarantine Mechanism & Dead-Letter Isolation
Corrupted or inconsistent data is automatically diverted away from the production analytical warehouse into `quarantine.transactions_errors`. Each quarantined row includes:
- The entire rejected payload (for audit and replaying).
- `error_message`: Plaintext description explaining exactly which check(s) failed.
- `error_timestamp`: ISO 8601 UTC timestamp capturing when the rejection occurred.

### 4. Declarative Partitioning & Indexing
To guarantee scale for multi-year analytical queries, `public.fact_transactions` is created with native PostgreSQL declarative partitioning:
- **Partition Key**: `PARTITION BY RANGE (transaction_date)`.
- **Primary Key**: `PRIMARY KEY (transaction_id, transaction_date)` (adheres to PostgreSQL requirement that unique keys must encompass partition keys).
- **Monthly Partitions**: Pre-configured declarative monthly ranges for 2024 through 2027.
- **Default Fallback Partition**: `public.fact_transactions_default` is mounted to capture any unexpected historical or future dates without failing queries.
- **Targeted Indexes**: Indexes created on `customer_id`, `transaction_date`, `product_id`, and `status`.

### 5. Idempotency & Fault Tolerance
- **Staging Isolation**: Staging tables are truncated prior to loading current batch deltas.
- **Atomic UPSERT**: Loading into `public.fact_transactions` utilizes `INSERT ... ON CONFLICT (transaction_id, transaction_date) DO UPDATE SET ... WHERE EXCLUDED.updated_at >= public.fact_transactions.updated_at`.
- **Empty File Handling**: If a daily CSV is empty or headers-only, the pipeline logs a warning and exits cleanly without failing tasks.
- **Airflow Alerting**: Custom failure callbacks log structured diagnostics and persist alerts to `logs/pipeline_alerts.log`.

---

## Database Schemas

### 1. `staging.transactions`
```sql
CREATE TABLE staging.transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);
```

### 2. `public.fact_transactions`
```sql
CREATE TABLE public.fact_transactions (
    transaction_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (transaction_id, transaction_date)
) PARTITION BY RANGE (transaction_date);

-- Analytical Indexes:
CREATE INDEX idx_fact_transactions_customer_id ON public.fact_transactions (customer_id);
CREATE INDEX idx_fact_transactions_date ON public.fact_transactions (transaction_date);
CREATE INDEX idx_fact_transactions_product_id ON public.fact_transactions (product_id);
CREATE INDEX idx_fact_transactions_status ON public.fact_transactions (status);
```

### 3. `quarantine.transactions_errors`
```sql
CREATE TABLE quarantine.transactions_errors (
    error_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    amount NUMERIC(10, 2),
    transaction_date DATE,
    status VARCHAR(20),
    updated_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT NOT NULL,
    error_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_quarantine_transaction_id ON quarantine.transactions_errors (transaction_id);
CREATE INDEX idx_quarantine_error_timestamp ON quarantine.transactions_errors (error_timestamp);
```

### 4. `public.products`
```sql
CREATE TABLE public.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price NUMERIC(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

---

## Project Structure

```
.
├── docker-compose.yml          # Production multi-service orchestration definition
├── Dockerfile                  # Custom Airflow image with data processing dependencies
├── requirements.txt            # Python dependencies (Airflow, Pandas, Psycopg2, PyTest)
├── .env.example                # Environment variables template
├── .env                        # Local active environment variables
├── .gitignore                  # Git exclusion rules
├── config/
│   └── data_quality_rules.yaml # Declarative 5-pillar DQ configuration
├── dags/
│   └── e-commerce_transactions_etl.py # Airflow DAG pipeline definition
├── scripts/
│   ├── __init__.py
│   ├── db_utils.py             # Database connection & SQL execution utilities
│   ├── generate_data.py        # Simulates daily transaction CSVs with updates/anomalies
│   ├── ingest.py               # Ingestion, incremental delta detection & staging loader
│   ├── data_quality.py         # 5-check data quality validation engine
│   └── load_fact.py            # UPSERT to fact table & quarantine error loader
├── sql/
│   ├── init_db.sql             # Schemas initialization and 15 seeded products
│   └── create_tables.sql       # DDL for staging, quarantine, fact table partitions & indexes
├── data/
│   └── raw/                    # Storage directory for daily transaction CSVs
├── tests/
│   ├── __init__.py
│   ├── test_ingest.py          # Unit tests for CSV parsing, empty files, incremental delta
│   └── test_data_quality.py    # Unit tests for all 5 data quality gates & composite errors
└── README.md                   # Complete architectural and operational documentation
```

---

## Prerequisites & Environment Setup

- **Docker Desktop**: version 20.10+ (supports Compose v2)
- **Python**: version 3.10 or 3.11 (for running local unit tests)
- **Git**: Installed and configured

Copy the environment template if `.env` does not already exist:
```bash
cp .env.example .env
```

---

## Quickstart Guide (Docker Compose)

### 1. Build and Start All Services
From the project root directory, run:
```bash
docker compose up -d --build
```

This starts:
- `postgres-airflow`: Internal Airflow metadata database.
- `postgres-dw`: Dedicated analytical data warehouse on host port `5433`.
- `airflow-redis`: Celery broker.
- `airflow-init`: Pre-runs migrations and creates admin credentials.
- `airflow-webserver`: Web UI exposed on `http://localhost:8080`.
- `airflow-scheduler`: Workflow scheduler.
- `airflow-worker`: Celery task execution worker.

### 2. Verify Service Health
Check that all containers are healthy:
```bash
docker compose ps
```

### 3. Teardown
To cleanly stop and tear down the stack:
```bash
docker compose down
```
To also remove persistent volumes:
```bash
docker compose down -v
```

---

## DAG Execution & Monitoring

### Accessing Airflow Web UI
1. Navigate to [http://localhost:8080](http://localhost:8080) in your web browser.
2. Sign in with the default credentials:
   - **Username**: `admin`
   - **Password**: `admin`
3. Locate the DAG **`ecommerce_transactions_etl`**.

### Triggering the DAG via UI or CLI

#### Via Web UI:
- Click the **Unpause** toggle button next to `ecommerce_transactions_etl`.
- Click the **Trigger DAG** button (Play icon) on the right side.

#### Via Docker CLI:
```bash
docker compose exec airflow-webserver airflow dags trigger ecommerce_transactions_etl
```

### Inspecting the Analytical Warehouse
Connect to the analytical data warehouse directly from your host or within Docker:
```bash
# Via psql inside Docker container:
docker compose exec postgres-dw psql -U dw_user -d analytical_dw

# Check Fact Table records:
SELECT transaction_id, customer_id, product_id, amount, transaction_date, status, updated_at
FROM public.fact_transactions
ORDER BY updated_at DESC LIMIT 10;

# Check Quarantined errors:
SELECT error_id, transaction_id, error_message, error_timestamp
FROM quarantine.transactions_errors
ORDER BY error_timestamp DESC LIMIT 10;

# Check Partition distribution:
SELECT tableoid::regclass AS partition_name, count(*)
FROM public.fact_transactions
GROUP BY partition_name;
```

---

## Testing & Verification Evidence

All pipeline components have been designed for isolated testing without requiring external database dependencies.

### Running Unit Tests Locally
Create a local Python virtual environment or use existing Python 3.10/3.11:
```bash
# Install test requirements
pip install -r requirements.txt

# Run full PyTest test suite
pytest tests/ -v
```

### Test Coverage Highlights:
- **`tests/test_ingest.py`**:
  - `test_read_source_csv_valid`: Verifies standard CSV ingestion.
  - `test_read_source_csv_empty_zero_bytes`: Tests 0-byte file handling without errors.
  - `test_read_source_csv_headers_only`: Verifies headers-only file returns 0 rows safely.
  - `test_read_source_csv_missing_file`: Verifies non-existent path returns empty DataFrame.
  - `test_parse_and_clean_dataframe`: Validates string whitespace stripping.
  - `test_identify_incremental_delta_all_new`: Verifies initial batch marks all records as NEW.
  - `test_identify_incremental_delta_with_updates_and_stale`: Tests detection of true updates (`source > target`) while filtering out stale/unchanged records (`source <= target`).
  - `test_identify_incremental_preserves_dirty_records`: Ensures malformed/null ID records are kept for quality gate evaluation rather than dropped.
- **`tests/test_data_quality.py`**:
  - `test_clean_batch_passes_all_checks`: Verifies clean data passes 100%.
  - `test_schema_validation_missing_columns`: Confirms schema mismatch is detected.
  - `test_uniqueness_check_detects_duplicates`: Verifies duplicate transaction IDs are isolated.
  - `test_completeness_check_flags_nulls`: Verifies NULL/empty values in mandatory columns fail.
  - `test_validity_check_flags_non_positive_amounts`: Verifies `amount <= 0` is flagged.
  - `test_referential_integrity_check`: Confirms foreign key lookup against `public.products`.
  - `test_composite_multi_rule_failure`: Verifies multi-violation records aggregate all error reasons.
  - `test_load_rules_config_from_yaml`: Verifies custom YAML configuration parser.

---

## Troubleshooting & FAQ

#### Q: How does the pipeline guarantee idempotency?
**A**:
1. Staging tables are truncated prior to ingestion (`TRUNCATE TABLE staging.transactions`).
2. The fact table loader uses PostgreSQL UPSERT (`ON CONFLICT (transaction_id, transaction_date) DO UPDATE SET ... WHERE EXCLUDED.updated_at >= fact_transactions.updated_at`).
Re-running the DAG multiple times with the exact same input data produces identical database states without duplicate records.

#### Q: Why does `public.fact_transactions` include `transaction_date` in its primary key?
**A**:
PostgreSQL requires that partitioned tables with `PRIMARY KEY` or `UNIQUE` constraints include all partition key columns in the constraint. Therefore, `PRIMARY KEY (transaction_id, transaction_date)` satisfies this constraint while ensuring unique transaction tracking per transaction date.

#### Q: Where are Airflow logs and pipeline alerts stored?
**A**:
Task execution logs are stored in `logs/` and mapped to `/opt/airflow/logs/`. Pipeline failure and success events are recorded in `logs/pipeline_alerts.log`.

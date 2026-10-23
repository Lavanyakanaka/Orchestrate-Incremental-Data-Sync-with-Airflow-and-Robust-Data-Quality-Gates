-- Analytical Database Tables DDL
-- Creates staging, quarantine, dimension, and partitioned fact tables

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- 1. Dimension Table: Products (for referential integrity)
CREATE TABLE IF NOT EXISTS public.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price NUMERIC(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Staging Tables: Transient staging layer for incoming batch delta
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    amount NUMERIC(10, 2),
    transaction_date DATE,
    status VARCHAR(20),
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Holding tables populated by data quality validation step
CREATE TABLE IF NOT EXISTS staging.valid_transactions (
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    amount NUMERIC(10, 2),
    transaction_date DATE,
    status VARCHAR(20),
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS staging.invalid_transactions (
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

-- 3. Quarantine Table: Dead-letter error table for records failing quality gates
CREATE TABLE IF NOT EXISTS quarantine.transactions_errors (
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

-- 4. Analytical Fact Table: Partitioned by Range on transaction_date
-- PostgreSQL requires partition keys in primary key constraints
CREATE TABLE IF NOT EXISTS public.fact_transactions (
    transaction_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (transaction_id, transaction_date)
) PARTITION BY RANGE (transaction_date);

-- Declarative monthly partitions for 2024, 2025, 2026, 2027
CREATE TABLE IF NOT EXISTS public.fact_transactions_y2024 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2025 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m01 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m02 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m03 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m04 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m05 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m06 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m07 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m08 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m09 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m10 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m11 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m12 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS public.fact_transactions_y2027 PARTITION OF public.fact_transactions
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

-- Default fallback partition for any dates outside explicitly defined ranges
CREATE TABLE IF NOT EXISTS public.fact_transactions_default PARTITION OF public.fact_transactions DEFAULT;

-- 5. Analytical Indexes for Query Optimization
CREATE INDEX IF NOT EXISTS idx_fact_transactions_customer_id ON public.fact_transactions (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_date ON public.fact_transactions (transaction_date);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_product_id ON public.fact_transactions (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_status ON public.fact_transactions (status);

-- Indexes for Quarantine Table
CREATE INDEX IF NOT EXISTS idx_quarantine_transaction_id ON quarantine.transactions_errors (transaction_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_error_timestamp ON quarantine.transactions_errors (error_timestamp);

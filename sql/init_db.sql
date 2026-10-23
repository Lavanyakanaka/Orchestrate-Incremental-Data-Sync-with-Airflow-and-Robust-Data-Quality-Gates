-- Analytical Database Initialization Script
-- Initializes schemas and the products dimension table for referential integrity checks

-- 1. Create required schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- 2. Create products dimension table
CREATE TABLE IF NOT EXISTS public.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Seed at least 10 sample products (with ON CONFLICT for idempotency)
INSERT INTO public.products (product_id, product_name, category, price)
VALUES
    ('PROD-001', 'Wireless Noise-Canceling Headphones', 'Electronics', 199.99),
    ('PROD-002', 'Ergonomic Mechanical Keyboard', 'Electronics', 129.50),
    ('PROD-003', 'Ultra HD 4K Monitor 27-inch', 'Electronics', 349.00),
    ('PROD-004', 'Organic Cotton T-Shirt', 'Apparel', 24.99),
    ('PROD-005', 'Waterproof Trail Running Shoes', 'Footwear', 89.95),
    ('PROD-006', 'Insulated Stainless Steel Water Bottle', 'Home & Kitchen', 19.99),
    ('PROD-007', 'Ceramic Pour-Over Coffee Maker', 'Home & Kitchen', 34.50),
    ('PROD-008', 'Ergonomic Mesh Office Chair', 'Furniture', 249.99),
    ('PROD-009', 'Smart Fitness Tracker Band', 'Wearables', 79.99),
    ('PROD-010', 'Compact USB-C Fast Charger 65W', 'Electronics', 29.99),
    ('PROD-011', 'Non-Stick Cast Iron Skillet 12-inch', 'Home & Kitchen', 45.00),
    ('PROD-012', 'Polarized Outdoor Sunglasses', 'Accessories', 39.95),
    ('PROD-013', 'Noise-Isolating Gaming Headset', 'Electronics', 59.99),
    ('PROD-014', 'Bamboo Cutting Board Set (3 pcs)', 'Home & Kitchen', 27.50),
    ('PROD-015', 'Lightweight Travel Backpack 30L', 'Accessories', 64.95)
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    price = EXCLUDED.price;

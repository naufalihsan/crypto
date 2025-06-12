-- ClickHouse initialization script
-- Create database if not exists
CREATE DATABASE IF NOT EXISTS crypto_db;

-- Use the database
USE crypto_db;

-- Create table for raw crypto data
CREATE TABLE IF NOT EXISTS crypto_prices (
    symbol String,
    price Float64,
    timestamp DateTime64(3),
    volume Float64,
    high_24h Float64,
    low_24h Float64,
    change_24h Float64,
    created_at DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (symbol, timestamp)
PARTITION BY toYYYYMM(timestamp);

-- Create table for aggregated data
CREATE TABLE IF NOT EXISTS crypto_price_aggregates (
    symbol String,
    avg_price Float64,
    min_price Float64,
    max_price Float64,
    total_volume Float64,
    price_change Float64,
    window_start DateTime64(3),
    window_end DateTime64(3),
    created_at DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (symbol, window_start)
PARTITION BY toYYYYMM(window_start); 
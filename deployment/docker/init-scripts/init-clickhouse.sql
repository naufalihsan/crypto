-- ClickHouse initialization script for crypto OLAP analytics

-- Create database
CREATE DATABASE IF NOT EXISTS crypto_analytics;

-- Use the database
USE crypto_analytics;

-- Raw crypto prices table (source data)
CREATE TABLE IF NOT EXISTS crypto_prices_raw (
    symbol String,
    price Decimal64(8),
    volume Decimal64(8),
    timestamp DateTime64(3),
    source String
) ENGINE = MergeTree()
ORDER BY (symbol, timestamp)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- Price analytics aggregation table
CREATE TABLE IF NOT EXISTS price_analytics (
    symbol String,
    timestamp DateTime64(3),
    period_minutes UInt32,
    open_price Decimal64(8),
    close_price Decimal64(8),
    high_price Decimal64(8),
    low_price Decimal64(8),
    avg_price Decimal64(8),
    volume Decimal64(8),
    volatility Decimal64(8)
) ENGINE = MergeTree()
ORDER BY (symbol, period_minutes, timestamp)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- Market trends table
CREATE TABLE IF NOT EXISTS market_trends (
    symbol String,
    trend_direction String,
    strength Float64,
    confidence Nullable(Float64),
    duration_hours Nullable(UInt32),
    start_time Nullable(DateTime64(3)),
    end_time Nullable(DateTime64(3)),
    timestamp DateTime64(3),
    volume Nullable(Decimal64(8)),
    market_cap Nullable(Decimal64(8))
) ENGINE = MergeTree()
ORDER BY (symbol, timestamp)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- Technical indicators aggregation table
CREATE TABLE IF NOT EXISTS technical_indicators_agg (
    symbol String,
    timestamp DateTime64(3),
    sma_20 Nullable(Decimal64(8)),
    ema_12 Nullable(Decimal64(8)),
    rsi Nullable(Decimal64(8)),
    macd_line Nullable(Decimal64(8)),
    macd_signal Nullable(Decimal64(8)),
    bollinger_upper Nullable(Decimal64(8)),
    bollinger_lower Nullable(Decimal64(8))
) ENGINE = MergeTree()
ORDER BY (symbol, timestamp)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- Market anomalies aggregation table (simplified)
CREATE TABLE IF NOT EXISTS market_anomalies_agg (
    symbol String,
    anomaly_type String,
    severity String,
    description String,
    confidence_score Float64,
    timestamp DateTime64(3),
    metadata String
) ENGINE = MergeTree()
ORDER BY (symbol, timestamp)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

-- Create materialized views for real-time aggregations

-- 5-minute price analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS price_analytics_5m_mv
TO price_analytics
AS SELECT
    symbol,
    time_bucket as timestamp,
    5 as period_minutes,
    argMin(price, timestamp) as open_price,
    argMax(price, timestamp) as close_price,
    max(price) as high_price,
    min(price) as low_price,
    avg(price) as avg_price,
    sum(volume) as volume,
    stddevPop(price) as volatility
FROM (
    SELECT 
        symbol,
        price,
        volume,
        timestamp,
        toStartOfFiveMinutes(timestamp) as time_bucket
    FROM crypto_prices_raw
)
GROUP BY symbol, time_bucket;

-- Hourly price analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS price_analytics_1h_mv
TO price_analytics
AS SELECT
    symbol,
    time_bucket as timestamp,
    60 as period_minutes,
    argMin(price, timestamp) as open_price,
    argMax(price, timestamp) as close_price,
    max(price) as high_price,
    min(price) as low_price,
    avg(price) as avg_price,
    sum(volume) as volume,
    stddevPop(price) as volatility
FROM (
    SELECT 
        symbol,
        price,
        volume,
        timestamp,
        toStartOfHour(timestamp) as time_bucket
    FROM crypto_prices_raw
)
GROUP BY symbol, time_bucket;

-- Daily price analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS price_analytics_1d_mv
TO price_analytics
AS SELECT
    symbol,
    time_bucket as timestamp,
    1440 as period_minutes,
    argMin(price, timestamp) as open_price,
    argMax(price, timestamp) as close_price,
    max(price) as high_price,
    min(price) as low_price,
    avg(price) as avg_price,
    sum(volume) as volume,
    stddevPop(price) as volatility
FROM (
    SELECT 
        symbol,
        price,
        volume,
        timestamp,
        toStartOfDay(timestamp) as time_bucket
    FROM crypto_prices_raw
)
GROUP BY symbol, time_bucket;

-- Create useful views for common queries

-- Latest prices view
CREATE VIEW IF NOT EXISTS latest_prices AS
SELECT 
    symbol,
    argMax(price, timestamp) as current_price,
    max(timestamp) as last_update,
    argMax(volume, timestamp) as current_volume
FROM crypto_prices_raw
GROUP BY symbol;

-- Market overview view (fixed to handle empty data)
CREATE VIEW IF NOT EXISTS market_overview AS
SELECT 
    COUNT(DISTINCT symbol) as total_symbols,
    CASE 
        WHEN AVG(low_price) > 0 THEN AVG(high_price - low_price) / AVG(low_price) * 100
        ELSE 0
    END as avg_daily_range,
    SUM(volume) as total_volume_24h
FROM price_analytics 
WHERE period_minutes = 1440 
    AND timestamp >= now() - INTERVAL 1 DAY;
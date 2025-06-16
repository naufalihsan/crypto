-- PostgreSQL initialization script for Crypto Pipeline
-- This script creates all necessary tables and indexes for the OLTP service

-- ===========================================
-- Core Tables
-- ===========================================

-- Create table for real-time crypto price data
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(20, 8) NOT NULL CHECK (price > 0),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    volume DECIMAL(20, 8) CHECK (volume IS NULL OR volume >= 0),
    high_24h DECIMAL(20, 8),
    low_24h DECIMAL(20, 8),
    change_24h DECIMAL(10, 4),
    source VARCHAR(50) DEFAULT 'unknown',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp, source)
);

-- Create table for technical indicators
CREATE TABLE IF NOT EXISTS technical_indicators (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    sma DECIMAL(20, 8),
    ema DECIMAL(20, 8),
    rsi DECIMAL(10, 4) CHECK (rsi IS NULL OR (rsi >= 0 AND rsi <= 100)),
    macd DECIMAL(20, 8),
    bollinger_upper DECIMAL(20, 8),
    bollinger_lower DECIMAL(20, 8),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp)
);

-- Create table for market anomalies
CREATE TABLE IF NOT EXISTS market_anomalies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    description TEXT,
    severity VARCHAR(20) DEFAULT 'medium' CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create table for aggregated price data (for analytics)
CREATE TABLE IF NOT EXISTS crypto_price_aggregates (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    avg_price DECIMAL(20, 8) NOT NULL,
    min_price DECIMAL(20, 8) NOT NULL,
    max_price DECIMAL(20, 8) NOT NULL,
    total_volume DECIMAL(20, 8),
    price_change DECIMAL(10, 4),
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, window_start, window_end)
);

-- ===========================================
-- Performance Indexes
-- ===========================================

-- Indexes for crypto_prices table
CREATE INDEX IF NOT EXISTS idx_crypto_prices_symbol ON crypto_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_timestamp ON crypto_prices(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_symbol_timestamp ON crypto_prices(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_source ON crypto_prices(source);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_created_at ON crypto_prices(created_at DESC);

-- Indexes for technical_indicators table
CREATE INDEX IF NOT EXISTS idx_technical_indicators_symbol ON technical_indicators(symbol);
CREATE INDEX IF NOT EXISTS idx_technical_indicators_timestamp ON technical_indicators(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_technical_indicators_symbol_timestamp ON technical_indicators(symbol, timestamp DESC);

-- Indexes for market_anomalies table
CREATE INDEX IF NOT EXISTS idx_market_anomalies_symbol ON market_anomalies(symbol);
CREATE INDEX IF NOT EXISTS idx_market_anomalies_timestamp ON market_anomalies(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_anomalies_type ON market_anomalies(alert_type);
CREATE INDEX IF NOT EXISTS idx_market_anomalies_severity ON market_anomalies(severity);
CREATE INDEX IF NOT EXISTS idx_market_anomalies_symbol_timestamp ON market_anomalies(symbol, timestamp DESC);

-- Indexes for aggregated data
CREATE INDEX IF NOT EXISTS idx_crypto_aggregates_symbol ON crypto_price_aggregates(symbol);
CREATE INDEX IF NOT EXISTS idx_crypto_aggregates_window ON crypto_price_aggregates(window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_crypto_aggregates_symbol_window ON crypto_price_aggregates(symbol, window_start DESC);

-- ===========================================
-- Comments for Documentation
-- ===========================================

COMMENT ON TABLE crypto_prices IS 'Real-time cryptocurrency price data from various sources';
COMMENT ON TABLE technical_indicators IS 'Technical analysis indicators calculated from price data';
COMMENT ON TABLE market_anomalies IS 'Detected market anomalies and alerts';
COMMENT ON TABLE crypto_price_aggregates IS 'Pre-aggregated price data for analytics and reporting';

COMMENT ON COLUMN crypto_prices.source IS 'Data source identifier (e.g., binance_websocket, coinbase_api)';
COMMENT ON COLUMN technical_indicators.rsi IS 'Relative Strength Index (0-100)';
COMMENT ON COLUMN market_anomalies.metadata IS 'Additional context data for the anomaly in JSON format';
COMMENT ON COLUMN market_anomalies.severity IS 'Alert severity level: low, medium, high, critical';

-- ===========================================
-- Schema Migration Tracking
-- ===========================================

-- Create schema migrations table for version tracking
CREATE TABLE IF NOT EXISTS schema_migrations (
    id SERIAL PRIMARY KEY,
    version VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Record initial schema version
INSERT INTO schema_migrations (version, description) 
VALUES ('1.0.0', 'Initial schema with crypto_prices, technical_indicators, market_anomalies, and crypto_price_aggregates tables')
ON CONFLICT (version) DO NOTHING;
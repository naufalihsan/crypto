-- PostgreSQL initialization script
-- Create database if not exists (this is handled by POSTGRES_DB env var)

-- Create table for raw crypto data
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    volume DECIMAL(20, 8),
    high_24h DECIMAL(20, 8),
    low_24h DECIMAL(20, 8),
    change_24h DECIMAL(10, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_crypto_prices_symbol ON crypto_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_timestamp ON crypto_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_symbol_timestamp ON crypto_prices(symbol, timestamp);

-- Create table for aggregated data
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for aggregated data
CREATE INDEX IF NOT EXISTS idx_crypto_aggregates_symbol ON crypto_price_aggregates(symbol);
CREATE INDEX IF NOT EXISTS idx_crypto_aggregates_window ON crypto_price_aggregates(window_start, window_end); 
#!/bin/bash

# Robust ClickHouse initialization script
# This script ensures the database is properly initialized even if ClickHouse skips automatic initialization

set -e  # Exit on any error

echo "=== ClickHouse Robust Initialization ==="

# ClickHouse connection parameters
CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-clickhouse}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-admin}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-admin}

# Function to wait for ClickHouse to be ready
wait_for_clickhouse() {
    echo "Waiting for ClickHouse to be ready..."
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if clickhouse-client --host="$CLICKHOUSE_HOST" --port="$CLICKHOUSE_PORT" --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --query "SELECT 1" > /dev/null 2>&1; then
            echo "ClickHouse is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: ClickHouse not ready yet, waiting..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: ClickHouse failed to become ready after $max_attempts attempts"
    return 1
}

# Function to check if database exists
database_exists() {
    clickhouse-client --host="$CLICKHOUSE_HOST" --port="$CLICKHOUSE_PORT" --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --query "EXISTS DATABASE crypto_analytics" 2>/dev/null | grep -q "1"
}

# Function to check if tables exist
tables_exist() {
    local table_count=$(clickhouse-client --host="$CLICKHOUSE_HOST" --port="$CLICKHOUSE_PORT" --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --database=crypto_analytics --query "SELECT count() FROM system.tables WHERE database = 'crypto_analytics'" 2>/dev/null || echo "0")
    [ "$table_count" -ge 10 ]  # We expect at least 10 objects (tables + views)
}

# Function to run initialization
run_initialization() {
    echo "Running ClickHouse initialization..."
    
    # First, create the database
    echo "Creating database..."
    clickhouse-client --host="$CLICKHOUSE_HOST" --port="$CLICKHOUSE_PORT" --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --query "CREATE DATABASE IF NOT EXISTS crypto_analytics"
    
    # Then run the full initialization script
    echo "Creating tables and views..."
    clickhouse-client --host="$CLICKHOUSE_HOST" --port="$CLICKHOUSE_PORT" --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --multiquery < /docker-entrypoint-initdb.d/01-init-clickhouse.sql
    
    if [ $? -eq 0 ]; then
        echo "✅ ClickHouse initialization completed successfully!"
        return 0
    else
        echo "❌ ClickHouse initialization failed!"
        return 1
    fi
}

# Function to verify initialization
verify_initialization() {
    echo "Verifying initialization..."
    
    # Check database exists
    if ! database_exists; then
        echo "❌ Database crypto_analytics does not exist"
        return 1
    fi
    
    # Check tables exist
    if ! tables_exist; then
        echo "❌ Not all tables/views were created"
        return 1
    fi
    
    # List all created objects
    echo "✅ Verification successful! Created objects:"
    clickhouse-client --host="$CLICKHOUSE_HOST" --port="$CLICKHOUSE_PORT" --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --database=crypto_analytics --query "SHOW TABLES" | sed 's/^/  - /'
    
    return 0
}

# Main execution
main() {
    # Wait for ClickHouse to be ready
    if ! wait_for_clickhouse; then
        exit 1
    fi
    
    # Check if already initialized
    if database_exists && tables_exist; then
        echo "✅ ClickHouse already initialized, skipping..."
        verify_initialization
        return 0
    fi
    
    # Run initialization
    if ! run_initialization; then
        echo "❌ Initialization failed, exiting..."
        exit 1
    fi
    
    # Verify initialization
    if ! verify_initialization; then
        echo "❌ Verification failed, exiting..."
        exit 1
    fi
    
    echo "🎉 ClickHouse initialization completed successfully!"
}

# Run main function
main "$@" 
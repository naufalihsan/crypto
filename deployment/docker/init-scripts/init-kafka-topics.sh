#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:29092 1 30

# Create topics with proper configurations
echo "Creating Kafka topics..."

# crypto-prices topic
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic crypto-prices \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete

# technical-indicators topic
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic technical-indicators \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete

# market-anomalies topic
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic market-anomalies \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete

# crypto-market-data topic
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic crypto-market-data \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete

echo "Kafka topics created successfully!" 
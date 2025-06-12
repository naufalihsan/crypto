#!/usr/bin/env python3
"""
Kafka to ClickHouse Connector
Consumes cryptocurrency data from Kafka and inserts into ClickHouse OLAP database
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

import clickhouse_connect
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OLAPConnector:
    """Handles connection and operations with ClickHouse OLAP database"""
    
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.client = None
        self.connect()
    
    def connect(self):
        """Establish connection to ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password
            )
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}")
            
            # Test connection
            result = self.client.query("SELECT 1")
            logger.info("ClickHouse connection test successful")
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def insert_price_data(self, data: List[Dict[str, Any]]):
        """Insert price data into ClickHouse OLAP table"""
        if not data:
            return
        
        try:
            # Prepare data for insertion
            rows = []
            for record in data:
                row = [
                    record.get('symbol', ''),
                    datetime.fromtimestamp(record.get('timestamp', 0) / 1000),
                    float(record.get('price', 0)),
                    float(record.get('volume', 0)),
                    float(record.get('high_24h', 0)),
                    float(record.get('low_24h', 0)),
                    float(record.get('change_24h', 0)),
                    float(record.get('market_cap', 0))
                ]
                rows.append(row)
            
            # Insert data
            self.client.insert(
                'price_data_olap',
                rows,
                column_names=[
                    'symbol', 'timestamp', 'price', 'volume',
                    'high_24h', 'low_24h', 'change_24h', 'market_cap'
                ]
            )
            
            logger.info(f"Inserted {len(rows)} price records into ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to insert price data: {e}")
            raise
    
    def insert_technical_indicators(self, data: List[Dict[str, Any]]):
        """Insert technical indicators into ClickHouse OLAP table"""
        if not data:
            return
        
        try:
            rows = []
            for record in data:
                row = [
                    record.get('symbol', ''),
                    datetime.fromtimestamp(record.get('timestamp', 0) / 1000),
                    record.get('indicator_type', ''),
                    float(record.get('value', 0)),
                    int(record.get('period', 0))
                ]
                rows.append(row)
            
            self.client.insert(
                'technical_indicators_olap',
                rows,
                column_names=[
                    'symbol', 'timestamp', 'indicator_type', 'value', 'period'
                ]
            )
            
            logger.info(f"Inserted {len(rows)} technical indicator records into ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to insert technical indicators: {e}")
            raise
    
    def insert_market_anomalies(self, data: List[Dict[str, Any]]):
        """Insert market anomalies into ClickHouse OLAP table"""
        if not data:
            return
        
        try:
            rows = []
            for record in data:
                row = [
                    record.get('symbol', ''),
                    datetime.fromtimestamp(record.get('timestamp', 0) / 1000),
                    record.get('anomaly_type', ''),
                    float(record.get('severity', 0)),
                    record.get('description', ''),
                    json.dumps(record.get('metadata', {}))
                ]
                rows.append(row)
            
            self.client.insert(
                'market_anomalies_olap',
                rows,
                column_names=[
                    'symbol', 'timestamp', 'anomaly_type', 'severity', 'description', 'metadata'
                ]
            )
            
            logger.info(f"Inserted {len(rows)} anomaly records into ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to insert market anomalies: {e}")
            raise

class KafkaClickHouseConnector:
    """Main connector class that consumes from Kafka and writes to ClickHouse"""
    
    def __init__(self):
        # Kafka configuration
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topics = ['crypto-prices', 'technical-indicators', 'market-anomalies']
        
        # ClickHouse configuration
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        self.clickhouse_db = os.getenv('CLICKHOUSE_DB', 'crypto_olap')
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER', 'admin')
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'admin')
        
        # Initialize connections
        self.clickhouse = OLAPConnector(
            self.clickhouse_host,
            self.clickhouse_port,
            self.clickhouse_db,
            self.clickhouse_user,
            self.clickhouse_password
        )
        
        self.consumer = None
        self.batch_size = 100
        self.batch_timeout = 30  # seconds
        
    def create_kafka_consumer(self):
        """Create and configure Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.kafka_servers,
                group_id='clickhouse-connector',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=self.batch_timeout * 1000
            )
            logger.info(f"Created Kafka consumer for topics: {self.topics}")
            
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise
    
    def process_batch(self, batch: List[tuple]):
        """Process a batch of messages and insert into ClickHouse"""
        if not batch:
            return
        
        # Group messages by topic
        price_data = []
        technical_indicators = []
        market_anomalies = []
        
        for topic, message in batch:
            try:
                if topic == 'crypto-prices':
                    price_data.append(message)
                elif topic == 'technical-indicators':
                    technical_indicators.append(message)
                elif topic == 'market-anomalies':
                    market_anomalies.append(message)
                    
            except Exception as e:
                logger.error(f"Error processing message from {topic}: {e}")
                continue
        
        # Insert data into ClickHouse
        try:
            if price_data:
                self.clickhouse.insert_price_data(price_data)
            if technical_indicators:
                self.clickhouse.insert_technical_indicators(technical_indicators)
            if market_anomalies:
                self.clickhouse.insert_market_anomalies(market_anomalies)
                
            logger.info(f"Processed batch: {len(price_data)} prices, "
                       f"{len(technical_indicators)} indicators, "
                       f"{len(market_anomalies)} anomalies")
                       
        except Exception as e:
            logger.error(f"Error inserting batch data: {e}")
            raise
    
    def run(self):
        """Main processing loop"""
        logger.info("Starting Kafka to ClickHouse connector...")
        
        try:
            self.create_kafka_consumer()
            
            batch = []
            last_batch_time = time.time()
            
            for message in self.consumer:
                try:
                    batch.append((message.topic, message.value))
                    
                    # Process batch when size limit reached or timeout exceeded
                    current_time = time.time()
                    if (len(batch) >= self.batch_size or 
                        current_time - last_batch_time >= self.batch_timeout):
                        
                        self.process_batch(batch)
                        batch = []
                        last_batch_time = current_time
                        
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, shutting down...")
                    break
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
            
            # Process remaining messages in batch
            if batch:
                self.process_batch(batch)
                
        except Exception as e:
            logger.error(f"Error in main processing loop: {e}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("Kafka to ClickHouse connector stopped")


def main():
    """Main entry point"""
    try:
        connector = KafkaClickHouseConnector()
        connector.run()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise


if __name__ == "__main__":
    main() 
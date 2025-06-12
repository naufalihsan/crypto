import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
import asyncio
import asyncpg
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaPSQLConnector:
    """Kafka to PostgreSQL connector for real-time data ingestion"""
    
    def __init__(
        self,
        kafka_bootstrap_servers="kafka:29092",
        postgres_host="postgres-oltp",
        postgres_port=5432,
        postgres_db="crypto_oltp",
        postgres_user="admin",
        postgres_password="admin"
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.postgres_config = {
            'host': postgres_host,
            'port': postgres_port,
            'database': postgres_db,
            'user': postgres_user,
            'password': postgres_password
        }
        
        self.consumers = {}
        self.db_pool = None
        self.running = False
        
        # Topic to table mapping
        self.topic_handlers = {
            'crypto-prices': self.handle_crypto_prices,
            'crypto-market-data': self.handle_market_data,
            'crypto-indicators': self.handle_indicators,
            'crypto-anomalies': self.handle_anomalies,
            'crypto-volume-analysis': self.handle_volume_anomalies
        }

    async def create_db_pool(self):
        """Create PostgreSQL connection pool"""
        try:
            self.db_pool = await asyncpg.create_pool(
                **self.postgres_config,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            logger.info("PostgreSQL connection pool created")
            
            # Test connection
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval("SELECT version()")
                logger.info(f"Connected to PostgreSQL: {result}")
                
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise

    def create_kafka_consumers(self):
        """Create Kafka consumers for each topic"""
        try:
            for topic in self.topic_handlers.keys():
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    group_id=f'oltp-connector-{topic}',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    key_deserializer=lambda m: m.decode('utf-8') if m else None,
                    auto_offset_reset='latest',  # Start from latest messages
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    consumer_timeout_ms=1000  # Timeout for polling
                )
                self.consumers[topic] = consumer
                logger.info(f"Created Kafka consumer for topic: {topic}")
                
        except Exception as e:
            logger.error(f"Failed to create Kafka consumers: {e}")
            raise

    async def handle_crypto_prices(self, data: Dict[str, Any]) -> bool:
        """Handle crypto price data"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO crypto_prices (
                        symbol, price, price_change, volume, high, low,
                        timestamp_ms, source, produced_at, producer_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """, 
                    data.get('symbol'),
                    float(data.get('price', 0)),
                    float(data.get('price_change', 0)),
                    float(data.get('volume', 0)),
                    float(data.get('high', 0)),
                    float(data.get('low', 0)),
                    int(data.get('timestamp', 0)),
                    data.get('source'),
                    datetime.fromisoformat(data['produced_at'].replace('Z', '+00:00')) if data.get('produced_at') else None,
                    data.get('producer_id')
                )
            return True
        except Exception as e:
            logger.error(f"Error inserting crypto price data: {e}")
            return False

    async def handle_market_data(self, data: Dict[str, Any]) -> bool:
        """Handle market data"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO crypto_market_data (
                        coin_id, symbol, name, current_price, market_cap,
                        price_change_24h, volume_24h, timestamp_ms, source,
                        produced_at, producer_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                    data.get('id'),
                    data.get('symbol'),
                    data.get('name'),
                    float(data.get('current_price', 0)) if data.get('current_price') else None,
                    int(data.get('market_cap', 0)) if data.get('market_cap') else None,
                    float(data.get('price_change_24h', 0)) if data.get('price_change_24h') else None,
                    float(data.get('volume_24h', 0)) if data.get('volume_24h') else None,
                    int(data.get('timestamp', 0)),
                    data.get('source'),
                    datetime.fromisoformat(data['produced_at'].replace('Z', '+00:00')) if data.get('produced_at') else None,
                    data.get('producer_id')
                )
            return True
        except Exception as e:
            logger.error(f"Error inserting market data: {e}")
            return False

    async def handle_indicators(self, data: Dict[str, Any]) -> bool:
        """Handle technical indicators"""
        try:
            # Parse JSON string if needed
            if isinstance(data, str):
                data = json.loads(data)
                
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO crypto_indicators (
                        symbol, sma, ema, rsi, macd, bollinger_upper,
                        bollinger_lower, window_end_ms
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                    data.get('symbol'),
                    float(data.get('sma', 0)) if data.get('sma') else None,
                    float(data.get('ema', 0)) if data.get('ema') else None,
                    float(data.get('rsi', 0)) if data.get('rsi') else None,
                    float(data.get('macd', 0)) if data.get('macd') else None,
                    float(data.get('bollinger_upper', 0)) if data.get('bollinger_upper') else None,
                    float(data.get('bollinger_lower', 0)) if data.get('bollinger_lower') else None,
                    int(data.get('window_end', 0))
                )
            return True
        except Exception as e:
            logger.error(f"Error inserting indicators data: {e}")
            return False

    async def handle_anomalies(self, data: Dict[str, Any]) -> bool:
        """Handle anomaly alerts"""
        try:
            # Parse JSON string if needed
            if isinstance(data, str):
                data = json.loads(data)
                
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO crypto_anomalies (
                        symbol, alert_type, price, price_change, timestamp_ms
                    ) VALUES ($1, $2, $3, $4, $5)
                """,
                    data.get('symbol'),
                    data.get('alert_type'),
                    float(data.get('price', 0)) if data.get('price') else None,
                    float(data.get('price_change', 0)) if data.get('price_change') else None,
                    int(data.get('timestamp', 0))
                )
            return True
        except Exception as e:
            logger.error(f"Error inserting anomaly data: {e}")
            return False

    async def handle_volume_anomalies(self, data: Dict[str, Any]) -> bool:
        """Handle volume anomaly data"""
        try:
            # Parse JSON string if needed
            if isinstance(data, str):
                data = json.loads(data)
                
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO crypto_volume_anomalies (
                        symbol, alert_type, volume, mean_volume, timestamp_ms
                    ) VALUES ($1, $2, $3, $4, $5)
                """,
                    data.get('symbol'),
                    data.get('alert_type'),
                    float(data.get('volume', 0)) if data.get('volume') else None,
                    float(data.get('mean_volume', 0)) if data.get('mean_volume') else None,
                    int(data.get('timestamp', 0))
                )
            return True
        except Exception as e:
            logger.error(f"Error inserting volume anomaly data: {e}")
            return False

    async def process_topic_messages(self, topic: str):
        """Process messages from a specific topic"""
        consumer = self.consumers[topic]
        handler = self.topic_handlers[topic]
        
        try:
            while self.running:
                message_batch = consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    await asyncio.sleep(0.1)
                    continue
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            success = await handler(message.value)
                            if success:
                                logger.debug(f"Processed message from {topic}: {message.key}")
                            else:
                                logger.warning(f"Failed to process message from {topic}")
                                
                        except Exception as e:
                            logger.error(f"Error processing message from {topic}: {e}")
                            continue
                            
        except Exception as e:
            logger.error(f"Error in topic processor for {topic}: {e}")
        finally:
            consumer.close()
            logger.info(f"Closed consumer for topic: {topic}")

    async def health_check(self):
        """Perform health checks"""
        while self.running:
            try:
                # Check database connection
                if self.db_pool:
                    async with self.db_pool.acquire() as conn:
                        await conn.fetchval("SELECT 1")
                        
                # Check Kafka consumers
                for topic, consumer in self.consumers.items():
                    if not consumer.bootstrap_connected():
                        logger.warning(f"Kafka consumer for {topic} is not connected")
                        
                logger.debug("Health check passed")
                await asyncio.sleep(30)  # Health check every 30 seconds
                
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                await asyncio.sleep(10)

    async def run(self):
        """Main run method"""
        logger.info("Starting Kafka to PostgreSQL connector...")
        self.running = True
        
        try:
            # Initialize database connection
            await self.create_db_pool()
            
            # Create Kafka consumers
            self.create_kafka_consumers()
            
            # Start processing tasks for each topic
            tasks = []
            for topic in self.topic_handlers.keys():
                task = asyncio.create_task(self.process_topic_messages(topic))
                tasks.append(task)
                
            # Start health check task
            health_task = asyncio.create_task(self.health_check())
            tasks.append(health_task)
            
            logger.info(f"Started processing {len(self.topic_handlers)} topics")
            
            # Wait for all tasks
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error in main run loop: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self):
        """Stop the connector gracefully"""
        logger.info("Stopping Kafka to PostgreSQL connector...")
        self.running = False
        
        # Close Kafka consumers
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
                logger.info(f"Closed consumer for topic: {topic}")
            except Exception as e:
                logger.error(f"Error closing consumer for {topic}: {e}")
        
        # Close database pool
        if self.db_pool:
            await self.db_pool.close()
            logger.info("Closed database connection pool")
        
        logger.info("Kafka to PostgreSQL connector stopped")

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

async def main():
    """Main entry point"""
    connector = KafkaPSQLConnector()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, connector.signal_handler)
    signal.signal(signal.SIGTERM, connector.signal_handler)
    
    try:
        await connector.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        await connector.stop()

if __name__ == "__main__":
    asyncio.run(main()) 
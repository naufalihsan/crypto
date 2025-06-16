import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from decimal import Decimal

import asyncpg
from aiohttp import web
from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal and datetime objects"""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@dataclass
class DatabaseConfig:
    """Database configuration"""

    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "crypto_db")
    user: str = os.getenv("POSTGRES_USER", "admin")
    password: str = os.getenv("POSTGRES_PASSWORD", "admin")
    min_pool_size: int = int(os.getenv("POSTGRES_MIN_POOL_SIZE", "5"))
    max_pool_size: int = int(os.getenv("POSTGRES_MAX_POOL_SIZE", "20"))
    command_timeout: int = int(os.getenv("POSTGRES_COMMAND_TIMEOUT", "60"))


@dataclass
class KafkaConfig:
    """Kafka configuration"""

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "oltp-service")
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    enable_auto_commit: bool = (
        os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() == "true"
    )


@dataclass
class PriceData:
    """Price data model"""

    symbol: str
    price: Decimal
    timestamp: datetime
    volume: Optional[Decimal] = None
    high_24h: Optional[Decimal] = None
    low_24h: Optional[Decimal] = None
    change_24h: Optional[Decimal] = None
    source: str = "unknown"


@dataclass
class TechnicalIndicator:
    """Technical indicator data model"""

    symbol: str
    sma: Optional[Decimal] = None
    ema: Optional[Decimal] = None
    rsi: Optional[Decimal] = None
    macd: Optional[Decimal] = None
    bollinger_upper: Optional[Decimal] = None
    bollinger_lower: Optional[Decimal] = None
    timestamp: Optional[datetime] = None


@dataclass
class MarketAnomaly:
    """Market anomaly data model"""

    symbol: str
    alert_type: str
    description: str
    severity: str
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None


class DatabaseManager:
    """Manages PostgreSQL database connections and operations"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
        self._connection_string = (
            f"postgresql://{config.user}:{config.password}"
            f"@{config.host}:{config.port}/{config.database}"
        )

    async def initialize(self):
        """Initialize database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                self._connection_string,
                min_size=self.config.min_pool_size,
                max_size=self.config.max_pool_size,
                command_timeout=self.config.command_timeout,
                server_settings={
                    "application_name": "crypto-oltp-service",
                    "timezone": "UTC",
                },
            )
            logger.info(
                f"Database pool initialized: {self.config.min_pool_size}-{self.config.max_pool_size} connections"
            )
            await self._verify_database_schema()
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    async def close(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")

    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        async with self.pool.acquire() as connection:
            yield connection

    async def _verify_database_schema(self):
        """Verify that required database tables exist"""
        required_tables = [
            "crypto_prices",
            "technical_indicators",
            "market_anomalies",
            "crypto_price_aggregates",
        ]

        async with self.get_connection() as conn:
            # Create schema_migrations table if it doesn't exist
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id SERIAL PRIMARY KEY,
                    version VARCHAR(50) NOT NULL UNIQUE,
                    description TEXT,
                    applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Check current schema version
            current_version = await conn.fetchval(
                "SELECT version FROM schema_migrations ORDER BY applied_at DESC LIMIT 1"
            )

            # Verify required tables exist
            for table_name in required_tables:
                exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = $1
                    )
                """,
                    table_name,
                )

                if not exists:
                    raise RuntimeError(
                        f"Required table '{table_name}' does not exist. "
                        "Please ensure the database initialization script has been run."
                    )

            logger.info(
                f"Database schema verified - all required tables exist (version: {current_version or 'unknown'})"
            )

    async def apply_migration(
        self, version: str, description: str, sql_commands: List[str]
    ):
        """Apply a database migration"""
        async with self.get_connection() as conn:
            # Check if migration already applied
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)",
                version,
            )

            if exists:
                logger.info(f"Migration {version} already applied, skipping")
                return

            # Apply migration in transaction
            async with conn.transaction():
                for sql_command in sql_commands:
                    await conn.execute(sql_command)

                # Record migration
                await conn.execute(
                    """
                    INSERT INTO schema_migrations (version, description)
                    VALUES ($1, $2)
                """,
                    version,
                    description,
                )

                logger.info(f"Applied migration {version}: {description}")

    async def get_migration_history(self) -> List[Dict[str, Any]]:
        """Get migration history"""
        async with self.get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT version, description, applied_at
                FROM schema_migrations
                ORDER BY applied_at DESC
            """
            )
            return [dict(row) for row in rows]

    async def get_table_info(self) -> Dict[str, Any]:
        """Get information about database tables"""
        async with self.get_connection() as conn:
            tables_info = {}

            # Get table row counts
            for table in ["crypto_prices", "technical_indicators", "market_anomalies"]:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                tables_info[f"{table}_count"] = count

            # Get latest data timestamps
            latest_price = await conn.fetchval(
                "SELECT MAX(timestamp) FROM crypto_prices"
            )
            latest_indicator = await conn.fetchval(
                "SELECT MAX(timestamp) FROM technical_indicators"
            )
            latest_anomaly = await conn.fetchval(
                "SELECT MAX(timestamp) FROM market_anomalies"
            )

            tables_info.update(
                {
                    "latest_price_timestamp": (
                        latest_price.isoformat() if latest_price else None
                    ),
                    "latest_indicator_timestamp": (
                        latest_indicator.isoformat() if latest_indicator else None
                    ),
                    "latest_anomaly_timestamp": (
                        latest_anomaly.isoformat() if latest_anomaly else None
                    ),
                }
            )

            return tables_info


class OLTPService:
    """Main OLTP service for real-time cryptocurrency data operations"""

    def __init__(self):
        self.db_config = DatabaseConfig()
        self.kafka_config = KafkaConfig()
        self.db_manager = DatabaseManager(self.db_config)
        self.kafka_consumer = None
        self.running = False
        self.main_loop = None
        self.health_status = {
            "status": "starting",
            "database_connected": False,
            "kafka_connected": False,
            "last_data_time": None,
            "processed_messages": 0,
            "errors": 0,
        }
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def initialize(self):
        """Initialize the OLTP service"""
        try:
            logger.info("Initializing OLTP Service...")

            # Initialize database
            await self.db_manager.initialize()
            self.health_status["database_connected"] = True

            # Initialize Kafka consumer
            await self._initialize_kafka()
            self.health_status["kafka_connected"] = True

            self.health_status["status"] = "running"
            logger.info("OLTP Service initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize OLTP Service: {e}")
            self.health_status["status"] = "error"
            raise

    async def _initialize_kafka(self):
        """Initialize Kafka consumer"""
        try:
            self.kafka_consumer = KafkaConsumer(
                "crypto-prices",
                "crypto-market-data",
                "technical-indicators",
                "market-anomalies",
                bootstrap_servers=self.kafka_config.bootstrap_servers,
                group_id=self.kafka_config.consumer_group,
                auto_offset_reset=self.kafka_config.auto_offset_reset,
                enable_auto_commit=self.kafka_config.enable_auto_commit,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
                max_poll_records=100,
                fetch_min_bytes=1,
                fetch_max_wait_ms=500,
            )
            logger.info("Kafka consumer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    async def start(self):
        """Start the OLTP service"""
        self.running = True
        self.main_loop = asyncio.get_running_loop()
        logger.info("Starting OLTP Service...")

        # Start Kafka consumer in background
        kafka_task = asyncio.create_task(self._consume_kafka_messages())

        # Start HTTP server
        http_task = asyncio.create_task(self._start_http_server())

        # Wait for both tasks
        await asyncio.gather(kafka_task, http_task, return_exceptions=True)

    async def stop(self):
        """Stop the OLTP service"""
        logger.info("Stopping OLTP Service...")
        self.running = False

        # Close Kafka consumer
        if self.kafka_consumer:
            try:
                self.kafka_consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")

        # Close database connections
        try:
            await self.db_manager.close()
            logger.info("Database connections closed")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")

        # Shutdown thread pool executor
        try:
            self.executor.shutdown(wait=True)
            logger.info("Thread pool executor shutdown")
        except Exception as e:
            logger.error(f"Error shutting down thread pool executor: {e}")

        logger.info("OLTP Service stopped")

    async def _consume_kafka_messages(self):
        """Consume messages from Kafka topics"""
        logger.info("Starting Kafka message consumption...")

        def consume_messages():
            consecutive_errors = 0
            max_consecutive_errors = 5

            while self.running:
                try:
                    message_batch = self.kafka_consumer.poll(timeout_ms=1000)
                    if message_batch:
                        future = asyncio.run_coroutine_threadsafe(
                            self._process_message_batch(message_batch), self.main_loop
                        )
                        future.result(timeout=30)
                        consecutive_errors = 0
                    else:
                        time.sleep(0.1)

                except Exception as e:
                    consecutive_errors += 1
                    logger.error(
                        f"Kafka consumption error ({consecutive_errors}/{max_consecutive_errors}): {e}"
                    )
                    self.health_status["errors"] += 1

                    if consecutive_errors >= max_consecutive_errors:
                        logger.warning(
                            "Too many consecutive errors, attempting to reconnect Kafka consumer..."
                        )
                        try:
                            if self.kafka_consumer:
                                self.kafka_consumer.close()
                            # Reinitialize consumer
                            self.kafka_consumer = KafkaConsumer(
                                "crypto-prices",
                                "crypto-market-data",
                                "technical-indicators",
                                "market-anomalies",
                                bootstrap_servers=self.kafka_config.bootstrap_servers,
                                group_id=self.kafka_config.consumer_group,
                                auto_offset_reset=self.kafka_config.auto_offset_reset,
                                enable_auto_commit=self.kafka_config.enable_auto_commit,
                                value_deserializer=lambda m: json.loads(
                                    m.decode("utf-8")
                                ),
                                consumer_timeout_ms=1000,
                                session_timeout_ms=30000,
                                heartbeat_interval_ms=3000,
                                max_poll_records=100,
                                fetch_min_bytes=1,
                                fetch_max_wait_ms=500,
                            )
                            consecutive_errors = 0
                            logger.info("Kafka consumer reconnected successfully")
                        except Exception as reconnect_error:
                            logger.error(
                                f"Failed to reconnect Kafka consumer: {reconnect_error}"
                            )
                            time.sleep(10)
                    else:
                        time.sleep(5)

        # Run Kafka consumer in thread pool
        await self.main_loop.run_in_executor(self.executor, consume_messages)

    async def _process_message_batch(self, message_batch):
        """Process a batch of Kafka messages"""
        for topic_partition, messages in message_batch.items():
            topic = topic_partition.topic

            for message in messages:
                try:
                    await self._process_single_message(topic, message.value)
                    self.health_status["processed_messages"] += 1
                    self.health_status["last_data_time"] = datetime.now().isoformat()
                except Exception as e:
                    logger.error(f"Error processing message from {topic}: {e}")
                    self.health_status["errors"] += 1

    async def _process_single_message(self, topic: str, data: Dict[str, Any]):
        """Process a single message based on topic"""
        if topic == "crypto-prices":
            await self._store_price_data(data)
        elif topic == "crypto-market-data":
            await self._store_market_data(data)
        elif topic == "technical-indicators":
            await self._store_technical_indicators(data)
        elif topic == "market-anomalies":
            await self._store_market_anomaly(data)
        else:
            logger.warning(f"Unknown topic: {topic}")

    async def _store_price_data(self, data: Dict[str, Any]):
        """Store price data in database"""
        try:
            price_data = PriceData(
                symbol=data.get("symbol", ""),
                price=Decimal(str(data.get("price", 0))),
                timestamp=datetime.fromtimestamp(data.get("timestamp", 0) / 1000),
                volume=(
                    Decimal(str(data.get("volume", 0))) if data.get("volume") else None
                ),
                high_24h=(
                    Decimal(str(data.get("high", 0))) if data.get("high") else None
                ),
                low_24h=Decimal(str(data.get("low", 0))) if data.get("low") else None,
                change_24h=(
                    Decimal(str(data.get("price_change", 0)))
                    if data.get("price_change")
                    else None
                ),
                source=data.get("source", "unknown"),
            )

            async with self.db_manager.get_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO crypto_prices 
                    (symbol, price, timestamp, volume, high_24h, low_24h, change_24h, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (symbol, timestamp, source) DO UPDATE SET
                    price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    high_24h = EXCLUDED.high_24h,
                    low_24h = EXCLUDED.low_24h,
                    change_24h = EXCLUDED.change_24h
                """,
                    price_data.symbol,
                    price_data.price,
                    price_data.timestamp,
                    price_data.volume,
                    price_data.high_24h,
                    price_data.low_24h,
                    price_data.change_24h,
                    price_data.source,
                )

        except Exception as e:
            logger.error(f"Error storing price data: {e}")
            raise

    async def _store_market_data(self, data: Dict[str, Any]):
        """Store market data in database"""
        try:
            # Convert CoinGecko data format to our price data format
            price_data = PriceData(
                symbol=data.get("symbol", ""),
                price=Decimal(str(data.get("current_price", 0))),
                timestamp=datetime.fromtimestamp(data.get("timestamp", 0) / 1000),
                volume=(
                    Decimal(str(data.get("volume_24h", 0)))
                    if data.get("volume_24h")
                    else None
                ),
                high_24h=Decimal(str(data.get("current_price", 0))),
                low_24h=Decimal(str(data.get("current_price", 0))),
                change_24h=(
                    Decimal(str(data.get("price_change_24h", 0)))
                    if data.get("price_change_24h")
                    else None
                ),
                source=data.get("source", "coingecko_api"),
            )

            async with self.db_manager.get_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO crypto_prices 
                    (symbol, price, timestamp, volume, high_24h, low_24h, change_24h, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (symbol, timestamp, source) DO UPDATE SET
                    price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    high_24h = EXCLUDED.high_24h,
                    low_24h = EXCLUDED.low_24h,
                    change_24h = EXCLUDED.change_24h
                """,
                    price_data.symbol,
                    price_data.price,
                    price_data.timestamp,
                    price_data.volume,
                    price_data.high_24h,
                    price_data.low_24h,
                    price_data.change_24h,
                    price_data.source,
                )

        except Exception as e:
            logger.error(f"Error storing market data: {e}")
            raise

    async def _store_technical_indicators(self, data: Dict[str, Any]):
        """Store technical indicators in database"""
        try:
            indicator = TechnicalIndicator(
                symbol=data.get("symbol", ""),
                sma=Decimal(str(data.get("sma", 0))) if data.get("sma") else None,
                ema=Decimal(str(data.get("ema", 0))) if data.get("ema") else None,
                rsi=Decimal(str(data.get("rsi", 0))) if data.get("rsi") else None,
                macd=Decimal(str(data.get("macd", 0))) if data.get("macd") else None,
                bollinger_upper=(
                    Decimal(str(data.get("bollinger_upper", 0)))
                    if data.get("bollinger_upper")
                    else None
                ),
                bollinger_lower=(
                    Decimal(str(data.get("bollinger_lower", 0)))
                    if data.get("bollinger_lower")
                    else None
                ),
                timestamp=datetime.fromtimestamp(
                    data.get("processed_at", time.time() * 1000) / 1000
                ),
            )

            async with self.db_manager.get_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO technical_indicators 
                    (symbol, sma, ema, rsi, macd, bollinger_upper, bollinger_lower, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    sma = EXCLUDED.sma,
                    ema = EXCLUDED.ema,
                    rsi = EXCLUDED.rsi,
                    macd = EXCLUDED.macd,
                    bollinger_upper = EXCLUDED.bollinger_upper,
                    bollinger_lower = EXCLUDED.bollinger_lower
                """,
                    indicator.symbol,
                    indicator.sma,
                    indicator.ema,
                    indicator.rsi,
                    indicator.macd,
                    indicator.bollinger_upper,
                    indicator.bollinger_lower,
                    indicator.timestamp,
                )

        except Exception as e:
            logger.error(f"Error storing technical indicators: {e}")
            raise

    async def _store_market_anomaly(self, data: Dict[str, Any]):
        """Store market anomaly in database"""
        try:
            anomaly = MarketAnomaly(
                symbol=data.get("symbol", ""),
                alert_type=data.get("alert_type", "UNKNOWN"),
                description=f"{data.get('alert_type', 'Unknown')} detected for {data.get('symbol', 'Unknown')}",
                severity=(
                    "high" if data.get("alert_type") == "VOLATILITY_SPIKE" else "medium"
                ),
                timestamp=datetime.fromtimestamp(
                    data.get("timestamp", time.time() * 1000) / 1000
                ),
                metadata=data,
            )

            async with self.db_manager.get_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO market_anomalies 
                    (symbol, alert_type, description, severity, timestamp, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """,
                    anomaly.symbol,
                    anomaly.alert_type,
                    anomaly.description,
                    anomaly.severity,
                    anomaly.timestamp,
                    json.dumps(anomaly.metadata),
                )

            logger.info(
                f"Stored market anomaly: {anomaly.alert_type} for {anomaly.symbol}"
            )

        except Exception as e:
            logger.error(f"Error storing market anomaly: {e}")
            raise

    # Query Methods
    async def get_latest_prices(
        self, symbols: Optional[List[str]] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get latest prices for symbols"""
        async with self.db_manager.get_connection() as conn:
            if symbols:
                placeholders = ",".join(f"${i+1}" for i in range(len(symbols)))
                query = f"""
                    SELECT DISTINCT ON (symbol) symbol, price, timestamp, volume, 
                           high_24h, low_24h, change_24h, source
                    FROM crypto_prices 
                    WHERE symbol = ANY(${len(symbols)+1})
                    ORDER BY symbol, timestamp DESC
                    LIMIT ${len(symbols)+2}
                """
                rows = await conn.fetch(query, *symbols, symbols, limit)
            else:
                query = """
                    SELECT DISTINCT ON (symbol) symbol, price, timestamp, volume, 
                           high_24h, low_24h, change_24h, source
                    FROM crypto_prices 
                    ORDER BY symbol, timestamp DESC
                    LIMIT $1
                """
                rows = await conn.fetch(query, limit)

            return [dict(row) for row in rows]

    async def get_price_history(
        self, symbol: str, hours: int = 24, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get price history for a symbol"""
        async with self.db_manager.get_connection() as conn:
            since = datetime.now() - timedelta(hours=hours)
            rows = await conn.fetch(
                """
                SELECT symbol, price, timestamp, volume, high_24h, low_24h, change_24h, source
                FROM crypto_prices 
                WHERE symbol = $1 AND timestamp >= $2
                ORDER BY timestamp DESC
                LIMIT $3
            """,
                symbol,
                since,
                limit,
            )

            return [dict(row) for row in rows]

    async def get_latest_indicators(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get latest technical indicators for a symbol"""
        async with self.db_manager.get_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT symbol, sma, ema, rsi, macd, bollinger_upper, bollinger_lower, timestamp
                FROM technical_indicators 
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            """,
                symbol,
            )

            return dict(row) if row else None

    async def get_recent_anomalies(
        self, hours: int = 24, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent market anomalies"""
        async with self.db_manager.get_connection() as conn:
            since = datetime.now() - timedelta(hours=hours)
            rows = await conn.fetch(
                """
                SELECT symbol, alert_type, description, severity, timestamp, metadata
                FROM market_anomalies 
                WHERE timestamp >= $1
                ORDER BY timestamp DESC
                LIMIT $2
            """,
                since,
                limit,
            )

            return [dict(row) for row in rows]

    async def get_market_summary(self) -> Dict[str, Any]:
        """Get market summary statistics"""
        async with self.db_manager.get_connection() as conn:
            # Get total symbols
            symbol_count = await conn.fetchval(
                "SELECT COUNT(DISTINCT symbol) FROM crypto_prices"
            )

            # Get latest prices count
            latest_prices_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT ON (symbol) symbol FROM crypto_prices 
                    WHERE timestamp >= NOW() - INTERVAL '1 hour'
                    ORDER BY symbol, timestamp DESC
                ) t
            """
            )

            # Get recent anomalies count
            anomalies_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM market_anomalies 
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """
            )

            # Get top movers
            top_gainers = await conn.fetch(
                """
                SELECT DISTINCT ON (symbol) symbol, price, change_24h
                FROM crypto_prices 
                WHERE change_24h IS NOT NULL AND timestamp >= NOW() - INTERVAL '1 hour'
                ORDER BY symbol, timestamp DESC, change_24h DESC
                LIMIT 5
            """
            )

            return {
                "total_symbols": symbol_count,
                "active_symbols_1h": latest_prices_count,
                "anomalies_24h": anomalies_count,
                "top_gainers": [dict(row) for row in top_gainers],
                "last_updated": datetime.now().isoformat(),
            }

    # HTTP API Methods
    def _json_response(self, data: Any, status: int = 200) -> web.Response:
        """Create JSON response with custom encoder for Decimal and datetime objects"""
        return web.Response(
            text=json.dumps(data, cls=DecimalEncoder),
            content_type="application/json",
            status=status,
        )

    async def _start_http_server(self):
        """Start HTTP API server"""
        app = web.Application()

        # Health check endpoint
        app.router.add_get("/health", self._health_handler)

        # API endpoints
        app.router.add_get("/api/prices/latest", self._latest_prices_handler)
        app.router.add_get("/api/prices/{symbol}/history", self._price_history_handler)
        app.router.add_get("/api/indicators/{symbol}", self._indicators_handler)
        app.router.add_get("/api/anomalies", self._anomalies_handler)
        app.router.add_get("/api/market/summary", self._market_summary_handler)

        # Admin endpoints
        app.router.add_get("/api/admin/schema", self._schema_info_handler)

        # Start server
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", 8001)
        await site.start()
        logger.info("HTTP API server started on port 8001")

        # Keep server running
        while self.running:
            await asyncio.sleep(1)

    async def _health_handler(self, request):
        """Health check endpoint"""
        try:
            health_data = self.health_status.copy()

            if self.health_status.get("database_connected"):
                try:
                    table_info = await self.db_manager.get_table_info()
                    health_data["database_info"] = table_info
                except Exception as e:
                    health_data["database_info"] = {"error": str(e)}

            return self._json_response(health_data)
        except Exception as e:
            logger.error(f"Error in health handler: {e}")
            return self._json_response({"status": "error", "error": str(e)}, status=500)

    async def _latest_prices_handler(self, request):
        """Get latest prices"""
        try:
            symbols = (
                request.query.get("symbols", "").split(",")
                if request.query.get("symbols")
                else None
            )
            limit = int(request.query.get("limit", 100))

            prices = await self.get_latest_prices(symbols, limit)
            return self._json_response(
                {"success": True, "data": prices, "count": len(prices)}
            )
        except Exception as e:
            logger.error(f"Error in latest prices handler: {e}")
            return self._json_response({"success": False, "error": str(e)}, status=500)

    async def _price_history_handler(self, request):
        """Get price history for a symbol"""
        try:
            symbol = request.match_info["symbol"].upper()
            hours = int(request.query.get("hours", 24))
            limit = int(request.query.get("limit", 1000))

            history = await self.get_price_history(symbol, hours, limit)
            return self._json_response(
                {
                    "success": True,
                    "symbol": symbol,
                    "data": history,
                    "count": len(history),
                }
            )
        except Exception as e:
            logger.error(f"Error in price history handler: {e}")
            return self._json_response({"success": False, "error": str(e)}, status=500)

    async def _indicators_handler(self, request):
        """Get technical indicators for a symbol"""
        try:
            symbol = request.match_info["symbol"].upper()
            indicators = await self.get_latest_indicators(symbol)

            return self._json_response(
                {"success": True, "symbol": symbol, "data": indicators}
            )
        except Exception as e:
            logger.error(f"Error in indicators handler: {e}")
            return self._json_response({"success": False, "error": str(e)}, status=500)

    async def _anomalies_handler(self, request):
        """Get recent market anomalies"""
        try:
            hours = int(request.query.get("hours", 24))
            limit = int(request.query.get("limit", 100))

            anomalies = await self.get_recent_anomalies(hours, limit)
            return self._json_response(
                {"success": True, "data": anomalies, "count": len(anomalies)}
            )
        except Exception as e:
            logger.error(f"Error in anomalies handler: {e}")
            return self._json_response({"success": False, "error": str(e)}, status=500)

    async def _market_summary_handler(self, request):
        """Get market summary"""
        try:
            summary = await self.get_market_summary()
            return self._json_response({"success": True, "data": summary})
        except Exception as e:
            logger.error(f"Error in market summary handler: {e}")
            return self._json_response({"success": False, "error": str(e)}, status=500)

    async def _schema_info_handler(self, request):
        """Get database schema information"""
        try:
            # Get table information
            table_info = await self.db_manager.get_table_info()

            # Get migration history
            migration_history = await self.db_manager.get_migration_history()

            schema_info = {
                "table_info": table_info,
                "migration_history": migration_history,
                "schema_version": (
                    migration_history[0]["version"] if migration_history else "unknown"
                ),
                "last_migration": (
                    migration_history[0]["applied_at"] if migration_history else None
                ),
            }

            return self._json_response({"success": True, "data": schema_info})
        except Exception as e:
            logger.error(f"Error in schema info handler: {e}")
            return self._json_response({"success": False, "error": str(e)}, status=500)


async def main():
    """Main entry point"""
    service = OLTPService()

    try:
        await service.initialize()
        await service.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())

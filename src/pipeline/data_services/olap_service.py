import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from decimal import Decimal
import uuid

import clickhouse_connect
from aiohttp import web
from kafka import KafkaConsumer
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder for Decimal and datetime objects"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@dataclass
class ClickHouseConfig:
    """ClickHouse configuration"""
    host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    database: str = os.getenv("CLICKHOUSE_DB", "crypto_analytics")
    user: str = os.getenv("CLICKHOUSE_USER", "default")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    
    # Performance settings
    max_execution_time: int = int(os.getenv("CLICKHOUSE_MAX_EXECUTION_TIME", "60"))
    max_memory_usage: int = int(os.getenv("CLICKHOUSE_MAX_MEMORY_USAGE", "5000000000"))  # 5GB


@dataclass
class KafkaConfig:
    """Kafka configuration"""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "olap-analytics")


class ClickHouseClient:
    """ClickHouse client wrapper"""
    
    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.client = None
        self._client_settings = {
            'max_execution_time': config.max_execution_time,
            'max_memory_usage': config.max_memory_usage,
            'use_uncompressed_cache': 1,
            'optimize_read_in_order': 1
        }
    
    async def connect(self):
        """Initialize ClickHouse connection"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                username=self.config.user,
                password=self.config.password,
                settings=self._client_settings
            )
            logger.info(f"Connected to ClickHouse: {self.config.host}:{self.config.port}")
            await self._verify_tables()
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def _get_client(self):
        """Get a new client instance for thread-safe operations"""
        return clickhouse_connect.get_client(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            username=self.config.user,
            password=self.config.password,
            settings=self._client_settings,
            session_id=str(uuid.uuid4())
        )
    
    async def _verify_tables(self):
        """Verify required tables exist"""
        required_tables = ['crypto_prices_raw', 'price_analytics', 'market_trends', 'technical_indicators_agg', 'market_anomalies_agg']
        
        missing_tables = []
        for table in required_tables:
            exists = self.client.command(f"EXISTS TABLE {table}")
            if not exists:
                missing_tables.append(table)
        
        if missing_tables:
            logger.warning(f"Missing tables: {', '.join(missing_tables)}")
        else:
            logger.info("All required tables verified")
    
    def query(self, sql: str, parameters: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute query and return results as list of dictionaries"""
        try:
            client = self._get_client()
            result = client.query(sql, parameters=parameters or {})
            client.close()
            
            if result.result_rows:
                columns = [col[0] for col in result.column_names]
                return [dict(zip(columns, row)) for row in result.result_rows]
            return []
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def insert(self, table: str, data: List[Dict[str, Any]]):
        """Insert data into table"""
        if not data:
            return
        
        try:
            client = self._get_client()
            
            # Get table schema to determine column order
            schema_query = f"DESCRIBE TABLE {table}"
            schema_result = client.query(schema_query)
            column_names = [row[0] for row in schema_result.result_rows]
            
            # Convert dictionary data to list format expected by clickhouse_connect
            list_data = []
            for record in data:
                row = []
                for col in column_names:
                    value = record.get(col)
                    # Handle None values and ensure proper types
                    if value is None:
                        row.append(None)
                    elif isinstance(value, datetime):
                        # Ensure datetime objects are properly formatted
                        row.append(value)
                    elif isinstance(value, (int, float)):
                        row.append(value)
                    elif isinstance(value, str):
                        row.append(value)
                    else:
                        # Convert other types to string
                        row.append(str(value) if value is not None else None)
                list_data.append(row)
            
            client.insert(table, list_data)
            client.close()
        except Exception as e:
            logger.error(f"Insert failed for table {table}: {e}")
            # Log the problematic data for debugging
            logger.error(f"Problematic data sample: {data[:1] if data else 'No data'}")
            raise
    
    def close(self):
        """Close connection"""
        if self.client:
            self.client.close()


class AnalyticsService:
    """Core analytics service with clean separation of concerns"""
    
    def __init__(self, clickhouse_client: ClickHouseClient):
        self.ch = clickhouse_client
    
    async def get_price_analytics(self, symbol: str, period_minutes: int = 60, hours: int = 24) -> List[Dict[str, Any]]:
        """Get price analytics for a symbol"""
        query = """
        WITH aggregated_data AS (
            SELECT 
                symbol,
                toStartOfInterval(timestamp, INTERVAL %(period_minutes)s MINUTE) as period_start,
                period_minutes,
                anyLast(open_price) as open_price,
                anyLast(close_price) as close_price,
                max(high_price) as high_price,
                min(low_price) as low_price,
                avg(avg_price) as avg_price,
                sum(volume) as total_volume
            FROM price_analytics
            WHERE symbol = %(symbol)s 
                AND period_minutes = %(period_minutes)s
                AND timestamp >= now() - INTERVAL %(hours)s HOUR
            GROUP BY symbol, period_start, period_minutes
        )
        SELECT 
            symbol,
            period_start,
            period_minutes,
            open_price,
            close_price,
            high_price,
            low_price,
            avg_price,
            total_volume,
            close_price - open_price as price_change,
            ((close_price - open_price) / open_price) * 100 as price_change_pct
        FROM aggregated_data
        ORDER BY period_start DESC
        LIMIT 1000
        """
        
        return self.ch.query(query, {
            'symbol': symbol,
            'period_minutes': period_minutes,
            'hours': hours
        })
    
    async def get_market_trends(self, symbol: Optional[str] = None, hours: int = 24) -> List[Dict[str, Any]]:
        """Get market trends"""
        base_query = """
        SELECT 
            symbol,
            trend_direction,
            strength,
            confidence,
            duration_hours,
            start_time,
            end_time
        FROM market_trends
        WHERE start_time >= now() - INTERVAL %(hours)s HOUR
        """
        
        params = {'hours': hours}
        
        if symbol:
            query = base_query + " AND symbol = %(symbol)s"
            params['symbol'] = symbol
        else:
            query = base_query
        
        query += " ORDER BY confidence DESC, start_time DESC LIMIT 100"
        
        return self.ch.query(query, params)
    
    async def get_volatility_analysis(self, symbols: List[str], hours: int = 24) -> List[Dict[str, Any]]:
        """Get volatility analysis for symbols"""
        if not symbols:
            return []
        
        symbols_str = "', '".join(symbols)
        query = f"""
        SELECT 
            symbol,
            avg(volatility) as avg_volatility,
            max(volatility) as max_volatility,
            min(volatility) as min_volatility,
            stddevPop(volatility) as volatility_stddev,
            count() as data_points
        FROM price_analytics
        WHERE symbol IN ('{symbols_str}')
            AND timestamp >= now() - INTERVAL {hours} HOUR
        GROUP BY symbol
        ORDER BY avg_volatility DESC
        """
        
        return self.ch.query(query)
    
    async def get_technical_indicators(self, symbol: str, hours: int = 24) -> List[Dict[str, Any]]:
        """Get technical indicators for a symbol"""
        query = """
        SELECT 
            symbol,
            timestamp,
            sma_20,
            ema_12,
            rsi,
            macd_line,
            macd_signal,
            bollinger_upper,
            bollinger_lower
        FROM technical_indicators_agg
        WHERE symbol = %(symbol)s
            AND timestamp >= now() - INTERVAL %(hours)s HOUR
        ORDER BY timestamp DESC
        LIMIT 100
        """
        
        return self.ch.query(query, {'symbol': symbol, 'hours': hours})


class KafkaProcessor:
    """Simple Kafka message processor"""
    
    def __init__(self, config: KafkaConfig, clickhouse_client: ClickHouseClient):
        self.config = config
        self.ch = clickhouse_client
        self.consumer = None
        self.running = False
        self.consumer_thread = None
    
    def _safe_timestamp_conversion(self, timestamp_value: Any, context: str = "unknown") -> datetime:
        """Safely convert timestamp to datetime object"""
        if timestamp_value is None or timestamp_value == 0:
            logger.warning(f"Missing or zero timestamp in {context}, using current time")
            return datetime.now()
        
        try:
            # Handle both milliseconds and seconds timestamps
            if isinstance(timestamp_value, (int, float)):
                if timestamp_value > 1e12:  # Milliseconds
                    return datetime.fromtimestamp(timestamp_value / 1000)
                else:  # Seconds
                    return datetime.fromtimestamp(timestamp_value)
            else:
                logger.error(f"Invalid timestamp type {type(timestamp_value)} in {context}: {timestamp_value}")
                return datetime.now()
        except (ValueError, OSError, OverflowError) as e:
            logger.error(f"Invalid timestamp {timestamp_value} in {context}: {e}")
            return datetime.now()
    
    def start(self):
        """Start Kafka consumer in background thread"""
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_messages, daemon=True)
        self.consumer_thread.start()
        logger.info("Kafka processor started")
    
    def stop(self):
        """Stop Kafka consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        logger.info("Kafka processor stopped")
    
    def _consume_messages(self):
        """Consume and process Kafka messages"""
        try:
            self.consumer = KafkaConsumer(
                'crypto-prices',
                'crypto-market-data',
                'market-anomalies',
                'technical-indicators',
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.consumer_group,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            
            logger.info("Started consuming Kafka messages")
            
            while self.running:
                try:
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self._process_message(topic_partition.topic, message.value)
                
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
                    time.sleep(1)
        
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
    
    def _process_message(self, topic: str, data: Dict[str, Any]):
        """Process individual message based on topic"""
        try:
            # Validate that data is a dictionary
            if not isinstance(data, dict):
                logger.error(f"Invalid message format for topic {topic}: expected dict, got {type(data)}")
                return
            
            if topic == "crypto-prices":
                self._store_price_data(data)
            elif topic == "crypto-market-data":
                self._store_market_data(data)
            elif topic == "market-anomalies":
                self._store_market_anomaly(data)
            elif topic == "technical-indicators":
                self._store_technical_indicator(data)
            else:
                logger.warning(f"Unknown topic: {topic}")
                
        except Exception as e:
            logger.error(f"Error storing {topic} data: {e}")
            logger.error(f"Problematic message data: {data}")
            # Don't re-raise to prevent consumer from stopping
    
    def _store_price_data(self, data: Dict[str, Any]):
        """Store raw price data and create aggregates"""
        try:
            # Validate required fields
            if not data.get('symbol') or data.get('price') is None:
                logger.error(f"Missing required fields: symbol={data.get('symbol')}, price={data.get('price')}")
                return
            
            # Handle timestamp conversion
            timestamp = self._safe_timestamp_conversion(
                data.get('timestamp'), 
                f"price data for {data.get('symbol')}"
            )
            
            # Store raw price data
            raw_record = {
                'symbol': data.get('symbol'),
                'price': float(data.get('price')),
                'volume': float(data.get('volume', 0)),
                'timestamp': timestamp,
                'source': data.get('source', 'unknown')
            }
            
            self.ch.insert('crypto_prices_raw', [raw_record])
            
            # Create price analytics aggregate
            analytics_record = {
                'symbol': data.get('symbol'),
                'timestamp': timestamp,
                'period_minutes': 1,
                'open_price': float(data.get('price')),
                'close_price': float(data.get('price')),
                'high_price': float(data.get('high', data.get('price'))),
                'low_price': float(data.get('low', data.get('price'))),
                'avg_price': float(data.get('price')),
                'volume': float(data.get('volume', 0)),
                'volatility': abs(float(data.get('price_change', 0)))
            }
            
            self.ch.insert('price_analytics', [analytics_record])
            
        except Exception as e:
            logger.error(f"Error storing price data: {e}")
            raise

    def _store_market_data(self, data: Dict[str, Any]):
        """Store CoinGecko market data and create aggregates"""
        try:
            # Extract and validate required fields
            symbol = data.get('symbol')
            current_price = data.get('current_price')
            timestamp_ms = data.get('timestamp')
            
            if not symbol or current_price is None or timestamp_ms is None:
                logger.error(f"Missing required fields in market data: symbol={symbol}, price={current_price}, timestamp={timestamp_ms}")
                return
            
            # Convert timestamp
            try:
                if timestamp_ms > 1e12:  # Milliseconds
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                else:  # Seconds
                    timestamp = datetime.fromtimestamp(timestamp_ms)
            except (ValueError, OSError) as e:
                logger.error(f"Invalid timestamp {timestamp_ms}: {e}")
                timestamp = datetime.now()
            
            # Store raw market data
            raw_record = {
                'symbol': symbol.upper(),
                'price': float(current_price),
                'volume': float(data.get('volume_24h', 0)),
                'timestamp': timestamp,
                'source': data.get('source', 'coingecko_api')
            }
            
            self.ch.insert('crypto_prices_raw', [raw_record])
            
            # Create price analytics aggregate
            analytics_record = {
                'symbol': symbol.upper(),
                'timestamp': timestamp,
                'period_minutes': 1,
                'open_price': float(current_price),
                'close_price': float(current_price),
                'high_price': float(current_price),
                'low_price': float(current_price),
                'avg_price': float(current_price),
                'volume': float(data.get('volume_24h', 0)),
                'volatility': abs(float(data.get('price_change_24h', 0)))
            }
            
            self.ch.insert('price_analytics', [analytics_record])
            
            # Create market trends data
            price_change_24h = float(data.get('price_change_24h', 0))
            trend_record = {
                'symbol': symbol.upper(),
                'trend_direction': 'up' if price_change_24h > 0 else 'down',
                'strength': min(abs(price_change_24h) / 100, 1.0),
                'confidence': 0.8,
                'duration_hours': 1,
                'start_time': timestamp,
                'end_time': timestamp + timedelta(hours=1),
                'timestamp': timestamp,
                'volume': float(data.get('volume_24h', 0)),
                'market_cap': float(data.get('market_cap', 0))
            }
            
            self.ch.insert('market_trends', [trend_record])
            
        except Exception as e:
            logger.error(f"Error storing market data: {e}")
            raise

    def _store_market_anomaly(self, data: Dict[str, Any]):
        """Store market anomaly data and create trend if needed"""
        try:
            # Validate required fields
            if not data.get('symbol'):
                logger.error(f"Missing symbol in market anomaly data: {data}")
                return
            
            # Handle timestamp conversion
            timestamp = self._safe_timestamp_conversion(
                data.get('timestamp'), 
                f"market anomaly data for {data.get('symbol')}"
            )
            
            # Store anomaly
            anomaly_record = {
                'symbol': data.get('symbol'),
                'anomaly_type': data.get('alert_type', 'UNKNOWN'),
                'severity': 'high' if data.get('alert_type') == 'VOLATILITY_SPIKE' else 'medium',
                'description': f"{data.get('alert_type', 'Unknown')} detected for {data.get('symbol', 'Unknown')}",
                'confidence_score': float(data.get('confidence', 0.8)),
                'timestamp': timestamp,
                'metadata': json.dumps(data)
            }
            
            self.ch.insert('market_anomalies_agg', [anomaly_record])
            
            # Create a simple trend based on anomaly
            if data.get('alert_type') == 'VOLATILITY_SPIKE':
                trend_direction = 'bullish' if data.get('price_change', 0) > 0 else 'bearish'
            else:
                trend_direction = 'sideways'
                
            trend_record = {
                'symbol': data.get('symbol'),
                'trend_direction': trend_direction,
                'strength': min(abs(float(data.get('price_change', 0))) / 10.0, 1.0),
                'confidence': float(data.get('confidence', 0.7)),
                'duration_hours': 1,
                'start_time': timestamp,
                'end_time': timestamp + timedelta(hours=1)
            }
            
            self.ch.insert('market_trends', [trend_record])
            
        except Exception as e:
            logger.error(f"Error storing market anomaly: {e}")
            raise
    
    def _store_technical_indicator(self, data: Dict[str, Any]):
        """Store technical indicator data"""
        try:
            # Validate required fields
            if not data.get('symbol'):
                logger.error(f"Missing symbol in technical indicator data: {data}")
                return
            
            # Handle timestamp conversion
            timestamp_value = data.get('processed_at') or data.get('timestamp')
            timestamp = self._safe_timestamp_conversion(
                timestamp_value, 
                f"technical indicator data for {data.get('symbol')}"
            )
            
            record = {
                'symbol': data.get('symbol'),
                'timestamp': timestamp,
                'sma_20': float(data.get('sma')) if data.get('sma') is not None else None,
                'ema_12': float(data.get('ema')) if data.get('ema') is not None else None,
                'rsi': float(data.get('rsi')) if data.get('rsi') is not None else None,
                'macd_line': float(data.get('macd')) if data.get('macd') is not None else None,
                'macd_signal': float(data.get('macd_signal')) if data.get('macd_signal') is not None else None,
                'bollinger_upper': float(data.get('bollinger_upper')) if data.get('bollinger_upper') is not None else None,
                'bollinger_lower': float(data.get('bollinger_lower')) if data.get('bollinger_lower') is not None else None
            }
            
            self.ch.insert('technical_indicators_agg', [record])
            
        except Exception as e:
            logger.error(f"Error storing technical indicator: {e}")
            raise


class OLAPService:
    """Main OLAP service with clean architecture"""
    
    def __init__(self):
        self.ch_config = ClickHouseConfig()
        self.kafka_config = KafkaConfig()
        self.ch_client = ClickHouseClient(self.ch_config)
        self.analytics = None
        self.kafka_processor = None
        self.running = False
        
        # Health metrics
        self.metrics = {
            "start_time": datetime.now(),
            "queries_processed": 0,
            "errors": 0
        }
    
    async def start(self):
        """Start the OLAP service"""
        try:
            logger.info("Starting OLAP Service...")
            
            # Initialize ClickHouse
            await self.ch_client.connect()
            
            # Initialize analytics service
            self.analytics = AnalyticsService(self.ch_client)
            
            # Start Kafka processor
            self.kafka_processor = KafkaProcessor(self.kafka_config, self.ch_client)
            self.kafka_processor.start()
            
            # Start HTTP server
            self.running = True
            await self._start_http_server()
            
            logger.info("OLAP Service started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start OLAP service: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the OLAP service"""
        logger.info("Stopping OLAP Service...")
        self.running = False
        
        if self.kafka_processor:
            self.kafka_processor.stop()
        
        if self.ch_client:
            self.ch_client.close()
        
        logger.info("OLAP Service stopped")
    
    async def _start_http_server(self):
        """Start HTTP API server"""
        app = web.Application()
        
        # Add routes
        app.router.add_get('/health', self._health_handler)
        app.router.add_get('/analytics/price/{symbol}', self._price_analytics_handler)
        app.router.add_get('/analytics/trends', self._trends_handler)
        app.router.add_get('/analytics/trends/{symbol}', self._symbol_trends_handler)
        app.router.add_get('/analytics/volatility', self._volatility_handler)
        app.router.add_get('/analytics/indicators/{symbol}', self._indicators_handler)
        app.router.add_get('/metrics', self._metrics_handler)
        
        # Start server
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8002)
        await site.start()
        
        logger.info("HTTP server started on port 8002")
        
        # Keep server running
        while self.running:
            await asyncio.sleep(1)
    
    def _json_response(self, data: Any, status: int = 200) -> web.Response:
        """Create JSON response"""
        return web.Response(
            text=json.dumps(data, cls=DecimalEncoder, indent=2),
            content_type='application/json',
            status=status
        )
    
    async def _health_handler(self, request):
        """Health check endpoint"""
        try:
            # Test ClickHouse connection
            self.ch_client.client.command("SELECT 1")
            
            uptime = datetime.now() - self.metrics["start_time"]
            
            health_data = {
                "status": "healthy",
                "service": "olap-analytics",
                "uptime_seconds": int(uptime.total_seconds()),
                "clickhouse_connected": True,
                "kafka_connected": self.kafka_processor.running if self.kafka_processor else False,
                "metrics": self.metrics
            }
            
            return self._json_response(health_data)
        
        except Exception as e:
            return self._json_response({
                "status": "unhealthy",
                "error": str(e)
            }, status=503)
    
    async def _price_analytics_handler(self, request):
        """Get price analytics for a symbol"""
        try:
            symbol = request.match_info['symbol'].upper()
            period_minutes = int(request.query.get('period_minutes', 60))
            hours = int(request.query.get('hours', 24))
            
            # Validate period
            if period_minutes not in [1, 5, 15, 30, 60, 240, 1440]:
                return self._json_response({
                    "error": "Invalid period_minutes. Allowed: 1, 5, 15, 30, 60, 240, 1440"
                }, status=400)
            
            analytics = await self.analytics.get_price_analytics(symbol, period_minutes, hours)
            self.metrics["queries_processed"] += 1
            
            return self._json_response({
                "success": True,
                "symbol": symbol,
                "period_minutes": period_minutes,
                "hours": hours,
                "data": analytics
            })
        
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Error in price analytics: {e}")
            return self._json_response({"error": str(e)}, status=500)
    
    async def _trends_handler(self, request):
        """Get market trends"""
        try:
            hours = int(request.query.get('hours', 24))
            trends = await self.analytics.get_market_trends(hours=hours)
            self.metrics["queries_processed"] += 1
            
            return self._json_response({
                "success": True,
                "hours": hours,
                "data": trends
            })
        
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Error in trends: {e}")
            return self._json_response({"error": str(e)}, status=500)
    
    async def _symbol_trends_handler(self, request):
        """Get trends for specific symbol"""
        try:
            symbol = request.match_info['symbol'].upper()
            hours = int(request.query.get('hours', 24))
            trends = await self.analytics.get_market_trends(symbol=symbol, hours=hours)
            self.metrics["queries_processed"] += 1
            
            return self._json_response({
                "success": True,
                "symbol": symbol,
                "hours": hours,
                "data": trends
            })
        
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Error in symbol trends: {e}")
            return self._json_response({"error": str(e)}, status=500)
    
    async def _volatility_handler(self, request):
        """Get volatility analysis"""
        try:
            symbols_param = request.query.get('symbols', 'BTCUSDT,ETHUSDT')
            symbols = [s.strip().upper() for s in symbols_param.split(',')]
            hours = int(request.query.get('hours', 24))
            
            if len(symbols) > 20:
                return self._json_response({
                    "error": "Too many symbols. Maximum 20 allowed."
                }, status=400)
            
            volatility = await self.analytics.get_volatility_analysis(symbols, hours)
            self.metrics["queries_processed"] += 1
            
            return self._json_response({
                "success": True,
                "symbols": symbols,
                "hours": hours,
                "data": volatility
            })
        
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Error in volatility analysis: {e}")
            return self._json_response({"error": str(e)}, status=500)
    
    async def _indicators_handler(self, request):
        """Get technical indicators"""
        try:
            symbol = request.match_info['symbol'].upper()
            hours = int(request.query.get('hours', 24))
            indicators = await self.analytics.get_technical_indicators(symbol, hours)
            self.metrics["queries_processed"] += 1
            
            return self._json_response({
                "success": True,
                "symbol": symbol,
                "hours": hours,
                "data": indicators
            })
        
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Error in indicators: {e}")
            return self._json_response({"error": str(e)}, status=500)
    
    async def _metrics_handler(self, request):
        """Get service metrics"""
        uptime = datetime.now() - self.metrics["start_time"]
        
        metrics_data = {
            **self.metrics,
            "uptime_seconds": int(uptime.total_seconds()),
            "clickhouse_connected": self.ch_client.client is not None,
            "kafka_connected": self.kafka_processor.running if self.kafka_processor else False
        }
        
        return self._json_response(metrics_data)


async def main():
    """Main entry point"""
    service = OLAPService()
    
    try:
        await service.start()
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
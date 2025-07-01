import json
import time
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
import asyncio
import websockets
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from aiohttp import web

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CryptoProducer:
    """
    Multi-source crypto data producer with automatic failover support.
    Streams real-time cryptocurrency data from multiple exchanges to Kafka.
    """
    
    def __init__(self, bootstrap_servers="localhost:9092"):
        """Initialize Kafka producer and crypto data sources"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.running = False
        
        # Health monitoring
        self.health_status = {
            "status": "starting",
            "websocket_connected": False,
            "last_data_time": None,
            "active_source": None,
            "failed_sources": [],
        }
        
        # Configuration
        symbols_env = os.getenv("CRYPTO_SYMBOLS", "BTCUSDT,ETHUSDT,DOGEUSDT,BNBUSDT,XRPUSDT")
        self.symbols = symbols_env.split(",")
        self.data_sources = self._configure_data_sources()
        self.connection_failures = {}
        self.max_connection_failures = 3

    # ==================== CONFIGURATION ====================
    
    def _configure_data_sources(self) -> List[Dict[str, Any]]:
        """Configure multiple data sources with fallback priority"""
        sources = [
            {
                "name": "binance",
                "type": "websocket",
                "base_url": "wss://stream.binance.com:9443/ws/",
                "enabled": os.getenv("ENABLE_BINANCE", "true").lower() == "true",
                "priority": 1
            },
            {
                "name": "coinbase",
                "type": "websocket", 
                "base_url": "wss://ws-feed.exchange.coinbase.com",
                "enabled": os.getenv("ENABLE_COINBASE", "true").lower() == "true",
                "priority": 2
            },
            {
                "name": "kraken",
                "type": "websocket",
                "base_url": "wss://ws.kraken.com",
                "enabled": os.getenv("ENABLE_KRAKEN", "true").lower() == "true",
                "priority": 3
            },
            {
                "name": "coingecko_rest",
                "type": "rest",
                "enabled": True,
                "priority": 4
            }
        ]
        
        # Filter enabled sources and sort by priority
        enabled_sources = [s for s in sources if s["enabled"]]
        enabled_sources.sort(key=lambda x: x["priority"])
        
        logger.info(f"Configured data sources: {[s['name'] for s in enabled_sources]}")
        return enabled_sources

    # ==================== SYMBOL MAPPING ====================
    
    def _binance_to_coinbase_symbol(self, binance_symbol: str) -> Optional[str]:
        """Convert Binance symbol format to Coinbase format"""
        symbol_map = {
            "BTCUSDT": "BTC-USD",
            "ETHUSDT": "ETH-USD", 
            "DOGEUSDT": "DOGE-USD",
            "BNBUSDT": None,  # BNB not available on Coinbase
            "XRPUSDT": "XRP-USD"
        }
        return symbol_map.get(binance_symbol.upper())

    def _binance_to_kraken_symbol(self, binance_symbol: str) -> Optional[str]:
        """Convert Binance symbol format to Kraken format"""
        symbol_map = {
            "BTCUSDT": "XBT/USD",
            "ETHUSDT": "ETH/USD",
            "DOGEUSDT": "DOGE/USD", 
            "BNBUSDT": None,  # BNB not available on Kraken
            "XRPUSDT": "XRP/USD"
        }
        return symbol_map.get(binance_symbol.upper())

    def _coinbase_to_binance_symbol(self, coinbase_symbol: str) -> str:
        """Convert Coinbase symbol format back to Binance format"""
        symbol_map = {
            "BTC-USD": "BTCUSDT",
            "ETH-USD": "ETHUSDT",
            "DOGE-USD": "DOGEUSDT",
            "XRP-USD": "XRPUSDT"
        }
        return symbol_map.get(coinbase_symbol, coinbase_symbol.replace("-", ""))

    def _kraken_to_binance_symbol(self, kraken_symbol: str) -> str:
        """Convert Kraken symbol format back to Binance format"""
        symbol_map = {
            "XBT/USD": "BTCUSDT",
            "ETH/USD": "ETHUSDT", 
            "DOGE/USD": "DOGEUSDT",
            "XRP/USD": "XRPUSDT"
        }
        return symbol_map.get(kraken_symbol, kraken_symbol.replace("/", ""))

    # ==================== KAFKA OPERATIONS ====================
    
    def create_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                batch_size=16384,
                linger_ms=10,
                compression_type="gzip",
            )
            logger.info("Kafka producer created successfully")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise

    def send_to_kafka(self, topic: str, key: str, data: Dict[Any, Any]):
        """Send data to Kafka topic"""
        try:
            enriched_data = {
                **data,
                "produced_at": datetime.now().isoformat(),
                "producer_id": "crypto-stream-producer",
            }

            future = self.producer.send(topic=topic, key=key, value=enriched_data)
            record_metadata = future.get(timeout=10)
            logger.debug(f"Sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")

        except KafkaError as e:
            logger.error(f"Failed to send to Kafka: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

    # ==================== HEALTH MONITORING ====================
    
    async def health_check_handler(self, request):
        """Health check endpoint"""
        return web.json_response(self.health_status)

    async def start_health_server(self):
        """Start health check HTTP server"""
        app = web.Application()
        app.router.add_get("/health", self.health_check_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", 8000)
        await site.start()
        logger.info("Health check server started on port 8000")

    def _mark_source_failed(self, source_name: str):
        """Mark a source as failed and update health status"""
        if source_name not in self.health_status["failed_sources"]:
            self.health_status["failed_sources"].append(source_name)
        
        self.connection_failures[source_name] = self.connection_failures.get(source_name, 0) + 1

    # ==================== DATA STREAMING ORCHESTRATION ====================
    
    async def stream_crypto_data(self):
        """Stream live price data with automatic fallback between sources"""
        logger.info("Starting crypto data streaming with fallback support")
        
        for source in self.data_sources:
            if source["type"] == "websocket":
                try:
                    logger.info(f"Attempting to connect to {source['name']} WebSocket...")
                    success = await self._try_websocket_source(source)
                    if success:
                        self.health_status["active_source"] = source["name"]
                        logger.info(f"✅ Successfully connected to {source['name']}")
                        return
                    else:
                        self._mark_source_failed(source["name"])
                        logger.warning(f"❌ Failed to connect to {source['name']}, trying next source...")
                        continue
                        
                except Exception as e:
                    logger.error(f"❌ Error with {source['name']}: {e}")
                    self._mark_source_failed(source["name"])
                    continue
        
        # Fallback to REST API polling
        logger.warning("All WebSocket sources failed, falling back to REST API polling")
        self.health_status["active_source"] = "coingecko_rest"
        await self.periodic_market_data()

    async def _try_websocket_source(self, source: Dict[str, Any]) -> bool:
        """Try to connect to a specific WebSocket source"""
        source_handlers = {
            "binance": self._stream_binance_data,
            "coinbase": self._stream_coinbase_data,
            "kraken": self._stream_kraken_data
        }
        
        handler = source_handlers.get(source["name"])
        if handler:
            return await handler()
        return False

    # ==================== BINANCE WEBSOCKET ====================
    
    async def _stream_binance_data(self) -> bool:
        """Stream live price data from Binance WebSocket"""
        logger.info(f"Starting Binance WebSocket streams for symbols: {self.symbols}")
        
        try:
            tasks = [
                asyncio.create_task(self._stream_binance_single_symbol(symbol.lower()))
                for symbol in self.symbols
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful_streams = sum(1 for r in results if r is True)
            
            if successful_streams > 0:
                logger.info(f"✅ {successful_streams}/{len(tasks)} Binance streams successful")
                return True
            else:
                logger.error("❌ All Binance streams failed")
                return False
                
        except Exception as e:
            logger.error(f"Error in Binance WebSocket streaming: {e}")
            return False

    async def _stream_binance_single_symbol(self, symbol: str) -> bool:
        """Stream data for a single symbol from Binance"""
        ws_url = f"wss://stream.binance.com:9443/ws/{symbol}@ticker"
        logger.info(f"Starting Binance stream for {symbol.upper()}: {ws_url}")

        for attempt in range(3):  # max_retries = 3
            try:
                logger.info(f"Binance connection attempt {attempt + 1}/3 for {symbol.upper()}")

                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10, open_timeout=30) as websocket:
                    logger.info(f"✅ Connected to Binance WebSocket for {symbol.upper()}!")
                    
                    # Update health status on first successful connection
                    if not self.health_status["websocket_connected"]:
                        self.health_status["websocket_connected"] = True
                        self.health_status["status"] = "running"

                    while self.running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                            data = json.loads(message)

                            processed_data = {
                                "symbol": data["s"],
                                "price": float(data["c"]),
                                "price_change": float(data["P"]),
                                "volume": float(data["v"]),
                                "high": float(data["h"]),
                                "low": float(data["l"]),
                                "timestamp": int(data["E"]),
                                "source": "binance_websocket",
                            }

                            self.send_to_kafka(topic="crypto-prices", key=data["s"], data=processed_data)
                            self.health_status["last_data_time"] = datetime.now().isoformat()

                            logger.info(f"📈 {data['s']}: ${processed_data['price']:.4f} ({processed_data['price_change']:+.2f}%)")

                        except asyncio.TimeoutError:
                            logger.debug(f"Binance WebSocket timeout for {symbol.upper()}, continuing...")
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"Binance WebSocket connection closed for {symbol.upper()}")
                            break
                        except Exception as e:
                            logger.error(f"Error processing Binance WebSocket message for {symbol.upper()}: {e}")
                            continue

                    return True

            except Exception as e:
                logger.error(f"Binance WebSocket connection attempt {attempt + 1} failed for {symbol.upper()}: {e}")
                if attempt < 2:  # max_retries - 1
                    logger.info(f"Retrying Binance {symbol.upper()} in 10 seconds...")
                    await asyncio.sleep(10)
                else:
                    logger.error(f"All Binance WebSocket connection attempts failed for {symbol.upper()}")

        return False

    # ==================== COINBASE WEBSOCKET ====================
    
    async def _stream_coinbase_data(self) -> bool:
        """Stream live price data from Coinbase Pro WebSocket"""
        logger.info("Starting Coinbase Pro WebSocket stream")
        
        coinbase_symbols = [
            self._binance_to_coinbase_symbol(symbol) 
            for symbol in self.symbols 
            if self._binance_to_coinbase_symbol(symbol)
        ]
        
        if not coinbase_symbols:
            logger.warning("No symbols available for Coinbase Pro")
            return False
        
        logger.info(f"Coinbase symbols: {coinbase_symbols}")
        
        try:
            async with websockets.connect("wss://ws-feed.exchange.coinbase.com") as websocket:
                subscribe_message = {
                    "type": "subscribe",
                    "product_ids": coinbase_symbols,
                    "channels": ["ticker"]
                }
                
                await websocket.send(json.dumps(subscribe_message))
                logger.info("✅ Connected to Coinbase Pro WebSocket!")
                
                if not self.health_status["websocket_connected"]:
                    self.health_status["websocket_connected"] = True
                    self.health_status["status"] = "running"
                
                while self.running:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                        data = json.loads(message)
                        
                        if data.get("type") == "ticker":
                            processed_data = {
                                "symbol": self._coinbase_to_binance_symbol(data["product_id"]),
                                "price": float(data["price"]),
                                "price_change": 0.0,  # Coinbase doesn't provide 24h change in ticker
                                "volume": float(data.get("volume_24h", 0)),
                                "high": float(data.get("high_24h", data["price"])),
                                "low": float(data.get("low_24h", data["price"])),
                                "timestamp": int(datetime.fromisoformat(data["time"].replace('Z', '+00:00')).timestamp() * 1000),
                                "source": "coinbase_websocket",
                            }
                            
                            self.send_to_kafka(topic="crypto-prices", key=processed_data["symbol"], data=processed_data)
                            self.health_status["last_data_time"] = datetime.now().isoformat()
                            
                            logger.info(f"📈 {processed_data['symbol']}: ${processed_data['price']:.4f} (Coinbase)")
                    
                    except asyncio.TimeoutError:
                        logger.debug("Coinbase WebSocket timeout, continuing...")
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("Coinbase WebSocket connection closed")
                        break
                    except Exception as e:
                        logger.error(f"Error processing Coinbase WebSocket message: {e}")
                        continue
                
                return True
                
        except Exception as e:
            logger.error(f"Coinbase WebSocket connection failed: {e}")
            return False

    # ==================== KRAKEN WEBSOCKET ====================
    
    async def _stream_kraken_data(self) -> bool:
        """Stream live price data from Kraken WebSocket"""
        logger.info("Starting Kraken WebSocket stream")
        
        kraken_symbols = [
            self._binance_to_kraken_symbol(symbol) 
            for symbol in self.symbols 
            if self._binance_to_kraken_symbol(symbol)
        ]
        
        if not kraken_symbols:
            logger.warning("No symbols available for Kraken")
            return False
        
        logger.info(f"Kraken symbols: {kraken_symbols}")
        
        try:
            async with websockets.connect("wss://ws.kraken.com") as websocket:
                subscribe_message = {
                    "event": "subscribe",
                    "pair": kraken_symbols,
                    "subscription": {"name": "ticker"}
                }
                
                await websocket.send(json.dumps(subscribe_message))
                logger.info("✅ Connected to Kraken WebSocket!")
                
                if not self.health_status["websocket_connected"]:
                    self.health_status["websocket_connected"] = True
                    self.health_status["status"] = "running"
                
                while self.running:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                        data = json.loads(message)
                        
                        if isinstance(data, list) and len(data) >= 4:
                            ticker_data = data[1]
                            pair = data[3]
                            
                            if isinstance(ticker_data, dict) and 'c' in ticker_data:
                                processed_data = {
                                    "symbol": self._kraken_to_binance_symbol(pair),
                                    "price": float(ticker_data['c'][0]),
                                    "price_change": 0.0,
                                    "volume": float(ticker_data.get('v', [0, 0])[1]),
                                    "high": float(ticker_data.get('h', [0, 0])[1]),
                                    "low": float(ticker_data.get('l', [0, 0])[1]),
                                    "timestamp": int(time.time() * 1000),
                                    "source": "kraken_websocket",
                                }
                                
                                self.send_to_kafka(topic="crypto-prices", key=processed_data["symbol"], data=processed_data)
                                self.health_status["last_data_time"] = datetime.now().isoformat()
                                
                                logger.info(f"📈 {processed_data['symbol']}: ${processed_data['price']:.4f} (Kraken)")
                    
                    except asyncio.TimeoutError:
                        logger.debug("Kraken WebSocket timeout, continuing...")
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("Kraken WebSocket connection closed")
                        break
                    except Exception as e:
                        logger.error(f"Error processing Kraken WebSocket message: {e}")
                        continue
                
                return True
                
        except Exception as e:
            logger.error(f"Kraken WebSocket connection failed: {e}")
            return False

    # ==================== REST API FALLBACK ====================
    
    def fetch_market_data(self):
        """Fetch market overview data from CoinGecko API"""
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 10,
                "page": 1,
                "sparkline": False,
            }

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            }

            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()

            market_data = response.json()

            for coin in market_data:
                processed_data = {
                    "id": coin["id"],
                    "symbol": coin["symbol"].upper(),
                    "name": coin["name"],
                    "current_price": coin["current_price"],
                    "market_cap": coin["market_cap"],
                    "price_change_24h": coin["price_change_percentage_24h"],
                    "volume_24h": coin["total_volume"],
                    "timestamp": int(time.time() * 1000),
                    "source": "coingecko_api",
                }

                self.send_to_kafka(topic="crypto-market-data", key=coin["symbol"].upper(), data=processed_data)

            logger.info(f"Fetched market data for {len(market_data)} coins")

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error fetching market data: {e}")
        except Exception as e:
            logger.error(f"Error fetching market data: {e}")

    async def periodic_market_data(self):
        """Fetch market data periodically as fallback"""
        logger.info("Starting periodic market data fetching (fallback mode)")
        while self.running:
            self.fetch_market_data()
            await asyncio.sleep(30)

    # ==================== MAIN EXECUTION ====================
    
    async def run(self):
        """Main method to run the producer"""
        logger.info("Starting Crypto Kafka Producer with Multi-Source Support")

        self.producer = self.create_producer()
        self.running = True

        try:
            await self.start_health_server()
            logger.info("Producer is running. Topics: crypto-prices, crypto-market-data")
            logger.info(f"Available data sources: {[s['name'] for s in self.data_sources]}")

            await asyncio.gather(
                self.stream_crypto_data(),
                self.periodic_market_data(),
                return_exceptions=True,
            )

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            await self.stop()

    async def stop(self):
        """Stop the producer"""
        logger.info("Stopping producer...")
        self.running = False

        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    """Main entry point"""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Using Kafka bootstrap servers: {bootstrap_servers}")

    producer = CryptoProducer(bootstrap_servers=bootstrap_servers)
    try:
        asyncio.run(producer.run())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")


if __name__ == "__main__":
    main()

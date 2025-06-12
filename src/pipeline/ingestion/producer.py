import json
import time
import logging
import os
from datetime import datetime
from typing import Dict, Any
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
    def __init__(self, bootstrap_servers="localhost:9092"):
        """Initialize Kafka producer and crypto data sources"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.running = False
        self.health_status = {
            "status": "starting",
            "websocket_connected": False,
            "last_data_time": None,
        }

        # Crypto symbols to track
        symbols_env = os.getenv(
            "CRYPTO_SYMBOLS", "BTCUSDT,ETHUSDT,DOGEUSDT,BNBUSDT,XRPUSDT"
        )
        self.symbols = symbols_env.split(",")

        # WebSocket URL
        self.binance_ws_url = "wss://stream.binance.com:9443/ws/"

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
            # Add metadata
            enriched_data = {
                **data,
                "produced_at": datetime.now().isoformat(),
                "producer_id": "crypto-stream-producer",
            }

            # Send to Kafka
            future = self.producer.send(topic=topic, key=key, value=enriched_data)
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"Sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )

        except KafkaError as e:
            logger.error(f"Failed to send to Kafka: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

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

    async def stream_binance_data(self):
        """Stream live price data from Binance WebSocket"""
        logger.info(f"Starting WebSocket streams for symbols: {self.symbols}")
        
        # Create tasks for each symbol
        tasks = []
        for symbol in self.symbols:
            task = asyncio.create_task(
                self.stream_single_symbol(symbol.lower())
            )
            tasks.append(task)
        
        # Wait for all streams to complete
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error in WebSocket streaming: {e}")

    async def stream_single_symbol(self, symbol: str):
        """Stream data for a single symbol"""
        ws_url = f"{self.binance_ws_url}{symbol}@ticker"
        logger.info(f"Starting stream for {symbol.upper()}: {ws_url}")

        max_retries = 3
        retry_delay = 10

        for attempt in range(max_retries):
            try:
                logger.info(f"Connection attempt {attempt + 1}/{max_retries} for {symbol.upper()}")

                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=30,
                ) as websocket:
                    logger.info(f"✅ Connected to WebSocket for {symbol.upper()}!")
                    
                    # Update health status when first connection is successful
                    if not self.health_status["websocket_connected"]:
                        self.health_status["websocket_connected"] = True
                        self.health_status["status"] = "running"

                    while self.running:
                        try:
                            message = await asyncio.wait_for(
                                websocket.recv(), timeout=30.0
                            )
                            data = json.loads(message)

                            # Process ticker data
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

                            # Send to Kafka
                            self.send_to_kafka(
                                topic="crypto-prices", key=data["s"], data=processed_data
                            )

                            # Update health status
                            self.health_status["last_data_time"] = datetime.now().isoformat()

                            logger.info(
                                f"📈 {data['s']}: ${processed_data['price']:.4f} ({processed_data['price_change']:+.2f}%)"
                            )

                        except asyncio.TimeoutError:
                            logger.debug(f"WebSocket timeout for {symbol.upper()}, continuing...")
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"WebSocket connection closed for {symbol.upper()}")
                            break
                        except Exception as e:
                            logger.error(f"Error processing WebSocket message for {symbol.upper()}: {e}")
                            continue

                    return

            except Exception as e:
                logger.error(f"WebSocket connection attempt {attempt + 1} failed for {symbol.upper()}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying {symbol.upper()} in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"All WebSocket connection attempts failed for {symbol.upper()}")

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

                self.send_to_kafka(
                    topic="crypto-market-data",
                    key=coin["symbol"].upper(),
                    data=processed_data,
                )

            logger.info(f"Fetched market data for {len(market_data)} coins")

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error fetching market data: {e}")
        except Exception as e:
            logger.error(f"Error fetching market data: {e}")

    async def periodic_market_data(self):
        """Fetch market data periodically"""
        while self.running:
            self.fetch_market_data()
            await asyncio.sleep(60)  # Fetch every minute

    async def run(self):
        """Main method to run the producer"""
        logger.info("Starting Crypto Kafka Producer")

        # Create Kafka producer
        self.producer = self.create_producer()
        self.running = True

        try:
            # Start health check server
            await self.start_health_server()

            logger.info("Producer is running. Topics: crypto-prices, crypto-market-data")

            # Run both WebSocket streaming and periodic market data fetching
            await asyncio.gather(
                self.stream_binance_data(),
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

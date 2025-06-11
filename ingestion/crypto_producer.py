import json
import time
import logging
from datetime import datetime
from typing import Dict, Any
import asyncio
import websockets
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CryptoKafkaProducer:
    def __init__(self, bootstrap_servers="localhost:9092"):
        """Initialize Kafka producer and crypto data sources"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.running = False

        # Crypto symbols to track
        self.symbols = ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "BNBUSDT", "XRPUSDT"]

        # Binance WebSocket URL
        self.binance_ws_url = "wss://stream.binance.com:9443/ws/"

    def create_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # Wait for all replicas
                retries=3,
                batch_size=16384,
                linger_ms=10,  # Small delay for batching
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

            # Optional: Wait for send to complete
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"Sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )

        except KafkaError as e:
            logger.error(f"Failed to send to Kafka: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

    async def stream_binance_data(self):
        """Stream live price data from Binance WebSocket"""
        # Create stream URLs for multiple symbols
        streams = [f"{symbol.lower()}@ticker" for symbol in self.symbols]
        ws_url = f"{self.binance_ws_url}{'streams/' if len(streams) > 1 else ''}"

        if len(streams) > 1:
            ws_url += f"streams/{'/'.join(streams)}"
        else:
            ws_url += streams[0]

        logger.info(f"Connecting to Binance WebSocket: {ws_url}")

        try:
            async with websockets.connect(ws_url) as websocket:
                logger.info("Connected to Binance WebSocket")

                while self.running:
                    try:
                        # Receive data from WebSocket
                        message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                        data = json.loads(message)

                        # Handle single stream vs multi-stream format
                        if "stream" in data:
                            # Multi-stream format
                            stream_data = data["data"]
                            symbol = stream_data["s"]
                        else:
                            # Single stream format
                            stream_data = data
                            symbol = stream_data["s"]

                        # Process ticker data
                        processed_data = {
                            "symbol": symbol,
                            "price": float(stream_data["c"]),  # Current price
                            "price_change": float(stream_data["P"]),  # Price change %
                            "volume": float(stream_data["v"]),  # Volume
                            "high": float(stream_data["h"]),  # 24h high
                            "low": float(stream_data["l"]),  # 24h low
                            "timestamp": int(stream_data["E"]),  # Event time
                            "source": "binance_websocket",
                        }

                        # Send to Kafka
                        self.send_to_kafka(
                            topic="crypto-prices", key=symbol, data=processed_data
                        )

                        logger.info(
                            f"{symbol}: ${processed_data['price']:.4f} ({processed_data['price_change']:+.2f}%)"
                        )

                    except asyncio.TimeoutError:
                        logger.debug("WebSocket timeout, continuing...")
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("WebSocket connection closed")
                        break
                    except Exception as e:
                        logger.error(f"Error processing WebSocket message: {e}")
                        continue

        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")

    def fetch_market_data(self):
        """Fetch market overview data from REST API"""
        try:
            # Get top cryptocurrencies by market cap
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 10,
                "page": 1,
                "sparkline": False,
            }

            response = requests.get(url, params=params, timeout=10)
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

                # Send to Kafka
                self.send_to_kafka(
                    topic="crypto-market-data",
                    key=coin["symbol"].upper(),
                    data=processed_data,
                )

            logger.info(f"Fetched market data for {len(market_data)} coins")

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
            # Create topics (optional - topics will be auto-created)
            logger.info(
                "Producer is running. Topics: crypto-prices, crypto-market-data"
            )

            # Run both WebSocket streaming and periodic market data fetching
            await asyncio.gather(
                self.stream_binance_data(), self.periodic_market_data()
            )

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            await self.stop()

    async def stop(self):
        """Stop the producer gracefully"""
        logger.info("Stopping producer...")
        self.running = False

        if self.producer:
            self.producer.flush()  # Ensure all messages are sent
            self.producer.close()

        logger.info("Producer stopped")


def main():
    """Main function"""
    producer = CryptoKafkaProducer()

    try:
        asyncio.run(producer.run())
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    main()

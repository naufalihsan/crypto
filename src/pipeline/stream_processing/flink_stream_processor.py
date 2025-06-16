import json
import time
import os
import logging
from typing import Iterable

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common import Row, RestartStrategies

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "flink-crypto")
KAFKA_SOURCE_TOPIC = os.getenv("KAFKA_SOURCE_TOPIC", "crypto-prices")
KAFKA_INDICATORS_TOPIC = os.getenv("KAFKA_INDICATORS_TOPIC", "technical-indicators")
KAFKA_ANOMALIES_TOPIC = os.getenv("KAFKA_ANOMALIES_TOPIC", "market-anomalies")

# Event schema
EVENT_TYPEINFO = Types.ROW_NAMED(
    ["symbol", "price", "price_change", "volume", "high", "low", "timestamp", "source"],
    [
        Types.STRING(),
        Types.FLOAT(),
        Types.FLOAT(),
        Types.FLOAT(),
        Types.FLOAT(),
        Types.FLOAT(),
        Types.LONG(),
        Types.STRING(),
    ],
)


def calculate_sma(prices, window):
    """Calculate Simple Moving Average"""
    if len(prices) < window:
        return sum(prices) / len(prices) if prices else 0
    return sum(prices[-window:]) / window


def calculate_ema(prices, window):
    """Calculate Exponential Moving Average"""
    if not prices:
        return 0
    ema = prices[0]
    alpha = 2.0 / (window + 1)
    for price in prices[1:]:
        ema = price * alpha + ema * (1 - alpha)
    return ema


def calculate_rsi(prices, window):
    """Calculate Relative Strength Index"""
    if len(prices) < window + 1:
        return 50.0

    gains, losses = [], []
    for i in range(1, min(len(prices), window + 1)):
        delta = prices[i] - prices[i - 1]
        if delta > 0:
            gains.append(delta)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(-delta)

    if not gains or not losses:
        return 50.0

    avg_gain = sum(gains) / len(gains)
    avg_loss = sum(losses) / len(losses)

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_macd(prices):
    """Calculate MACD"""
    if len(prices) < 26:
        return 0.0
    ema12 = calculate_ema(prices, 12)
    ema26 = calculate_ema(prices, 26)
    return ema12 - ema26


def calculate_bollinger(prices, window):
    """Calculate Bollinger Bands"""
    subset = prices[-window:] if len(prices) >= window else prices
    if not subset:
        return 0, 0

    mean = sum(subset) / len(subset)
    variance = sum((x - mean) ** 2 for x in subset) / len(subset)
    stddev = variance**0.5
    upper = mean + 2 * stddev
    lower = mean - 2 * stddev
    return upper, lower


class TechnicalIndicatorProcessor(ProcessWindowFunction):
    """Process function for technical indicators"""

    def process(
        self, key: str, context: ProcessWindowFunction.Context, elements: Iterable
    ) -> Iterable[str]:
        try:
            events_list = list(elements)
            if not events_list:
                return []

            events_sorted = sorted(events_list, key=lambda e: e.timestamp)
            prices = [float(e.price) for e in events_sorted if e.price and e.price > 0]

            if not prices:
                return []

            window_size = min(14, len(prices))

            indicator = {
                "symbol": key,
                "sma": calculate_sma(prices, window_size),
                "ema": calculate_ema(prices, window_size),
                "rsi": calculate_rsi(prices, window_size),
                "macd": calculate_macd(prices),
                "bollinger_upper": calculate_bollinger(prices, window_size)[0],
                "bollinger_lower": calculate_bollinger(prices, window_size)[1],
                "current_price": prices[-1],
                "price_count": len(prices),
                "window_start": events_sorted[0].timestamp,
                "window_end": events_sorted[-1].timestamp,
                "processed_at": int(time.time() * 1000),
            }

            return [json.dumps(indicator, ensure_ascii=False)]

        except Exception as e:
            logger.error(f"Error processing indicators for {key}: {e}")
            return []


class VolumeAnomalyProcessor(ProcessWindowFunction):
    """Process function for volume anomaly detection"""

    def process(
        self, key: str, context: ProcessWindowFunction.Context, elements: Iterable
    ) -> Iterable[str]:
        try:
            events_list = list(elements)
            if not events_list:
                return []

            events_sorted = sorted(events_list, key=lambda e: e.timestamp)
            volumes = [
                float(e.volume) for e in events_sorted if e.volume and e.volume > 0
            ]

            if not volumes:
                return []

            mean_volume = sum(volumes) / len(volumes)
            last_volume = volumes[-1]
            volume_ratio = last_volume / mean_volume if mean_volume > 0 else 0

            # Volume spike threshold
            if mean_volume > 0 and last_volume > 1.2 * mean_volume:
                alert = {
                    "symbol": key,
                    "alert_type": "VOLUME_SPIKE",
                    "volume": last_volume,
                    "mean_volume": mean_volume,
                    "volume_ratio": volume_ratio,
                    "timestamp": events_sorted[-1].timestamp,
                    "processed_at": int(time.time() * 1000),
                }
                return [json.dumps(alert, ensure_ascii=False)]

            return []

        except Exception as e:
            logger.error(f"Error processing volume anomaly for {key}: {e}")
            return []


class CryptoTimestampAssigner(TimestampAssigner):
    """Timestamp assigner for crypto events"""

    def extract_timestamp(self, value, record_timestamp):
        try:
            return (
                int(value.timestamp)
                if hasattr(value, "timestamp") and value.timestamp
                else int(time.time() * 1000)
            )
        except:
            return int(time.time() * 1000)


def setup_flink_environment():
    """Setup Flink environment"""
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        parallelism = int(os.getenv("FLINK_PARALLELISM", "2"))
        env.set_parallelism(parallelism)
        env.get_config().set_auto_watermark_interval(50)
        env.set_restart_strategy(
            RestartStrategies.failure_rate_restart(3, 300000, 10000)
        )

        logger.info(f"Flink environment initialized with parallelism={parallelism}")
        return env

    except Exception as e:
        logger.error(f"Environment setup failed: {e}")
        raise


def parse_event(value):
    """Parse JSON event into Row object"""
    try:
        if not value:
            return None

        data = json.loads(value)
        if not data.get("symbol") or data.get("price") is None:
            return None

        return Row(
            symbol=str(data.get("symbol", "")),
            price=float(data.get("price", 0)),
            price_change=float(data.get("price_change", 0)),
            volume=float(data.get("volume", 0)),
            high=float(data.get("high", data.get("price", 0))),
            low=float(data.get("low", data.get("price", 0))),
            timestamp=int(data.get("timestamp", int(time.time() * 1000))),
            source=str(data.get("source", "unknown")),
        )
    except Exception as e:
        logger.warning(f"Failed to parse event: {e}")
        return None


def detect_volatility_anomaly(event):
    """Detect volatility anomalies in real-time"""
    try:
        if event and event.price_change and abs(float(event.price_change)) > 2.5:
            alert = {
                "symbol": event.symbol,
                "alert_type": "VOLATILITY_SPIKE",
                "price_change": abs(float(event.price_change)),
                "price": float(event.price),
                "timestamp": int(event.timestamp),
                "processed_at": int(time.time() * 1000),
            }
            return json.dumps(alert, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Volatility detection error: {e}")
    return None


def detect_price_movement_anomaly(event):
    """Detect price movement anomalies"""
    try:
        if event and event.price_change and abs(float(event.price_change)) > 1.5:
            alert = {
                "symbol": event.symbol,
                "alert_type": "PRICE_MOVEMENT",
                "price_change": float(event.price_change),
                "price": float(event.price),
                "timestamp": int(event.timestamp),
                "processed_at": int(time.time() * 1000),
                "severity": "medium",
            }
            return json.dumps(alert, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Price movement detection error: {e}")
    return None


def main():
    """Main stream processing job"""
    try:
        logger.info("Starting Flink crypto stream processor")
        env = setup_flink_environment()

        # Kafka source
        kafka_source = (
            KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
            .set_topics(KAFKA_SOURCE_TOPIC)
            .set_group_id(KAFKA_CONSUMER_GROUP)
            .set_value_only_deserializer(SimpleStringSchema())
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .build()
        )

        # Watermark strategy
        watermark_strategy = (
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(500))
            .with_timestamp_assigner(CryptoTimestampAssigner())
            .with_idleness(Duration.of_seconds(2))
        )

        # Create data stream
        parsed_stream = (
            env.from_source(kafka_source, watermark_strategy, "kafka-source")
            .set_parallelism(env.get_parallelism())
            .map(parse_event, output_type=EVENT_TYPEINFO)
            .filter(lambda event: event is not None and event.symbol != "")
        )

        keyed_stream = parsed_stream.key_by(lambda event: event.symbol)

        # Processing streams
        indicators_stream = (
            keyed_stream.window(TumblingEventTimeWindows.of(Time.seconds(20)))
            .process(TechnicalIndicatorProcessor(), output_type=Types.STRING())
            .filter(lambda x: x is not None and x.strip() != "")
        )

        anomaly_stream = (
            keyed_stream.window(TumblingEventTimeWindows.of(Time.seconds(30)))
            .process(VolumeAnomalyProcessor(), output_type=Types.STRING())
            .filter(lambda x: x is not None and x.strip() != "")
        )

        volatility_stream = parsed_stream.map(
            detect_volatility_anomaly, output_type=Types.STRING()
        ).filter(lambda x: x is not None)

        price_movement_stream = parsed_stream.map(
            detect_price_movement_anomaly, output_type=Types.STRING()
        ).filter(lambda x: x is not None)

        # Kafka sinks
        indicators_sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(KAFKA_INDICATORS_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

        anomaly_sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(KAFKA_ANOMALIES_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

        # Connect streams to sinks
        indicators_stream.sink_to(indicators_sink)
        anomaly_stream.sink_to(anomaly_sink)
        volatility_stream.sink_to(anomaly_sink)
        price_movement_stream.sink_to(anomaly_sink)

        logger.info("All streams configured, starting execution...")
        env.execute("Crypto Stream Processing Job")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

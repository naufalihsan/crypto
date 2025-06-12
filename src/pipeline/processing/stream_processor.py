import json
import time
import os
from typing import Iterable

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common import Row  # Import Row

# --- Kafka properties ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# --- Define strict schema with Types.ROW_NAMED ---
EVENT_TYPEINFO = Types.ROW_NAMED(
    ["symbol", "price", "price_change", "volume", "high", "low", "timestamp", "source"],
    [
        Types.STRING(),  # symbol
        Types.FLOAT(),  # price
        Types.FLOAT(),  # price_change
        Types.FLOAT(),  # volume
        Types.FLOAT(),  # high
        Types.FLOAT(),  # low
        Types.LONG(),  # timestamp
        Types.STRING(),  # source
    ],
)


# --- Helper functions for indicators (updated to work with Row objects) ---
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
        return 0
    gains = 0
    losses = 0
    for i in range(-window, 0):
        if i - 1 >= -len(prices):
            delta = prices[i] - prices[i - 1]
            if delta > 0:
                gains += delta
            else:
                losses -= delta
    avg_gain = gains / window if window > 0 else 0
    avg_loss = losses / window if window > 0 else 0
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_macd(prices):
    """Calculate MACD (Moving Average Convergence Divergence)"""
    ema12 = calculate_ema(prices, 12)
    ema26 = calculate_ema(prices, 26)
    return ema12 - ema26


def calculate_bollinger(prices, window):
    """Calculate Bollinger Bands"""
    if len(prices) < window:
        subset = prices
    else:
        subset = prices[-window:]
    if not subset:
        return 0, 0
    mean = sum(subset) / len(subset)
    variance = sum((x - mean) ** 2 for x in subset) / len(subset)
    stddev = variance**0.5
    upper = mean + 2 * stddev
    lower = mean - 2 * stddev
    return upper, lower


# --- Process Functions (updated to work with Row objects) ---
class TechnicalIndicatorProcessor(ProcessWindowFunction):
    """Process window function for calculating technical indicators"""
    
    def process(
        self, key: str, context: ProcessWindowFunction.Context, elements: Iterable
    ) -> Iterable[str]:
        # elements is an iterable of Row objects
        events_list = [element for element in elements]
        if not events_list:
            return []
        events_sorted = sorted(events_list, key=lambda e: e.timestamp)  # Use field name
        prices = [e.price for e in events_sorted]  # Use field name
        window = min(14, len(prices))
        sma = calculate_sma(prices, window)
        ema = calculate_ema(prices, window)
        rsi = calculate_rsi(prices, window)
        macd = calculate_macd(prices)
        boll_upper, boll_lower = calculate_bollinger(prices, window)
        indicator = {
            "symbol": key,
            "sma": sma,
            "ema": ema,
            "rsi": rsi,
            "macd": macd,
            "bollinger_upper": boll_upper,
            "bollinger_lower": boll_lower,
            "window_end": events_sorted[-1].timestamp,  # Use field name
        }
        # Ensure we return a proper string
        result = json.dumps(indicator, ensure_ascii=False)
        yield result


class VolumeAnomalyProcessor(ProcessWindowFunction):
    """Process window function for detecting volume anomalies"""
    
    def process(
        self, key: str, context: ProcessWindowFunction.Context, elements: Iterable
    ) -> Iterable[str]:
        events_list = [element for element in elements]
        if not events_list:
            return []
        events_sorted = sorted(events_list, key=lambda e: e.timestamp)  # Use field name
        volumes = [e.volume for e in events_sorted]  # Use field name
        mean_volume = sum(volumes) / len(volumes) if volumes else 0
        last_event = events_sorted[-1]
        if mean_volume > 0 and last_event.volume > 2 * mean_volume:  # Use field name
            alert = {
                "symbol": key,
                "alert_type": "VOLUME_SPIKE",
                "volume": last_event.volume,  # Use field name
                "mean_volume": mean_volume,
                "timestamp": last_event.timestamp,  # Use field name
            }
            # Ensure we return a proper string
            result = json.dumps(alert, ensure_ascii=False)
            yield result


# --- TimestampAssigner implementation for Row ---
class CryptoTimestampAssigner(TimestampAssigner):
    """Custom timestamp assigner for crypto events"""
    
    def extract_timestamp(self, value, record_timestamp):
        return value.timestamp  # Use field name instead of index


# --- Main job ---
def main():
    """Main stream processing job entry point"""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Add Kafka connector JAR
    flink_jars_path = os.getenv("FLINK_JARS_PATH", "/app/flink-jars")
    kafka_jar_path = f"{flink_jars_path}/flink-sql-connector-kafka-3.3.0-1.20.jar"
    
    if os.path.exists(kafka_jar_path):
        env.add_jars(f"file://{kafka_jar_path}")
    else:
        print(f"Warning: Kafka connector JAR not found at {kafka_jar_path}")

    # Kafka source using new KafkaSource API
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics("crypto-prices")
        .set_group_id("flink-crypto")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Parse events (JSON strings) into Row objects (NOT tuples)
    def parse_event(value):
        """Parse JSON string into Row object"""
        try:
            data = json.loads(value)
            return Row(
                symbol=data.get("symbol", ""),
                price=float(data.get("price", 0)),
                price_change=float(data.get("price_change", 0)),
                volume=float(data.get("volume", 0)),
                high=float(data.get("high", 0)),
                low=float(data.get("low", 0)),
                timestamp=int(data.get("timestamp", 0)),
                source=data.get("source", ""),
            )
        except Exception as e:
            return Row(
                symbol="",
                price=0.0,
                price_change=0.0,
                volume=0.0,
                high=0.0,
                low=0.0,
                timestamp=int(time.time() * 1000),
                source="",
            )

    # Watermark strategy using custom TimestampAssigner
    watermark_strategy = (
        WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
            CryptoTimestampAssigner()
        )
    )

    # Create data stream
    data_stream = (
        env.from_source(kafka_source, watermark_strategy, "kafka-source")
        .map(parse_event, output_type=EVENT_TYPEINFO)
        .key_by(lambda event: event.symbol)
    )

    # Technical indicators stream
    indicators_stream = (
        data_stream.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
        .process(TechnicalIndicatorProcessor(), output_type=Types.STRING())
    )

    # Volume anomaly detection stream
    anomaly_stream = (
        data_stream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(2)))
        .process(VolumeAnomalyProcessor(), output_type=Types.STRING())
    )

    # Volatility detection using CEP-like logic
    def detect_volatility_anomaly(event):
        """Detect volatility anomalies"""
        # Use field names instead of indices
        if abs(event.price_change) > 5.0:  # 5% change threshold
            result = json.dumps({
                "symbol": event.symbol,
                "alert_type": "VOLATILITY_SPIKE",
                "price_change": event.price_change,
                "price": event.price,
                "timestamp": event.timestamp,
            }, ensure_ascii=False)
            return result
        return None

    volatility_stream = (
        data_stream.map(detect_volatility_anomaly, output_type=Types.STRING())
        .filter(lambda x: x is not None)
    )

    # Kafka sinks
    indicators_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("technical-indicators")
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
            .set_topic("market-anomalies")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Add sinks
    indicators_stream.sink_to(indicators_sink)
    anomaly_stream.sink_to(anomaly_sink)
    volatility_stream.sink_to(anomaly_sink)

    # Execute the job
    env.execute("Crypto Stream Processing Job")


if __name__ == "__main__":
    main()

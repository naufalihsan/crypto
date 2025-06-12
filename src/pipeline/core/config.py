"""
Configuration management for the crypto pipeline.

Centralizes all configuration settings and provides environment-based configuration
with proper defaults and validation.
"""

import os
from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path


@dataclass
class KafkaConfig:
    """Kafka configuration settings."""
    bootstrap_servers: str = "localhost:9092"
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 1000
    consumer_timeout_ms: int = 1000
    batch_size: int = 100
    batch_timeout: int = 30


@dataclass
class PostgreSQLConfig:
    """PostgreSQL OLTP database configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "crypto_oltp"
    user: str = "admin"
    password: str = "admin"
    min_pool_size: int = 2
    max_pool_size: int = 10
    command_timeout: int = 60


@dataclass
class ClickHouseConfig:
    """ClickHouse OLAP database configuration."""
    host: str = "localhost"
    port: int = 8123
    database: str = "crypto_olap"
    user: str = "admin"
    password: str = "admin"


@dataclass
class FlinkConfig:
    """Apache Flink configuration."""
    parallelism: int = 2
    job_manager_rpc_address: str = "localhost"
    job_manager_rpc_port: int = 6123
    task_manager_slots: int = 2


@dataclass
class DataSourceConfig:
    """Data source configuration."""
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/"
    coingecko_api_url: str = "https://api.coingecko.com/api/v3"
    crypto_symbols: List[str] = None
    
    def __post_init__(self):
        if self.crypto_symbols is None:
            self.crypto_symbols = [
                "BTCUSDT", "ETHUSDT", "DOGEUSDT", "BNBUSDT", 
                "XRPUSDT", "ADAUSDT", "SOLUSDT", "DOTUSDT",
                "LINKUSDT", "LTCUSDT"
            ]


@dataclass
class Config:
    """Main configuration class that aggregates all configuration sections."""
    
    # Configuration sections
    kafka: KafkaConfig
    postgresql: PostgreSQLConfig
    clickhouse: ClickHouseConfig
    flink: FlinkConfig
    data_sources: DataSourceConfig
    
    # Application settings
    log_level: str = "INFO"
    environment: str = "development"
    debug: bool = False
    
    @classmethod
    def from_env(cls) -> "Config":
        """Create configuration from environment variables."""
        
        # Kafka configuration
        kafka = KafkaConfig(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
            enable_auto_commit=os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() == "true",
            auto_commit_interval_ms=int(os.getenv("KAFKA_AUTO_COMMIT_INTERVAL_MS", "1000")),
            consumer_timeout_ms=int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "1000")),
            batch_size=int(os.getenv("KAFKA_BATCH_SIZE", "100")),
            batch_timeout=int(os.getenv("KAFKA_BATCH_TIMEOUT", "30"))
        )
        
        # PostgreSQL configuration
        postgresql = PostgreSQLConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "crypto_oltp"),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "admin"),
            min_pool_size=int(os.getenv("POSTGRES_MIN_POOL_SIZE", "2")),
            max_pool_size=int(os.getenv("POSTGRES_MAX_POOL_SIZE", "10")),
            command_timeout=int(os.getenv("POSTGRES_COMMAND_TIMEOUT", "60"))
        )
        
        # ClickHouse configuration
        clickhouse = ClickHouseConfig(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            database=os.getenv("CLICKHOUSE_DB", "crypto_olap"),
            user=os.getenv("CLICKHOUSE_USER", "admin"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "admin")
        )
        
        # Flink configuration
        flink = FlinkConfig(
            parallelism=int(os.getenv("FLINK_PARALLELISM", "2")),
            job_manager_rpc_address=os.getenv("JOB_MANAGER_RPC_ADDRESS", "localhost"),
            job_manager_rpc_port=int(os.getenv("JOB_MANAGER_RPC_PORT", "6123")),
            task_manager_slots=int(os.getenv("TASK_MANAGER_SLOTS", "2"))
        )
        
        # Data sources configuration
        symbols_str = os.getenv("CRYPTO_SYMBOLS", "")
        symbols = symbols_str.split(",") if symbols_str else None
        
        data_sources = DataSourceConfig(
            binance_ws_url=os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/"),
            coingecko_api_url=os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3"),
            crypto_symbols=symbols
        )
        
        return cls(
            kafka=kafka,
            postgresql=postgresql,
            clickhouse=clickhouse,
            flink=flink,
            data_sources=data_sources,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            environment=os.getenv("ENVIRONMENT", "development"),
            debug=os.getenv("DEBUG", "false").lower() == "true"
        )
    
    @classmethod
    def from_file(cls, config_path: Path) -> "Config":
        """Load configuration from a file (YAML/JSON)."""
        # TODO: Implement file-based configuration loading
        raise NotImplementedError("File-based configuration not yet implemented")
    
    def validate(self) -> None:
        """Validate configuration settings."""
        # Validate Kafka settings
        if not self.kafka.bootstrap_servers:
            raise ValueError("Kafka bootstrap servers must be specified")
        
        # Validate database connections
        if not all([self.postgresql.host, self.postgresql.database]):
            raise ValueError("PostgreSQL host and database must be specified")
        
        if not all([self.clickhouse.host, self.clickhouse.database]):
            raise ValueError("ClickHouse host and database must be specified")
        
        # Validate data sources
        if not self.data_sources.crypto_symbols:
            raise ValueError("At least one crypto symbol must be specified")
    
    def get_kafka_consumer_config(self) -> dict:
        """Get Kafka consumer configuration as dictionary."""
        return {
            "bootstrap_servers": self.kafka.bootstrap_servers,
            "auto_offset_reset": self.kafka.auto_offset_reset,
            "enable_auto_commit": self.kafka.enable_auto_commit,
            "auto_commit_interval_ms": self.kafka.auto_commit_interval_ms,
            "consumer_timeout_ms": self.kafka.consumer_timeout_ms
        }
    
    def get_postgresql_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return (
            f"postgresql://{self.postgresql.user}:{self.postgresql.password}"
            f"@{self.postgresql.host}:{self.postgresql.port}/{self.postgresql.database}"
        )
    
    def get_clickhouse_connection_params(self) -> dict:
        """Get ClickHouse connection parameters."""
        return {
            "host": self.clickhouse.host,
            "port": self.clickhouse.port,
            "database": self.clickhouse.database,
            "username": self.clickhouse.user,
            "password": self.clickhouse.password
        } 
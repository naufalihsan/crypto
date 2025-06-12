"""
Crypto Pipeline - Real-time Cryptocurrency Data Processing Pipeline

A comprehensive data engineering solution for ingesting, processing, and analyzing
cryptocurrency market data using Apache Kafka, Apache Flink, PostgreSQL, and ClickHouse.
"""

__version__ = "0.1.0"
__author__ = "Naufal Ihsan"
__email__ = "naufal.ihsan21@gmail.com"

# Package imports for easier access
from .core.config import Config
from .core.logging import setup_logging

__all__ = [
    "Config",
    "setup_logging",
] 
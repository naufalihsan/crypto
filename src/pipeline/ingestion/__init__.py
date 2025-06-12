"""
Data ingestion components for the crypto pipeline.

This module contains producers and data ingestion logic for streaming
cryptocurrency data from various sources into Kafka.
"""

from .producer import CryptoProducer

__all__ = [
    "CryptoProducer",
] 
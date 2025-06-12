"""
Stream processing components for the crypto pipeline.

This module contains PyFlink jobs and other stream processing logic for
real-time data transformation and analysis.
"""

from .stream_processor import main as stream_processor_main

__all__ = [
    "stream_processor_main",
] 
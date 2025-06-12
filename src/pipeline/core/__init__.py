"""
Core utilities and shared components for the crypto pipeline.

This module contains configuration management, logging setup, database connections,
and other shared utilities used across the pipeline components.
"""

from .config import Config
from .logging import setup_logging

__all__ = [
    "Config",
    "setup_logging", 
] 
"""
Logging configuration for the crypto pipeline.

Provides centralized logging setup with proper formatting, levels, and handlers
for different environments and components.
"""

import logging
import logging.config
import sys
from pathlib import Path
from typing import Optional


def setup_logging(
    level: str = "INFO",
    log_file: Optional[Path] = None,
    component_name: Optional[str] = None,
    environment: str = "development"
) -> logging.Logger:
    """
    Set up logging configuration for the crypto pipeline.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for logging output
        component_name: Name of the component (ingestion, processing, etc.)
        environment: Environment (development, production, testing)
    
    Returns:
        Configured logger instance
    """
    
    # Define log format based on environment
    if environment == "production":
        log_format = (
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(filename)s:%(lineno)d - %(funcName)s - %(message)s"
        )
    else:
        log_format = (
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    
    # Configure logging
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": log_format,
                "datefmt": "%Y-%m-%d %H:%M:%S"
            },
            "detailed": {
                "format": (
                    "%(asctime)s - %(name)s - %(levelname)s - "
                    "%(filename)s:%(lineno)d - %(funcName)s - "
                    "PID:%(process)d - TID:%(thread)d - %(message)s"
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level,
                "formatter": "standard",
                "stream": sys.stdout
            }
        },
        "loggers": {
            "pipeline": {
                "level": level,
                "handlers": ["console"],
                "propagate": False
            },
            "kafka": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False
            },
            "asyncpg": {
                "level": "WARNING", 
                "handlers": ["console"],
                "propagate": False
            },
            "clickhouse_connect": {
                "level": "WARNING",
                "handlers": ["console"], 
                "propagate": False
            }
        },
        "root": {
            "level": level,
            "handlers": ["console"]
        }
    }
    
    # Add file handler if log file is specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        logging_config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": level,
            "formatter": "detailed" if environment == "production" else "standard",
            "filename": str(log_file),
            "maxBytes": 10 * 1024 * 1024,  # 10MB
            "backupCount": 5,
            "encoding": "utf-8"
        }
        
        # Add file handler to all loggers
        for logger_name in logging_config["loggers"]:
            logging_config["loggers"][logger_name]["handlers"].append("file")
        
        logging_config["root"]["handlers"].append("file")
    
    # Apply logging configuration
    logging.config.dictConfig(logging_config)
    
    # Get logger for the specific component
    logger_name = f"pipeline.{component_name}" if component_name else "pipeline"
    logger = logging.getLogger(logger_name)
    
    # Log startup message
    logger.info(f"Logging initialized for {logger_name} at level {level}")
    if log_file:
        logger.info(f"Log file: {log_file}")
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific component.
    
    Args:
        name: Logger name (usually module name)
    
    Returns:
        Logger instance
    """
    return logging.getLogger(f"pipeline.{name}")


class LoggerMixin:
    """
    Mixin class to add logging capability to any class.
    
    Usage:
        class MyClass(LoggerMixin):
            def some_method(self):
                self.logger.info("This is a log message")
    """
    
    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class."""
        class_name = self.__class__.__name__
        module_name = self.__class__.__module__.split('.')[-1]
        return get_logger(f"{module_name}.{class_name}")


# Convenience function for quick logger setup
def quick_setup(component_name: str, level: str = "INFO") -> logging.Logger:
    """
    Quick logging setup for development and testing.
    
    Args:
        component_name: Name of the component
        level: Logging level
    
    Returns:
        Configured logger
    """
    return setup_logging(level=level, component_name=component_name) 
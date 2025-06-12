"""
Database connection management for the crypto pipeline.

Provides centralized database connection handling for both PostgreSQL (OLTP)
and ClickHouse (OLAP) with connection pooling and error handling.
"""

import asyncio
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

import asyncpg
import clickhouse_connect
from clickhouse_connect.driver import Client

from .config import Config, PostgreSQLConfig, ClickHouseConfig
from .logging import LoggerMixin


class PostgreSQLManager(LoggerMixin):
    """PostgreSQL connection manager with connection pooling."""
    
    def __init__(self, config: PostgreSQLConfig):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
        self._connection_string = (
            f"postgresql://{config.user}:{config.password}"
            f"@{config.host}:{config.port}/{config.database}"
        )
    
    async def initialize(self) -> None:
        """Initialize the connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                min_size=self.config.min_pool_size,
                max_size=self.config.max_pool_size,
                command_timeout=self.config.command_timeout
            )
            
            # Test connection
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT version()")
                self.logger.info(f"Connected to PostgreSQL: {result}")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            raise
    
    async def close(self) -> None:
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            self.logger.info("PostgreSQL connection pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool."""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self.pool.acquire() as conn:
            try:
                yield conn
            except Exception as e:
                self.logger.error(f"Database operation failed: {e}")
                raise
    
    async def execute_query(self, query: str, *args) -> Any:
        """Execute a query and return the result."""
        async with self.get_connection() as conn:
            return await conn.fetch(query, *args)
    
    async def execute_command(self, command: str, *args) -> str:
        """Execute a command and return the status."""
        async with self.get_connection() as conn:
            return await conn.execute(command, *args)
    
    async def health_check(self) -> bool:
        """Check if the database connection is healthy."""
        try:
            async with self.get_connection() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            self.logger.error(f"PostgreSQL health check failed: {e}")
            return False


class ClickHouseManager(LoggerMixin):
    """ClickHouse connection manager."""
    
    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.client: Optional[Client] = None
    
    def initialize(self) -> None:
        """Initialize the ClickHouse client."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                username=self.config.user,
                password=self.config.password
            )
            
            # Test connection
            result = self.client.query("SELECT version()")
            self.logger.info(f"Connected to ClickHouse: {result.result_rows[0][0]}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def close(self) -> None:
        """Close the ClickHouse client."""
        if self.client:
            self.client.close()
            self.logger.info("ClickHouse connection closed")
    
    def execute_query(self, query: str, parameters: Optional[list] = None) -> Any:
        """Execute a query and return the result."""
        if not self.client:
            raise RuntimeError("ClickHouse client not initialized")
        
        try:
            return self.client.query(query, parameters or [])
        except Exception as e:
            self.logger.error(f"ClickHouse query failed: {e}")
            raise
    
    def insert_data(self, table: str, data: list, column_names: Optional[list] = None) -> None:
        """Insert data into a ClickHouse table."""
        if not self.client:
            raise RuntimeError("ClickHouse client not initialized")
        
        try:
            self.client.insert(table, data, column_names=column_names)
            self.logger.debug(f"Inserted {len(data)} rows into {table}")
        except Exception as e:
            self.logger.error(f"ClickHouse insert failed: {e}")
            raise
    
    def health_check(self) -> bool:
        """Check if the ClickHouse connection is healthy."""
        try:
            if not self.client:
                return False
            self.client.query("SELECT 1")
            return True
        except Exception as e:
            self.logger.error(f"ClickHouse health check failed: {e}")
            return False


class DatabaseManager(LoggerMixin):
    """Main database manager that handles both PostgreSQL and ClickHouse connections."""
    
    def __init__(self, config: Config):
        self.config = config
        self.postgresql = PostgreSQLManager(config.postgresql)
        self.clickhouse = ClickHouseManager(config.clickhouse)
        self._initialized = False
    
    async def initialize(self) -> None:
        """Initialize all database connections."""
        if self._initialized:
            self.logger.warning("Database manager already initialized")
            return
        
        self.logger.info("Initializing database connections...")
        
        # Initialize PostgreSQL
        await self.postgresql.initialize()
        
        # Initialize ClickHouse
        self.clickhouse.initialize()
        
        self._initialized = True
        self.logger.info("All database connections initialized successfully")
    
    async def close(self) -> None:
        """Close all database connections."""
        if not self._initialized:
            return
        
        self.logger.info("Closing database connections...")
        
        # Close PostgreSQL
        await self.postgresql.close()
        
        # Close ClickHouse
        self.clickhouse.close()
        
        self._initialized = False
        self.logger.info("All database connections closed")
    
    async def health_check(self) -> Dict[str, bool]:
        """Check health of all database connections."""
        return {
            "postgresql": await self.postgresql.health_check(),
            "clickhouse": self.clickhouse.health_check()
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


# Singleton instance for global access
_database_manager: Optional[DatabaseManager] = None


def get_database_manager(config: Optional[Config] = None) -> DatabaseManager:
    """
    Get the global database manager instance.
    
    Args:
        config: Configuration object (required for first call)
    
    Returns:
        DatabaseManager instance
    """
    global _database_manager
    
    if _database_manager is None:
        if config is None:
            raise ValueError("Config required for first database manager initialization")
        _database_manager = DatabaseManager(config)
    
    return _database_manager


async def initialize_databases(config: Config) -> DatabaseManager:
    """
    Initialize and return the database manager.
    
    Args:
        config: Configuration object
    
    Returns:
        Initialized DatabaseManager instance
    """
    db_manager = get_database_manager(config)
    await db_manager.initialize()
    return db_manager 
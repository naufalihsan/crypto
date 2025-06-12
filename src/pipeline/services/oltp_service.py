#!/usr/bin/env python3
"""
OLTP Database Service
Provides database operations and queries for the crypto OLTP database
"""

import asyncio
import asyncpg
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import argparse


class OLTPService:
    def __init__(
        self,
        host="localhost",
        port=5433,
        database="crypto_oltp",
        user="admin",
        password="admin"
    ):
        self.connection_config = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }

    async def connect(self):
        """Create database connection"""
        return await asyncpg.connect(**self.connection_config)

    async def get_latest_prices(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest prices for all symbols"""
        conn = await self.connect()
        try:
            query = """
                SELECT * FROM latest_crypto_prices 
                ORDER BY event_time DESC 
                LIMIT $1
            """
            rows = await conn.fetch(query, limit)
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def get_price_history(
        self, 
        symbol: str, 
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get price history for a specific symbol"""
        conn = await self.connect()
        try:
            query = """
                SELECT symbol, price, price_change, volume, event_time
                FROM crypto_prices 
                WHERE symbol = $1 
                    AND event_time >= $2
                ORDER BY event_time DESC
            """
            cutoff_time = datetime.now() - timedelta(hours=hours)
            rows = await conn.fetch(query, symbol, cutoff_time)
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def get_recent_anomalies(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent anomalies"""
        conn = await self.connect()
        try:
            query = """
                SELECT * FROM recent_anomalies
                WHERE event_time >= $1
                ORDER BY event_time DESC
            """
            cutoff_time = datetime.now() - timedelta(hours=hours)
            rows = await conn.fetch(query, cutoff_time)
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def get_latest_indicators(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get latest technical indicators"""
        conn = await self.connect()
        try:
            if symbol:
                query = """
                    SELECT * FROM latest_indicators 
                    WHERE symbol = $1
                """
                rows = await conn.fetch(query, symbol)
            else:
                query = "SELECT * FROM latest_indicators ORDER BY symbol"
                rows = await conn.fetch(query)
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def get_market_overview(self) -> List[Dict[str, Any]]:
        """Get market overview"""
        conn = await self.connect()
        try:
            query = "SELECT * FROM market_overview"
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def get_price_statistics(
        self, 
        symbol: str, 
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get price statistics for a symbol"""
        conn = await self.connect()
        try:
            query = "SELECT * FROM get_price_stats($1, $2)"
            row = await conn.fetchrow(query, symbol, hours)
            return dict(row) if row else {}
        finally:
            await conn.close()

    async def get_top_volatile_symbols(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most volatile symbols in the last 24 hours"""
        conn = await self.connect()
        try:
            query = """
                SELECT 
                    symbol,
                    COUNT(*) as price_updates,
                    AVG(ABS(price_change)) as avg_volatility,
                    MAX(ABS(price_change)) as max_volatility,
                    MIN(price) as min_price,
                    MAX(price) as max_price
                FROM crypto_prices 
                WHERE event_time >= $1
                GROUP BY symbol
                ORDER BY avg_volatility DESC
                LIMIT $2
            """
            cutoff_time = datetime.now() - timedelta(hours=24)
            rows = await conn.fetch(query, cutoff_time, limit)
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def get_volume_leaders(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get symbols with highest volume in the last 24 hours"""
        conn = await self.connect()
        try:
            query = """
                SELECT 
                    symbol,
                    SUM(volume) as total_volume,
                    AVG(volume) as avg_volume,
                    COUNT(*) as updates_count
                FROM crypto_prices 
                WHERE event_time >= $1 AND volume > 0
                GROUP BY symbol
                ORDER BY total_volume DESC
                LIMIT $2
            """
            cutoff_time = datetime.now() - timedelta(hours=24)
            rows = await conn.fetch(query, cutoff_time, limit)
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def get_data_quality_summary(self) -> Dict[str, Any]:
        """Get data quality summary"""
        conn = await self.connect()
        try:
            queries = {
                'total_price_records': "SELECT COUNT(*) FROM crypto_prices",
                'total_indicators': "SELECT COUNT(*) FROM crypto_indicators",
                'total_anomalies': "SELECT COUNT(*) FROM crypto_anomalies",
                'unique_symbols': "SELECT COUNT(DISTINCT symbol) FROM crypto_prices",
                'latest_price_update': "SELECT MAX(event_time) FROM crypto_prices",
                'latest_indicator_update': "SELECT MAX(window_end_time) FROM crypto_indicators",
                'unacknowledged_anomalies': "SELECT COUNT(*) FROM crypto_anomalies WHERE acknowledged = FALSE"
            }
            
            results = {}
            for key, query in queries.items():
                result = await conn.fetchval(query)
                results[key] = result
            
            return results
        finally:
            await conn.close()

    async def acknowledge_anomaly(self, anomaly_id: int, acknowledged_by: str) -> bool:
        """Acknowledge an anomaly"""
        conn = await self.connect()
        try:
            query = """
                UPDATE crypto_anomalies 
                SET acknowledged = TRUE, 
                    acknowledged_by = $2, 
                    acknowledged_at = NOW()
                WHERE id = $1
                RETURNING id
            """
            result = await conn.fetchval(query, anomaly_id, acknowledged_by)
            return result is not None
        finally:
            await conn.close()

    async def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """Clean up old data beyond retention period"""
        conn = await self.connect()
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            query = "DELETE FROM crypto_prices WHERE event_time < $1"
            result = await conn.execute(query, cutoff_date)
            # Extract number of deleted rows from result string
            deleted_count = int(result.split()[-1]) if result.startswith('DELETE') else 0
            return deleted_count
        finally:
            await conn.close()


def format_output(data: Any, format_type: str = "json") -> str:
    """Format output data"""
    if format_type == "json":
        return json.dumps(data, indent=2, default=str)
    elif format_type == "table":
        if isinstance(data, list) and data:
            # Simple table format
            headers = list(data[0].keys())
            result = " | ".join(headers) + "\n"
            result += "-" * len(result) + "\n"
            for row in data:
                result += " | ".join(str(row.get(h, "")) for h in headers) + "\n"
            return result
        else:
            return str(data)
    else:
        return str(data)


async def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Crypto OLTP Database Queries")
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5433, help="Database port")
    parser.add_argument("--database", default="crypto_oltp", help="Database name")
    parser.add_argument("--user", default="admin", help="Database user")
    parser.add_argument("--password", default="admin", help="Database password")
    parser.add_argument("--format", choices=["json", "table"], default="json", help="Output format")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Latest prices command
    latest_parser = subparsers.add_parser("latest", help="Get latest prices")
    latest_parser.add_argument("--limit", type=int, default=10, help="Number of records")
    
    # Price history command
    history_parser = subparsers.add_parser("history", help="Get price history")
    history_parser.add_argument("symbol", help="Crypto symbol")
    history_parser.add_argument("--hours", type=int, default=24, help="Hours of history")
    
    # Anomalies command
    anomalies_parser = subparsers.add_parser("anomalies", help="Get recent anomalies")
    anomalies_parser.add_argument("--hours", type=int, default=24, help="Hours to look back")
    
    # Market overview command
    subparsers.add_parser("overview", help="Get market overview")
    
    # Data quality command
    subparsers.add_parser("quality", help="Get data quality summary")
    
    # Volatile symbols command
    volatile_parser = subparsers.add_parser("volatile", help="Get most volatile symbols")
    volatile_parser.add_argument("--limit", type=int, default=10, help="Number of symbols")
    
    # Volume leaders command
    volume_parser = subparsers.add_parser("volume", help="Get volume leaders")
    volume_parser.add_argument("--limit", type=int, default=10, help="Number of symbols")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Create service instance
    service = OLTPService(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password
    )
    
    try:
        # Execute command
        if args.command == "latest":
            data = await service.get_latest_prices(args.limit)
        elif args.command == "history":
            data = await service.get_price_history(args.symbol, args.hours)
        elif args.command == "anomalies":
            data = await service.get_recent_anomalies(args.hours)
        elif args.command == "overview":
            data = await service.get_market_overview()
        elif args.command == "quality":
            data = await service.get_data_quality_summary()
        elif args.command == "volatile":
            data = await service.get_top_volatile_symbols(args.limit)
        elif args.command == "volume":
            data = await service.get_volume_leaders(args.limit)
        else:
            print(f"Unknown command: {args.command}")
            return
        
        # Output results
        print(format_output(data, args.format))
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 
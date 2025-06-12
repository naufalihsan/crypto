#!/usr/bin/env python3
"""
OLAP Database Service
Provides analytical queries and operations for the crypto OLAP database (ClickHouse)
"""

import asyncio
import json
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import clickhouse_connect
from clickhouse_connect.driver import Client


class OLAPService:
    def __init__(
        self,
        host="localhost",
        port=8123,
        database="crypto_olap",
        user="admin",
        password="admin"
    ):
        self.connection_config = {
            'host': host,
            'port': port,
            'database': database,
            'username': user,
            'password': password
        }
        self.client: Optional[Client] = None

    def connect(self) -> Client:
        """Create ClickHouse client connection"""
        if not self.client:
            self.client = clickhouse_connect.get_client(**self.connection_config)
        return self.client

    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()
            self.client = None

    def execute_query(self, query: str, parameters: Optional[list] = None) -> Any:
        """Execute a query and return results"""
        client = self.connect()
        try:
            result = client.query(query, parameters or [])
            return result.result_rows
        except Exception as e:
            print(f"Query execution error: {e}")
            raise

    def get_price_aggregates(
        self, 
        symbol: str, 
        interval: str = "1h", 
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get price aggregates for a symbol over time intervals"""
        query = f"""
            SELECT 
                toStartOfInterval(event_time, INTERVAL {interval}) as time_bucket,
                symbol,
                avg(price) as avg_price,
                min(price) as min_price,
                max(price) as max_price,
                sum(volume) as total_volume,
                count() as data_points
            FROM crypto_prices_mv
            WHERE symbol = %s 
                AND event_time >= now() - INTERVAL {hours} HOUR
            GROUP BY time_bucket, symbol
            ORDER BY time_bucket DESC
        """
        
        rows = self.execute_query(query, [symbol])
        return [
            {
                'time_bucket': row[0],
                'symbol': row[1],
                'avg_price': float(row[2]),
                'min_price': float(row[3]),
                'max_price': float(row[4]),
                'total_volume': float(row[5]),
                'data_points': int(row[6])
            }
            for row in rows
        ]

    def get_market_summary(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get market summary for all symbols"""
        query = f"""
            SELECT 
                symbol,
                count() as total_updates,
                avg(price) as avg_price,
                min(price) as min_price,
                max(price) as max_price,
                sum(volume) as total_volume,
                stddevPop(price) as price_volatility,
                (max(price) - min(price)) / min(price) * 100 as price_range_pct
            FROM crypto_prices_mv
            WHERE event_time >= now() - INTERVAL {hours} HOUR
            GROUP BY symbol
            ORDER BY total_volume DESC
        """
        
        rows = self.execute_query(query)
        return [
            {
                'symbol': row[0],
                'total_updates': int(row[1]),
                'avg_price': float(row[2]),
                'min_price': float(row[3]),
                'max_price': float(row[4]),
                'total_volume': float(row[5]),
                'price_volatility': float(row[6]),
                'price_range_pct': float(row[7])
            }
            for row in rows
        ]

    def get_volume_analysis(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get volume analysis across symbols"""
        query = f"""
            SELECT 
                symbol,
                sum(volume) as total_volume,
                avg(volume) as avg_volume,
                max(volume) as max_volume,
                count() as volume_updates,
                sum(volume * price) as volume_weighted_value
            FROM crypto_prices_mv
            WHERE event_time >= now() - INTERVAL {hours} HOUR
                AND volume > 0
            GROUP BY symbol
            ORDER BY total_volume DESC
        """
        
        rows = self.execute_query(query)
        return [
            {
                'symbol': row[0],
                'total_volume': float(row[1]),
                'avg_volume': float(row[2]),
                'max_volume': float(row[3]),
                'volume_updates': int(row[4]),
                'volume_weighted_value': float(row[5])
            }
            for row in rows
        ]

    def get_price_correlation(
        self, 
        symbol1: str, 
        symbol2: str, 
        hours: int = 24
    ) -> Dict[str, Any]:
        """Calculate price correlation between two symbols"""
        query = f"""
            WITH price_data AS (
                SELECT 
                    toStartOfInterval(event_time, INTERVAL 5 MINUTE) as time_bucket,
                    symbol,
                    avg(price) as avg_price
                FROM crypto_prices_mv
                WHERE symbol IN (%s, %s)
                    AND event_time >= now() - INTERVAL {hours} HOUR
                GROUP BY time_bucket, symbol
            ),
            pivot_data AS (
                SELECT 
                    time_bucket,
                    sumIf(avg_price, symbol = %s) as price1,
                    sumIf(avg_price, symbol = %s) as price2
                FROM price_data
                GROUP BY time_bucket
                HAVING price1 > 0 AND price2 > 0
            )
            SELECT 
                corr(price1, price2) as correlation,
                count() as data_points
            FROM pivot_data
        """
        
        rows = self.execute_query(query, [symbol1, symbol2, symbol1, symbol2])
        if rows:
            return {
                'symbol1': symbol1,
                'symbol2': symbol2,
                'correlation': float(rows[0][0]) if rows[0][0] is not None else 0.0,
                'data_points': int(rows[0][1])
            }
        return {'symbol1': symbol1, 'symbol2': symbol2, 'correlation': 0.0, 'data_points': 0}

    def get_volatility_ranking(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get symbols ranked by volatility"""
        query = f"""
            SELECT 
                symbol,
                stddevPop(price) as price_volatility,
                (max(price) - min(price)) / avg(price) * 100 as price_range_pct,
                count() as data_points,
                avg(price) as avg_price
            FROM crypto_prices_mv
            WHERE event_time >= now() - INTERVAL {hours} HOUR
            GROUP BY symbol
            HAVING data_points >= 10
            ORDER BY price_volatility DESC
        """
        
        rows = self.execute_query(query)
        return [
            {
                'symbol': row[0],
                'price_volatility': float(row[1]),
                'price_range_pct': float(row[2]),
                'data_points': int(row[3]),
                'avg_price': float(row[4])
            }
            for row in rows
        ]

    def get_time_series_data(
        self, 
        symbol: str, 
        interval: str = "5m", 
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get time series data for charting"""
        interval_map = {
            '1m': '1 MINUTE',
            '5m': '5 MINUTE', 
            '15m': '15 MINUTE',
            '1h': '1 HOUR',
            '4h': '4 HOUR',
            '1d': '1 DAY'
        }
        
        clickhouse_interval = interval_map.get(interval, '5 MINUTE')
        
        query = f"""
            SELECT 
                toStartOfInterval(event_time, INTERVAL {clickhouse_interval}) as timestamp,
                first_value(price) as open_price,
                max(price) as high_price,
                min(price) as low_price,
                last_value(price) as close_price,
                sum(volume) as volume
            FROM crypto_prices_mv
            WHERE symbol = %s 
                AND event_time >= now() - INTERVAL {hours} HOUR
            GROUP BY timestamp
            ORDER BY timestamp ASC
        """
        
        rows = self.execute_query(query, [symbol])
        return [
            {
                'timestamp': row[0],
                'open': float(row[1]),
                'high': float(row[2]),
                'low': float(row[3]),
                'close': float(row[4]),
                'volume': float(row[5])
            }
            for row in rows
        ]

    def get_anomaly_detection(
        self, 
        symbol: str, 
        hours: int = 24,
        threshold: float = 2.0
    ) -> List[Dict[str, Any]]:
        """Detect price anomalies using statistical methods"""
        query = f"""
            WITH stats AS (
                SELECT 
                    avg(price) as mean_price,
                    stddevPop(price) as std_price
                FROM crypto_prices_mv
                WHERE symbol = %s 
                    AND event_time >= now() - INTERVAL {hours} HOUR
            ),
            anomalies AS (
                SELECT 
                    event_time,
                    price,
                    abs(price - stats.mean_price) / stats.std_price as z_score
                FROM crypto_prices_mv, stats
                WHERE symbol = %s 
                    AND event_time >= now() - INTERVAL {hours} HOUR
                    AND abs(price - stats.mean_price) / stats.std_price > %s
            )
            SELECT 
                event_time,
                price,
                z_score
            FROM anomalies
            ORDER BY z_score DESC
            LIMIT 100
        """
        
        rows = self.execute_query(query, [symbol, symbol, threshold])
        return [
            {
                'timestamp': row[0],
                'price': float(row[1]),
                'z_score': float(row[2])
            }
            for row in rows
        ]

    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        queries = {
            'total_records': "SELECT count() FROM crypto_prices_mv",
            'unique_symbols': "SELECT uniq(symbol) FROM crypto_prices_mv",
            'date_range': """
                SELECT 
                    min(event_time) as earliest,
                    max(event_time) as latest
                FROM crypto_prices_mv
            """,
            'table_size': """
                SELECT 
                    formatReadableSize(sum(bytes_on_disk)) as size_on_disk,
                    sum(rows) as total_rows
                FROM system.parts 
                WHERE table = 'crypto_prices_mv' AND active = 1
            """
        }
        
        results = {}
        for key, query in queries.items():
            try:
                rows = self.execute_query(query)
                if key == 'date_range':
                    results['earliest_record'] = rows[0][0] if rows else None
                    results['latest_record'] = rows[0][1] if rows else None
                elif key == 'table_size':
                    results['size_on_disk'] = rows[0][0] if rows else "0 B"
                    results['total_rows'] = int(rows[0][1]) if rows else 0
                else:
                    results[key] = rows[0][0] if rows else 0
            except Exception as e:
                results[key] = f"Error: {e}"
        
        return results

    def optimize_table(self, table_name: str = "crypto_prices_mv") -> str:
        """Optimize table performance"""
        query = f"OPTIMIZE TABLE {table_name} FINAL"
        try:
            self.execute_query(query)
            return f"Table {table_name} optimized successfully"
        except Exception as e:
            return f"Optimization failed: {e}"


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
    parser = argparse.ArgumentParser(description="Crypto OLAP Database Analytics")
    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse port")
    parser.add_argument("--database", default="crypto_olap", help="Database name")
    parser.add_argument("--user", default="admin", help="Database user")
    parser.add_argument("--password", default="admin", help="Database password")
    parser.add_argument("--format", choices=["json", "table"], default="json", help="Output format")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Price aggregates command
    agg_parser = subparsers.add_parser("aggregates", help="Get price aggregates")
    agg_parser.add_argument("symbol", help="Crypto symbol")
    agg_parser.add_argument("--interval", default="1h", choices=["1m", "5m", "15m", "1h", "4h", "1d"], help="Time interval")
    agg_parser.add_argument("--hours", type=int, default=24, help="Hours of data")
    
    # Market summary command
    market_parser = subparsers.add_parser("market", help="Get market summary")
    market_parser.add_argument("--hours", type=int, default=24, help="Hours of data")
    
    # Volume analysis command
    volume_parser = subparsers.add_parser("volume", help="Get volume analysis")
    volume_parser.add_argument("--hours", type=int, default=24, help="Hours of data")
    
    # Correlation command
    corr_parser = subparsers.add_parser("correlation", help="Get price correlation")
    corr_parser.add_argument("symbol1", help="First symbol")
    corr_parser.add_argument("symbol2", help="Second symbol")
    corr_parser.add_argument("--hours", type=int, default=24, help="Hours of data")
    
    # Volatility ranking command
    vol_parser = subparsers.add_parser("volatility", help="Get volatility ranking")
    vol_parser.add_argument("--hours", type=int, default=24, help="Hours of data")
    
    # Time series command
    ts_parser = subparsers.add_parser("timeseries", help="Get time series data")
    ts_parser.add_argument("symbol", help="Crypto symbol")
    ts_parser.add_argument("--interval", default="5m", choices=["1m", "5m", "15m", "1h", "4h", "1d"], help="Time interval")
    ts_parser.add_argument("--hours", type=int, default=24, help="Hours of data")
    
    # Anomaly detection command
    anomaly_parser = subparsers.add_parser("anomalies", help="Detect price anomalies")
    anomaly_parser.add_argument("symbol", help="Crypto symbol")
    anomaly_parser.add_argument("--hours", type=int, default=24, help="Hours of data")
    anomaly_parser.add_argument("--threshold", type=float, default=2.0, help="Z-score threshold")
    
    # Database stats command
    subparsers.add_parser("stats", help="Get database statistics")
    
    # Optimize command
    opt_parser = subparsers.add_parser("optimize", help="Optimize table")
    opt_parser.add_argument("--table", default="crypto_prices_mv", help="Table name")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Create service instance
    service = OLAPService(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password
    )
    
    try:
        # Execute command
        if args.command == "aggregates":
            data = service.get_price_aggregates(args.symbol, args.interval, args.hours)
        elif args.command == "market":
            data = service.get_market_summary(args.hours)
        elif args.command == "volume":
            data = service.get_volume_analysis(args.hours)
        elif args.command == "correlation":
            data = service.get_price_correlation(args.symbol1, args.symbol2, args.hours)
        elif args.command == "volatility":
            data = service.get_volatility_ranking(args.hours)
        elif args.command == "timeseries":
            data = service.get_time_series_data(args.symbol, args.interval, args.hours)
        elif args.command == "anomalies":
            data = service.get_anomaly_detection(args.symbol, args.hours, args.threshold)
        elif args.command == "stats":
            data = service.get_database_stats()
        elif args.command == "optimize":
            result = service.optimize_table(args.table)
            print(result)
            return
        else:
            print(f"Unknown command: {args.command}")
            return
        
        # Output results
        print(format_output(data, args.format))
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        service.close()


if __name__ == "__main__":
    asyncio.run(main()) 
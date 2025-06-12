#!/usr/bin/env python3
"""
Test script to verify Binance WebSocket connectivity
This script tests different WebSocket endpoints to find working ones
"""

import asyncio
import json
import logging
import websockets
import time
from typing import List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebSocketTester:
    def __init__(self):
        self.endpoints = [
            "wss://stream.binance.com:443/ws/",
            "wss://stream.binance.com:9443/ws/",
            "wss://data-stream.binance.vision/ws/",
            "wss://stream.binance.us:9443/ws/",
        ]
        
    async def test_endpoint(self, endpoint: str, symbol: str = "btcusdt", timeout: int = 10) -> dict:
        """Test a single WebSocket endpoint"""
        ws_url = f"{endpoint}{symbol}@ticker"
        result = {
            "endpoint": endpoint,
            "url": ws_url,
            "status": "failed",
            "error": None,
            "response_time": None,
            "sample_data": None
        }
        
        start_time = time.time()
        
        try:
            logger.info(f"Testing: {ws_url}")
            
            async with websockets.connect(
                ws_url,
                open_timeout=timeout,
                ping_interval=None,
                ping_timeout=None,
                extra_headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            ) as websocket:
                # Wait for first message
                message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                data = json.loads(message)
                
                result["status"] = "success"
                result["response_time"] = round(time.time() - start_time, 2)
                result["sample_data"] = {
                    "symbol": data.get("s"),
                    "price": data.get("c"),
                    "event_type": data.get("e")
                }
                
                logger.info(f"✅ SUCCESS: {endpoint} - Response time: {result['response_time']}s")
                
        except asyncio.TimeoutError:
            result["error"] = "Connection timeout"
            logger.error(f"❌ TIMEOUT: {endpoint}")
        except websockets.exceptions.InvalidStatusCode as e:
            result["error"] = f"Invalid status code: {e.status_code}"
            logger.error(f"❌ INVALID STATUS: {endpoint} - {e}")
        except websockets.exceptions.ConnectionClosed as e:
            result["error"] = f"Connection closed: {e.code}"
            logger.error(f"❌ CONNECTION CLOSED: {endpoint} - {e}")
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"❌ ERROR: {endpoint} - {e}")
            
        return result
    
    async def test_all_endpoints(self) -> List[dict]:
        """Test all WebSocket endpoints concurrently"""
        logger.info("Starting WebSocket endpoint tests...")
        
        tasks = [self.test_endpoint(endpoint) for endpoint in self.endpoints]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions from gather
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "endpoint": self.endpoints[i],
                    "status": "failed",
                    "error": str(result),
                    "response_time": None,
                    "sample_data": None
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    def print_results(self, results: List[dict]):
        """Print test results in a formatted way"""
        print("\n" + "="*80)
        print("BINANCE WEBSOCKET CONNECTIVITY TEST RESULTS")
        print("="*80)
        
        working_endpoints = []
        failed_endpoints = []
        
        for result in results:
            if result["status"] == "success":
                working_endpoints.append(result)
                print(f"✅ WORKING: {result['endpoint']}")
                print(f"   Response Time: {result['response_time']}s")
                print(f"   Sample Data: {result['sample_data']}")
            else:
                failed_endpoints.append(result)
                print(f"❌ FAILED: {result['endpoint']}")
                print(f"   Error: {result['error']}")
            print()
        
        print("="*80)
        print(f"SUMMARY: {len(working_endpoints)} working, {len(failed_endpoints)} failed")
        
        if working_endpoints:
            print("\nRECOMMENDED ENDPOINTS (fastest first):")
            sorted_endpoints = sorted(working_endpoints, key=lambda x: x['response_time'])
            for i, endpoint in enumerate(sorted_endpoints, 1):
                print(f"{i}. {endpoint['endpoint']} ({endpoint['response_time']}s)")
        else:
            print("\n⚠️  NO WORKING ENDPOINTS FOUND!")
            print("This indicates that Binance WebSocket services may be blocked in your region.")
            print("Consider using:")
            print("- VPN service")
            print("- Alternative DNS servers (8.8.8.8, 1.1.1.1)")
            print("- REST API only mode")
        
        print("="*80)

async def main():
    """Main test function"""
    tester = WebSocketTester()
    results = await tester.test_all_endpoints()
    tester.print_results(results)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test failed with error: {e}") 
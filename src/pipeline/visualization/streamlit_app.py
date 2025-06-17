import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
from datetime import datetime, timedelta
import time
import asyncio
import aiohttp
from typing import Dict, List, Optional, Any
import numpy as np

# Configure Streamlit page
st.set_page_config(
    page_title="Crypto Pipeline Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration
OLTP_SERVICE_URL = "http://oltp-service:8001"
OLAP_SERVICE_URL = "http://olap-service:8002"

# Custom CSS for better styling
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        border: 1px solid #e0e0e0;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .crypto-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 2rem;
        text-align: center;
    }
    .alert-box {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .alert-high {
        background-color: #ffebee;
        border-left: 4px solid #f44336;
    }
    .alert-medium {
        background-color: #fff3e0;
        border-left: 4px solid #ff9800;
    }
    .alert-low {
        background-color: #f3e5f5;
        border-left: 4px solid #9c27b0;
    }
</style>
""", unsafe_allow_html=True)

class DataService:
    """Service class to handle API calls to OLTP and OLAP services"""
    
    def __init__(self):
        self.oltp_url = OLTP_SERVICE_URL
        self.olap_url = OLAP_SERVICE_URL
    
    def check_service_health(self) -> Dict[str, bool]:
        """Check health of both OLTP and OLAP services"""
        health_status = {
            'oltp_healthy': False,
            'olap_healthy': False
        }
        
        # Check OLTP service
        try:
            response = requests.get(f"{self.oltp_url}/health", timeout=5)
            health_status['oltp_healthy'] = response.status_code == 200
            if not health_status['oltp_healthy']:
                st.error(f"OLTP service unhealthy: {response.status_code} - {response.text}")
        except Exception as e:
            st.error(f"Cannot connect to OLTP service: {e}")
        
        # Check OLAP service
        try:
            response = requests.get(f"{self.olap_url}/health", timeout=5)
            health_status['olap_healthy'] = response.status_code == 200
            if not health_status['olap_healthy']:
                st.error(f"OLAP service unhealthy: {response.status_code} - {response.text}")
        except Exception as e:
            st.error(f"Cannot connect to OLAP service: {e}")
        
        return health_status
    
    @st.cache_data(ttl=30)  # Cache for 30 seconds
    def get_latest_prices(_self, symbols: Optional[List[str]] = None) -> List[Dict]:
        """Get latest prices from OLTP service"""
        try:
            params = {}
            if symbols:
                params['symbols'] = ','.join(symbols)
            
            url = f"{_self.oltp_url}/api/prices/latest"
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                st.error(f"API returned status {response.status_code}: {response.text}")
                return []
            
            response.raise_for_status()
            result = response.json()
            
            # Handle the API response structure
            if result.get('success') and 'data' in result:
                return result['data']
            else:
                st.error(f"API returned unsuccessful response: {result}")
                return []
        except requests.exceptions.RequestException as e:
            st.error(f"Network error connecting to OLTP service: {e}")
            return []
        except Exception as e:
            st.error(f"Failed to fetch latest prices: {e}")
            return []
    
    @st.cache_data(ttl=60)  # Cache for 1 minute
    def get_price_history(_self, symbol: str, hours: int = 24) -> List[Dict]:
        """Get price history from OLTP service"""
        try:
            params = {'hours': hours}
            response = requests.get(f"{_self.oltp_url}/api/prices/{symbol}/history", params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            # Handle the API response structure
            if result.get('success') and 'data' in result:
                return result['data']
            else:
                st.error(f"API returned unsuccessful response: {result}")
                return []
        except Exception as e:
            st.error(f"Failed to fetch price history for {symbol}: {e}")
            return []
    
    @st.cache_data(ttl=60)
    def get_price_analytics(_self, symbol: str, period_minutes: int = 60, hours: int = 24) -> List[Dict]:
        """Get price analytics from OLAP service"""
        try:
            params = {'period_minutes': period_minutes, 'hours': hours}
            response = requests.get(f"{_self.olap_url}/analytics/price/{symbol}", params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            # Handle the API response structure
            if result.get('success') and 'data' in result:
                return result['data']
            else:
                st.error(f"API returned unsuccessful response: {result}")
                return []
        except Exception as e:
            st.error(f"Failed to fetch price analytics for {symbol}: {e}")
            return []
    
    @st.cache_data(ttl=120)  # Cache for 2 minutes
    def get_market_trends(_self, symbol: Optional[str] = None, hours: int = 24) -> List[Dict]:
        """Get market trends from OLAP service"""
        try:
            params = {'hours': hours}
            
            if symbol:
                url = f"{_self.olap_url}/analytics/trends/{symbol}"
            else:
                url = f"{_self.olap_url}/analytics/trends"
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            # Handle the API response structure
            if result.get('success') and 'data' in result:
                return result['data']
            else:
                st.error(f"API returned unsuccessful response: {result}")
                return []
        except Exception as e:
            st.error(f"Failed to fetch market trends: {e}")
            return []
    
    @st.cache_data(ttl=60)
    def get_technical_indicators(_self, symbol: str) -> Optional[Dict]:
        """Get technical indicators from OLTP service"""
        try:
            response = requests.get(f"{_self.oltp_url}/api/indicators/{symbol}", timeout=10)
            response.raise_for_status()
            result = response.json()
            
            # Handle the API response structure
            if result.get('success') and 'data' in result:
                return result['data']
            else:
                st.error(f"API returned unsuccessful response: {result}")
                return None
        except Exception as e:
            st.error(f"Failed to fetch technical indicators for {symbol}: {e}")
            return None
    
    @st.cache_data(ttl=30)
    def get_market_anomalies(_self, hours: int = 24) -> List[Dict]:
        """Get market anomalies from OLTP service"""
        try:
            params = {'hours': hours}
            response = requests.get(f"{_self.oltp_url}/api/anomalies", params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            # Handle the API response structure
            if result.get('success') and 'data' in result:
                return result['data']
            else:
                st.error(f"API returned unsuccessful response: {result}")
                return []
        except Exception as e:
            st.error(f"Failed to fetch market anomalies: {e}")
            return []
    
    @st.cache_data(ttl=60)
    def get_volatility_analysis(_self, symbols: List[str], hours: int = 24) -> List[Dict]:
        """Get volatility analysis from OLAP service"""
        try:
            params = {'symbols': ','.join(symbols), 'hours': hours}
            response = requests.get(f"{_self.olap_url}/analytics/volatility", params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            # Handle the API response structure
            if result.get('success') and 'data' in result:
                return result['data']
            else:
                st.error(f"API returned unsuccessful response: {result}")
                return []
        except Exception as e:
            st.error(f"Failed to fetch volatility analysis: {e}")
            return []

def create_price_chart(price_data: List[Dict], symbol: str) -> go.Figure:
    """Create a candlestick chart for price data"""
    if not price_data:
        return go.Figure()
    
    df = pd.DataFrame(price_data)
    
    # Handle different timestamp field names from different APIs
    timestamp_field = None
    if 'timestamp' in df.columns:
        timestamp_field = 'timestamp'
    elif 'period_start' in df.columns:
        timestamp_field = 'period_start'
    else:
        # If no timestamp field found, create a simple line chart with current data
        st.warning(f"No timestamp field found in data for {symbol}. Available fields: {list(df.columns)}")
        return go.Figure()
    
    df[timestamp_field] = pd.to_datetime(df[timestamp_field])
    
    # Check if we have OHLC data or just price data
    has_ohlc = all(col in df.columns for col in ['open_price', 'close_price', 'high_price', 'low_price'])
    
    if has_ohlc:
        # Create candlestick chart
        fig = go.Figure(data=go.Candlestick(
            x=df[timestamp_field],
            open=df['open_price'],
            high=df['high_price'],
            low=df['low_price'],
            close=df['close_price'],
            name=symbol
        ))
    else:
        # Create line chart with available price data
        price_col = 'price' if 'price' in df.columns else 'avg_price' if 'avg_price' in df.columns else 'close_price'
        if price_col in df.columns:
            fig = go.Figure(data=go.Scatter(
                x=df[timestamp_field],
                y=df[price_col],
                mode='lines',
                name=symbol,
                line=dict(color='blue', width=2)
            ))
        else:
            st.warning(f"No price data found for {symbol}. Available columns: {list(df.columns)}")
            return go.Figure()
    
    fig.update_layout(
        title=f"{symbol} Price Chart",
        xaxis_title="Time",
        yaxis_title="Price (USDT)",
        template="plotly_white",
        height=500
    )
    
    return fig

def create_volume_chart(price_data: List[Dict], symbol: str) -> go.Figure:
    """Create a volume chart"""
    if not price_data:
        return go.Figure()
    
    df = pd.DataFrame(price_data)
    
    # Handle different timestamp field names from different APIs
    timestamp_field = None
    if 'timestamp' in df.columns:
        timestamp_field = 'timestamp'
    elif 'period_start' in df.columns:
        timestamp_field = 'period_start'
    else:
        # If no timestamp field found, return empty chart
        return go.Figure()
    
    df[timestamp_field] = pd.to_datetime(df[timestamp_field])
    
    # Handle different volume field names
    volume_col = None
    if 'volume' in df.columns:
        volume_col = 'volume'
    elif 'total_volume' in df.columns:
        volume_col = 'total_volume'
    else:
        # No volume data available
        return go.Figure()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df[timestamp_field],
        y=df[volume_col],
        name='Volume',
        marker_color='rgba(158,202,225,0.8)'
    ))
    
    fig.update_layout(
        title=f"{symbol} Volume Chart",
        xaxis_title="Time",
        yaxis_title="Volume",
        template="plotly_white",
        height=300
    )
    
    return fig

def create_technical_indicators_chart(indicators: Dict, symbol: str) -> go.Figure:
    """Create technical indicators visualization"""
    if not indicators:
        return go.Figure()
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Moving Averages', 'RSI', 'MACD', 'Bollinger Bands'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Moving Averages (placeholder - would need historical data)
    if indicators.get('sma') and indicators.get('ema'):
        fig.add_trace(
            go.Scatter(x=[datetime.now()], y=[float(indicators['sma'])], name='SMA', mode='markers'),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(x=[datetime.now()], y=[float(indicators['ema'])], name='EMA', mode='markers'),
            row=1, col=1
        )
    
    # RSI
    if indicators.get('rsi'):
        rsi_val = float(indicators['rsi'])
        fig.add_trace(
            go.Scatter(x=[datetime.now()], y=[rsi_val], name='RSI', mode='markers+text', 
                      text=[f"RSI: {rsi_val:.2f}"], textposition="top center"),
            row=1, col=2
        )
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=1, col=2)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=1, col=2)
    
    # MACD
    if indicators.get('macd'):
        fig.add_trace(
            go.Scatter(x=[datetime.now()], y=[float(indicators['macd'])], name='MACD', mode='markers'),
            row=2, col=1
        )
    
    # Bollinger Bands
    if indicators.get('bollinger_upper') and indicators.get('bollinger_lower'):
        fig.add_trace(
            go.Scatter(x=[datetime.now()], y=[float(indicators['bollinger_upper'])], 
                      name='Upper Band', mode='markers'),
            row=2, col=2
        )
        fig.add_trace(
            go.Scatter(x=[datetime.now()], y=[float(indicators['bollinger_lower'])], 
                      name='Lower Band', mode='markers'),
            row=2, col=2
        )
    
    fig.update_layout(
        title=f"{symbol} Technical Indicators",
        template="plotly_white",
        height=600,
        showlegend=True
    )
    
    return fig

def create_volatility_chart(volatility_data: List[Dict]) -> go.Figure:
    """Create volatility comparison chart"""
    if not volatility_data:
        return go.Figure()
    
    try:
        # Ensure all data has the same structure
        processed_data = []
        for item in volatility_data:
            if isinstance(item, dict) and 'symbol' in item:
                # Handle different volatility field names from the API
                volatility_value = None
                if 'volatility' in item:
                    volatility_value = item['volatility']
                elif 'avg_volatility' in item:
                    volatility_value = item['avg_volatility']
                elif 'max_volatility' in item:
                    volatility_value = item['max_volatility']
                
                if volatility_value is not None:
                    processed_data.append({
                        'symbol': item['symbol'],
                        'volatility': float(volatility_value) if volatility_value is not None else 0.0
                    })
        
        if not processed_data:
            return go.Figure()
        
        df = pd.DataFrame(processed_data)
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df['symbol'],
            y=df['volatility'],
            name='Volatility',
            marker_color='rgba(255,99,71,0.8)'
        ))
        
        fig.update_layout(
            title="Cryptocurrency Volatility Comparison",
            xaxis_title="Symbol",
            yaxis_title="Volatility (%)",
            template="plotly_white",
            height=400
        )
        
        return fig
    except Exception as e:
        st.error(f"Error creating volatility chart: {e}")
        return go.Figure()

def display_market_anomalies(anomalies: List[Dict]):
    """Display market anomalies with color coding"""
    if not anomalies:
        st.info("No recent market anomalies detected.")
        return
    
    st.subheader("🚨 Market Anomalies")
    
    for anomaly in anomalies[:10]:  # Show latest 10 anomalies
        severity = anomaly.get('severity', 'low').lower()
        alert_class = f"alert-{severity}"
        
        timestamp = pd.to_datetime(anomaly['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        
        st.markdown(f"""
        <div class="alert-box {alert_class}">
            <strong>{anomaly['symbol']} - {anomaly['alert_type'].upper()}</strong><br>
            <em>{timestamp}</em><br>
            {anomaly['description']}
        </div>
        """, unsafe_allow_html=True)

def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown("""
    <div class="crypto-header">
        <h1>₿ Crypto Pipeline Dashboard</h1>
        <p>Real-time cryptocurrency data visualization and analytics</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize data service
    data_service = DataService()
    
    # Check service health first
    health_status = data_service.check_service_health()
    
    # Display service status in sidebar
    st.sidebar.subheader("🔧 Service Status")
    if health_status['oltp_healthy']:
        st.sidebar.success("OLTP Service: ✅ Healthy")
    else:
        st.sidebar.error("OLTP Service: ❌ Unhealthy")
    
    if health_status['olap_healthy']:
        st.sidebar.success("OLAP Service: ✅ Healthy")
    else:
        st.sidebar.error("OLAP Service: ❌ Unhealthy")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Configuration")
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)  # Default to False to prevent constant reloading during development
    
    # Symbol selection
    available_symbols = ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "BNBUSDT", "XRPUSDT"]
    selected_symbols = st.sidebar.multiselect(
        "Select Cryptocurrencies",
        available_symbols,
        default=["BTCUSDT", "ETHUSDT"]
    )
    
    # Time range selection
    time_range = st.sidebar.selectbox(
        "Time Range",
        ["1 Hour", "6 Hours", "24 Hours", "7 Days"],
        index=2
    )
    
    time_range_mapping = {
        "1 Hour": 1,
        "6 Hours": 6,
        "24 Hours": 24,
        "7 Days": 168
    }
    hours = time_range_mapping[time_range]
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview", "📈 Price Charts", "🔍 Technical Analysis", 
        "📉 Volatility", "🚨 Anomalies"
    ])
    
    with tab1:
        st.header("Market Overview")
        
        # Get latest prices
        latest_prices = data_service.get_latest_prices(selected_symbols)
        
        if latest_prices:
            # Display metrics in columns
            cols = st.columns(len(selected_symbols) if len(selected_symbols) <= 5 else 5)
            
            for i, price_data in enumerate(latest_prices[:len(selected_symbols)]):
                with cols[i % len(cols)]:
                    symbol = price_data['symbol']
                    price = float(price_data['price'])
                    change_24h = float(price_data.get('change_24h', 0))
                    
                    # Color based on change
                    delta_color = "normal"
                    if change_24h > 0:
                        delta_color = "normal"
                    elif change_24h < 0:
                        delta_color = "inverse"
                    
                    st.metric(
                        label=symbol,
                        value=f"${price:,.4f}",
                        delta=f"{change_24h:+.2f}%",
                        delta_color=delta_color
                    )
            
            # Market summary table
            st.subheader("Detailed Market Data")
            df = pd.DataFrame(latest_prices)
            
            # Ensure numeric columns are properly converted
            numeric_columns = ['price', 'volume', 'change_24h', 'high_24h', 'low_24h']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(4)
            
            display_columns = ['symbol', 'price', 'volume', 'high_24h', 'low_24h', 'change_24h']
            available_columns = [col for col in display_columns if col in df.columns]
            
            st.dataframe(
                df[available_columns],
                use_container_width=True
            )
        else:
            st.warning("No price data available. Please check if the services are running.")
    
    with tab2:
        st.header("Price Charts")
        
        if not selected_symbols:
            st.warning("Please select at least one cryptocurrency from the sidebar.")
        else:
            for symbol in selected_symbols:
                st.subheader(f"{symbol} Analysis")
                
                # Get price history and analytics
                price_history = data_service.get_price_history(symbol, hours)
                price_analytics = data_service.get_price_analytics(symbol, 60, hours)
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # Price chart
                    if price_analytics:
                        price_fig = create_price_chart(price_analytics, symbol)
                        st.plotly_chart(price_fig, use_container_width=True)
                    elif price_history:
                        price_fig = create_price_chart(price_history, symbol)
                        st.plotly_chart(price_fig, use_container_width=True)
                    else:
                        st.warning(f"No price data available for {symbol}")
                
                with col2:
                    # Volume chart
                    if price_analytics:
                        volume_fig = create_volume_chart(price_analytics, symbol)
                        st.plotly_chart(volume_fig, use_container_width=True)
                    elif price_history:
                        volume_fig = create_volume_chart(price_history, symbol)
                        st.plotly_chart(volume_fig, use_container_width=True)
                
                st.divider()
    
    with tab3:
        st.header("Technical Analysis")
        
        if not selected_symbols:
            st.warning("Please select at least one cryptocurrency from the sidebar.")
        else:
            for symbol in selected_symbols:
                st.subheader(f"{symbol} Technical Indicators")
                
                indicators = data_service.get_technical_indicators(symbol)
                
                if indicators:
                    # Display current indicator values
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        if indicators.get('sma'):
                            st.metric("SMA", f"{float(indicators['sma']):,.4f}")
                    
                    with col2:
                        if indicators.get('ema'):
                            st.metric("EMA", f"{float(indicators['ema']):,.4f}")
                    
                    with col3:
                        if indicators.get('rsi'):
                            rsi_val = float(indicators['rsi'])
                            rsi_status = "Overbought" if rsi_val > 70 else "Oversold" if rsi_val < 30 else "Normal"
                            st.metric("RSI", f"{rsi_val:.2f}", rsi_status)
                    
                    with col4:
                        if indicators.get('macd'):
                            st.metric("MACD", f"{float(indicators['macd']):,.4f}")
                    
                    # Technical indicators chart
                    tech_fig = create_technical_indicators_chart(indicators, symbol)
                    st.plotly_chart(tech_fig, use_container_width=True)
                else:
                    st.warning(f"No technical indicators available for {symbol}")
                
                st.divider()
    
    with tab4:
        st.header("Volatility Analysis")
        
        if not selected_symbols:
            st.warning("Please select at least one cryptocurrency from the sidebar.")
        else:
            volatility_data = data_service.get_volatility_analysis(selected_symbols, hours)
            
            if volatility_data:
                # Volatility chart
                vol_fig = create_volatility_chart(volatility_data)
                st.plotly_chart(vol_fig, use_container_width=True)
                
                # Volatility table
                st.subheader("Volatility Details")
                try:
                    df_vol = pd.DataFrame(volatility_data)
                    st.dataframe(df_vol, use_container_width=True)
                except Exception as e:
                    st.error(f"Error displaying volatility table: {e}")
                    st.json(volatility_data)  # Show raw data for debugging
            else:
                st.warning("No volatility data available")
    
    with tab5:
        st.header("Market Anomalies")
        
        anomalies = data_service.get_market_anomalies(hours)
        display_market_anomalies(anomalies)
    
    # Auto-refresh functionality
    if auto_refresh:
        time.sleep(30)
        st.rerun()

if __name__ == "__main__":
    main() 
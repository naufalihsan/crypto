import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests
from datetime import datetime
import time
from typing import Dict, List, Optional

# Configure Streamlit page
st.set_page_config(
    page_title="Crypto Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Configuration
OLTP_SERVICE_URL = "http://oltp-service:8001"
OLAP_SERVICE_URL = "http://olap-service:8002"

# Minimal CSS for clean look
st.markdown(
    """
<style>
    .main > div { padding-top: 1rem; }
    .crypto-header {
        background: linear-gradient(90deg, #f7931a 0%, #ff6b35 100%);
        color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1.5rem;
        text-align: center;
    }
    .alert-critical {
        background-color: #fff3e0;
        border-left: 4px solid #f7931a;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        color: #d84315;
    }
</style>
""",
    unsafe_allow_html=True,
)


class DataService:
    def __init__(self):
        self.oltp_url = OLTP_SERVICE_URL
        self.olap_url = OLAP_SERVICE_URL

    @st.cache_data(ttl=30)
    def get_latest_prices(_self, symbols: Optional[List[str]] = None) -> List[Dict]:
        """Get latest prices from OLTP service"""
        try:
            params = {}
            if symbols:
                params["symbols"] = ",".join(symbols)

            response = requests.get(
                f"{_self.oltp_url}/api/prices/latest", params=params, timeout=10
            )
            response.raise_for_status()
            result = response.json()

            if result.get("success") and "data" in result:
                return result["data"]
            return []
        except Exception as e:
            st.error(f"Failed to fetch prices: {e}")
            return []

    @st.cache_data(ttl=60)
    def get_price_history(_self, symbol: str, hours: int = 24) -> List[Dict]:
        """Get price history from OLTP service"""
        try:
            params = {"hours": hours}
            response = requests.get(
                f"{_self.oltp_url}/api/prices/{symbol}/history",
                params=params,
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()

            if result.get("success") and "data" in result:
                return result["data"]
            return []
        except Exception as e:
            st.error(f"Failed to fetch price history: {e}")
            return []

    @st.cache_data(ttl=60)
    def get_technical_indicators(_self, symbol: str) -> Optional[Dict]:
        """Get technical indicators from OLTP service"""
        try:
            response = requests.get(
                f"{_self.oltp_url}/api/indicators/{symbol}", timeout=10
            )
            response.raise_for_status()
            result = response.json()

            if result.get("success") and "data" in result:
                return result["data"]
            return None
        except Exception as e:
            st.error(f"Failed to fetch indicators: {e}")
            return None

    @st.cache_data(ttl=30)
    def get_critical_alerts(_self, hours: int = 24) -> List[Dict]:
        """Get only critical market anomalies"""
        try:
            params = {"hours": hours}
            response = requests.get(
                f"{_self.oltp_url}/api/anomalies", params=params, timeout=10
            )
            response.raise_for_status()
            result = response.json()

            if result.get("success") and "data" in result:
                # Filter only high severity alerts
                return [
                    alert
                    for alert in result["data"]
                    if alert.get("severity", "").lower() == "high"
                ]
            return []
        except Exception as e:
            return []


def create_price_chart(price_data: List[Dict], symbol: str) -> go.Figure:
    """Create a simple price line chart"""
    if not price_data:
        return go.Figure()

    df = pd.DataFrame(price_data)

    # Handle timestamp field - API returns abbreviated field names
    timestamp_field = None
    if "t" in df.columns:  # 't' is timestamp in abbreviated format
        timestamp_field = "t"
    elif "timestamp" in df.columns:
        timestamp_field = "timestamp"
    elif "period_start" in df.columns:
        timestamp_field = "period_start"
    else:
        st.warning(
            f"No timestamp field found in data for {symbol}. Available fields: {list(df.columns)}"
        )
        return go.Figure()

    df[timestamp_field] = pd.to_datetime(df[timestamp_field])

    # Get price field - API returns abbreviated field names
    price_col = None
    if "p" in df.columns:  # 'p' is price in abbreviated format
        price_col = "p"
    elif "c" in df.columns:  # 'c' is close price
        price_col = "c"
    elif "price" in df.columns:
        price_col = "price"
    elif "close_price" in df.columns:
        price_col = "close_price"
    elif "avg_price" in df.columns:
        price_col = "avg_price"
    else:
        st.warning(
            f"No price field found in data for {symbol}. Available fields: {list(df.columns)}"
        )
        return go.Figure()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df[timestamp_field],
            y=df[price_col],
            mode="lines",
            name=symbol,
            line=dict(color="#667eea", width=2),
        )
    )

    fig.update_layout(
        title=f"{symbol} Price Trend",
        xaxis_title="Time",
        yaxis_title="Price (USDT)",
        template="plotly_white",
        height=400,
        margin=dict(l=0, r=0, t=40, b=0),
    )

    return fig


def main():
    """Streamlined main application"""

    # Header
    st.markdown(
        """
    <div class="crypto-header">
        <h1>₿ Crypto Dashboard</h1>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Initialize service
    data_service = DataService()

    # Configuration in a compact form
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        available_symbols = ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "BNBUSDT", "XRPUSDT"]
        selected_symbols = st.multiselect(
            "Select Cryptocurrencies",
            available_symbols,
            default=["BTCUSDT", "ETHUSDT"],
            key="symbols",
        )

    with col2:
        time_range = st.selectbox(
            "Time Range", ["6 Hours", "24 Hours", "7 Days"], index=1
        )
        hours = {"6 Hours": 6, "24 Hours": 24, "7 Days": 168}[time_range]

    with col3:
        chart_symbol = st.selectbox(
            "Chart Symbol",
            selected_symbols if selected_symbols else ["BTCUSDT"],
            index=0,
        )

    # Critical alerts at the top
    critical_alerts = data_service.get_critical_alerts(hours)
    if critical_alerts:
        st.markdown("### 🚨 Critical Alerts")
        for alert in critical_alerts[:3]:  # Show max 3 critical alerts
            timestamp = pd.to_datetime(alert["timestamp"]).strftime("%H:%M")
            st.markdown(
                f"""
            <div class="alert-critical">
                <strong>{alert['symbol']} - {alert['alert_type'].upper()}</strong> ({timestamp})<br>
                {alert['description']}
            </div>
            """,
                unsafe_allow_html=True,
            )

    # Main content in two columns
    col_left, col_right = st.columns([2, 1])

    with col_left:
        # Price chart for selected symbol
        if chart_symbol:
            price_history = data_service.get_price_history(chart_symbol, hours)
            if price_history:
                price_fig = create_price_chart(price_history, chart_symbol)
                st.plotly_chart(price_fig, use_container_width=True)
            else:
                st.warning(f"No price data available for {chart_symbol}")

    with col_right:
        # Current prices overview
        st.markdown("### 💰 Current Prices")
        latest_prices = data_service.get_latest_prices(selected_symbols)

        if latest_prices:
            for price_data in latest_prices:
                symbol = price_data["symbol"]
                price = float(price_data["price"])
                change_24h = float(price_data.get("change_24h", 0))

                # Simple price display with change
                change_color = (
                    "🟢" if change_24h > 0 else "🔴" if change_24h < 0 else "⚪"
                )
                st.markdown(
                    f"""
                **{symbol}** {change_color}  
                ${price:,.4f} ({change_24h:+.2f}%)
                """
                )

        # Key technical indicators for chart symbol
        if chart_symbol:
            st.markdown("### 📊 Key Indicators")
            indicators = data_service.get_technical_indicators(chart_symbol)

            if indicators:
                # RSI with simple interpretation
                if indicators.get("rsi"):
                    rsi_val = float(indicators["rsi"])
                    rsi_status = (
                        "Overbought"
                        if rsi_val > 70
                        else "Oversold" if rsi_val < 30 else "Normal"
                    )
                    rsi_emoji = "🔴" if rsi_val > 70 else "🟢" if rsi_val < 30 else "🟡"
                    st.markdown(f"**RSI:** {rsi_val:.1f} {rsi_emoji} ({rsi_status})")

                # MACD trend
                if indicators.get("macd"):
                    macd_val = float(indicators["macd"])
                    macd_trend = "Bullish" if macd_val > 0 else "Bearish"
                    macd_emoji = "🟢" if macd_val > 0 else "🔴"
                    st.markdown(f"**MACD:** {macd_trend} {macd_emoji}")

    # Auto-refresh toggle (minimal)
    if st.checkbox("Auto-refresh (30s)", key="refresh"):
        time.sleep(30)
        st.rerun()


if __name__ == "__main__":
    main()

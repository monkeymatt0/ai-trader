"""
Bybit AI Trading Agent MCP Server (SSE Transport)

This MCP (Model Context Protocol) server exposes Bybit market data fetching tools
to AI agents via SSE (Server-Sent Events) transport, enabling them to retrieve 
cryptocurrency market information including historical OHLCV data and available 
trading symbols.

The server provides tools for:
- Fetching historical candlestick (OHLCV) data with automatic pagination
- Getting available trading symbols for different product categories
- Supporting multiple timeframes and market categories (spot, derivatives, options)

Transport: SSE (Server-Sent Events)
Port: Configurable via PORT environment variable (default: 3001)
SSE Endpoint: http://host:port/sse

Compatible with n8n MCP Client Tool nodes and other SSE-based MCP clients.

Tags: #cryptocurrency #trading #market-data #bybit #ohlcv #technical-analysis #ai-trading #sse #n8n
"""

from fastmcp import FastMCP
from typing import Optional, List, Dict, Any
import json
from datetime import datetime
import logging
import os

# Import our Bybit functions
from bybit.fetch_bybit import get_ohlcv, get_available_symbols

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the MCP server (FastMCP 2.0 style)
mcp = FastMCP("Bybit Trading Agent 🚀")


@mcp.tool
def fetch_historical_ohlcv(
    symbol: str,
    interval: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: str = "spot"
) -> Dict[str, Any]:
    """
    Fetch historical OHLCV (Open, High, Low, Close, Volume) candlestick data from Bybit.
    
    This tool retrieves comprehensive market data with automatic pagination support,
    making it perfect for technical analysis, backtesting, and market research.
    
    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT', 'SOLUSDT')
        interval (str): Candlestick timeframe - Available options:
            • Minutes: '1', '3', '5', '15', '30'
            • Hours: '60', '120', '240', '360', '720'  
            • Periods: 'D' (daily), 'W' (weekly), 'M' (monthly)
        start_date (Optional[str]): Start date in 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' format
            If not provided, fetches recent data
        end_date (Optional[str]): End date in 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' format
            If not provided, uses current time
        category (str): Market category - Options:
            • 'spot': Spot trading pairs (default)
            • 'linear': Linear derivatives (USDT perpetuals)
            • 'inverse': Inverse derivatives  
            • 'option': Options contracts
    
    Returns:
        Dict containing:
        - success (bool): Whether the operation succeeded
        - data (List[Dict]): List of candlestick data with fields:
            • timestamp: Unix timestamp (milliseconds)
            • datetime: Human-readable UTC datetime
            • open, high, low, close: Price levels
            • volume: Trading volume
            • turnover: Trading turnover (derivatives only)
        - count (int): Number of candles fetched
        - symbol (str): The requested symbol
        - interval (str): The requested interval
        - date_range (Dict): Start and end dates of the data
    
    Tags: #ohlcv #candlestick #historical-data #price-data #volume #technical-analysis
          #cryptocurrency #trading #market-data #time-series #bybit #pagination
    
    Example usage:
        • Get recent Bitcoin daily data: fetch_historical_ohlcv('BTCUSDT', 'D')
        • Get Ethereum hourly data for specific period: 
          fetch_historical_ohlcv('ETHUSDT', '60', '2024-01-01', '2024-01-07')
        • Get Solana 5-minute data with auto-pagination:
          fetch_historical_ohlcv('SOLUSDT', '5', '2024-01-01', '2024-01-31')
    """
    try:
        logger.info(f"Fetching OHLCV data for {symbol} ({interval}) from {start_date} to {end_date}")
        
        # Fetch data using our Bybit function
        df = get_ohlcv(
            symbol=symbol,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
            category=category
        )
        
        if df.empty:
            return {
                "success": False,
                "error": "No data available for the specified parameters",
                "symbol": symbol,
                "interval": interval,
                "count": 0
            }
        
        # Convert DataFrame to list of dictionaries for JSON serialization
        data_records = df.to_dict('records')
        
        # Convert datetime objects to strings for JSON serialization
        for record in data_records:
            if 'datetime' in record and hasattr(record['datetime'], 'isoformat'):
                record['datetime'] = record['datetime'].isoformat()
        
        result = {
            "success": True,
            "data": data_records,
            "count": len(data_records),
            "symbol": symbol,
            "interval": interval,
            "category": category,
            "date_range": {
                "start": data_records[0]['datetime'] if data_records else None,
                "end": data_records[-1]['datetime'] if data_records else None
            },
            "data_info": {
                "columns": list(df.columns),
                "first_timestamp": int(data_records[0]['timestamp']) if data_records else None,
                "last_timestamp": int(data_records[-1]['timestamp']) if data_records else None,
                "timeframe_coverage": f"{len(data_records)} candles"
            }
        }
        
        logger.info(f"Successfully fetched {len(data_records)} candles for {symbol}")
        return result
        
    except Exception as e:
        error_msg = f"Error fetching OHLCV data: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "symbol": symbol,
            "interval": interval,
            "count": 0
        }


@mcp.tool  
def get_trading_symbols(category: str = "spot") -> Dict[str, Any]:
    """
    Retrieve all available trading symbols for a specific market category from Bybit.
    
    This tool provides a comprehensive list of all tradeable instruments, helping you
    discover available markets and validate symbol names before fetching data.
    
    Args:
        category (str): Market category to fetch symbols for:
            • 'spot': Spot trading pairs (BTC/USDT, ETH/USDT, etc.)
            • 'linear': Linear derivatives (USDT perpetual contracts)
            • 'inverse': Inverse derivatives (BTC, ETH perpetuals)
            • 'option': Options contracts
    
    Returns:
        Dict containing:
        - success (bool): Whether the operation succeeded
        - symbols (List[str]): Sorted list of available trading symbols
        - count (int): Total number of symbols
        - category (str): The requested category
        - sample_symbols (List[str]): First 10 symbols as examples
        - popular_pairs (List[str]): Common/popular trading pairs if available
    
    Tags: #symbols #instruments #trading-pairs #market-discovery #validation
          #cryptocurrency #spot #derivatives #options #bybit
    
    Example usage:
        • Get all spot trading pairs: get_trading_symbols('spot')
        • Get linear perpetual contracts: get_trading_symbols('linear')
        • Discover available options: get_trading_symbols('option')
    """
    try:
        logger.info(f"Fetching available symbols for category: {category}")
        
        # Fetch symbols using our Bybit function
        symbols = get_available_symbols(category=category)
        
        if not symbols:
            return {
                "success": False,
                "error": f"No symbols found for category '{category}' or invalid category",
                "category": category,
                "count": 0
            }
        
        # Extract popular/common pairs for easier discovery
        popular_patterns = ['USDT', 'USD', 'BTC', 'ETH']
        popular_symbols = []
        
        for symbol in symbols[:50]:  # Check first 50 symbols
            if any(pattern in symbol for pattern in popular_patterns):
                popular_symbols.append(symbol)
                if len(popular_symbols) >= 10:
                    break
        
        result = {
            "success": True,
            "symbols": symbols,
            "count": len(symbols),
            "category": category,
            "sample_symbols": symbols[:10],
            "popular_pairs": popular_symbols,
            "categories_info": {
                "spot": "Spot trading pairs (immediate settlement)",
                "linear": "Linear derivatives (USDT margined perpetuals)",
                "inverse": "Inverse derivatives (coin margined perpetuals)", 
                "option": "Options contracts"
            }
        }
        
        logger.info(f"Successfully fetched {len(symbols)} symbols for {category}")
        return result
        
    except Exception as e:
        error_msg = f"Error fetching symbols: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "category": category,
            "count": 0
        }


@mcp.tool
def analyze_price_movement(
    symbol: str,
    interval: str = "D",
    days_back: int = 30,
    category: str = "spot"
) -> Dict[str, Any]:
    """
    Analyze recent price movements and provide trading insights for a cryptocurrency symbol.
    
    This tool fetches recent market data and performs basic technical analysis,
    providing insights on price trends, volatility, and key levels.
    
    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        interval (str): Analysis timeframe ('D' for daily, '60' for hourly, etc.)
        days_back (int): Number of days of historical data to analyze (default: 30)
        category (str): Market category ('spot', 'linear', 'inverse', 'option')
    
    Returns:
        Dict containing:
        - success (bool): Whether the analysis succeeded
        - symbol (str): Analyzed symbol
        - current_price (float): Latest closing price
        - price_change_24h (Dict): 24h price change info
        - volatility (Dict): Price volatility metrics
        - trend_analysis (Dict): Trend direction and strength
        - key_levels (Dict): Support and resistance levels
        - volume_analysis (Dict): Volume trend information
        - summary (str): Human-readable analysis summary
    
    Tags: #technical-analysis #price-analysis #trading-insights #volatility
          #trend-analysis #support-resistance #volume-analysis #market-analysis
    
    Example usage:
        • Analyze Bitcoin daily trends: analyze_price_movement('BTCUSDT', 'D', 30)
        • Check Ethereum hourly volatility: analyze_price_movement('ETHUSDT', '60', 7)
    """
    try:
        from datetime import timedelta
        
        # Calculate date range
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        logger.info(f"Analyzing price movement for {symbol} over {days_back} days")
        
        # Fetch historical data
        df = get_ohlcv(
            symbol=symbol,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
            category=category
        )
        
        if df.empty:
            return {
                "success": False,
                "error": "No data available for analysis",
                "symbol": symbol
            }
        
        # Basic analysis calculations
        current_price = float(df['close'].iloc[-1])
        previous_price = float(df['close'].iloc[-2]) if len(df) > 1 else current_price
        
        # Price change analysis
        price_change_abs = current_price - previous_price
        price_change_pct = (price_change_abs / previous_price * 100) if previous_price > 0 else 0
        
        # Volatility analysis
        df['returns'] = df['close'].pct_change()
        volatility = float(df['returns'].std() * 100) if len(df) > 1 else 0
        
        # High/Low analysis
        period_high = float(df['high'].max())
        period_low = float(df['low'].min())
        
        # Volume analysis
        avg_volume = float(df['volume'].mean())
        current_volume = float(df['volume'].iloc[-1])
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0
        
        # Trend analysis (simple moving average comparison)
        if len(df) >= 5:
            recent_avg = df['close'].tail(5).mean()
            earlier_avg = df['close'].head(5).mean()
            trend_direction = "Bullish" if recent_avg > earlier_avg else "Bearish"
        else:
            trend_direction = "Neutral"
        
        # Generate summary
        summary = f"{symbol} is currently trading at ${current_price:.4f}, "
        summary += f"{'up' if price_change_pct > 0 else 'down'} {abs(price_change_pct):.2f}% "
        summary += f"from previous {interval}. "
        summary += f"The {days_back}-day volatility is {volatility:.2f}%. "
        summary += f"Volume is {'above' if volume_ratio > 1 else 'below'} average. "
        summary += f"Overall trend appears {trend_direction.lower()}."
        
        result = {
            "success": True,
            "symbol": symbol,
            "interval": interval,
            "analysis_period": f"{days_back} days",
            "current_price": current_price,
            "price_change_24h": {
                "absolute": price_change_abs,
                "percentage": price_change_pct,
                "direction": "up" if price_change_pct > 0 else "down"
            },
            "volatility": {
                "percentage": volatility,
                "level": "High" if volatility > 5 else "Medium" if volatility > 2 else "Low"
            },
            "trend_analysis": {
                "direction": trend_direction,
                "strength": "Strong" if abs(price_change_pct) > 5 else "Moderate" if abs(price_change_pct) > 2 else "Weak"
            },
            "key_levels": {
                "period_high": period_high,
                "period_low": period_low,
                "current_vs_high": ((current_price / period_high) - 1) * 100,
                "current_vs_low": ((current_price / period_low) - 1) * 100
            },
            "volume_analysis": {
                "current_volume": current_volume,
                "average_volume": avg_volume,
                "volume_ratio": volume_ratio,
                "status": "Above Average" if volume_ratio > 1.2 else "Below Average" if volume_ratio < 0.8 else "Normal"
            },
            "summary": summary,
            "data_points": len(df)
        }
        
        logger.info(f"Successfully analyzed {symbol}: {trend_direction} trend, {volatility:.2f}% volatility")
        return result
        
    except Exception as e:
        error_msg = f"Error analyzing price movement: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "symbol": symbol
        }


@mcp.tool
def get_market_overview(
    symbols: List[str] = None,
    category: str = "spot",
    interval: str = "D"
) -> Dict[str, Any]:
    """
    Get a comprehensive market overview for multiple cryptocurrency symbols.
    
    This tool provides a quick snapshot of multiple markets, perfect for monitoring
    portfolios, identifying opportunities, or getting a general market sentiment.
    
    Args:
        symbols (List[str]): List of symbols to analyze. If None, uses popular defaults
        category (str): Market category ('spot', 'linear', 'inverse', 'option')  
        interval (str): Timeframe for analysis ('D' for daily, '60' for hourly, etc.)
    
    Returns:
        Dict containing:
        - success (bool): Whether the overview succeeded
        - overview (List[Dict]): Market data for each symbol
        - market_summary (Dict): Overall market statistics
        - top_performers (List): Best performing symbols
        - top_decliners (List): Worst performing symbols
        - total_symbols (int): Number of symbols analyzed
    
    Tags: #market-overview #portfolio #monitoring #performance #ranking
          #cryptocurrency #market-sentiment #comparison #dashboard
    
    Example usage:
        • Get major crypto overview: get_market_overview(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])
        • Quick market check: get_market_overview()
    """
    try:
        # Use default popular symbols if none provided
        if symbols is None:
            symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOGEUSDT', 'AVAXUSDT']
        
        logger.info(f"Getting market overview for {len(symbols)} symbols")
        
        overview_data = []
        successful_fetches = 0
        
        for symbol in symbols:
            try:
                # Get recent data for each symbol
                df = get_ohlcv(symbol=symbol, interval=interval, category=category)
                
                if not df.empty and len(df) >= 2:
                    current_price = float(df['close'].iloc[-1])
                    previous_price = float(df['close'].iloc[-2])
                    volume = float(df['volume'].iloc[-1])
                    
                    price_change = current_price - previous_price
                    price_change_pct = (price_change / previous_price * 100) if previous_price > 0 else 0
                    
                    overview_data.append({
                        "symbol": symbol,
                        "current_price": current_price,
                        "price_change": price_change,
                        "price_change_percentage": price_change_pct,
                        "volume": volume,
                        "status": "✅"
                    })
                    successful_fetches += 1
                else:
                    overview_data.append({
                        "symbol": symbol,
                        "error": "Insufficient data",
                        "status": "❌"
                    })
                    
            except Exception as e:
                overview_data.append({
                    "symbol": symbol,
                    "error": str(e),
                    "status": "❌"
                })
        
        # Calculate market summary statistics
        valid_data = [item for item in overview_data if "price_change_percentage" in item]
        
        if valid_data:
            price_changes = [item["price_change_percentage"] for item in valid_data]
            avg_change = sum(price_changes) / len(price_changes)
            
            # Sort for top performers and decliners
            sorted_by_performance = sorted(valid_data, key=lambda x: x["price_change_percentage"], reverse=True)
            top_performers = sorted_by_performance[:3]
            top_decliners = sorted_by_performance[-3:]
            
            market_summary = {
                "average_change": avg_change,
                "positive_symbols": len([x for x in price_changes if x > 0]),
                "negative_symbols": len([x for x in price_changes if x < 0]),
                "neutral_symbols": len([x for x in price_changes if x == 0]),
                "market_sentiment": "Bullish" if avg_change > 1 else "Bearish" if avg_change < -1 else "Neutral"
            }
        else:
            market_summary = {
                "average_change": 0,
                "positive_symbols": 0,
                "negative_symbols": 0,
                "neutral_symbols": 0,
                "market_sentiment": "Unknown"
            }
        
        result = {
            "success": successful_fetches > 0,
            "overview": overview_data,
            "market_summary": market_summary,
            "top_performers": top_performers if valid_data else [],
            "top_decliners": top_decliners if valid_data else [],
            "total_symbols": len(symbols),
            "successful_fetches": successful_fetches,
            "interval": interval,
            "category": category,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Market overview completed: {successful_fetches}/{len(symbols)} symbols processed")
        return result
        
    except Exception as e:
        error_msg = f"Error generating market overview: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "total_symbols": len(symbols) if symbols else 0
        }


# Server metadata and information
@mcp.tool
def get_server_info() -> Dict[str, Any]:
    """
    Get information about this MCP server and its capabilities.
    
    Returns comprehensive information about the available tools, their purposes,
    and how to use them effectively for cryptocurrency market analysis.
    
    Returns:
        Dict containing server information, available tools, and usage examples
    
    Tags: #server-info #help #documentation #capabilities #tools
    """
    return {
        "server_name": "Bybit Trading Agent MCP Server",
        "version": "1.0.0",
        "description": "Advanced MCP server for cryptocurrency market data analysis via Bybit API",
        "capabilities": [
            "Historical OHLCV data fetching with pagination",
            "Real-time market symbol discovery",
            "Technical analysis and price movement insights",
            "Multi-symbol market overview and monitoring",
            "Support for spot, derivatives, and options markets"
        ],
        "available_tools": {
            "fetch_historical_ohlcv": "Fetch historical candlestick data with advanced filtering",
            "get_trading_symbols": "Discover available trading symbols by category",
            "analyze_price_movement": "Perform technical analysis on price movements",
            "get_market_overview": "Get comprehensive overview of multiple symbols",
            "get_server_info": "Get information about server capabilities"
        },
        "supported_intervals": {
            "minutes": ["1", "3", "5", "15", "30"],
            "hours": ["60", "120", "240", "360", "720"],
            "periods": ["D", "W", "M"]
        },
        "supported_categories": {
            "spot": "Spot trading pairs (immediate settlement)",
            "linear": "Linear derivatives (USDT margined)",
            "inverse": "Inverse derivatives (coin margined)",
            "option": "Options contracts"
        },
        "usage_examples": {
            "basic_data": "fetch_historical_ohlcv('BTCUSDT', 'D')",
            "date_range": "fetch_historical_ohlcv('ETHUSDT', '60', '2024-01-01', '2024-01-07')",
            "symbols": "get_trading_symbols('spot')",
            "analysis": "analyze_price_movement('BTCUSDT', 'D', 30)",
            "overview": "get_market_overview(['BTCUSDT', 'ETHUSDT'])"
        },
        "tags": [
            "cryptocurrency", "trading", "market-data", "bybit", "ohlcv",
            "technical-analysis", "ai-trading", "mcp", "api", "real-time"
        ]
    }


if __name__ == "__main__":
    # Get port and host from environment variables or use defaults
    port = int(os.getenv('PORT', 3001))  # Changed default port to 3001 for SSE
    host = os.getenv('HOST', '0.0.0.0')
    
    # Add startup logging
    logger.info("🚀 Starting Bybit Trading Agent MCP Server (SSE Transport)")
    logger.info(f"🌐 Server accessible at: http://{host}:{port}")
    logger.info(f"📡 SSE Endpoint: http://{host}:{port}/sse")
    logger.info(f"🔗 n8n Docker Endpoint: http://host.docker.internal:{port}/sse")
    logger.info("🔧 Available tools:")
    logger.info("   - fetch_historical_ohlcv: Get historical OHLCV data")
    logger.info("   - get_trading_symbols: List available symbols")
    logger.info("   - analyze_price_movement: Perform technical analysis")
    logger.info("   - get_market_overview: Multi-symbol market overview")
    logger.info("   - get_server_info: Server capabilities info")
    logger.info("✅ Server ready to accept SSE connections...")
    logger.info("💡 For n8n: Use 'http://host.docker.internal:3001/sse' as SSE Endpoint")
    
    try:
        # Run the MCP server with SSE transport
        # FastMCP 2.0 supports SSE transport for n8n and other streaming clients
        mcp.run(transport="sse", host=host, port=port)
    except KeyboardInterrupt:
        logger.info("🛑 Server shutdown requested by user")
    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        raise 
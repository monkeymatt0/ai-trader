"""
Bybit Data Fetcher Module

This module provides functions to fetch historical OHLCV (Open, High, Low, Close, Volume) 
data from Bybit V5 API with automatic pagination support.
"""

import requests
import pandas as pd
from datetime import datetime, timezone
import time
from typing import Optional, List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bybit V5 API constants
BYBIT_BASE_URL = "https://api.bybit.com"
KLINE_ENDPOINT = "/v5/market/kline"
MAX_LIMIT = 1000  # Maximum number of candles per request according to Bybit API
DEFAULT_LIMIT = 200  # Default limit for single requests


def get_ohlcv(
    symbol: str,
    interval: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: str = "spot"
) -> pd.DataFrame:
    """
    Fetch historical OHLCV data from Bybit V5 API with automatic pagination.
    
    This function retrieves historical candlestick data for a specified trading pair
    from Bybit's V5 API. It automatically handles pagination when the requested
    time range exceeds the API's limit of 1000 candles per request.
    
    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        interval (str): Candlestick interval. Valid values:
            - '1' (1 minute), '3', '5', '15', '30' (minutes)
            - '60', '120', '240', '360', '720' (minutes) 
            - 'D' (1 day), 'W' (1 week), 'M' (1 month)
        start_date (Optional[str]): Start date in format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.
            If None, fetches recent data based on end_date or current time.
        end_date (Optional[str]): End date in format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.
            If None, uses current time.
        category (str): Product category. Default is 'spot'. 
            Valid values: 'spot', 'linear', 'inverse', 'option'
    
    Returns:
        pd.DataFrame: DataFrame with columns:
            - timestamp: Unix timestamp in milliseconds
            - datetime: Human-readable datetime (UTC)
            - open: Opening price
            - high: Highest price
            - low: Lowest price
            - close: Closing price
            - volume: Trading volume
            - turnover: Trading turnover (only for derivatives)
    
    Raises:
        requests.RequestException: If API request fails
        ValueError: If invalid parameters are provided
        KeyError: If API response format is unexpected
    
    Example:
        >>> # Get recent 100 candles
        >>> df = get_ohlcv('BTCUSDT', '1', category='spot')
        
        >>> # Get data for specific date range
        >>> df = get_ohlcv('BTCUSDT', '1', '2024-01-01', '2024-01-02')
        
        >>> # Get data for longer period (will use pagination automatically)
        >>> df = get_ohlcv('BTCUSDT', '1', '2024-01-01', '2024-01-31')
    """
    
    # Validate inputs
    if not symbol:
        raise ValueError("Symbol cannot be empty")
    
    if not interval:
        raise ValueError("Interval cannot be empty")
    
    valid_categories = ['spot', 'linear', 'inverse', 'option']
    if category not in valid_categories:
        raise ValueError(f"Category must be one of {valid_categories}")
    
    # Convert dates to timestamps if provided
    start_timestamp = None
    end_timestamp = None
    
    if start_date:
        start_timestamp = _parse_date_to_timestamp(start_date)
    
    if end_date:
        end_timestamp = _parse_date_to_timestamp(end_date)
    else:
        # If no end_date provided, use current time
        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    # Collect all data
    all_data = []
    current_end = end_timestamp
    
    logger.info(f"Fetching OHLCV data for {symbol} with interval {interval}")
    
    while True:
        # Prepare request parameters
        params = {
            'category': category,
            'symbol': symbol,
            'interval': interval,
            'limit': MAX_LIMIT
        }
        
        # Add end timestamp to get data before this time
        if current_end:
            params['end'] = current_end
        
        # Add start timestamp if provided and we're in the range
        if start_timestamp and current_end:
            params['start'] = start_timestamp
        
        try:
            # Make API request
            response = _make_api_request(params)
            data = response.get('result', {}).get('list', [])
            
            if not data:
                logger.info("No more data available")
                break
            
            # Add data to collection
            all_data.extend(data)
            logger.info(f"Fetched {len(data)} candles")
            
            # Check if we have enough data or reached start_date
            oldest_timestamp = int(data[-1][0])  # Last item is oldest due to Bybit's desc order
            
            if start_timestamp and oldest_timestamp <= start_timestamp:
                logger.info("Reached start date")
                break
            
            if len(data) < MAX_LIMIT:
                logger.info("Fetched all available data")
                break
            
            # Update current_end for next iteration (oldest timestamp from current batch)
            current_end = oldest_timestamp - 1
            
            # Add small delay to avoid rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise
    
    if not all_data:
        logger.warning("No data fetched")
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = _convert_to_dataframe(all_data, category)
    
    # Filter by date range if start_date was provided
    if start_timestamp:
        df = df[df['timestamp'] >= start_timestamp]
    
    # Sort by timestamp (oldest first)
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    logger.info(f"Successfully fetched {len(df)} candles for {symbol}")
    return df


def _parse_date_to_timestamp(date_str: str) -> int:
    """
    Parse date string to Unix timestamp in milliseconds.
    
    Args:
        date_str (str): Date string in format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
    
    Returns:
        int: Unix timestamp in milliseconds
    """
    try:
        # Try parsing with time
        if ' ' in date_str:
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        else:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Ensure timezone is UTC
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_str}. Use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'") from e


def _make_api_request(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make API request to Bybit V5 kline endpoint.
    
    Args:
        params (Dict[str, Any]): Request parameters
    
    Returns:
        Dict[str, Any]: API response data
    """
    url = f"{BYBIT_BASE_URL}{KLINE_ENDPOINT}"
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Check API response status
        if data.get('retCode') != 0:
            error_msg = data.get('retMsg', 'Unknown API error')
            raise requests.RequestException(f"Bybit API error: {error_msg}")
        
        return data
    
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise


def _convert_to_dataframe(data: List[List], category: str) -> pd.DataFrame:
    """
    Convert raw API data to pandas DataFrame.
    
    Args:
        data (List[List]): Raw candlestick data from API
        category (str): Product category
    
    Returns:
        pd.DataFrame: Formatted DataFrame
    """
    if not data:
        return pd.DataFrame()
    
    # Define columns based on category
    if category == 'spot':
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover']
    else:  # derivatives
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover']
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
    # Convert data types
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    df['open'] = pd.to_numeric(df['open'], errors='coerce')
    df['high'] = pd.to_numeric(df['high'], errors='coerce')
    df['low'] = pd.to_numeric(df['low'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df['turnover'] = pd.to_numeric(df['turnover'], errors='coerce')
    
    # Add human-readable datetime column
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    
    # Reorder columns
    column_order = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume']
    if 'turnover' in df.columns:
        column_order.append('turnover')
    
    return df[column_order]


def get_available_symbols(category: str = "spot") -> List[str]:
    """
    Get list of available trading symbols for a given category.
    
    Args:
        category (str): Product category ('spot', 'linear', 'inverse', 'option')
    
    Returns:
        List[str]: List of available symbols
    """
    url = f"{BYBIT_BASE_URL}/v5/market/instruments-info"
    params = {'category': category}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get('retCode') != 0:
            error_msg = data.get('retMsg', 'Unknown API error')
            raise requests.RequestException(f"Bybit API error: {error_msg}")
        
        instruments = data.get('result', {}).get('list', [])
        symbols = [instrument['symbol'] for instrument in instruments]
        
        return sorted(symbols)
    
    except Exception as e:
        logger.error(f"Error fetching available symbols: {e}")
        return []

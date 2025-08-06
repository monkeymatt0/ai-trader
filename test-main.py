"""
Test Main Module for Bybit Data Fetcher

This module contains test cases and examples for the get_ohlcv function
from fetch_bybit.py module.
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# Add the current directory to Python path to import fetch_bybit
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fetch_bybit import get_ohlcv, get_available_symbols


def test_basic_fetch():
    """Test basic OHLCV data fetching without date range."""
    print("=" * 60)
    print("TEST 1: Basic fetch (recent 200 candles)")
    print("=" * 60)
    
    try:
        df = get_ohlcv(
            symbol='BTCUSDT',
            interval='D',  # 1 day
            category='spot'
        )
        
        print(f"✅ Successfully fetched {len(df)} candles")
        print("\nFirst 5 rows:")
        print(df.head())
        print("\nLast 5 rows:")
        print(df.tail())
        print(f"\nData types:\n{df.dtypes}")
        
    except Exception as e:
        print(f"❌ Error in basic fetch: {e}")


def test_date_range_fetch():
    """Test OHLCV data fetching with specific date range."""
    print("\n" + "=" * 60)
    print("TEST 2: Date range fetch (specific period)")
    print("=" * 60)
    
    try:
        # Get data for the last 7 days
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        print(f"Fetching data from {start_date} to {end_date}")
        
        df = get_ohlcv(
            symbol='ETHUSDT',
            interval='60',  # 1 hour
            start_date=start_date,
            end_date=end_date,
            category='spot'
        )
        
        print(f"✅ Successfully fetched {len(df)} hourly candles")
        print(f"\nDate range: {df['datetime'].min()} to {df['datetime'].max()}")
        print("\nBasic statistics:")
        print(df[['open', 'high', 'low', 'close', 'volume']].describe())
        
    except Exception as e:
        print(f"❌ Error in date range fetch: {e}")


def test_pagination():
    """Test pagination functionality with larger date range."""
    print("\n" + "=" * 60)
    print("TEST 3: Pagination test (larger dataset)")
    print("=" * 60)
    
    try:
        # Get data for the last 30 days with 1-minute intervals
        # This should trigger pagination as it's more than 1000 candles
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        
        print(f"Fetching 1-minute data from {start_date} to {end_date}")
        print("This should trigger pagination...")
        
        df = get_ohlcv(
            symbol='BTCUSDT',
            interval='1',  # 1 minute
            start_date=start_date,
            end_date=end_date,
            category='spot'
        )
        
        print(f"✅ Successfully fetched {len(df)} candles using pagination")
        print(f"Expected ~{3 * 24 * 60} candles (3 days * 24 hours * 60 minutes)")
        
        if len(df) > 1000:
            print("✅ Pagination worked correctly (more than 1000 candles)")
        
    except Exception as e:
        print(f"❌ Error in pagination test: {e}")


def test_derivatives():
    """Test fetching derivatives data."""
    print("\n" + "=" * 60)
    print("TEST 4: Derivatives data test")
    print("=" * 60)
    
    try:
        df = get_ohlcv(
            symbol='BTCUSDT',
            interval='60',  # 1 hour
            category='linear'  # Linear derivatives
        )
        
        print(f"✅ Successfully fetched {len(df)} linear derivatives candles")
        print("\nColumns available:")
        print(df.columns.tolist())
        print("\nFirst 3 rows:")
        print(df.head(3))
        
    except Exception as e:
        print(f"❌ Error in derivatives test: {e}")


def test_available_symbols():
    """Test getting available symbols."""
    print("\n" + "=" * 60)
    print("TEST 5: Available symbols test")
    print("=" * 60)
    
    try:
        # Test spot symbols
        spot_symbols = get_available_symbols('spot')
        print(f"✅ Found {len(spot_symbols)} spot symbols")
        print(f"First 10 spot symbols: {spot_symbols[:10]}")
        
        # Test linear derivatives symbols
        linear_symbols = get_available_symbols('linear')
        print(f"✅ Found {len(linear_symbols)} linear derivatives symbols")
        print(f"First 10 linear symbols: {linear_symbols[:10]}")
        
    except Exception as e:
        print(f"❌ Error fetching available symbols: {e}")


def test_error_handling():
    """Test error handling with invalid parameters."""
    print("\n" + "=" * 60)
    print("TEST 6: Error handling test")
    print("=" * 60)
    
    # Test invalid symbol
    try:
        df = get_ohlcv('INVALID_SYMBOL', '1')
        print("❌ Should have failed with invalid symbol")
    except Exception as e:
        print(f"✅ Correctly handled invalid symbol: {type(e).__name__}")
    
    # Test invalid date format
    try:
        df = get_ohlcv('BTCUSDT', '1', start_date='invalid-date')
        print("❌ Should have failed with invalid date")
    except Exception as e:
        print(f"✅ Correctly handled invalid date: {type(e).__name__}")
    
    # Test invalid category
    try:
        df = get_ohlcv('BTCUSDT', '1', category='invalid')
        print("❌ Should have failed with invalid category")
    except Exception as e:
        print(f"✅ Correctly handled invalid category: {type(e).__name__}")


def run_all_tests():
    """Run all test functions."""
    print("🚀 Starting Bybit Data Fetcher Tests")
    print("This will test various scenarios of the get_ohlcv function")
    
    test_functions = [
        test_basic_fetch,
        test_date_range_fetch,
        test_pagination,
        test_derivatives,
        test_available_symbols,
        test_error_handling
    ]
    
    for test_func in test_functions:
        try:
            test_func()
        except KeyboardInterrupt:
            print("\n❌ Tests interrupted by user")
            break
        except Exception as e:
            print(f"\n❌ Unexpected error in {test_func.__name__}: {e}")
    
    print("\n" + "=" * 60)
    print("🏁 Tests completed!")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests() 
#!/usr/bin/env python3
"""
Bybit Trading Agent MCP Server Startup Script (SSE Transport)

This script starts the MCP server with SSE (Server-Sent Events) transport for 
n8n compatibility and web accessibility. It can be used to start the server 
from different environments and provides clear feedback about the server status.

The server will be accessible via SSE endpoints for streaming MCP communication.
Compatible with n8n MCP Client Tool nodes and other SSE-based MCP clients.
"""

import sys
import os
import subprocess
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if required dependencies are installed."""
    try:
        import fastmcp
        import pandas
        import requests
        from bybit.fetch_bybit import get_ohlcv, get_available_symbols
        logger.info("✅ All dependencies are available")
        return True
    except ImportError as e:
        logger.error(f"❌ Missing dependency: {e}")
        logger.error("Please install required packages:")
        logger.error("pip install fastmcp pandas requests")
        return False

def start_server(port: int = None, host: str = "0.0.0.0"):
    """Start the MCP server with SSE transport."""
    try:
        # Get port from parameter, environment variable, or use default
        if port is None:
            port = int(os.getenv('PORT', 3001))  # Changed default to 3001 for SSE
        
        logger.info("🚀 Starting Bybit Trading Agent MCP Server (SSE Transport)...")
        logger.info(f"🌐 Server will be accessible at: http://{host}:{port}")
        logger.info(f"📡 SSE Endpoint: http://{host}:{port}/sse")
        logger.info(f"🔗 n8n Docker Endpoint: http://host.docker.internal:{port}/sse")
        
        # Check dependencies first
        if not check_dependencies():
            return False
        
        # Set environment variables for SSE transport
        os.environ['PORT'] = str(port)
        os.environ['HOST'] = host
        
        # Import and configure the server for SSE transport
        from mcp_server import mcp
        
        logger.info("✅ Server initialized successfully")
        logger.info("🔧 Available tools:")
        logger.info("   - fetch_historical_ohlcv: Get historical OHLCV data")
        logger.info("   - get_trading_symbols: List available symbols")
        logger.info("   - analyze_price_movement: Perform technical analysis")
        logger.info("   - get_market_overview: Multi-symbol market overview")
        logger.info("   - get_server_info: Server capabilities info")
        logger.info("✅ Server ready to accept SSE connections...")
        logger.info(f"📡 Access the SSE endpoint at: http://{host}:{port}/sse")
        logger.info("🔗 Example SSE test:")
        logger.info(f"   curl -N -H \"Accept: text/event-stream\" http://{host}:{port}/sse")
        logger.info("💡 For n8n MCP Client: Use 'http://host.docker.internal:3001/sse'")
        
        # Run the server with SSE transport
        # FastMCP 2.0 automatically provides SSE endpoints when transport="sse"
        mcp.run(transport="sse", host=host, port=port)
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Server shutdown requested by user")
        return True
    except Exception as e:
        logger.error(f"❌ Error starting server: {e}")
        logger.error("💡 Troubleshooting tips:")
        logger.error("   1. Make sure you're in the correct directory")
        logger.error("   2. Check that bybit package is available")
        logger.error("   3. Verify all dependencies are installed")
        logger.error("   4. Check FastMCP installation: pip install fastmcp")
        logger.error(f"   5. Make sure port {port} is not already in use")
        logger.error("   6. Try a different port: python start_mcp_server.py --port 3002")
        return False

if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Start Bybit Trading Agent MCP Server')
    parser.add_argument('--port', '-p', type=int, default=None,
                       help='Port to run the server on (default: 3000 or PORT env var)')
    parser.add_argument('--host', default='0.0.0.0',
                       help='Host to bind the server to (default: 0.0.0.0)')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚀 Bybit Trading Agent MCP Server (SSE Transport)")
    print("=" * 70)
    print("Advanced MCP server for cryptocurrency market data analysis")
    print("Powered by Bybit V5 API with FastMCP 2.0")
    print("📡 Transport: SSE (Server-Sent Events)")
    print(f"🌐 Host: {args.host}")
    print(f"🔌 Port: {args.port or os.getenv('PORT', 3001)}")
    print(f"📡 SSE Endpoint: http://{args.host}:{args.port or os.getenv('PORT', 3001)}/sse")
    print("💡 For n8n: Use 'http://host.docker.internal:3001/sse'")
    print("=" * 70)
    
    success = start_server(port=args.port, host=args.host)
    
    if success:
        print("\n✅ Server stopped gracefully")
    else:
        print("\n❌ Server encountered an error")
        sys.exit(1) 
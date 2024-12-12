import lakeapi
import pandas as pd
import numpy as np
import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_trade_data(symbol: str, exchanges: list, days: int = 2, is_futures: bool = False) -> pd.DataFrame:
    """Fetch trade data for analysis."""
    try:
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(days=days)
        
        logger.info(f"Fetching {symbol} trades from {', '.join(exchanges)}...")
        df = lakeapi.load_data(
            table="trades",
            start=start_time,
            end=end_time,
            symbols=[symbol],
            exchanges=exchanges,
        )
        
        # Convert timestamp to datetime
        if 'timestamp' in df.columns:
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        elif 'received_time' in df.columns:
            df['datetime'] = pd.to_datetime(df['received_time'])
        
        return df

    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        raise

def calculate_cvd(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate Cumulative Volume Delta."""
    df['volume_delta'] = df.apply(lambda x: x['quantity'] if x['side'] == 'buy' else -x['quantity'], axis=1)
    df['cvd'] = df['volume_delta'].cumsum()
    return df

def categorize_trades(df: pd.DataFrame) -> pd.DataFrame:
    """Categorize trades by size."""
    # Calculate volume percentiles for trade categorization
    volume_percentiles = df['quantity'].quantile([0.5, 0.75, 0.9])
    
    conditions = [
        (df['quantity'] < volume_percentiles[0.5]),
        (df['quantity'] >= volume_percentiles[0.5]) & (df['quantity'] < volume_percentiles[0.75]),
        (df['quantity'] >= volume_percentiles[0.75])
    ]
    
    categories = ['small', 'medium', 'large']
    df['category'] = np.select(conditions, categories, default='small')
    
    return df, volume_percentiles

def analyze_liquidity(df: pd.DataFrame, timeframe: str = '1H') -> pd.DataFrame:
    """Analyze liquidity by timeframe."""
    df_liq = df.set_index('datetime').resample(timeframe).agg({
        'quantity': 'sum',
        'price': ['first', 'last', 'min', 'max'],
        'cvd': 'last'
    })
    
    df_liq.columns = ['volume', 'open', 'close', 'low', 'high', 'cvd']
    df_liq['volatility'] = (df_liq['high'] - df_liq['low']) / df_liq['open'] * 100
    return df_liq

def analyze_market_maker_activity(df: pd.DataFrame, volume_percentiles) -> dict:
    """Detailed market maker and large trader analysis."""
    # Separate by categories
    categories = ['small', 'medium', 'large']
    analysis = {}
    
    for cat in categories:
        cat_df = df[df['category'] == cat]
        buys = cat_df[cat_df['side'] == 'buy']
        sells = cat_df[cat_df['side'] == 'sell']
        
        analysis[cat] = {
            'total_trades': len(cat_df),
            'buy_trades': len(buys),
            'sell_trades': len(sells),
            'buy_volume': buys['quantity'].sum(),
            'sell_volume': sells['quantity'].sum(),
            'net_volume': buys['quantity'].sum() - sells['quantity'].sum(),
            'avg_trade_size': cat_df['quantity'].mean(),
            'avg_price_impact': cat_df['price'].pct_change().abs().mean() * 100
        }
    
    # Calculate market maker dominance
    total_volume = df['quantity'].sum()
    mm_volume = analysis['large']['buy_volume'] + analysis['large']['sell_volume']
    analysis['market_maker_dominance'] = mm_volume / total_volume * 100
    
    # Add CVD analysis
    analysis['cvd_stats'] = {
        'final_cvd': df['cvd'].iloc[-1],
        'max_cvd': df['cvd'].max(),
        'min_cvd': df['cvd'].min(),
        'cvd_volatility': df['cvd'].std()
    }
    
    return analysis

def analyze_market(df: pd.DataFrame, market_type: str, symbol: str):
    """Analyze a single market (spot or futures)"""
    try:
        # Process data
        df = calculate_cvd(df)
        df, volume_percentiles = categorize_trades(df)
        liquidity = analyze_liquidity(df)
        analysis = analyze_market_maker_activity(df, volume_percentiles)
        
        # Print Analysis
        print(f"\n=== {market_type} MARKET ===")
        
        print("\n--- Trade Distribution ---")
        for category in ['small', 'medium', 'large']:
            cat_data = analysis[category]
            print(f"\n{category.upper()}:")
            print(f"Total Trades: {cat_data['total_trades']:,}")
            print(f"Buy Volume: {cat_data['buy_volume']:,.2f}")
            print(f"Sell Volume: {cat_data['sell_volume']:,.2f}")
            print(f"Net Volume: {cat_data['net_volume']:,.2f}")
            print(f"Avg Trade Size: {cat_data['avg_trade_size']:.4f}")
            print(f"Avg Price Impact: {cat_data['avg_price_impact']:.4f}%")
        
        print("\n--- Market Maker Activity ---")
        print(f"Market Maker Dominance: {analysis['market_maker_dominance']:.2f}%")
        
        print("\n--- CVD Analysis ---")
        print(f"Final CVD: {analysis['cvd_stats']['final_cvd']:,.2f}")
        print(f"Max CVD: {analysis['cvd_stats']['max_cvd']:,.2f}")
        print(f"Min CVD: {analysis['cvd_stats']['min_cvd']:,.2f}")
        print(f"CVD Volatility: {analysis['cvd_stats']['cvd_volatility']:,.2f}")
        
        # Market Direction Analysis
        if analysis['cvd_stats']['final_cvd'] > 0:
            direction = "BULLISH"
        else:
            direction = "BEARISH"
        print(f"\nOverall Direction: {direction}")
        
        # Return analysis for comparison
        return analysis
        
    except Exception as e:
        logger.error(f"Error analyzing {market_type} market: {str(e)}")
        return None

def get_market_symbols(base_symbol: str):
    """Return both spot and PERP symbols for a given base symbol."""
    spot_symbol = base_symbol
    perp_symbol = f"{base_symbol.replace('-PERP', '')}-PERP"
    return spot_symbol, perp_symbol

def main():
    # Example base symbol
    base_symbol = "BTC-USDT"
    spot_symbol, perp_symbol = get_market_symbols(base_symbol)
    
    # List of exchanges
    exchanges = [
        "BINANCE",
        # "ASCENDEX",
        # "KUCOIN",
        # "GATEIO",
        # "SERUM",
        # "COINBASE",
        # "HUOBI",
        # "HUOBI_SWAP",
        # "OKX",
        # "DYDX",
        # "BYBIT",
        # "COINMATE"
    ]
    
    
    # Fetch and analyze spot market
    try:
        spot_data = fetch_trade_data(spot_symbol, exchanges, days=2, is_futures=False)
        analyze_market(spot_data, "SPOT", spot_symbol)
    except Exception as e:
        logger.error(f"Error processing spot market: {str(e)}")
    
    # Fetch and analyze PERP market
    try:
        perp_data = fetch_trade_data(perp_symbol, ["BINANCE_FUTURES"], days=2, is_futures=True)
        analyze_market(perp_data, "PERP", perp_symbol)
    except Exception as e:
        logger.error(f"Error processing PERP market: {str(e)}")

if __name__ == "__main__":
    main()
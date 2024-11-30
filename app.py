from mexc_sdk import Spot
import os
from dotenv import load_dotenv
import time
from datetime import datetime

# Load environment variables
load_dotenv()

class MEXCTradingBot:
    def __init__(self):
        # Initialize MEXC client
        self.client = Spot(
            api_key=os.getenv('MEXC_API_KEY'),
            api_secret=os.getenv('MEXC_SECRET')
        )
        self.symbol = 'BTC_USDT'  # MEXC uses underscore format
        
    def test_connection(self):
        """Test API connection and account access"""
        try:
            # Test server connection
            server_time = self.client.time()
            print(f"Server connection successful - Time: {server_time}")
            
            # Test account access
            account_info = self.client.account_info()
            print("\nAccount Info:")
            print(account_info)
            return True
            
        except Exception as e:
            print(f"Connection Error: {e}")
            return False
    
    def get_market_info(self):
        """Get current market information"""
        try:
            # Get ticker price
            ticker = self.client.ticker_price(self.symbol)
            print(f"\nCurrent {self.symbol} Price:")
            print(ticker)
            
            # Get order book
            depth = self.client.depth(self.symbol, {'limit': 5})
            print("\nOrder Book (Top 5):")
            print(depth)
            
            return ticker, depth
            
        except Exception as e:
            print(f"Market Info Error: {e}")
            return None, None
            
    def place_test_order(self, side, quantity):
        """Place a test order"""
        try:
            order = self.client.newOrderTest(
                symbol=self.symbol,
                side=side,  # 'BUY' or 'SELL'
                orderType='MARKET',
                options={
                    'quantity': quantity
                }
            )
            print(f"\nTest Order Placed Successfully:")
            print(order)
            return order
            
        except Exception as e:
            print(f"Test Order Error: {e}")
            return None

def main():
    # Initialize bot
    bot = MEXCTradingBot()
    
    # Test connection
    if not bot.test_connection():
        print("Failed to connect. Exiting...")
        return
    
    # Get market info
    ticker, depth = bot.get_market_info()
    
    # Place test order
    test_order = bot.place_test_order('BUY', 0.001)
    
if __name__ == "__main__":
    main()

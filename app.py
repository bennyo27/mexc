from mexc_sdk import Spot
import os
from dotenv import load_dotenv
import time
from datetime import datetime

# Load environment variables
load_dotenv()

class MEXCTradingBot:
    def __init__(self):
        self.client = Spot(
            api_key=os.getenv('MEXC_API_KEY'),
            api_secret=os.getenv('MEXC_SECRET')
        )
        self.symbol = 'BTC_USDT'
        self.time_offset = 0  # Store time difference
        
    def sync_time(self):
        """Sync with server time and store offset"""
        try:
            server_time = self.client.time()
            server_timestamp = server_time['serverTime']
            local_timestamp = int(time.time() * 1000)
            
            # Calculate and store the offset
            self.time_offset = server_timestamp - local_timestamp
            
            print(f"Server Time: {server_timestamp}")
            print(f"Local Time: {local_timestamp}")
            print(f"Time Difference: {local_timestamp - server_timestamp}ms")
            print(f"Offset set to: {self.time_offset}ms")
            
            return True
            
        except Exception as e:
            print(f"Time sync error: {e}")
            return False
        
    def get_timestamp(self):
        """Get current timestamp adjusted for server time"""
        return int(time.time() * 1000) + self.time_offset
        
    def test_connection(self):
        """Test API connection and account access"""
        try:
            # Sync time first
            if not self.sync_time():
                return False
            
            # Wait a moment for sync
            time.sleep(1)
            
            # Test account access with adjusted timestamp
            account_info = self.client.account_info()
            print("\nAccount Info:")
            print(account_info)
            return True
            
        except Exception as e:
            print(f"Connection Error: {e}")
            return False

def main():
    # Initialize bot
    bot = MEXCTradingBot()
    
    # Test connection with time sync
    if not bot.test_connection():
        print("Failed to connect. Exiting...")
        return
    
if __name__ == "__main__":
    main()

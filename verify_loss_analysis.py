import requests
import json
import time

def verify_loss_analysis():
    BASE_URL = "http://127.0.0.1:5000"
    
    print("\n--- Verifying Seasonal Loss Analysis ---")
    
    # 1. Trigger cache update if needed (direction=loss)
    print("Step 1: Requesting Loss Data (min_gain=10, direction=loss)...")
    try:
        response = requests.get(f"{BASE_URL}/api/seasonal-screener?min_gain=10&direction=loss")
        data = response.json()
        
        if data.get('status') == 'calculating':
            print("Cache is being built. Waiting 30 seconds for initial stocks...")
            time.sleep(30)
            response = requests.get(f"{BASE_URL}/api/seasonal-screener?min_gain=10&direction=loss")
            data = response.json()
            
        if 'stocks' in data and len(data['stocks']) > 0:
            print(f"Success! Found {len(data['stocks'])} stocks.")
            sample = data['stocks'][0]
            print(f"Sample Stock: {sample['ticker']}")
            print(f"Avg Gain/Loss: {sample['best_month_avg_gain']}%")
            print(f"Avg Drawdown/Recovery: {sample.get('best_month_drawdown', 'N/A')}%")
            
            if 'best_month_drawdown' in sample:
                print("New 'best_month_drawdown' field found successfully.")
            
            # Verify direction flag
            if data.get('direction') == 'loss':
                print("API correctly reports direction=loss.")
            else:
                print("WARNING: API did not report direction=loss correctly.")
        else:
            print("No loss data found yet. Background update might still be running or no stocks match.")
            print(f"Full response: {json.dumps(data, indent=2)}")
            
    except Exception as e:
        print(f"Error during verification: {e}")

if __name__ == "__main__":
    verify_loss_analysis()

import requests
import json

URL = "https://unique-motivation-production.up.railway.app/api/seasonal-screener"

def compare_thresholds(ticker, low, high):
    print(f"\n--- Comparing {ticker} at {low}% vs {high}% ---")
    try:
        data_low = requests.get(f"{URL}?min_gain={low}").json()
        data_high = requests.get(f"{URL}?min_gain={high}").json()
        
        if data_low.get('status') == 'calculating' or data_high.get('status') == 'calculating':
            print("Status: Calculating... try again in 1 min.")
            return

        stock_low = next((s for s in data_low.get('stocks', []) if s['ticker'] == ticker), None)
        stock_high = next((s for s in data_high.get('stocks', []) if s['ticker'] == ticker), None)
        
        if stock_low:
            print(f"{ticker} @ {low}%: {stock_low['total_rallies']} rallies")
        else:
            print(f"{ticker} not found @ {low}%")

        if stock_high:
            print(f"{ticker} @ {high}%: {stock_high['total_rallies']} rallies")
        else:
            print(f"{ticker} not found @ {high}% (Expected for high thresholds)")
            
        if stock_low:
            low_r = stock_low['total_rallies']
            high_r = stock_high['total_rallies'] if stock_high else 0
            if high_r < low_r:
                print("SUCCESS: Rallies decreased as threshold increased.")
            else:
                print(f"FAILED: Rally count did not decrease ({low_r} -> {high_r}).")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    compare_thresholds("RELIANCE", 10, 30)
    compare_thresholds("TCS", 10, 30)
    compare_thresholds("INFY", 5, 50)

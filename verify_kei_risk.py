import requests
import json

def verify_kei_cross_trend():
    print("--- Verifying KEI Cross-Trend Risk Metrics ---")
    
    # Target: KEI in March, Gain Mode, Min Gain 9%
    url = "http://localhost:5000/api/seasonal-screener?min_gain=9&direction=gain"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        stocks = data.get('stocks', [])
        kei = next((s for s in stocks if s['ticker'] == 'KEI'), None)
        
        if not kei:
            print("Error: KEI not found in results.")
            return

        print(f"Stock: {kei['ticker']}")
        print(f"Best Month: {kei['best_month']}")
        
        # Check March specifically
        march_stats = next((m for m in kei['monthly_stats'] if m['month'] == 'March'), None)
        
        if march_stats:
            print(f"March Stats:")
            print(f"  Avg Gain: {march_stats['avg_gain']}%")
            print(f"  Min Gain: {march_stats['min_gain']}%")
            print(f"  Success Rate: {march_stats['success_rate']}%")
            print(f"  Opposition Trend (Losses) in same month:")
            print(f"    Opp Count: {march_stats['opp_count']}")
            print(f"    Opp Avg Loss: {march_stats['opp_avg_gain']}%")
            print(f"    Opp Max Loss: {march_stats['opp_max_gain']}% (The KEI ~9.3% dip)")
            
            if march_stats['opp_max_gain'] > 0:
                print("✅ SUCCESS: Opposition trend (losses) correctly captured in Gain Mode.")
            else:
                print("❌ FAILURE: Opposition trend not found.")
        else:
            print("March stats not found for KEI.")

    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    verify_kei_cross_trend()

import requests
import json

def debug_consistency():
    print("--- Debugging 100% Consistency Data ---")
    
    # Check 1: 10% Gain, No specific month
    url = "http://localhost:5000/api/seasonal-screener?min_gain=10&direction=gain"
    
    try:
        response = requests.get(url)
        data = response.json()
        stocks = data.get('stocks', [])
        
        print(f"Total stocks found at 20% gain: {len(stocks)}")
        
        # Count stocks with 100% success in their 'best_month'
        consistent_best = [s for s in stocks if s.get('best_month_success') == 100]
        print(f"Stocks with 100% success in Best Month: {len(consistent_best)}")
        
        if consistent_best:
            print(f"Sample consistent stock: {consistent_best[0]['ticker']} ({consistent_best[0]['best_month']})")
        
        # Check specific month: March
        march_consistent = []
        for s in stocks:
            march_stats = next((m for m in s.get('monthly_stats', []) if m['month'] == 'March'), None)
            if march_stats and march_stats.get('success_rate') == 100 and march_stats.get('occurrences', 0) >= 5:
                march_consistent.append(s['ticker'])
        
        print(f"Stocks with 100% success in March (min 5 rallies): {len(march_consistent)}")
        if march_consistent:
            print(f"Sample March consistent stocks: {march_consistent[:5]}")

    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    debug_consistency()

import requests
import json

def analyze_kei_march():
    # Set high Min Gain for Gain Mode
    url = "http://localhost:5000/api/seasonal-screener?min_gain=20&direction=gain"
    
    try:
        response = requests.get(url)
        data = response.json()
        stocks = data.get('stocks', [])
        kei = next((s for s in stocks if s['ticker'] == 'KEI'), None)
        
        if not kei:
            print("KEI not found at 20% filter.")
            return

        print(f"KEI Best Month: {kei['best_month']}")
        print(f"KEI Best Success Rate: {kei['best_month_success']}%")
        
        march_stats = next((m for m in kei['monthly_stats'] if m['month'] == 'March'), None)
        if march_stats:
            print(f"March Stats (at 20% Gain):")
            print(f"  Rallies: {march_stats['occurrences']}")
            print(f"  Success Rate: {march_stats['success_rate']}%")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_kei_march()

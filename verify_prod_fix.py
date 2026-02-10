import requests

def verify_fix():
    ticker = 'RELIANCE'
    base_url = f"https://unique-motivation-production.up.railway.app/api/seasonal/{ticker}"
    
    print(f"Verifying filter accuracy for {ticker}...")
    
    # Test with 2% gain
    print("Fetching with min_gain=2...")
    r1 = requests.get(f"{base_url}?min_gain=2", timeout=30)
    count1 = len(r1.json().get('moves', []))
    print(f"Moves found (2%): {count1}")
    
    # Test with 20% gain
    print("Fetching with min_gain=20...")
    r2 = requests.get(f"{base_url}?min_gain=20", timeout=30)
    count2 = len(r2.json().get('moves', []))
    print(f"Moves found (20%): {count2}")
    
    if count1 > count2:
        print("✅ SUCCESS: Filters are working independently in the cache!")
    else:
        print("❌ FAILURE: Data mismatch detected.")

if __name__ == "__main__":
    verify_fix()

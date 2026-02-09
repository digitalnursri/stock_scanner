import requests
import time
import os

BASE_URL = "http://127.0.0.1:5000"
API_URL = f"{BASE_URL}/api/seasonal-screener?min_gain=7"

def test_screener_api():
    print(f"Testing API: {API_URL}")
    
    # 1. First call (might be empty if no cache exists)
    print("\n[Test 1] First call...")
    try:
        start_time = time.time()
        response = requests.get(API_URL, timeout=10)
        duration = time.time() - start_time
        print(f"Status Code: {response.status_code}")
        print(f"Response Time: {duration:.2f}s")
        data = response.json()
        print(f"Status: {data.get('status')}")
        print(f"Stocks count: {len(data.get('stocks', []))}")
        
        if data.get('status') == 'calculating':
            print("Successfully triggered initial calculation.")
        elif data.get('status') in ['fresh', 'stale_updating']:
            print("Successfully retrieved cached data.")
            
    except Exception as e:
        print(f"Error in Test 1: {e}")

    # 2. Wait a bit for background thread
    print("\nWaiting 10 seconds for background thread to make some progress...")
    time.sleep(10)

    # 3. Second call (should return data if background thread finished or is working)
    print("\n[Test 2] Second call...")
    try:
        start_time = time.time()
        response = requests.get(API_URL, timeout=10)
        duration = time.time() - start_time
        print(f"Status Code: {response.status_code}")
        print(f"Response Time: {duration:.2f}s")
        data = response.json()
        print(f"Status: {data.get('status')}")
        print(f"Stocks count: {len(data.get('stocks', []))}")
        
    except Exception as e:
        print(f"Error in Test 2: {e}")

if __name__ == "__main__":
    # Ensure the app is running before testing
    # Note: User needs to run `python app.py` in a separate terminal
    test_screener_api()

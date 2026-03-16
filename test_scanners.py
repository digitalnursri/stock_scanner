import requests
import time

def inject_test_price():
    # To test this, we would ideally just hit an endpoint that forces a price change
    # But for now, we'll just check if the app is serving the pages
    pages = [
        "http://127.0.0.1:5000/vcp-scanner",
        "http://127.0.0.1:5000/accumulation-scanner",
        "http://127.0.0.1:5000/seasonal-screener",
        "http://127.0.0.1:5000/api/vcp-results",
        "http://127.0.0.1:5000/api/accumulation-scanner",
        "http://127.0.0.1:5000/api/seasonal-screener"
    ]
    
    for url in pages:
        try:
            print(f"Testing {url}...")
            r = requests.get(url, timeout=5)
            print(f"Status code: {r.status_code}")
            if r.status_code != 200:
                print(f"Warning: {url} returned {r.status_code}")
        except Exception as e:
            print(f"Error testing {url}: {e}")
            
if __name__ == "__main__":
    inject_test_price()

import requests
import sys

try:
    print("Testing /api/forecast/RELIANCE...")
    # Using local server
    response = requests.get('http://127.0.0.1:5000/api/forecast/RELIANCE')
    data = response.json()
    
    if 'error' in data:
        print(f"Error returned: {data['error']}")
        sys.exit(1)
        
    print("Keys found:", data.keys())
    print("Historical points:", len(data['historical_prices']))
    print("Forecast points:", len(data['forecast_prices']))
    print("Current Price:", data['current_price'])
    print("Forecast 12m:", data['forecast_12m'])
    
    # Check for nulls or NaNs
    if any(x is None for x in data['forecast_prices']):
        print("Warning: Null values in forecast prices!")
        
    print("Test Passed!")
except Exception as e:
    print(f"Test Failed: {e}")

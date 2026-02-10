from seasonal_analysis import predict_future_dates
import json

def test_prediction_consistency():
    ticker = 'RELIANCE'
    # Test with 7% gain and 100% success rate
    print(f"Testing {ticker} with Min Gain 7% and Min Success Rate 100%...")
    res = predict_future_dates(ticker, 7, 100)
    
    if 'error' in res:
        print(f"Error: {res['error']}")
        return

    predictions = res['predictions']
    print(f"Total predictions found: {len(predictions)}")
    
    for p in predictions:
        print(f"Month: {p['month']}, Confidence: {p['confidence']}%, Success Rate: {p['success_rate']}%")
        
    # Check February specifically
    feb = [p for p in predictions if p['month'] == 'February']
    if feb:
        print(f"✅ February is present with {feb[0]['confidence']}% confidence.")
    else:
        print("❌ February was filtered out (Success rate < 100%).")

if __name__ == "__main__":
    test_prediction_consistency()

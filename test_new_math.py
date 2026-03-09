import sys
import os
# Add current dir to path to import local modules
sys.path.append(os.getcwd())

from seasonal_analysis import analyze_seasonal_patterns_v2
import yfinance as yf
import pandas as pd

def verify_new_math():
    ticker = "KEI.NS"
    print(f"--- Analyzing {ticker} with New Consistency Math ---")
    
    # 10% Gain threshold
    result = analyze_seasonal_patterns_v2("KEI", 10)
    
    if 'error' in result:
        print(f"Error: {result['error']}")
        return

    print(f"Total Years Analyzed: {result.get('total_years')}")
    avail = result.get('month_availability', {})
    print(f"March Availability: {avail.get('March')}")
    
    march_stats = next((m for m in result['monthly_stats'] if m['month'] == 'March'), None)
    if march_stats:
        occ = march_stats['occurrences']
        succ = march_stats['success_rate']
        print(f"March Stats (at 10% Gain):")
        print(f"  Rallies Found: {occ}")
        print(f"  Reported Success Rate: {succ}%")
        
        # Manual check
        denom = avail.get('March', 10)
        expected_succ = round((occ / denom) * 100, 0)
        print(f"  Expected Success (Rallies / Avail): {expected_succ}%")
        
        if succ == expected_succ:
            print("✅ SUCCESS: Formula is using monthly availability correctly.")
        else:
            print("❌ FAILURE: Formula mismatch.")

if __name__ == "__main__":
    verify_new_math()

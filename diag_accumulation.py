
import yfinance as yf
import pandas as pd
import numpy as np
from accumulation_detector import analyze_single_stock

def diagnostic():
    tickers = ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS', 'AXISBANK.NS', 'SBIN.NS']
    print(f"Checking {len(tickers)} major stocks...")
    
    # Download data
    data = yf.download(tickers, period='90d', group_by='ticker', progress=False)
    
    for t in tickers:
        try:
            hist = data[t].dropna()
            if hist.empty:
                print(f"{t}: Error - No data")
                continue
            
            # Call analyze but manually extract the score even if it's < 4
            # To do this, we need to bypass the None return in analyze_single_stock
            # Let's see the rules one by one
            
            from accumulation_detector import calculate_atr, calculate_rsi
            
            close = hist['Close'].values
            high = hist['High'].values
            low = hist['Low'].values
            volume = hist['Volume'].values
            
            # Rule 1: Consolidation
            range_30 = ((max(close[-30:]) - min(close[-30:])) / min(close[-30:])) * 100
            
            # Rule 5: Support (DMA 50)
            dma_50 = pd.Series(close).rolling(window=50).mean().values
            above_dma = np.sum(close[-30:] > dma_50[-30:])
            
            # Final output for diagnostic
            res = analyze_single_stock(t, hist)
            tag = res['tag'] if res else "None (Score < 4)"
            score = res['score'] if res else "Unknown" # We'll need more info
            
            print(f"{t}: Tag={tag} | Range30={range_30:.1f}% | AboveDMA50={above_dma}/30 days")
            
        except Exception as e:
            print(f"{t}: Error - {e}")

if __name__ == '__main__':
    diagnostic()

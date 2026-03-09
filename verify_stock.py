import yfinance as yf
import pandas as pd
import numpy as np
from accumulation_detector import analyze_single_stock

def verify_stock(ticker):
    print(f"\n{'='*50}")
    print(f"VERIFICATION AUDIT FOR: {ticker}")
    print(f"{'='*50}")

    # 1. Fetch Raw Data
    print(f"\n[1/3] Fetching raw 90-day data from Yahoo Finance...")
    stock = yf.Ticker(ticker + ".NS")
    df = stock.history(period="90d")
    
    if df.empty:
        print("❌ Error: Could not fetch data. Check ticker symbol.")
        return

    current_price = df['Close'].iloc[-1]
    current_vol = df['Volume'].iloc[-1]
    
    print(f"   - Current Price: {current_price:.2f}")
    print(f"   - Current Volume: {current_vol}")

    # 2. Run Algorithm
    print(f"\n[2/3] Running Accumulation Algorithm...")
    result = analyze_single_stock(ticker, df)
    
    if not result:
        print("❌ Error: Analysis failed.")
        return

    print(f"   - Assigned Tag: {result['tag']}")
    print(f"   - Final Score: {result['score']}/{result['max_score']}")
    
    # 3. Detailed Rule Audit
    print(f"\n[3/3] Detailed Rule Breakdown:")
    d = result['details']
    
    print(f"\n--- RULE 1: Consolidation ---")
    print(f"   - 30D Range: {d['consolidation']['range_30d']}% (Target: 8-12%)")
    print(f"   - 60D Range: {d['consolidation']['range_60d']}%")
    print(f"   - Status: {'✅' if d['consolidation']['pass'] else '❌'}")

    print(f"\n--- RULE 2: ATR Trend (Volatility) ---")
    print(f"   - Recent ATR: {d['atr']['recent_atr']}")
    print(f"   - Previous ATR: {d['atr']['previous_atr']}")
    print(f"   - Status: {'✅ (Decreasing)' if d['atr']['pass'] else '❌ (Increasing)'}")

    print(f"\n--- RULE 3: Volume Accumulation ---")
    print(f"   - 20D Avg Vol: {d['volume']['avg_20d']}")
    print(f"   - 50D Avg Vol: {d['volume']['avg_50d']}")
    print(f"   - Ratio: {d['volume']['ratio']}x (Target: > 1.0)")
    print(f"   - Status: {'✅' if d['volume']['pass'] else '❌'}")

    print(f"\n--- RULE 5: Support (DMA 50) ---")
    print(f"   - Current DMA 50: {d['support']['current_dma50']}")
    print(f"   - % Above DMA: {d['support']['pct_above']}% (Target: > 70%)")
    print(f"   - Status: {'✅' if d['support']['pass'] else '❌'}")

    print(f"\n--- RULE 6: Hidden Buying ---")
    print(f"   - Days Detected: {d['hidden_buying']['days_detected']} (Target: >= 2)")
    print(f"   - Status: {'✅' if d['hidden_buying']['pass'] else '❌'}")

    print(f"\n--- RULE 7: RSI Stability ---")
    print(f"   - Current RSI: {d['rsi']['current_rsi']}")
    print(f"   - % Days in 40-60 Range: {d['rsi']['pct_in_range']}% (Target: >= 70%)")
    print(f"   - Status: {'✅' if d['rsi']['pass'] else '❌'}")

    print(f"\n--- RULE 8: Overextended Check ---")
    print(f"   - Max Move (30D): {d['overextended']['max_move_30d']}% (Target: <= 20%)")
    print(f"   - Status: {'✅' if d['overextended']['pass'] else '❌'}")

    print(f"\n{'='*50}")
    print("ACTION: Cross-check the 'Current Price', 'RSI', and 'DMA 50'")
    print("values with your Chart (TradingView/Zerodha).")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    ticker = input("Enter Ticker to verify (e.g. AXISBANK): ").upper()
    verify_stock(ticker)

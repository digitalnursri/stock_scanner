import yfinance as yf
import pandas as pd
from seasonal_analysis import analyze_seasonal_patterns_v2

def verify_stock(ticker, threshold):
    print(f"--- Verifying {ticker} at {threshold}% threshold ---")
    stock = yf.Ticker(f"{ticker}.NS")
    hist = stock.history(period="10y")
    
    analysis = analyze_seasonal_patterns_v2(ticker, threshold, hist_data=hist)
    
    if 'error' in analysis:
        print(f"Error: {analysis['error']}")
        return

    print(f"Total Years Analyzed: {len(hist)//252} (approx)")
    
    print(f"Monthly Stats summary:")
    for m_stats in analysis['monthly_stats']:
        print(f"  {m_stats['month']:<10}: {m_stats['occurrences']} rallies, Avg Gain: {m_stats['avg_gain']}%, Success: {m_stats['success_rate']}%")
    
    # Check moves for February
    feb_moves = [move for move in analysis['moves'] if move['start_month'] == 'February']
    print("\nIndividual February Moves:")
    for move in feb_moves:
        print(f"  Year: {move['start_year']}, Gain: {move['gain']}%, Duration: {move['duration']} days")
    else:
        print("No February moves found at this threshold.")

if __name__ == "__main__":
    verify_stock("FACT", 20)

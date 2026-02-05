from data_fetcher import get_nifty250_tickers, get_realtime_data
from tabulate import tabulate
import pandas as pd

def main():
    print("=========================================")
    print("   Indian Stock Market - Top 250 Stocks   ")
    print("=========================================")
    
    # 1. Get Tickers
    tickers = get_nifty250_tickers()
    if not tickers:
        print("Failed to fetch tickers.")
        return

    print(f"Total tickers found: {len(tickers)}")
    
    # 2. Get Data
    df = get_realtime_data(tickers)
    
    if df.empty:
        print("No data fetched.")
        return

    # 3. Display Data
    # Sort by Price? Or maybe just alphabetical? Let's sort by Ticker for now.
    df = df.sort_values(by='Ticker')
    
    print("\n")
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
    print("\n")

if __name__ == "__main__":
    main()

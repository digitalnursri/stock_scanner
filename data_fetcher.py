import nselib
from nselib import capital_market
import yfinance as yf
import pandas as pd

def get_nifty250_tickers():
    """
    Fetches the tickers for Nifty 250 companies.
    Nifty 250 = Nifty 100 (Nifty 50 + Nifty Next 50) + Nifty Midcap 150
    """
    try:
        print("Fetching Nifty 50 tickers...")
        nifty50 = capital_market.nifty50_equity_list()
        
        print("Fetching Nifty Next 50 tickers...")
        nifty_next50 = capital_market.niftynext50_equity_list()
        
        print("Fetching Nifty Midcap 150 tickers...")
        nifty_midcap150 = capital_market.niftymidcap150_equity_list()
        
        # Extract 'Symbol' column
        tickers = pd.concat([
            nifty50['Symbol'], 
            nifty_next50['Symbol'], 
            nifty_midcap150['Symbol']
        ]).unique()
        
        # Append .NS for yfinance
        ns_tickers = [f"{ticker}.NS" for ticker in tickers]
        
        return ns_tickers

    except Exception as e:
        print(f"Error fetching tickers via nselib: {e}")
        return []

import concurrent.futures

def calculate_rsi(series, period=14):
    """
    Calculates RSI using Simple Moving Average (SMA) as per Groww.in article.
    RS = Average Gain / Average Loss
    Average Gain = Sum of Gains over N periods / N
    """
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def get_market_cap(ticker):
    """Fetches market cap for a single ticker."""
    try:
        t = yf.Ticker(ticker)
        return t.fast_info['marketCap']
    except:
        return 0

def get_realtime_data(tickers):
    """
    Fetches real-time data and technicals (RSI, DMA) for the given tickers.
    """
    if not tickers:
        return pd.DataFrame()

    print(f"Fetching price data for {len(tickers)} stocks (1y history)...")
    # Fetch 1y history for DMAs
    try:
        data = yf.download(tickers, period="1y", group_by='ticker', progress=True, threads=True)
    except Exception as e:
        print(f"Error fetching batch data: {e}")
        return pd.DataFrame()

    # Fetch Market Caps in parallel
    print(f"Fetching Market Caps for {len(tickers)} stocks...")
    market_caps = {}
    
    # yfinance Tickers object can be used to get info for multiple stocks
    # however, we'll stick to ThreadPoolExecutor but increase workers and handle it slightly better
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        future_to_ticker = {executor.submit(get_market_cap, t): t for t in tickers}
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                market_caps[ticker] = future.result()
            except Exception:
                market_caps[ticker] = 0

    stock_list = []
    
    for ticker in tickers:
        try:
            # Handle multi-level columns if multiple tickers
            if isinstance(data.columns, pd.MultiIndex):
                if ticker in data.columns.levels[0]: # Verify ticker is in data
                    df = data[ticker].copy()
                else:
                    continue
            else:
                 # Single ticker case (unlikely with this list but safe to handle)
                df = data.copy() if data.shape[1] > 0 else pd.DataFrame()
            
            if df.empty:
                continue
                
            # Calculate Technicals
            # DMA - Use Adjusted Close for long term averages to account for splits/dividends
            # This is the industry standard "best approach" for historical moving averages.
            # Using 'Adj Close' if available, else 'Close'.
            price_series = df['Adj Close'] if 'Adj Close' in df else df['Close']
            
            df['DMA50'] = price_series.rolling(window=50).mean()
            df['DMA100'] = price_series.rolling(window=100).mean()
            df['DMA200'] = price_series.rolling(window=200).mean()
            
            # RSI - Typically calculated on standard 'Close'
            df['RSI'] = calculate_rsi(df['Close'])

            # Get latest valid row
            last_row = df.iloc[-1]
            
            # Extract values
            price = round(last_row['Close'], 2) if pd.notna(last_row['Close']) else 0
            rsi_val = round(last_row['RSI'], 2) if pd.notna(last_row['RSI']) else None
            dma50_val = round(last_row['DMA50'], 2) if pd.notna(last_row['DMA50']) else None
            dma100_val = round(last_row['DMA100'], 2) if pd.notna(last_row['DMA100']) else None
            dma200_val = round(last_row['DMA200'], 2) if pd.notna(last_row['DMA200']) else None
            
            # ===== PROFESSIONAL TECHNICAL ANALYSIS =====
            score = 0  # Positive = Bullish, Negative = Bearish
            reasons = []
            
            # 1. TREND ANALYSIS (Price vs DMAs)
            # Price above all DMAs = Strong Uptrend (+3)
            # Price below all DMAs = Strong Downtrend (-3)
            if dma50_val and dma100_val and dma200_val:
                if price > dma50_val and price > dma100_val and price > dma200_val:
                    score += 3
                    reasons.append("Strong Uptrend")
                elif price < dma50_val and price < dma100_val and price < dma200_val:
                    score -= 3
                    reasons.append("Strong Downtrend")
                elif price > dma200_val:
                    score += 1  # Above long-term trend
                elif price < dma200_val:
                    score -= 1  # Below long-term trend
            
            # 2. GOLDEN CROSS / DEATH CROSS (DMA50 vs DMA200)
            # Golden Cross (50 > 200) = Bullish (+2)
            # Death Cross (50 < 200) = Bearish (-2)
            if dma50_val and dma200_val:
                if dma50_val > dma200_val:
                    score += 2
                    if "Uptrend" not in str(reasons):
                        reasons.append("Golden Cross")
                else:
                    score -= 2
                    if "Downtrend" not in str(reasons):
                        reasons.append("Death Cross")
            
            # 3. RSI ANALYSIS (Momentum)
            if rsi_val:
                if rsi_val < 30:
                    score += 3  # Oversold - Strong Buy opportunity
                    reasons.append("Oversold")
                elif rsi_val < 40:
                    score += 1  # Approaching oversold
                elif rsi_val > 70:
                    score -= 3  # Overbought - Strong Sell signal
                    reasons.append("Overbought")
                elif rsi_val > 60:
                    score -= 1  # Approaching overbought
            
            # 4. PRICE MOMENTUM (Distance from DMA200)
            if dma200_val and price > 0:
                pct_from_200 = ((price - dma200_val) / dma200_val) * 100
                if pct_from_200 > 20:
                    score -= 1  # Too extended above 200 DMA
                elif pct_from_200 < -20:
                    score += 1  # Deeply discounted below 200 DMA
            
            # ===== DETERMINE SIGNAL =====
            if score >= 3:
                signal = "Bullish"
            elif score <= -3:
                signal = "Bearish"
            else:
                signal = "Neutral"
            
            # ===== PROFESSIONAL SUGGESTION =====
            # Strong Buy: Good fundamentals + Oversold + Uptrend potential
            # Buy: Bullish with good entry point
            # Hold: Mixed signals or already positioned
            # Sell: Bearish with exit indicators
            # Strong Sell: Overbought + Downtrend
            
            if score >= 5:
                suggestion = "Strong Buy"
            elif score >= 3:
                suggestion = "Buy"
            elif score >= 1:
                suggestion = "Hold"
            elif score >= -2:
                suggestion = "Hold"
            elif score >= -4:
                suggestion = "Sell"
            else:
                suggestion = "Strong Sell"
            
            # Format Market Cap
            mc = market_caps.get(ticker, 0)
            mc_formatted = f"{mc / 1e7:.2f} Cr" if mc else "N/A"

            stock_info = {
                'Ticker': ticker.replace('.NS', ''),
                'Price': price,
                'Open': round(last_row['Open'], 2) if pd.notna(last_row['Open']) else 0,
                'High': round(last_row['High'], 2) if pd.notna(last_row['High']) else 0,
                'Low': round(last_row['Low'], 2) if pd.notna(last_row['Low']) else 0,
                'Market Cap': mc_formatted,
                'RSI': rsi_val if rsi_val else 'N/A',
                'DMA 50': dma50_val if dma50_val else 'N/A',
                'DMA 100': dma100_val if dma100_val else 'N/A',
                'DMA 200': dma200_val if dma200_val else 'N/A',
                'Signal': signal,
                'Suggestion': suggestion,
                'Score': score,
            }
            stock_list.append(stock_info)
        except Exception as e:
            # print(f"Error processing {ticker}: {e}")
            continue
            
    return pd.DataFrame(stock_list)

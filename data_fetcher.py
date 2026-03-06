import nselib
from nselib import capital_market
import yfinance as yf
import pandas as pd
import concurrent.futures

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

def calculate_rsi(series, period=14):
    """Calculates RSI."""
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
    """Fetches real-time data and technicals (RSI, DMA) for the given tickers."""
    if not tickers: return pd.DataFrame()

    print(f"Fetching price data for {len(tickers)} stocks...")
    try:
        data = yf.download(tickers, period="1y", group_by='ticker', progress=False)
    except Exception as e:
        print(f"Error fetching batch data: {e}")
        return pd.DataFrame()

    print(f"Fetching Market Caps...")
    market_caps = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        future_to_ticker = {executor.submit(get_market_cap, t): t for t in tickers}
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try: market_caps[ticker] = future.result()
            except: market_caps[ticker] = 0

    stock_list = []
    for ticker in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if ticker in data.columns.levels[0]:
                    df = data[ticker].copy().dropna()
                else: continue
            else:
                df = data.copy().dropna()
            
            if len(df) < 50: continue
                
            price_series = df['Adj Close'] if 'Adj Close' in df else df['Close']
            df['DMA50'] = price_series.rolling(window=50).mean()
            df['DMA100'] = price_series.rolling(window=100).mean()
            df['DMA200'] = price_series.rolling(window=200).mean()
            df['RSI'] = calculate_rsi(df['Close'])

            last_row = df.iloc[-1]
            price = round(last_row['Close'], 2)
            rsi_val = round(last_row['RSI'], 2) if pd.notna(last_row['RSI']) else None
            dma50_val = round(last_row['DMA50'], 2) if pd.notna(last_row['DMA50']) else None
            dma200_val = round(last_row['DMA200'], 2) if pd.notna(last_row['DMA200']) else None
            
            score = 0
            if dma50_val and dma200_val:
                if price > dma50_val: score += 1
                if dma50_val > dma200_val: score += 2
            
            if rsi_val:
                if rsi_val < 30: score += 3
                elif rsi_val > 70: score -= 3

            signal = "Bullish" if score >= 3 else "Bearish" if score <= -3 else "Neutral"
            suggestion = "Strong Buy" if score >= 5 else "Buy" if score >= 3 else "Hold" if score >= -2 else "Sell"

            mc = market_caps.get(ticker, 0)
            mc_formatted = f"{mc / 1e7:.2f} Cr" if mc else "N/A"

            stock_list.append({
                'Ticker': ticker.replace('.NS', ''),
                'Price': price,
                'Market Cap': mc_formatted,
                'RSI': rsi_val if rsi_val else 'N/A',
                'DMA 50': dma50_val if dma50_val else 'N/A',
                'DMA 200': dma200_val if dma200_val else 'N/A',
                'Signal': signal,
                'Suggestion': suggestion,
                'Score': score,
            })
        except: continue
            
    return pd.DataFrame(stock_list)

def get_sector_rankings():
    """Ranks sectors based on 1m and 3m performance."""
    sector_map = {
        'NIFTY IT': '^CNXIT', 'NIFTY BANK': '^CNXBNK', 'NIFTY AUTO': '^CNXAUTO',
        'NIFTY PHARMA': '^CNXPHARMA', 'NIFTY FMCG': '^CNXFMCG', 'NIFTY METAL': '^CNXMETAL',
        'NIFTY REALTY': '^CNXREALTY', 'NIFTY ENERGY': '^CNXENERGY', 'NIFTY INFRA': '^CNXINFRA',
        'NIFTY MEDIA': '^CNXMEDIA', 'NIFTY PSE': '^CNXPSE'
    }
    rankings = []
    try:
        import yfinance as yf
        # Fetching each one separately is slow but more reliable for this specific logic
        for name, ticker in sector_map.items():
            try:
                s_df = yf.download(ticker, period="6mo", progress=False)
                if s_df.empty: continue
                
                close = s_df['Close'].dropna()
                if len(close) < 65: continue
                
                # Handle possible MultiIndex from single download
                if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]
                
                perf_1m = ((close.iloc[-1] - close.iloc[-21]) / close.iloc[-21]) * 100
                perf_3m = ((close.iloc[-1] - close.iloc[-63]) / close.iloc[-63]) * 100
                
                rankings.append({
                    'sector': name, 'ticker': ticker,
                    'perf_1m': round(float(perf_1m), 2),
                    'perf_3m': round(float(perf_3m), 2),
                    'score': round(float(perf_1m * 0.6 + perf_3m * 0.4), 2)
                })
            except: continue
        return sorted(rankings, key=lambda x: x['score'], reverse=True)
    except Exception as e:
        print(f"Error ranking sectors: {e}")
        return []

def get_market_trend():
    """Checks if Nifty 50 is above 50 DMA."""
    try:
        import yfinance as yf
        nifty = yf.download('^NSEI', period='6mo', progress=False)
        if nifty.empty: return {'nifty_price': 0, 'nifty_sma50': 0, 'trend_up': False, 'uptrend': False}
        
        close_series = nifty['Close']
        if isinstance(close_series, pd.DataFrame): close_series = close_series.iloc[:, 0]
            
        current_price = close_series.iloc[-1]
        sma50_series = close_series.rolling(window=50).mean()
        
        if len(sma50_series) < 5:
             return {'nifty_price': round(float(current_price), 2), 'nifty_sma50': 0, 'trend_up': False, 'uptrend': False}

        sma50 = sma50_series.iloc[-1]
        sma50_prev = sma50_series.iloc[-5]
        
        return {
            'nifty_price': round(float(current_price), 2),
            'nifty_sma50': round(float(sma50), 2),
            'trend_up': bool(current_price > sma50 and sma50 > sma50_prev),
            'uptrend': bool(current_price > sma50)
        }
    except Exception as e:
        print(f"Error in get_market_trend: {e}")
        return {'nifty_price': 0, 'nifty_sma50': 0, 'trend_up': False, 'uptrend': False}

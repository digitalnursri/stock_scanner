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

def get_live_stats(ticker):
    """Fetches real-time price and market cap for a single ticker using fast_info."""
    try:
        t = yf.Ticker(ticker)
        info = t.fast_info
        return {
            'price': info['lastPrice'],
            'marketCap': info['marketCap']
        }
    except:
        return {'price': 0, 'marketCap': 0}

def get_realtime_data(tickers, progress_callback=None, on_batch_complete=None, fetch_technicals=True):
    """
    Fetches real-time data for tickers.
    If fetch_technicals is True, downloads 1y data for RSI/DMAs.
    If False, performs a high-speed pass for Prices and Market Cap only via fast_info.
    """
    if not tickers: return pd.DataFrame()

    total_tickers = len(tickers)

    if not fetch_technicals:
        print(f"  [FAST-MODE] Starting bulk price download for {total_tickers} tickers...")
        try:
            # Single bulk download for all tickers is MUCH faster than individual fast_info calls
            # period="1d", interval="1m" gives us the current day's data
            data = yf.download(tickers, period="1d", interval="1m", progress=False, group_by='ticker', timeout=30)
            print(f"  [FAST-MODE] Bulk download complete.")
            
            all_stocks = []
            for ticker in tickers:
                try:
                    # Get last price from dataframe
                    if isinstance(data.columns, pd.MultiIndex):
                        if ticker in data.columns.levels[0]:
                            stock_df = data[ticker].dropna()
                            if stock_df.empty: continue
                            price = round(stock_df['Close'].iloc[-1], 2)
                        else: continue
                    else:
                        price = round(data['Close'].iloc[-1], 2)

                    stock_data = {
                        'Ticker': ticker.replace('.NS', ''),
                        'Price': price,
                        'Market Cap': "N/A", # fast_info is better for MC, but let's prioritize Price for speed
                        'FetchMode': 'Price'
                    }
                    all_stocks.append(stock_data)
                except: continue
            
            if on_batch_complete and all_stocks:
                on_batch_complete(all_stocks)
            return pd.DataFrame(all_stocks)
        except Exception as e:
            print(f"Error in fast bulk download: {e}")
            return pd.DataFrame()

    # Original Technicals logic (Chunked 1y download)
    print(f"Fetching full technical data for {total_tickers} stocks in chunks...")
    all_stocks = []
    chunk_size = 25
    
    for i in range(0, total_tickers, chunk_size):
        chunk = tickers[i:i + chunk_size]
        current_batch = i // chunk_size + 1
        total_batches = (total_tickers + chunk_size - 1) // chunk_size
        
        print(f"  Downloading chunk {current_batch}/{total_batches}...")
        
        # Report progress if callback is provided
        if progress_callback:
            try:
                progress_callback(current_batch, total_batches)
            except:
                pass

        batch_stocks = []
        data = None
        try:
            if fetch_technicals:
                print(f"  [DEBUG] Calling yf.download for {len(chunk)} tickers...")
                # Use a slightly longer timeout for the download itself
                data = yf.download(chunk, period="1y", group_by='ticker', progress=False, timeout=60)
                print(f"  [DEBUG] yf.download returned for batch {current_batch}")
            else:
                print(f"  [DEBUG] Skipping yf.download (Prices-Only mode) for batch {current_batch}")
            
            # Fetch Live Stats (Price & Market Cap) for this chunk
            live_stats = {}
            print(f"  [DEBUG] Fetching live stats for batch {current_batch} via GreenPool...")
            
            import eventlet
            pool = eventlet.GreenPool(10) # 10 workers
            
            def fetch_single_stat(t):
                try:
                    with eventlet.Timeout(10): # 10s timeout per ticker
                        stats = get_live_stats(t)
                        return t, stats
                except:
                    return t, {'price': 0, 'marketCap': 0}

            for ticker, stats in pool.imap(fetch_single_stat, chunk):
                live_stats[ticker] = stats
                print(f"    [DEBUG] Stats for {ticker}: Price={stats['price']}, MC={stats['marketCap']}")

            print(f"  [DEBUG] Processing {len(chunk)} tickers in batch {current_batch}...")
            for ticker in chunk:
                print(f"    [DEBUG] Processing {ticker} stats...")
                try:
                    stats = live_stats.get(ticker, {})
                    price = round(stats.get('price', 0), 2)
                    
                    rsi_val = None
                    dma50_val = None
                    dma100_val = None
                    dma200_val = None
                    score = 0
                    
                    if fetch_technicals and data is not None:
                        if isinstance(data.columns, pd.MultiIndex):
                            if ticker in data.columns.levels[0]:
                                df = data[ticker].copy().dropna(how='all')
                            else: continue
                        else:
                            df = data.copy().dropna(how='all')
                        
                        if len(df) >= 50:
                            price_series = df['Adj Close'] if 'Adj Close' in df else df['Close']
                            df['DMA50'] = price_series.rolling(window=50).mean()
                            df['DMA100'] = price_series.rolling(window=100).mean()
                            df['DMA200'] = price_series.rolling(window=200).mean()
                            df['RSI'] = calculate_rsi(df['Close'])

                            last_row = df.iloc[-1]
                            if price == 0: price = round(last_row['Close'], 2)
                            
                            rsi_val = round(last_row['RSI'], 2) if pd.notna(last_row['RSI']) else None
                            dma50_val = round(last_row['DMA50'], 2) if pd.notna(last_row['DMA50']) else None
                            dma100_val = round(last_row['DMA100'], 2) if pd.notna(last_row['DMA100']) else None
                            dma200_val = round(last_row['DMA200'], 2) if pd.notna(last_row['DMA200']) else None
                            
                            if dma50_val and dma200_val:
                                if price > dma50_val: score += 1
                                if dma50_val > dma200_val: score += 2
                            
                            if rsi_val:
                                if rsi_val < 30: score += 3
                                elif rsi_val > 70: score -= 3

                    signal = "Bullish" if score >= 3 else "Bearish" if score <= -3 else "Neutral"
                    suggestion = "Strong Buy" if score >= 5 else "Buy" if score >= 3 else "Hold" if score >= -2 else "Sell"

                    mc = stats.get('marketCap', 0)
                    mc_formatted = f"{mc / 1e7:.2f} Cr" if mc else "N/A"

                    stock_data = {
                        'Ticker': ticker.replace('.NS', ''),
                        'Price': price,
                        'Market Cap': mc_formatted,
                        'RSI': rsi_val if rsi_val is not None else 'N/A',
                        'DMA 50': dma50_val if dma50_val is not None else 'N/A',
                        'DMA 100': dma100_val if dma100_val is not None else 'N/A',
                        'DMA 200': dma200_val if dma200_val is not None else 'N/A',
                        'Signal': signal if fetch_technicals else 'KEEP',
                        'Suggestion': suggestion if fetch_technicals else 'KEEP',
                        'Score': score if fetch_technicals else 'KEEP',
                        'FetchMode': 'Full' if fetch_technicals else 'Price'
                    }
                    print(f"      [DEBUG] {ticker}: Price={price}, RSI={rsi_val}, Score={score}")
                    all_stocks.append(stock_data)
                    batch_stocks.append(stock_data)
                except Exception as e:
                    continue
            
            # Call batch complete callback if provided
            if on_batch_complete and batch_stocks:
                try:
                    on_batch_complete(batch_stocks)
                except:
                    pass

        except Exception as e:
            print(f"Error fetching batch data: {e}")
            continue

    return pd.DataFrame(all_stocks)

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

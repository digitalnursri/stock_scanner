"""
Penny Stock Scanner
-------------------
Scans Nifty 250 (and potentially others) for high-probability, fundamentally 
safe penny stocks (Price < ₹50). Applies multi-layer logic:
1. Fundamental Filter
2. Debt Filter
3. Liquidity Filter
4. Smart Money / Breakout Logic
5. Red Flags
"""

import yfinance as yf
import pandas as pd
import numpy as np
from data_fetcher import get_nifty250_tickers
import concurrent.futures

def get_penny_universe():
    """Returns a list of potential penny stock tickers (Nifty 250 + Smallcap 250 + Fallbacks)."""
    try:
        from data_fetcher import get_nifty250_tickers
        import nselib
        from nselib import capital_market
        
        tickers = get_nifty250_tickers() # This gets top 250
        
        # Combine multiple lists for maximum penny stock coverage
        try:
            smallcaps = capital_market.niftysmallcap250_equity_list()
            if not smallcaps.empty:
                tickers.extend([f"{t}.NS" for t in smallcaps['Symbol'].tolist()])
        except: pass

        try:
            nifty500 = capital_market.nifty500_equity_list()
            if not nifty500.empty:
                tickers.extend([f"{t}.NS" for t in nifty500['Symbol'].tolist()])
        except: pass
            
        # Standard fallback penny stocks that are popular
        fallbacks = ["IDEA.NS", "SUZLON.NS", "YESBANK.NS", "ZOMATO.NS", "SOUTHBANK.NS", 
                    "VIKASLIFE.NS", "JPPOWER.NS", "GTLINFRA.NS", "RPOWER.NS", "IFCI.NS",
                    "IBULHSGFIN.NS", "HUDCO.NS", "IREDA.NS", "RVNL.NS", "NHPC.NS", "SJVN.NS"]
        
        tickers.extend(fallbacks)
        
        # Unique and cleaned
        return list(set([t if t.endswith('.NS') else f"{t}.NS" for t in tickers]))
    except Exception as e:
        print(f"[DEBUG] get_penny_universe Error: {e}", flush=True)
        return ["IDEA.NS", "SUZLON.NS", "YESBANK.NS", "YESBANK.NS", "JPPOWER.NS", "RPOWER.NS"]

def calculate_atr(high, low, close, period=14):
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

def analyze_single_penny_stock(ticker, df, info=None):
    """
    Analyze a single stock for penny stock criteria.
    Args:
        ticker: Stock ticker symbol (without .NS)
        df: DataFrame with OHLCV data
        info: Optional pre-fetched yfinance info
    """
    if df is None or df.empty or len(df) < 60:
        return None
        
    try:
        df = df.dropna(subset=['Close', 'High', 'Low', 'Volume'])
        if len(df) < 60:
            return None
            
        close = df['Close'].values
        high = df['High'].values
        low = df['Low'].values
        volume = df['Volume'].values
        
        current_price = close[-1]
        
        # 0. Price Filter (Penny Stock check <= ₹50 or ₹100 for some leeway, let's stick to <= 50)
        if current_price > 50:
            return None
            
        # Fetch fundamental info via yfinance if not provided
        if info is None:
            try:
                stock = yf.Ticker(f"{ticker}.NS")
                info = stock.info
            except:
                info = {}
            
        # Extract Fundamentals
        market_cap = info.get('marketCap', 0)
        promoter_holding = info.get('heldPercentInsiders', 0)
        # Note: yfinance heldPercentInsiders is a fraction (e.g., 0.35 for 35%)
        # But sometimes it's missing. We'll be lenient if missing but strict if available.
        promoter_holding_pct = promoter_holding * 100 if promoter_holding else None
        
        revenue_growth = info.get('revenueGrowth', None)
        net_income = info.get('netIncomeToCommon', None)
        
        debt_to_equity = info.get('debtToEquity', None) # Note: expressed out of 100 or as a ratio? Usually out of 100 (e.g., 50 means 0.5)
        
        # Convert Debt to Equity strictly to ratio
        if debt_to_equity is not None:
            # yfinance sometimes returns 35.5 for 0.355 or 0.35 for 0.35
            # We'll assume if it's > 10 it means percentage (100 = 1.0)
            if debt_to_equity > 10:
                debt_ratio = debt_to_equity / 100
            else:
                debt_ratio = debt_to_equity
        else:
            debt_ratio = None
            
        ebitda = info.get('ebitda', None)
        interest_expense = info.get('interestExpense', None) # rarely available on free API
        
        if ebitda is not None and interest_expense is not None and interest_expense < 0:
            icr = ebitda / abs(interest_expense)
        else:
            icr = None

        # ==========================================
        # 1. Fundamental Filter
        # ==========================================
        market_cap_cr = market_cap / 1e7
        fund_pass = True
        fund_reasons = []
        
        if market_cap_cr > 0 and market_cap_cr < 300:
            fund_pass = False
            fund_reasons.append(f"M.Cap ({market_cap_cr:.0f}Cr) < 300Cr")
            
        if promoter_holding_pct is not None and promoter_holding_pct < 35:
            fund_pass = False
            fund_reasons.append(f"Promoter ({promoter_holding_pct:.1f}%) < 35%")
            
        if revenue_growth is not None and revenue_growth < 0:
            # We allow turnaround if net income is positive
            if net_income is None or net_income <= 0:
                fund_pass = False
                fund_reasons.append("Negative Sales & No Turnaround")
                
        # ==========================================
        # 2. Debt Filter (Critical)
        # ==========================================
        debt_pass = True
        debt_reasons = []
        
        if debt_ratio is not None:
            if debt_ratio >= 1:
                debt_pass = False
                debt_reasons.append(f"D/E Ratio ({debt_ratio:.2f}) >= 1.0")
            elif debt_ratio > 0.5:
                # Need turnaround or good ICR
                if icr is not None and icr < 2:
                    debt_pass = False
                    debt_reasons.append(f"D/E ({debt_ratio:.2f}) & ICR ({icr:.1f} < 2)")
                elif icr is None and net_income is not None and net_income <= 0:
                     debt_pass = False
                     debt_reasons.append(f"D/E ({debt_ratio:.2f}) & No Profit")
                     
        # Auto Reject Red Flag: High debt, low earnings visibility
        if not debt_pass or not fund_pass:
            # if strict filtering is enabled, we could return None here
            # but let's score it low instead so the user sees it in scanner
            pass

        # ==========================================
        # 3. Liquidity Filter
        # ==========================================
        avg_vol_20 = np.mean(volume[-20:])
        avg_vol_50 = np.mean(volume[-50:])
        
        liq_pass = True
        liq_reasons = []
        
        if avg_vol_50 < 500000:
            liq_pass = False
            liq_reasons.append(f"Low Vol ({int(avg_vol_50)})")
            
        zero_vol_days = np.sum(volume[-30:] == 0)
        if zero_vol_days >= 3:
            liq_pass = False
            liq_reasons.append(f"Illiquid ({zero_vol_days} zero-vol days)")
            
        # Cannot get Delivery % reliably
        
        if not liq_pass:
            score_penalty = 2
        else:
            score_penalty = 0

        # ==========================================
        # 4 & 5. Accumulation & Breakout Logic
        # ==========================================
        score = 0
        tag = 'Neutral'
        
        # Accumulation Check:
        last_30 = close[-30:]
        range_30 = ((max(last_30) - min(last_30)) / min(last_30)) * 100
        
        is_accumulation = False
        if range_30 <= 15: # Tight sideways range
            if avg_vol_20 > avg_vol_50: # Gradual vol increase
                is_accumulation = True
                score += 3
                tag = 'Accumulation'
                
        # Breakout Check:
        hist_30 = high[-30:-1]
        highest_high_30 = max(hist_30) if len(hist_30) > 0 else 0
        current_volume = volume[-1]
        
        is_breakout = False
        if current_price > highest_high_30 and current_volume > 2 * avg_vol_50:
            latest_candle_body = abs(close[-1] - df['Open'].iloc[-1])
            latest_candle_total = high[-1] - low[-1]
            if latest_candle_total > 0:
                body_ratio = latest_candle_body / latest_candle_total
                close_to_high = (high[-1] - close[-1]) / latest_candle_total
                # Strong bullish candle
                if body_ratio > 0.5 and close_to_high < 0.25 and close[-1] > df['Open'].iloc[-1]:
                    is_breakout = True
                    score += 5
                    tag = 'Breakout'
                    
        # Retest Check (Fake Breakout Filter):
        # We need historical highest high of 60 days to see if we are currently holding above it after a few days
        hist_60 = high[-60:-10]
        hh_60 = max(hist_60) if len(hist_60) > 0 else 0
        is_retest = False
        if not is_breakout and tag != 'Accumulation':
            if hh_60 and current_price > hh_60 and current_price < hh_60 * 1.05:
                # Sitting right above breakout level
                if avg_vol_20 < avg_vol_50: # low selling pressure
                    is_retest = True
                    score += 4
                    tag = 'Retest'
                       # ==========================================
        # 6. Red Flags
        # ==========================================
        if tag in ['Breakout', 'Retest']:
            # Base formation check
            range_before_breakout = ((max(close[-60:-5]) - min(close[-60:-5])) / min(close[-60:-5])) * 100
            if range_before_breakout > 40:
                # Sudden spike without proper base
                score -= 3
                fund_reasons.append("Sudden spike, no base")
                
        # Calculate final Confidence Score (1-10)
        confidence = 5 # base
        if fund_pass: confidence += 1
        if debt_pass: confidence += 2
        if liq_pass: confidence += 1
        confidence += (score / 2)
        confidence -= score_penalty
        
        confidence = max(1, min(10, int(confidence)))
        
        # Determine Entry, Stop Loss, Target
        if tag == 'Breakout' and highest_high_30:
            entry_zone = f"₹{highest_high_30:.1f} - ₹{highest_high_30 * 1.02:.1f}"
            sl = highest_high_30 * 0.93 # 7% below
            target = current_price * 1.15 # 15% target
        elif tag == 'Retest' and hh_60:
            entry_zone = f"₹{hh_60 * 0.99:.1f} - ₹{hh_60 * 1.02:.1f}"
            sl = min(low[-5:]) # Recent swing low
            target = current_price * 1.20
        elif tag == 'Accumulation':
            entry_zone = f"₹{current_price:.1f} - ₹{current_price * 1.02:.1f}"
            sl = min(low[-20:])
            target = max(high[-30:]) * 1.10
        else:
            entry_zone = "N/A"
            sl = current_price * 0.95
            target = current_price * 1.10
            
        # Return structured data - LOOSENED: No more auto-reject
        return {
            'ticker': ticker,
            'tag': tag,
            'confidence': confidence,
            'price': round(float(current_price), 2),
            'market_cap': f"{market_cap_cr:.0f} Cr" if market_cap_cr > 0 else "N/A",
            'mcap_cr': round(float(market_cap_cr), 1),
            'debt_to_equity': round(float(debt_ratio), 2) if debt_ratio is not None else 0, # Defaulting to 0 for frontend
            'd_e': round(float(debt_ratio), 2) if debt_ratio is not None else 0,
            'promoter_holding': round(float(promoter_holding_pct), 1) if promoter_holding_pct is not None else 0,
            'icr': round(float(icr), 1) if icr is not None else 0,
            'breakout_level': round(float(highest_high_30), 2) if tag == 'Breakout' else (round(float(hh_60), 2) if tag=='Retest' else 0),
            'entry_zone': entry_zone,
            'stop_loss': round(float(sl), 2),
            'target': round(float(target), 2),
            'fund_pass': fund_pass,
            'debt_pass': debt_pass,
            'liq_pass': liq_pass,
            'avg_vol': int(avg_vol_50),
            'reasons': ", ".join(fund_reasons + debt_reasons + liq_reasons)
        }
        
    except Exception:
        return None

def fetch_info(ticker):
    """Helper to fetch yfinance info in a thread."""
    try:
        return ticker, yf.Ticker(f"{ticker}.NS").info
    except:
        return ticker, {}

def scan_penny_stocks(tickers=None, callback=None, limit_for_test=None):
    """
    Main entry point for penny stock scanner.
    """
    print(f"[DEBUG] scan_penny_stocks: Called with {len(tickers) if tickers else 'None'} tickers")
    if not tickers:
        print("[DEBUG] scan_penny_stocks: Fetching penny stock universe...")
        tickers = get_penny_universe()
        tickers = list(set(tickers)) # Unique
    
    if limit_for_test:
        tickers = tickers[:limit_for_test]
        
    total_tickers = len(tickers)
    print(f"[DEBUG] scan_penny_stocks: Universe fetched, count={total_tickers}", flush=True)

    if callback:
        print("[DEBUG] scan_penny_stocks: Sending initial 0/Total progress", flush=True)
        callback(0, total_tickers, [])
        
    # Ensure all have .NS suffix for batch download
    tickers = [t if t.endswith('.NS') else f"{t}.NS" for t in tickers]
    
    if not tickers:
        return {'error': 'Failed to fetch ticker list', 'stocks': []}
        
    # Extra smaller caps because penny stocks might not be in Nifty 250
    # The app has access to capital_market.nifty500_equity_list etc. if required
    # We will just process what is given in tickers.
    
    results = []
    
    batch_size = 25
    total_batches = (total_tickers + batch_size - 1) // batch_size
    print(f"[DEBUG] scan_penny_stocks: Starting loop with batch_size={batch_size}, total_batches={total_batches}", flush=True)
    
    for i in range(0, total_tickers, batch_size):
        try:
            print(f"[DEBUG] scan_penny_stocks: Batch {i}-{min(i+batch_size, total_tickers)}...", flush=True)
            batch = tickers[i:min(i + batch_size, total_tickers)]
            print(f"[DEBUG] scan_penny_stocks: Starting yf.download for {len(batch)} tickers...", flush=True)
            # disabling threads as they might conflict with eventlet
            data = yf.download(
                batch, period="100d", interval="1d",
                group_by='ticker', progress=False, threads=False, timeout=20
            )
            print(f"[DEBUG] scan_penny_stocks: Finished yf.download", flush=True)

            # Prefetch info in parallel to speed up analyze_single_penny_stock
            print(f"[DEBUG] scan_penny_stocks: Prefetching info for batch...", flush=True)
            batch_info = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as info_executor:
                clean_batch = [t.replace('.NS', '') for t in batch]
                futures = [info_executor.submit(fetch_info, t) for t in clean_batch]
                for future in concurrent.futures.as_completed(futures):
                    t, info = future.result()
                    batch_info[t] = info

            if data.empty:
                print(f"[DEBUG] scan_penny_stocks: yf.download returned empty for batch {i}", flush=True)
                if callback:
                    callback(min(i + batch_size, total_tickers), total_tickers, results)
                continue
            
            print(f"[DEBUG] scan_penny_stocks: Downloaded batch {i}, columns shape: {data.shape}", flush=True)
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_ticker = {}
                for ticker in batch:
                    try:
                        if isinstance(data.columns, pd.MultiIndex):
                            if ticker in data.columns.get_level_values(0):
                                hist = data[ticker].dropna(how='all')
                            else: continue
                        else:
                            hist = data.dropna(how='all')
                            
                        if hist.empty or hist['Close'].iloc[-1] > 100:
                            continue
                            
                        clean_ticker = ticker.replace('.NS', '')
                        
                        # Use prefetched info
                        info = batch_info.get(clean_ticker, {})
                        
                        # Modified analyze_single_penny_stock logic here or pass info to it
                        # Since I can't easily change the signature without changing all calls, 
                        # I'll modify analyze_single_penny_stock to take optional info
                        future = executor.submit(analyze_single_penny_stock, clean_ticker, hist, info)
                        future_to_ticker[future] = clean_ticker
                    except Exception:
                        continue
                        
                for future in concurrent.futures.as_completed(future_to_ticker):
                    try:
                        res = future.result()
                        if res:
                            results.append(res)
                    except Exception:
                        continue
                        
            if callback:
                callback(min(i + batch_size, total_tickers), total_tickers, results)
                
        except Exception as e:
            print(f"[DEBUG] scan_penny_stocks: Error in batch {i}: {e}", flush=True)
            continue
            
    # Sort by confidence
    results.sort(key=lambda x: x['confidence'], reverse=True)
    
    return {
        'stocks': results,
        'total_scanned': total_tickers,
        'total_matched': len(results),
        'breakdowns': {
            'breakout': sum(1 for r in results if r['tag'] == 'Breakout'),
            'retest': sum(1 for r in results if r['tag'] == 'Retest'),
            'accumulation': sum(1 for r in results if r['tag'] == 'Accumulation')
        }
    }

if __name__ == '__main__':
    test_tickers = ['SUZLON.NS', 'YESBANK.NS', 'IDEA.NS', 'GTLINFRA.NS', 'RPOWER.NS', 'IRFC.NS']
    print("Testing Penny Scanner...")
    res = scan_penny_stocks(test_tickers)
    for stock in res.get('stocks', []):
        t = stock['ticker']
        tag = stock['tag']
        conf = stock['confidence']
        price = stock['price']
        de = stock['debt_to_equity']
        entry = stock['entry_zone']
        print(f"{t} | {tag} | Score: {conf} | Price: {price} | D/E: {de} | Entry: {entry}")

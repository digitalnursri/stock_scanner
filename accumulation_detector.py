"""
Operator Accumulation Pattern Detector
---------------------------------------
Scans Nifty 250 stocks for operator accumulation patterns.

Tags stocks as:
  - Accumulation Zone (score >= 4)
  - Pre-Breakout (score >= 6, no breakout yet)
  - Breakout (breakout condition met)
"""

import yfinance as yf
import pandas as pd
import numpy as np
from data_fetcher import get_nifty250_tickers

def calculate_atr(high, low, close, period=14):
    """Calculate Average True Range."""
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

def calculate_rsi(series, period=14):
    """Calculate RSI."""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_single_stock(ticker, df):
    """
    Analyze a single stock for accumulation patterns.
    
    Args:
        ticker: Stock ticker symbol (without .NS)
        df: DataFrame with OHLCV data (at least 60 rows)
    
    Returns:
        dict with analysis results or None if insufficient data
    """
    if df is None or df.empty or len(df) < 60:
        return None
    
    try:
        # Make sure we work with clean data
        df = df.dropna(subset=['Close', 'High', 'Low', 'Volume'])
        if len(df) < 60:
            return None
        
        close = df['Close'].values
        high = df['High'].values
        low = df['Low'].values
        volume = df['Volume'].values
        
        # Current values
        current_price = close[-1]
        current_volume = volume[-1]
        
        score = 0
        max_score = 8
        details = {}
        
        # ===== RULE 1: Consolidation Phase =====
        # Price should remain within 8-12% range for last 30-60 days
        last_60 = close[-60:]
        last_30 = close[-30:]
        
        range_60 = ((max(last_60) - min(last_60)) / min(last_60)) * 100
        range_30 = ((max(last_30) - min(last_30)) / min(last_30)) * 100
        
        consolidation_pass = False
        consolidation_range = range_30
        
        if range_30 <= 12:
            consolidation_pass = True
            consolidation_range = range_30
        elif range_60 <= 12:
            consolidation_pass = True
            consolidation_range = range_60
        
        if consolidation_pass:
            score += 1
        
        details['consolidation'] = {
            'pass': bool(consolidation_pass),
            'range_30d': round(float(range_30), 1),
            'range_60d': round(float(range_60), 1)
        }
        
        # ===== RULE 2: Low Volatility (ATR decreasing) =====
        atr_series = calculate_atr(
            pd.Series(high), pd.Series(low), pd.Series(close), period=14
        )
        atr_values = atr_series.dropna().values
        
        atr_decreasing = False
        atr_recent = 0
        atr_previous = 0
        
        if len(atr_values) >= 14:
            atr_recent = np.mean(atr_values[-7:])     # Last 7 ATR values
            atr_previous = np.mean(atr_values[-14:-7]) # Previous 7 ATR values
            atr_decreasing = atr_recent < atr_previous
            if atr_decreasing:
                score += 1
        
        details['atr'] = {
            'pass': bool(atr_decreasing),
            'recent_atr': round(float(atr_recent), 2),
            'previous_atr': round(float(atr_previous), 2)
        }
        
        # ===== RULE 3: Volume Accumulation =====
        # 20-day avg volume > 50-day avg volume
        vol_20 = np.mean(volume[-20:])
        vol_50 = np.mean(volume[-50:])
        vol_ratio = vol_20 / vol_50 if vol_50 > 0 else 0
        vol_accumulation = vol_20 > vol_50
        
        if vol_accumulation:
            score += 1
        
        details['volume'] = {
            'pass': bool(vol_accumulation),
            'avg_20d': int(vol_20),
            'avg_50d': int(vol_50),
            'ratio': round(float(vol_ratio), 2)
        }
        
        # ===== RULE 4: Delivery % (skip if unavailable) =====
        # Delivery data is hard to get from free APIs
        # We'll mark as unavailable and score out of 7
        delivery_available = False
        delivery_pass = False
        
        details['delivery'] = {
            'pass': delivery_pass,
            'available': delivery_available,
            'note': 'Delivery data not available via free API'
        }
        
        if not delivery_available:
            max_score = 7  # Score out of 7 instead of 8
        
        # ===== RULE 5: Support Strength =====
        # Price should stay above 50 DMA for most of the period
        dma_50 = pd.Series(close).rolling(window=50).mean().values
        
        last_30_close = close[-30:]
        last_30_dma50 = dma_50[-30:]
        
        # Count days where price > DMA 50
        valid_dma = ~np.isnan(last_30_dma50)
        if valid_dma.any():
            above_dma = np.sum(last_30_close[valid_dma] > last_30_dma50[valid_dma])
            total_valid = np.sum(valid_dma)
            support_pct = (above_dma / total_valid) * 100 if total_valid > 0 else 0
        else:
            above_dma = 0
            total_valid = 0
            support_pct = 0
        
        support_pass = support_pct >= 70
        if support_pass:
            score += 1
        
        details['support'] = {
            'pass': bool(support_pass),
            'days_above_dma50': int(above_dma),
            'total_days': int(total_valid),
            'pct_above': round(float(support_pct), 1),
            'current_dma50': round(float(dma_50[-1]), 2) if not np.isnan(dma_50[-1]) else 0
        }
        
        # ===== RULE 6: Hidden Buying Signal =====
        # Days where volume > 1.5x avg but price change < 2%
        avg_vol_30 = np.mean(volume[-30:])
        hidden_buy_days = 0
        
        for i in range(-30, 0):
            if volume[i] > 1.5 * avg_vol_30:
                pct_change = abs((close[i] - close[i - 1]) / close[i - 1]) * 100
                if pct_change < 2:
                    hidden_buy_days += 1
        
        hidden_buy_pass = hidden_buy_days >= 2
        if hidden_buy_pass:
            score += 1
        
        details['hidden_buying'] = {
            'pass': bool(hidden_buy_pass),
            'days_detected': int(hidden_buy_days)
        }
        
        # ===== RULE 7: RSI Stability =====
        # RSI(14) between 40-60 for >= 70% of last 30 days
        rsi_series = calculate_rsi(pd.Series(close), period=14)
        rsi_values = rsi_series.values
        
        last_30_rsi = rsi_values[-30:]
        valid_rsi = ~np.isnan(last_30_rsi)
        
        if valid_rsi.any():
            rsi_in_range = np.sum(
                (last_30_rsi[valid_rsi] >= 40) & (last_30_rsi[valid_rsi] <= 60)
            )
            total_valid_rsi = np.sum(valid_rsi)
            rsi_stability_pct = (rsi_in_range / total_valid_rsi) * 100 if total_valid_rsi > 0 else 0
        else:
            rsi_stability_pct = 0
        
        rsi_pass = rsi_stability_pct >= 70
        current_rsi = rsi_values[-1] if not np.isnan(rsi_values[-1]) else 0
        
        if rsi_pass:
            score += 1
        
        details['rsi'] = {
            'pass': bool(rsi_pass),
            'current_rsi': round(float(current_rsi), 1),
            'pct_in_range': round(float(rsi_stability_pct), 1)
        }
        
        # ===== RULE 8: Not Overextended =====
        # Stock should not move more than 20% in last 30 days
        max_move_30 = ((max(last_30) - min(last_30)) / min(last_30)) * 100
        not_overextended = max_move_30 <= 20
        
        if not_overextended:
            score += 1
        
        details['overextended'] = {
            'pass': bool(not_overextended),
            'max_move_30d': round(float(max_move_30), 1)
        }
        
        # ===== BREAKOUT CONDITION =====
        # Close > Highest High of last 30 days AND Volume > 2x average volume
        highest_high_30 = max(high[-30:])
        avg_volume_30 = np.mean(volume[-30:])
        
        breakout = (
            current_price > highest_high_30 and 
            current_volume > 2 * avg_volume_30
        )
        
        details['breakout'] = {
            'triggered': bool(breakout),
            'highest_high_30d': round(float(highest_high_30), 2),
            'current_price': round(float(current_price), 2),
            'volume_ratio': round(float(current_volume / avg_volume_30), 2) if avg_volume_30 > 0 else 0,
            'volume_threshold': '2x'
        }
        
        # ===== TAGGING =====
        if breakout:
            tag = 'Breakout'
        elif score >= 6:
            tag = 'Pre-Breakout'
        elif score >= 4:
            tag = 'Accumulation Zone'
        else:
            tag = 'Neutral'
        
        # Flatten details for frontend Rule Dots
        rules = {
            'Consolidation Phase': details['consolidation']['pass'],
            'Low Volatility (ATR decreasing)': details['atr']['pass'],
            'Volume Accumulation': details['volume']['pass'],
            'Delivery %': details['delivery']['pass'],
            'Support Strength': details['support']['pass'],
            'Hidden Buying Signal': details['hidden_buying']['pass'],
            'RSI Stability': details['rsi']['pass'],
            'Not Overextended': details['overextended']['pass'],
            'Breakout': details['breakout']['triggered']
        }
        
        return {
            'ticker': ticker,
            'tag': tag,
            'score': int(score),
            'max_score': int(max_score),
            'price': round(float(current_price), 2),
            'price_range_30d': round(float(range_30), 1),
            'atr_trend': 'Decreasing' if atr_decreasing else 'Increasing',
            'vol_ratio': round(float(vol_ratio), 2),
            'rsi': round(float(current_rsi), 1),
            'dma50_status': 'Above' if (not np.isnan(dma_50[-1]) and current_price > dma_50[-1]) else 'Below',
            'hidden_buy_days': int(hidden_buy_days),
            'details': details,
            'rules': rules
        }
    
    except Exception as e:
        # print(f"Error analyzing {ticker}: {e}")
        return None


import concurrent.futures

def scan_accumulation(tickers=None, callback=None):
    """
    Scan all given tickers for accumulation patterns in parallel.
    
    Args:
        tickers: List of tickers with .NS suffix. If None, fetches Nifty 250.
        callback: Optional function called with (current, total, results_so_far)
    
    Returns:
        dict with 'stocks' list and metadata
    """
    if tickers is None:
        tickers = get_nifty250_tickers()
    
    if not tickers:
        return {'error': 'Failed to fetch ticker list', 'stocks': []}
    
    total_tickers = len(tickers)
    results = []
    
    # Process in larger batches to maximize yfinance throughput
    # but still allow progress updates
    batch_size = 50
    
    for i in range(0, total_tickers, batch_size):
        batch = tickers[i:min(i + batch_size, total_tickers)]
        
        try:
            # log progress
            msg = f"Scanning batch {i//batch_size + 1}... ({len(batch)} stocks)"
            with open('scanner_progress.txt', 'a') as f:
                f.write(f"[{pd.Timestamp.now()}] {msg}\n")
            
            # Batch download 90 days of daily data
            # Use a single download call for the entire batch
            data = yf.download(
                batch, period="90d", interval="1d",
                group_by='ticker', progress=False, threads=True, timeout=30
            )
            
            # Process results in parallel using ThreadPoolExecutor
            # The calculation is relatively fast, but parallelizing ensures 
            # we don't block the loop.
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_ticker = {}
                for ticker in batch:
                    # Extract ticker data from batch
                    try:
                        if isinstance(data.columns, pd.MultiIndex):
                            if ticker in data.columns.get_level_values(0):
                                hist = data[ticker].dropna(how='all')
                            else:
                                continue
                        else:
                            hist = data.dropna(how='all')
                        
                        if hist.empty or len(hist) < 60:
                            continue
                        
                        clean_ticker = ticker.replace('.NS', '')
                        future = executor.submit(analyze_single_stock, clean_ticker, hist)
                        future_to_ticker[future] = clean_ticker
                    except Exception:
                        continue
                
                for future in concurrent.futures.as_completed(future_to_ticker):
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                    except Exception:
                        continue
            
            if callback:
                callback(min(i + batch_size, total_tickers), total_tickers, results)
                
        except Exception as e:
            with open('scanner_progress.txt', 'a') as f:
                f.write(f"[{pd.Timestamp.now()}] Error in batch processing: {e}\n")
            continue
            
    with open('scanner_progress.txt', 'a') as f:
        f.write(f"[{pd.Timestamp.now()}] Scan complete. Matched {len(results)} stocks.\n")
    
    # Sort: Breakout first, then Pre-Breakout, then Accumulation Zone, then Neutral
    tag_order = {'Breakout': 0, 'Pre-Breakout': 1, 'Accumulation Zone': 2, 'Neutral': 3}
    results.sort(key=lambda x: (tag_order.get(x['tag'], 4), -x['score']))
    
    return {
        'stocks': results,
        'total_scanned': total_tickers,
        'total_matched': len(results),
        'breakdown': {
            'breakout': sum(1 for r in results if r['tag'] == 'Breakout'),
            'pre_breakout': sum(1 for r in results if r['tag'] == 'Pre-Breakout'),
            'accumulation': sum(1 for r in results if r['tag'] == 'Accumulation Zone')
        }
    }


if __name__ == '__main__':
    # Quick test with a small set
    test_tickers = ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS']
    print("Testing Accumulation Detector with 5 stocks...")
    result = scan_accumulation(test_tickers)
    
    print(f"\nScanned: {result['total_scanned']}")
    print(f"Matched: {result['total_matched']}")
    print(f"Breakdown: {result['breakdown']}")
    
    for stock in result['stocks']:
        print(f"\n  {stock['ticker']}: {stock['tag']} (Score: {stock['score']}/{stock['max_score']})")
        print(f"    Price: ₹{stock['price']} | Range: {stock['price_range_30d']}%")
        print(f"    ATR: {stock['atr_trend']} | Vol Ratio: {stock['vol_ratio']}")
        print(f"    RSI: {stock['rsi']} | DMA50: {stock['dma50_status']}")
        
        for rule, info in stock['details'].items():
            status = '✅' if info.get('pass') or info.get('triggered') else '❌'
            print(f"    {status} {rule}")

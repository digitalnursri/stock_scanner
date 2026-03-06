import pandas as pd
import numpy as np

def calculate_vcp_score(df, ticker):
    """
    Implements SMMS-VCP Strategy Logic:
    1. Trend Check (Nifty 50 and Stock 50 DMA)
    2. VCP Contraction detection (Successive pullbacks getting smaller)
    3. Resistance proximity
    4. Volume contraction during consolidation
    5. Scoring and Trade Parameters (Entry, SL, Target)
    """
    if df is None or len(df) < 50:
        return None

    details = {}
    score = 0
    max_score = 7

    # 1. Moving Average Trend Filter (Rule 5)
    # Price > 50 DMA and 50 DMA is trending up
    df['SMA50'] = df['Close'].rolling(window=50).mean()
    df['SMA200'] = df['Close'].rolling(window=200).mean()
    
    current_price = float(df['Close'].iloc[-1])
    current_sma50 = float(df['SMA50'].iloc[-1])
    prev_sma50 = float(df['SMA50'].iloc[-5]) # 1 week ago
    
    sma50_trending_up = current_sma50 > prev_sma50
    above_sma50 = current_price > current_sma50
    
    details['trend'] = {
        'pass': bool(above_sma50 and sma50_trending_up),
        'current_price': current_price,
        'sma50': round(current_sma50, 2),
        'trending_up': bool(sma50_trending_up)
    }
    if details['trend']['pass']: score += 1

    # 2. Consolidation & Resistance (Rule 3 & 6)
    # Identify resistance in last 40 days
    recent_df = df.tail(40)
    resistance = float(recent_df['High'].max())
    dist_from_res = ((resistance - current_price) / current_price) * 100
    
    details['resistance'] = {
        'level': round(resistance, 2),
        'pct_below': round(dist_from_res, 2),
        'pass': bool(dist_from_res <= 5.0 and dist_from_res >= -1.0) # Near or just breaking
    }
    if details['resistance']['pass']: score += 1

    # 3. Volatility Contraction Pattern (VCP) (Rule 4)
    # Measure pullbacks in the base
    # Simple VCP check: Successive 10-day volatility (Hi-Lo range) should decrease
    vol_1 = ((df['High'].iloc[-40:-30].max() - df['Low'].iloc[-40:-30].min()) / df['Low'].iloc[-40:-30].min()) * 100
    vol_2 = ((df['High'].iloc[-30:-20].max() - df['Low'].iloc[-30:-20].min()) / df['Low'].iloc[-30:-20].min()) * 100
    vol_3 = ((df['High'].iloc[-20:-10].max() - df['Low'].iloc[-20:-10].min()) / df['Low'].iloc[-20:-10].min()) * 100
    vol_4 = ((df['High'].iloc[-10:].max() - df['Low'].iloc[-10:].min()) / df['Low'].iloc[-10:].min()) * 100
    
    contraction = vol_4 < vol_3 or vol_4 < vol_2
    
    details['vcp'] = {
        'vol_stages': [round(vol_1, 2), round(vol_2, 2), round(vol_3, 2), round(vol_4, 2)],
        'pass': bool(contraction),
        'tightness': round(vol_4, 2)
    }
    if details['vcp']['pass']: score += 1
    if vol_4 < 4.0: score += 1 # Bonus for extreme tightness

    # 4. Volume Contraction (Rule 7)
    # Volume during consolidation should be lower than average
    avg_vol_20 = float(df['Volume'].tail(20).mean())
    avg_vol_50 = float(df['Volume'].tail(50).mean())
    vol_dry_up = avg_vol_20 < avg_vol_50
    
    details['volume'] = {
        'avg_20d': int(avg_vol_20),
        'avg_50d': int(avg_vol_50),
        'ratio': round(avg_vol_20 / avg_vol_50, 2),
        'pass': bool(vol_dry_up)
    }
    if details['volume']['pass']: score += 1

    # 5. Relative Strength (Internal vs Nifty - Placeholder for now)
    score += 1 # Placeholder for sector strength logic

    # 6. Breakout Confirmation (Rule 9 modified for optimal entry)
    # Strong bullish candle check
    last_open = float(df['Open'].iloc[-1])
    last_close = float(df['Close'].iloc[-1])
    last_high = float(df['High'].iloc[-1])
    last_low = float(df['Low'].iloc[-1])
    
    candle_size = last_high - last_low
    body_size = last_close - last_open
    
    # Strong Body: Close > Open and body is at least 50% of the entire candle range
    is_bullish_candle = (last_close > last_open) and (candle_size > 0) and (body_size >= 0.5 * candle_size)
    
    # Valid Breakout: Price > Resistance, Vol >= 2x 20-day avg, Strong Bullish Candle
    confirmed_breakout = (current_price > resistance) and (float(df['Volume'].iloc[-1]) >= 2.0 * avg_vol_20) and is_bullish_candle
    
    # 7. Optimal Buy Entry Calculation
    # Enter between 0.5% and 1% above resistance
    buy_price_min = round(resistance * 1.005, 2)
    buy_price_max = round(resistance * 1.010, 2)
    buy_stop_price = round(resistance * 1.008, 2)
    
    # Ensure no early entry: score adjustment if premature
    if not confirmed_breakout and current_price > resistance:
        # Penalize if it's breaking out but without volume or bullish candle (false breakout risk)
        score -= 1 

    # 8. Trade Parameters (Rule 10)
    avg_entry = (buy_price_min + buy_price_max) / 2
    stop_loss = round(avg_entry * 0.925, 2) # 7.5% below entry
    target_low = round(avg_entry * 1.15, 2) # 15% Target
    target_high = round(avg_entry * 1.40, 2) # 40% Target

    tag = "Neutral"
    if confirmed_breakout:
        tag = "Confirmed Breakout"
    elif score >= 5:
        tag = "High Probability VCP"
    elif score >= 3:
        tag = "VCP Forming"

    result = {
        'ticker': ticker,
        'tag': tag,
        'score': score,
        'max_score': max_score,
        'price': round(float(current_price), 2),
        'resistance': round(resistance, 2),
        'recommended_buy_price': f"₹{buy_price_min} - ₹{buy_price_max}",
        'buy_stop_price': buy_stop_price,
        'stop_loss': stop_loss,
        'target': f"₹{target_low} - ₹{target_high}",
        'details': details
    }

    return sanitize_data(result)

def sanitize_data(data):
    """Recursively replace NaN values with None for JSON compatibility."""
    import math
    if isinstance(data, dict):
        return {k: sanitize_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_data(v) for v in data]
    elif isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
    return data

from flask import Flask, render_template, jsonify
from data_fetcher import get_nifty250_tickers, get_realtime_data
import pandas as pd
import time
import threading

app = Flask(__name__)

import os
import json
import threading
from datetime import datetime, timedelta

# Global cache
CACHE = {
    "data": None,
    "last_updated": 0,
    "lock": threading.Lock()
}

CACHE_DURATION = 300  # 5 minutes
SEASONAL_CACHE_FILE = 'seasonal_screener_cache.json'
SEASONAL_CACHE_TTL = 86400  # 24 hours
ANALYTICS_CACHE_DIR = 'analytics_cache'

# Ensure analytics cache dir exists
if not os.path.exists(ANALYTICS_CACHE_DIR):
    os.makedirs(ANALYTICS_CACHE_DIR)

def get_analytics_cache(ticker, category):
    """Simple file-based cache for expensive analytics"""
    cache_path = os.path.join(ANALYTICS_CACHE_DIR, f"{ticker}_{category}.json")
    if os.path.exists(cache_path):
        mtime = os.path.getmtime(cache_path)
        if (time.time() - mtime) < SEASONAL_CACHE_TTL:
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except:
                pass
    return None

def save_analytics_cache(ticker, category, data):
    cache_path = os.path.join(ANALYTICS_CACHE_DIR, f"{ticker}_{category}.json")
    try:
        with open(cache_path, 'w') as f:
            json.dump(data, f)
    except:
        pass

def get_market_data():
    """
    Fetches data from source or cache.
    """
    with CACHE["lock"]:
        if CACHE["data"] is not None:
            # If data is stale, trigger background update but return stale data
            if (time.time() - CACHE["last_updated"] > CACHE_DURATION):
                is_running = any(t.name == "MainCacheUpdater" for t in threading.enumerate())
                if not is_running:
                    threading.Thread(target=refresh_main_cache, name="MainCacheUpdater", daemon=True).start()
            return CACHE["data"]
            
    # Initial fetch if cache is empty
    return refresh_main_cache()

def refresh_main_cache():
    """Synchronous cache refresh for initial load or background thread"""
    # print("Refreshing main cache...")
    tickers = get_nifty250_tickers()
    df = get_realtime_data(tickers)
    
    if not df.empty:
        df = df.sort_values(by='Ticker')
        data = df.to_dict(orient='records')
        
        with CACHE["lock"]:
            CACHE["data"] = data
            CACHE["last_updated"] = time.time()
        return data
    return []

@app.route('/')
def index():
    stocks = get_market_data()
    return render_template('index.html', stocks=stocks)

@app.route('/api/refresh')
def refresh():
    """
    Force refresh data
    """
    with CACHE["lock"]:
        CACHE["last_updated"] = 0 # Force expire
    get_market_data()
    return jsonify({"status": "success", "message": "Data refreshed"})

@app.route('/stock/<ticker>')
def stock_detail(ticker):
    """
    Stock detail page with historical chart
    """
    import yfinance as yf
    
    full_ticker = f"{ticker}.NS"
    
    try:
        stock = yf.Ticker(full_ticker)
        
        # Get current info
        info = stock.fast_info
        current_price = round(info.get('lastPrice', 0), 2)
        market_cap = info.get('marketCap', 0)
        market_cap_cr = round(market_cap / 1e7, 2) if market_cap else 0
        
        # Calculate returns from longer history
        hist_1y = stock.history(period="1y")
        hist_5y = stock.history(period="5y", interval="1mo")
        
        ytd_change = 0
        five_yr_change = 0
        
        if not hist_1y.empty and len(hist_1y) > 1:
            ytd_change = round(((hist_1y['Close'].iloc[-1] - hist_1y['Close'].iloc[0]) / hist_1y['Close'].iloc[0]) * 100, 2)
        
        if not hist_5y.empty and len(hist_5y) > 1:
            five_yr_change = round(((hist_5y['Close'].iloc[-1] - hist_5y['Close'].iloc[0]) / hist_5y['Close'].iloc[0]) * 100, 2)
        
        return render_template('detail.html', 
                               ticker=ticker,
                               current_price=current_price,
                               market_cap=market_cap_cr,
                               ytd_change=ytd_change,
                               five_yr_change=five_yr_change)
    except Exception as e:
        return render_template('detail.html', ticker=ticker, error=str(e))

@app.route('/api/chart/<ticker>')
def get_chart_data(ticker):
    """
    API to fetch chart data for different time periods
    """
    import yfinance as yf
    from flask import request
    
    full_ticker = f"{ticker}.NS"
    period = request.args.get('period', '1y')
    
    # Define period-to-interval mapping
    period_config = {
        '1d': {'period': '1d', 'interval': '5m'},
        '5d': {'period': '5d', 'interval': '15m'},
        '1mo': {'period': '1mo', 'interval': '1h'},
        '3mo': {'period': '3mo', 'interval': '1d'},
        '6mo': {'period': '6mo', 'interval': '1d'},
        '1y': {'period': '1y', 'interval': '1d'},
        '2y': {'period': '2y', 'interval': '1wk'},
        '5y': {'period': '5y', 'interval': '1wk'},
        '10y': {'period': '10y', 'interval': '1mo'},
    }
    
    config = period_config.get(period, period_config['1y'])
    
    try:
        stock = yf.Ticker(full_ticker)
        hist = stock.history(period=config['period'], interval=config['interval'])
        
        if hist.empty:
            return jsonify({'error': 'No data found'})
        
        # Format dates based on interval
        if config['interval'] in ['5m', '15m', '1h']:
            dates = hist.index.strftime('%d %b %H:%M').tolist()
        elif config['interval'] == '1d':
            dates = hist.index.strftime('%d %b').tolist()
        elif config['interval'] == '1wk':
            dates = hist.index.strftime('%b %Y').tolist()
        else:
            dates = hist.index.strftime('%b %Y').tolist()
        
        prices = hist['Close'].round(2).tolist()
        
        # Calculate change
        if len(prices) >= 2:
            change = round(((prices[-1] - prices[0]) / prices[0]) * 100, 2)
        else:
            change = 0
        
        return jsonify({
            'dates': dates,
            'prices': prices,
            'change': change
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/forecast/<ticker>')
def get_forecast(ticker):
    """
    API to get 12-month price forecast using Prophet
    """
    import yfinance as yf
    from prophet import Prophet
    import warnings
    warnings.filterwarnings('ignore')
    
    full_ticker = f"{ticker}.NS"
    
    try:
        # Check cache
        cached = get_analytics_cache(ticker, 'forecast')
        if cached:
            return jsonify(cached)

        stock = yf.Ticker(full_ticker)
        hist = stock.history(period="10y", interval="1mo")
        
        if hist.empty or len(hist) < 24:
            return jsonify({'error': 'Insufficient historical data'})
        
        # Prepare data for Prophet
        df = hist.reset_index()[['Date', 'Close']].copy()
        df.columns = ['ds', 'y']
        df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)
        
        # Train Prophet model
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=0.05
        )
        model.fit(df)
        
        # Forecast next 12 months
        future = model.make_future_dataframe(periods=12, freq='ME')
        forecast = model.predict(future)
        
        # Get last 12 months of forecast
        forecast_12m = forecast.tail(12)
        
        forecast_dates = forecast_12m['ds'].dt.strftime('%b %Y').tolist()
        forecast_prices = forecast_12m['yhat'].round(2).tolist()
        forecast_lower = forecast_12m['yhat_lower'].round(2).tolist()
        forecast_upper = forecast_12m['yhat_upper'].round(2).tolist()
        
        # Calculate confidence (based on uncertainty interval width)
        avg_uncertainty = (forecast_12m['yhat_upper'] - forecast_12m['yhat_lower']).mean()
        avg_price = forecast_12m['yhat'].mean()
        confidence = max(50, min(95, 100 - (avg_uncertainty / avg_price * 100)))
        
        # Get historical data for chart
        hist_dates = df['ds'].dt.strftime('%b %Y').tolist()
        hist_prices = df['y'].round(2).tolist()
        
        result = {
            'historical_dates': hist_dates,
            'historical_prices': hist_prices,
            'forecast_dates': forecast_dates,
            'forecast_prices': forecast_prices,
            'forecast_lower': forecast_lower,
            'forecast_upper': forecast_upper,
            'confidence': round(confidence, 1),
            'current_price': round(hist_prices[-1], 2),
            'forecast_12m': round(forecast_prices[-1], 2),
            'expected_return': round(((forecast_prices[-1] - hist_prices[-1]) / hist_prices[-1]) * 100, 2)
        }
        save_analytics_cache(ticker, 'forecast', result)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/uptrends/<ticker>')
def get_uptrends(ticker):
    """
    API to detect significant uptrends in the last 10 years
    """
    import yfinance as yf
    import numpy as np
    from flask import request
    
    full_ticker = f"{ticker}.NS"
    
    # Get filter parameters
    min_gain = float(request.args.get('min_gain', 1))  # Default 1%
    max_gain = float(request.args.get('max_gain', 200))  # Default 200%
    
    try:
        stock = yf.Ticker(full_ticker)
        hist = stock.history(period="10y", interval="1mo")
        
        if hist.empty or len(hist) < 12:
            return jsonify({'error': 'Insufficient data'})
        
        df = hist.reset_index()[['Date', 'Close']].copy()
        df['Date'] = pd.to_datetime(df['Date'])
        prices = df['Close'].values
        dates = df['Date'].values
        
        # Find local minima and maxima
        uptrends = []
        
        i = 0
        while i < len(prices) - 3:
            # Find local minimum (potential start of uptrend)
            if prices[i] <= min(prices[max(0, i-2):i+3]):
                start_idx = i
                start_price = prices[i]
                max_price = start_price
                max_idx = i
                
                # Look for the peak
                for j in range(i + 1, min(i + 36, len(prices))):  # Look up to 3 years ahead
                    if prices[j] > max_price:
                        max_price = prices[j]
                        max_idx = j
                    # If price drops 15% from peak, uptrend ended
                    if prices[j] < max_price * 0.85:
                        break
                
                gain = ((max_price - start_price) / start_price) * 100
                
                if gain >= min_gain and gain <= max_gain and max_idx > start_idx:
                    uptrends.append({
                        'start_date': pd.Timestamp(dates[start_idx]).strftime('%d %b %Y'),
                        'peak_date': pd.Timestamp(dates[max_idx]).strftime('%d %b %Y'),
                        'start_price': round(start_price, 2),
                        'peak_price': round(max_price, 2),
                        'gain': round(gain, 1),
                        'duration_months': max_idx - start_idx
                    })
                    i = max_idx  # Skip to after the peak
                else:
                    i += 1
            else:
                i += 1
        
        # Sort by gain and take top 5
        uptrends = sorted(uptrends, key=lambda x: x['gain'], reverse=True)[:5]
        
        return jsonify({
            'uptrends': uptrends,
            'total_found': len(uptrends)
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/seasonal/<ticker>')
def get_seasonal_analysis(ticker):
    """
    API to get seasonal pattern analysis
    """
    from seasonal_analysis import analyze_seasonal_patterns
    import warnings
    warnings.filterwarnings('ignore')
    
    # Get filter parameters
    from flask import request
    min_gain = float(request.args.get('min_gain', 20)) 
    
    # Check cache
    cached = get_analytics_cache(ticker, 'seasonal')
    if cached:
        return jsonify(cached)
        
    result = analyze_seasonal_patterns(ticker, min_gain)
    if 'error' not in result:
        save_analytics_cache(ticker, 'seasonal', result)
    return jsonify(result)

@app.route('/api/predictions/<ticker>')
def get_predictions(ticker):
    """
    API to get predicted future entry/exit dates based on historical patterns
    """
    from seasonal_analysis import predict_future_dates
    from flask import request
    import warnings
    warnings.filterwarnings('ignore')
    
    min_gain = float(request.args.get('min_gain', 20))
    min_success_rate = float(request.args.get('min_success_rate', 80))
    
    # Check cache
    cached = get_analytics_cache(ticker, 'predictions')
    if cached:
        return jsonify(cached)
        
    result = predict_future_dates(ticker, min_gain, min_success_rate)
    if 'error' not in result:
        save_analytics_cache(ticker, 'predictions', result)
    return jsonify(result)

@app.route('/seasonal-screener')
def seasonal_screener():
    """
    Seasonal Screener page - filter stocks by seasonal performance
    """
    return render_template('seasonal_screener.html')

@app.route('/api/seasonal-screener')
def get_seasonal_screener_data():
    """
    API to get seasonal analysis for all Nifty 250 stocks
    Returns aggregated data for filtering
    """
    from flask import request
    
    min_gain = float(request.args.get('min_gain', 20))
    
    # 1. Try to load from JSON cache file
    cached_data = None
    if os.path.exists(SEASONAL_CACHE_FILE):
        try:
            with open(SEASONAL_CACHE_FILE, 'r') as f:
                cached_data = json.load(f)
        except Exception as e:
            print(f"Error reading cache file: {e}")

    # 2. Check if cache is stale or missing
    is_stale = True
    if cached_data:
        updated_at = datetime.fromisoformat(cached_data.get('updated_at', '2000-01-01'))
        if datetime.now() - updated_at < timedelta(seconds=SEASONAL_CACHE_TTL):
            is_stale = False
            # Even if fresh, if it has very few stocks, maybe it's a partial previous run
            if len(cached_data.get('stocks', [])) < 10:
                is_stale = True

    # 3. Trigger background update if stale or missing (and not already running)
    if is_stale:
        # Check if a thread is already running for this (basic check)
        is_running = any(t.name == "SeasonalCacheUpdater" for t in threading.enumerate())
        if not is_running:
            print("Screener cache is stale or missing. Starting background update...")
            threading.Thread(target=update_seasonal_cache, name="SeasonalCacheUpdater", daemon=True).start()

    # 4. Return cached data if available (even if stale)
    if cached_data and cached_data.get('stocks'):
        return jsonify({
            'stocks': cached_data['stocks'],
            'total_analyzed': len(cached_data['stocks']),
            'min_gain_filter': min_gain,
            'updated_at': cached_data['updated_at'],
            'status': 'stale_updating' if is_stale else 'fresh'
        })

    # 5. If no cache exists at all, we HAVE to do a small sync run or return empty
    return jsonify({
        'stocks': [],
        'total_analyzed': 0,
        'min_gain_filter': min_gain,
        'status': 'calculating',
        'message': 'Initial analysis in progress. This usually takes 2-5 minutes. Please refresh soon.'
    })

def update_seasonal_cache():
    """
    Background worker to refresh the seasonal screener data
    """
    print("Background update of seasonal cache started...")
    from seasonal_analysis import analyze_seasonal_patterns
    import concurrent.futures
    import warnings
    warnings.filterwarnings('ignore')
    
    try:
        tickers = get_nifty250_tickers()
        if not tickers:
            print("Failed to fetch tickers for background update")
            return

        results = []
        base_min_gain = 5  
        
        # Batch process tickers to avoid memory issues while benefiting from batch download
        chunk_size = 20
        all_tickers = tickers # Process all 250 now
        
        for i in range(0, len(all_tickers), chunk_size):
            chunk = all_tickers[i:i + chunk_size]
            print(f"Processing chunk {i//chunk_size + 1}: {chunk[0]} to {chunk[-1]}")
            
            try:
                # Batch download 10y daily data for the chunk
                # Using group_by='ticker' to get a multi-index dataframe
                data = yf.download(chunk, period="10y", interval="1d", group_by='ticker', progress=False)
                
                for ticker in chunk:
                    try:
                        clean_ticker = ticker.replace('.NS', '')
                        # Extract ticker data from batch
                        if isinstance(data.columns, pd.MultiIndex):
                            hist = data[ticker].dropna(how='all')
                        else:
                            hist = data.dropna(how='all')
                            
                        if hist.empty:
                            continue
                            
                        from seasonal_analysis import analyze_seasonal_patterns_v2
                        analysis = analyze_seasonal_patterns_v2(clean_ticker, base_min_gain, hist_data=hist)
                        
                        if 'error' not in analysis:
                            best_month = analysis['best_months'][0] if analysis['best_months'] else None
                            total_rallies = sum(m['occurrences'] for m in analysis['monthly_stats'])
                            max_success_rate = max((m['success_rate'] for m in analysis['monthly_stats']), default=0)
                            
                            results.append({
                                'ticker': clean_ticker,
                                'total_rallies': total_rallies,
                                'best_month': best_month['month'] if best_month else 'N/A',
                                'best_month_rallies': best_month['occurrences'] if best_month else 0,
                                'best_month_avg_gain': best_month['avg_gain'] if best_month else 0,
                                'best_month_success': best_month['success_rate'] if best_month else 0,
                                'max_success_rate': max_success_rate,
                                'monthly_stats': analysis['monthly_stats']
                            })
                    except Exception as e:
                        # print(f"Error processing {ticker}: {e}")
                        pass
                
                # Incremental save
                results.sort(key=lambda x: x['total_rallies'], reverse=True)
                cache_content = {
                    'stocks': results,
                    'updated_at': datetime.now().isoformat(),
                    'total_tickers': len(tickers),
                    'in_progress': i + chunk_size < len(all_tickers)
                }
                
                with open(SEASONAL_CACHE_FILE, 'w') as f:
                    json.dump(cache_content, f)
                    
            except Exception as e:
                print(f"Error in batch download/process: {e}")
                continue

        print(f"Background update complete. {len(results)} stocks total cached.")
        
    except Exception as e:
        print(f"Critical error in update_seasonal_cache: {e}")

if __name__ == '__main__':
    app.run(debug=True, port=5000)


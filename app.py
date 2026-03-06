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
SEASONAL_CACHE_FILE = 'seasonal_cache_v7.json'
SEASONAL_CACHE_TTL = 86400  # 24 hours
ANALYTICS_CACHE_DIR = 'analytics_cache'

# Ensure analytics cache dir exists
if not os.path.exists(ANALYTICS_CACHE_DIR):
    os.makedirs(ANALYTICS_CACHE_DIR)

def get_analytics_cache(ticker, category, params=None):
    """Simple file-based cache for expensive analytics"""
    suffix = ""
    if params:
        suffix = "_" + "_".join([f"{k}{v}" for k, v in sorted(params.items())])
    cache_path = os.path.join(ANALYTICS_CACHE_DIR, f"{ticker}_{category}{suffix}.json")
    
    if os.path.exists(cache_path):
        mtime = os.path.getmtime(cache_path)
        if (time.time() - mtime) < SEASONAL_CACHE_TTL:
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except:
                pass
    return None

def save_analytics_cache(ticker, category, data, params=None):
    suffix = ""
    if params:
        suffix = "_" + "_".join([f"{k}{v}" for k, v in sorted(params.items())])
    cache_path = os.path.join(ANALYTICS_CACHE_DIR, f"{ticker}_{category}{suffix}.json")
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
    params = {'min_gain': min_gain}
    
    # Check cache
    cached = get_analytics_cache(ticker, 'seasonal', params)
    if cached:
        return jsonify(cached)
        
    result = analyze_seasonal_patterns(ticker, min_gain)
    if 'error' not in result:
        save_analytics_cache(ticker, 'seasonal', result, params)
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
    params = {'min_gain': min_gain, 'min_success_rate': min_success_rate}
    
    # Check cache
    cached = get_analytics_cache(ticker, 'predictions', params)
    if cached:
        return jsonify(cached)
        
    result = predict_future_dates(ticker, min_gain, min_success_rate)
    if 'error' not in result:
        save_analytics_cache(ticker, 'predictions', result, params)
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
    This endpoint now performs DYNAMIC filtering and aggregation for Gain OR Loss
    """
    from flask import request
    
    try:
        min_gain = float(request.args.get('min_gain', 20))
        direction = request.args.get('direction', 'gain') # 'gain' or 'loss'
        
        # Load from JSON cache file
        if not os.path.exists(SEASONAL_CACHE_FILE):
            is_running = any(t.name == "SeasonalCacheUpdater" for t in threading.enumerate())
            if not is_running:
                threading.Thread(target=update_seasonal_cache, name="SeasonalCacheUpdater", daemon=True).start()
                
            return jsonify({
                "status": "calculating", 
                "message": "Initializing seasonal analysis. This takes 2-3 minutes.",
                "stocks": []
            })
            
        with open(SEASONAL_CACHE_FILE, 'r') as f:
            cached_data = json.load(f)
            
        is_stale = False
        if 'updated_at' in cached_data:
            updated_at = datetime.fromisoformat(cached_data['updated_at'])
            if datetime.now() - updated_at > timedelta(seconds=SEASONAL_CACHE_TTL):
                is_stale = True
        
        is_updating = any(t.name == "SeasonalCacheUpdater" for t in threading.enumerate())
        if is_stale and not is_updating:
            threading.Thread(target=update_seasonal_cache, name="SeasonalCacheUpdater", daemon=True).start()
            
        raw_stocks = cached_data.get('stocks_baseline_5', [])
        processed_stocks = []
        
        month_names = ['January', 'February', 'March', 'April', 'May', 'June', 
                       'July', 'August', 'September', 'October', 'November', 'December']
        
        # Dynamic filtering & aggregation
        for stock in raw_stocks:
            ticker = stock['ticker']
            
            # Decide which moves list to use based on direction
            if direction == 'loss':
                all_moves = stock.get('fall_moves', [])
            else:
                all_moves = stock.get('all_moves', [])
            
            # Filter moves by exact threshold requested by user
            filtered_moves = [m for m in all_moves if m['gain'] >= min_gain]
            
            # ALSO calculate the other direction (risk profile) for that month
            # We DONT filter this as strictly as the primary trend so we see all historical dips > 5%
            if direction == 'loss':
                other_filtered = stock.get('all_moves', [])
            else:
                other_filtered = stock.get('fall_moves', [])
            
            # Re-calculate monthly stats for this specific min_gain/direction
            monthly_stats_map = {m: {
                'month': m, 'count': 0, 'total_gain': 0, 'min_gain': float('inf'), 
                'max_gain': 0, 'total_drwdn': 0, 'min_drwdn': 0, 
                'count_other': 0, 'total_gain_other': 0, 'min_gain_other': float('inf'), 'max_gain_other': 0,
                'years': set()
            } for m in month_names}
            
            for move in filtered_moves:
                m_name = move['start_month']
                s = monthly_stats_map[m_name]
                s['count'] += 1
                s['total_gain'] += move['gain']
                s['min_gain'] = min(s['min_gain'], move['gain'])
                s['max_gain'] = max(s['max_gain'], move['gain'])
                s['years'].add(move['start_year'])
                # Track intraday drawdown/recovery
                if direction == 'loss':
                    val = move.get('recovery', 0)
                    s['total_drwdn'] += val
                    if val > s['min_drwdn']: s['min_drwdn'] = val
                else:
                    val = move.get('drawdown', 0)
                    s['total_drwdn'] += val
                    if val < s['min_drwdn']: s['min_drwdn'] = val

            # Aggregate "Other" direction (Independent moves in same month)
            for move in other_filtered:
                m_name = move['start_month']
                s = monthly_stats_map[m_name]
                s['count_other'] += 1
                s['total_gain_other'] += move['gain']
                s['min_gain_other'] = min(s['min_gain_other'], move['gain'])
                s['max_gain_other'] = max(s['max_gain_other'], move['gain'])
                
            formatted_stats = []
            total_rallies = 0
            best_month = None
            max_rallies = -1
            max_avg_gain = -1
            
            month_availability = stock.get('month_availability', {})
            total_years = stock.get('total_years_analyzed', 10)
            
            for m_name in month_names:
                s = monthly_stats_map[m_name]
                if s['count'] > 0:
                    avg_g = s['total_gain'] / s['count']
                    avg_d = s['total_drwdn'] / s['count']
                    # SUCCESS RATE: (Years with Target / Total Times that Month Occurred in 10yrs)
                    denom = month_availability.get(m_name, total_years)
                    succ_r = min((len(s['years']) / denom) * 100, 100) if denom > 0 else 0
                    
                    stat_obj = {
                        'month': m_name,
                        'occurrences': s['count'],
                        'avg_gain': round(avg_g, 1),
                        'min_gain': round(s['min_gain'], 1),
                        'max_gain': round(s['max_gain'], 1),
                        'avg_drawdown': round(avg_d, 1),
                        'min_drawdown': round(float(s['min_drwdn']), 1),
                        'success_rate': round(succ_r, 0),
                        # Cross-trend metrics
                        'opp_count': s['count_other'],
                        'opp_avg_gain': round(s['total_gain_other'] / s['count_other'], 1) if s['count_other'] > 0 else 0,
                        'opp_max_gain': round(s['max_gain_other'], 1) if s['count_other'] > 0 else 0,
                        'opp_min_gain': round(s['min_gain_other'], 1) if s['count_other'] > 0 else 0
                    }
                    formatted_stats.append(stat_obj)
                    total_rallies += s['count']
                    
                    if s['count'] > max_rallies or (s['count'] == max_rallies and avg_g > max_avg_gain):
                        max_rallies = s['count']
                        max_avg_gain = avg_g
                        best_month = stat_obj
            
            if total_rallies > 0:
                processed_stocks.append({
                    'ticker': ticker,
                    'total_rallies': total_rallies,
                    'best_month': best_month['month'] if best_month else 'N/A',
                    'best_month_rallies': best_month['occurrences'] if best_month else 0,
                    'best_month_avg_gain': best_month['avg_gain'] if best_month else 0,
                    'best_month_min_gain': best_month['min_gain'] if best_month else 0,
                    'best_month_drawdown': best_month['avg_drawdown'] if best_month else 0,
                    'best_month_min_drawdown': best_month['min_drawdown'] if best_month else 0,
                    'best_month_success': best_month['success_rate'] if best_month else 0,
                    'best_month_opp_avg': best_month['opp_avg_gain'] if best_month else 0,
                    'best_month_opp_max': best_month['opp_max_gain'] if best_month else 0,
                    'best_month_opp_min': best_month['opp_min_gain'] if best_month else 0,
                    'monthly_stats': formatted_stats
                })
        
        processed_stocks.sort(key=lambda x: x['total_rallies'], reverse=True)
        
        return jsonify({
            'stocks': processed_stocks,
            'total_analyzed': len(raw_stocks),
            'min_gain_requested': min_gain,
            'direction': direction,
            'updated_at': cached_data.get('updated_at'),
            'status': 'stale_updating' if (is_stale or cached_data.get('in_progress')) else 'fresh'
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def update_seasonal_cache():
    """
    Background worker to refresh the seasonal screener data
    """
    print("Background update of seasonal cache started...")
    from seasonal_analysis import analyze_seasonal_patterns_v2
    import yfinance as yf
    import warnings
    warnings.filterwarnings('ignore')
    
    try:
        tickers = get_nifty250_tickers()
        if not tickers:
            print("Failed to fetch tickers for background update")
            return

        baseline_gain = 5
        results = []
        
        # Batch process tickers to avoid memory issues while benefiting from batch download
        chunk_size = 20
        all_tickers = tickers # Process all 250 now
        
        for i in range(0, len(all_tickers), chunk_size):
            chunk = all_tickers[i:i + chunk_size]
            print(f"Processing chunk {i//chunk_size + 1}: {chunk[0]} to {chunk[-1]}")
            
            try:
                # Batch download 10y daily data for the chunk
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
                        
                        # Analyze at 5% baseline to capture all relevant moves
                        analysis = analyze_seasonal_patterns_v2(clean_ticker, baseline_gain, hist_data=hist)
                        
                        if 'error' not in analysis:
                            results.append({
                                'ticker': clean_ticker,
                                'all_moves': analysis['moves'], # Uptrends
                                'fall_moves': analysis.get('fall_moves', []), # Downtrends
                                'total_years_analyzed': analysis.get('total_years', 10)
                            })
                    except Exception as e:
                        pass
                
                cache_content = {
                    'stocks_baseline_5': results,
                    'updated_at': datetime.now().isoformat(),
                    'total_tickers': len(tickers),
                    'in_progress': i + chunk_size < len(all_tickers)
                }
                
                with open(SEASONAL_CACHE_FILE, 'w') as f:
                    json.dump(cache_content, f)
                    
            except Exception as e:
                print(f"Error in batch download/process: {e}")
                continue

        # Final Summary
        print(f"Background update complete. {len(results)} stocks total cached.")
        
    except Exception as e:
        print(f"Critical error in update_seasonal_cache: {e}")

ACCUMULATION_CACHE_FILE = 'accumulation_cache.json'
ACCUMULATION_CACHE_TTL = 86400  # 24 hours

@app.route('/accumulation-scanner')
def accumulation_scanner():
    """Accumulation Scanner page"""
    return render_template('accumulation_scanner.html')

@app.route('/api/accumulation-scanner')
def get_accumulation_data():
    """
    API to get accumulation pattern scan results for all Nifty 250 stocks.
    Uses file-based cache with background refresh.
    """
    try:
        # Check if cache file exists
        if not os.path.exists(ACCUMULATION_CACHE_FILE):
            is_running = any(t.name == "AccumulationCacheUpdater" for t in threading.enumerate())
            if not is_running:
                threading.Thread(
                    target=update_accumulation_cache,
                    name="AccumulationCacheUpdater",
                    daemon=True
                ).start()
            return jsonify({
                "status": "calculating",
                "message": "Scanning stocks for accumulation patterns. This takes 2-3 minutes.",
                "stocks": []
            })

        with open(ACCUMULATION_CACHE_FILE, 'r') as f:
            cached_data = json.load(f)

        # Check staleness
        is_stale = False
        if 'updated_at' in cached_data:
            from datetime import datetime
            updated_at = datetime.fromisoformat(cached_data['updated_at'])
            if (datetime.now() - updated_at).total_seconds() > ACCUMULATION_CACHE_TTL:
                is_stale = True

        is_updating = any(t.name == "AccumulationCacheUpdater" for t in threading.enumerate())
        if is_stale and not is_updating:
            threading.Thread(
                target=update_accumulation_cache,
                name="AccumulationCacheUpdater",
                daemon=True
            ).start()

        # Apply client-side filters
        from flask import request
        min_score = int(request.args.get('min_score', 0))
        tag_filter = request.args.get('tag', 'all')

        stocks = cached_data.get('stocks', [])

        if min_score > 0:
            stocks = [s for s in stocks if s['score'] >= min_score]

        if tag_filter != 'all':
            stocks = [s for s in stocks if s['tag'] == tag_filter]

        return jsonify({
            'stocks': stocks,
            'total_scanned': cached_data.get('total_scanned', 0),
            'total_matched': len(stocks),
            'breakdown': cached_data.get('breakdown', {}),
            'updated_at': cached_data.get('updated_at'),
            'status': 'stale_updating' if is_stale else 'fresh'
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


def update_accumulation_cache():
    """Background worker to refresh accumulation scanner data."""
    print("Background update of accumulation cache started...")
    from accumulation_detector import scan_accumulation
    from datetime import datetime
    import os
    import json

    try:
        result = scan_accumulation()

        if 'error' not in result:
            cache_content = {
                'stocks': result['stocks'],
                'total_scanned': result['total_scanned'],
                'total_matched': result['total_matched'],
                'breakdown': result['breakdown'],
                'updated_at': datetime.now().isoformat()
            }

            # Write to temp file first for atomic swap
            temp_file = ACCUMULATION_CACHE_FILE + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(cache_content, f)
            
            # Atomic swap
            if os.path.exists(ACCUMULATION_CACHE_FILE):
                os.remove(ACCUMULATION_CACHE_FILE)
            os.rename(temp_file, ACCUMULATION_CACHE_FILE)
            
            print(f"Accumulation cache updated: {result['total_matched']} stocks matched out of {result['total_scanned']} scanned.")
        else:
            print(f"Accumulation scan error: {result['error']}")

    except Exception as e:
        print(f"Critical error in update_accumulation_cache: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    app.run(debug=True, port=5000)


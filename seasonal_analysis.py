import yfinance as yf
import pandas as pd
import numpy as np
from datetime import timedelta

def analyze_seasonal_patterns(ticker, min_gain_percent=20):
    """
    Analyzes 10 years of daily data to find repeated seasonal uptrends.
    
    Args:
        ticker (str): Stock ticker (e.g., 'RELIANCE').
        min_gain_percent (float): Minimum % gain to qualify as a move.
        
    Returns:
        dict: containing 'moves', 'monthly_stats', 'best_months', 'insights'
    """
    full_ticker = f"{ticker}.NS" if not ticker.endswith('.NS') else ticker
    
    try:
        # 1. Fetch Data (10 Years daily)
        stock = yf.Ticker(full_ticker)
        hist = stock.history(period="10y", interval="1d")
        
        if hist.empty or len(hist) < 250:
            return {'error': 'Insufficient historical data'}
            
        # Clean data
        df = hist.reset_index()
        # Ensure Date is timezone-naive for easier calculations
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
        # FORCE SORT by Date (oldest first)
        df = df.sort_values('Date', ascending=True).reset_index(drop=True)
        
        moves = []
        
        # Convert to Python lists for faster iteration
        dates_list = df['Date'].tolist()  # These are now pd.Timestamp objects
        lows = df['Low'].values
        highs = df['High'].values
        
        n_days = len(df)
        
        for i in range(n_days - 20):  # Stop before end
            start_date = dates_list[i]
            start_price = lows[i]  # Use Low for entry
            
            # Look forward up to ~3 months (65 trading days)
            max_gain = 0
            best_day_idx = -1
            
            look_ahead_idx = min(i + 65, n_days)
            
            for j in range(i + 1, look_ahead_idx):
                curr_high = highs[j]
                gain = ((curr_high - start_price) / start_price) * 100
                
                if gain > max_gain:
                    max_gain = gain
                    best_day_idx = j
            
            # Apply Filter
            if max_gain >= min_gain_percent and best_day_idx > i:
                end_date = dates_list[best_day_idx]
                duration_days = (end_date - start_date).days
                
                # Sanity check: end_date must be after start_date
                if duration_days <= 0:
                    continue
                
                # Check for duplicates (overlapping moves to same peak)
                is_duplicate = False
                for existing in moves:
                    existing_high = existing['high_date']
                    existing_low = existing['low_date']
                    
                    high_diff = abs((existing_high - end_date).days)
                    low_diff = abs((existing_low - start_date).days)
                    
                    if high_diff < 10 and low_diff < 20:
                        is_duplicate = True
                        # Keep the one with lower start price (better entry)
                        # But ONLY if the new move is valid (start before end)
                        if start_price < existing['start_price'] and start_date < end_date:
                            # Update all related fields consistently
                            existing['start_price'] = start_price
                            existing['low_date'] = start_date
                            existing['high_date'] = end_date  # Also update high_date!
                            existing['gain'] = round(float(max_gain), 2)
                            existing['duration'] = (end_date - start_date).days  # Recalculate!
                            existing['start_month'] = start_date.strftime('%B')
                            existing['start_month_idx'] = start_date.month
                            existing['start_year'] = start_date.year
                        break
                
                if not is_duplicate:
                    moves.append({
                        'low_date': start_date,
                        'high_date': end_date,
                        'start_price': start_price,
                        'end_price': highs[best_day_idx],
                        'gain': round(float(max_gain), 2),
                        'duration': duration_days,
                        'start_month': start_date.strftime('%B'),
                        'start_month_idx': start_date.month,
                        'start_year': start_date.year
                    })
        
        # Sort moves by date
        moves.sort(key=lambda x: x['low_date'])
        
        # 4. Monthly Statistics
        month_names = ['January', 'February', 'March', 'April', 'May', 'June', 
                       'July', 'August', 'September', 'October', 'November', 'December']
        
        monthly_stats = {}
        for m in month_names:
            monthly_stats[m] = {
                'count': 0,
                'total_gain': 0,
                'min_gain': float('inf'),
                'max_gain': 0,
                'durations': [],
                'years_with_rally': set()  # Track unique YEARS (not months)
            }
            
        for move in moves:
            m_name = move['start_month']
            stats = monthly_stats[m_name]
            stats['count'] += 1
            stats['total_gain'] += move['gain']
            stats['min_gain'] = min(stats['min_gain'], move['gain'])
            stats['max_gain'] = max(stats['max_gain'], move['gain'])
            stats['durations'].append(move['duration'])
            # Add the YEAR of the move (not month index!)
            stats['years_with_rally'].add(move['start_year'])
            
        # 5. Rank and Format
        formatted_stats = []
        for m_name in month_names:
            stats = monthly_stats[m_name]
            if stats['count'] > 0:
                avg_gain = stats['total_gain'] / stats['count']
                avg_duration = sum(stats['durations']) / stats['count']
                min_gain = stats['min_gain']
                max_gain = stats['max_gain']
            else:
                avg_gain = 0
                avg_duration = 0
                min_gain = 0
                max_gain = 0
                
            # Success Rate = (Years with at least 1 rally / Total Years) * 100
            # Cap at 100%
            num_years = len(stats['years_with_rally'])
            success_rate = min((num_years / 10) * 100, 100)
            
            formatted_stats.append({
                'month': m_name,
                'occurrences': stats['count'],
                'avg_gain': round(avg_gain, 1),
                'max_gain': round(max_gain, 1),
                'min_gain': round(min_gain, 1) if min_gain != float('inf') else 0,
                'avg_duration': round(avg_duration, 0),
                'success_rate': round(success_rate, 0)
            })
            
        # Rank by Occurrences + Avg Gain
        top_months = sorted(formatted_stats, key=lambda x: (x['occurrences'], x['avg_gain']), reverse=True)
        best_months = top_months[:3]
        
        # Insights
        insights = []
        if best_months:
            top = best_months[0]
            if top['occurrences'] >= 3:
                insights.append(f"Strongest Seasonality: {top['month']} has seen {top['occurrences']} massive rallies in the last 10 years.")
            
            avg_dur = top['avg_duration']
            if avg_dur < 30:
                insights.append(f"Fast Moves: Rallies starting in {top['month']} typically peak within 1 month (~{int(avg_dur)} days).")
            elif avg_dur < 60:
                insights.append(f"Medium Term: Moves from {top['month']} usually last about 2 months.")
            else:
                insights.append(f"Sustained Trends: {top['month']} moves tend to be longer sustained trending periods (~3 months).")
                
        # Format dates for JSON output
        for move in moves:
            move['low_date_str'] = move['low_date'].strftime('%d %b %Y')
            move['high_date_str'] = move['high_date'].strftime('%d %b %Y')
            # Remove raw date objects (not JSON serializable)
            del move['low_date']
            del move['high_date']

        return {
            'ticker': ticker,
            'moves': moves,
            'monthly_stats': formatted_stats,
            'best_months': best_months,
            'insights': insights,
            'total_years': 10
        }

    except Exception as e:
        import traceback
        print(f"Error in seasonal analysis: {e}")
        traceback.print_exc()
        return {'error': str(e)}
def predict_future_dates(ticker, min_gain_percent=20):
    """
    Predicts future entry and exit dates based on 10 years of historical seasonal patterns.
    
    Args:
        ticker (str): Stock ticker (e.g., 'RELIANCE').
        min_gain_percent (float): Minimum % gain to qualify as a historical move.
        
    Returns:
        dict: containing 'predictions' with future entry/exit dates
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    
    # First get historical analysis
    analysis = analyze_seasonal_patterns(ticker, min_gain_percent)
    
    if 'error' in analysis:
        return analysis
    
    moves = []
    # We need the raw moves with day information, re-parse from analysis
    full_ticker = f"{ticker}.NS" if not ticker.endswith('.NS') else ticker
    
    try:
        stock = yf.Ticker(full_ticker)
        hist = stock.history(period="10y", interval="1d")
        
        if hist.empty or len(hist) < 250:
            return {'error': 'Insufficient historical data'}
        
        df = hist.reset_index()
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
        df = df.sort_values('Date', ascending=True).reset_index(drop=True)
        
        dates_list = df['Date'].tolist()
        lows = df['Low'].values
        highs = df['High'].values
        n_days = len(df)
        
        # Collect moves with day information
        raw_moves = []
        for i in range(n_days - 20):
            start_date = dates_list[i]
            start_price = lows[i]
            
            max_gain = 0
            best_day_idx = -1
            look_ahead_idx = min(i + 65, n_days)
            
            for j in range(i + 1, look_ahead_idx):
                curr_high = highs[j]
                gain = ((curr_high - start_price) / start_price) * 100
                if gain > max_gain:
                    max_gain = gain
                    best_day_idx = j
            
            if max_gain >= min_gain_percent and best_day_idx > i:
                end_date = dates_list[best_day_idx]
                duration_days = (end_date - start_date).days
                if duration_days > 0:
                    raw_moves.append({
                        'start_date': start_date,
                        'end_date': end_date,
                        'start_day': start_date.day,
                        'start_month': start_date.month,
                        'start_year': start_date.year,
                        'duration': duration_days,
                        'gain': max_gain
                    })
        
        # Aggregate by month
        month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']
        
        monthly_data = {i+1: {'days': [], 'durations': [], 'gains': [], 'years': set()} 
                        for i in range(12)}
        
        for move in raw_moves:
            m = move['start_month']
            monthly_data[m]['days'].append(move['start_day'])
            monthly_data[m]['durations'].append(move['duration'])
            monthly_data[m]['gains'].append(move['gain'])
            monthly_data[m]['years'].add(move['start_year'])
        
        # Generate predictions for next 12 months
        today = datetime.now()
        predictions = []
        
        for month_offset in range(12):
            target_date = today + relativedelta(months=month_offset)
            target_month = target_date.month
            target_year = target_date.year
            
            data = monthly_data[target_month]
            
            if len(data['days']) >= 2:  # Need at least 2 occurrences
                avg_day = int(round(sum(data['days']) / len(data['days'])))
                avg_duration = int(round(sum(data['durations']) / len(data['durations'])))
                avg_gain = round(sum(data['gains']) / len(data['gains']), 1)
                
                # Clamp day to valid range
                import calendar
                max_day = calendar.monthrange(target_year, target_month)[1]
                avg_day = min(avg_day, max_day)
                
                entry_date = datetime(target_year, target_month, avg_day)
                exit_date = entry_date + relativedelta(days=avg_duration)
                
                # Calculate confidence based on success rate and occurrences
                success_rate = min((len(data['years']) / 10) * 100, 100)
                occurrences = len(data['days'])
                confidence = min(100, int(success_rate * 0.6 + occurrences * 4))
                
                predictions.append({
                    'month': month_names[target_month - 1],
                    'year': target_year,
                    'predicted_entry': entry_date.strftime('%d %b %Y'),
                    'predicted_exit': exit_date.strftime('%d %b %Y'),
                    'predicted_duration': avg_duration,
                    'expected_gain': avg_gain,
                    'confidence': confidence,
                    'historical_occurrences': occurrences,
                    'success_rate': round(success_rate, 0)
                })
        
        # Sort by confidence (highest first)
        predictions.sort(key=lambda x: x['confidence'], reverse=True)
        
        return {
            'ticker': ticker,
            'predictions': predictions,
            'analysis_period': '10 years',
            'min_gain_filter': min_gain_percent
        }
        
    except Exception as e:
        import traceback
        print(f"Error in prediction: {e}")
        traceback.print_exc()
        return {'error': str(e)}


if __name__ == "__main__":
    # Test
    res = analyze_seasonal_patterns("RELIANCE", 15)
    print(res)

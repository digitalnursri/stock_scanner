"""
Paper Trading Module for VCP Scanner
-------------------------------------
- Automatically executes trades on VCP signals
- Tracks P&L for each trade
- Efficient JSON-based storage (rotates files when too large)
- Provides trade history and performance analytics
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import threading

# Configuration
TRADES_DIR = 'paper_trades'
MAX_TRADES_PER_FILE = 1000  # Rotate file after 1000 trades
MAX_FILE_SIZE_MB = 10  # Or rotate if file > 10MB
CAPITAL_PER_TRADE = 100000  # ₹1 Lakh per trade

# Ensure trades directory exists
if not os.path.exists(TRADES_DIR):
    os.makedirs(TRADES_DIR)

# In-memory cache for active trades
_active_trades_cache = {}
_cache_lock = threading.Lock()


def _get_trades_file_path():
    """Get current trades file path (creates new file daily or when full)"""
    date_str = datetime.now().strftime('%Y%m')
    base_path = os.path.join(TRADES_DIR, f'trades_{date_str}')
    
    # Find the latest file number
    file_num = 1
    while os.path.exists(f'{base_path}_{file_num:03d}.json'):
        file_path = f'{base_path}_{file_num:03d}.json'
        # Check file size
        if os.path.getsize(file_path) < MAX_FILE_SIZE_MB * 1024 * 1024:
            # Check trade count
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if len(data.get('trades', [])) < MAX_TRADES_PER_FILE:
                        return file_path
            except:
                return file_path
        file_num += 1
    
    return f'{base_path}_{file_num:03d}.json'


def _load_all_trades() -> List[Dict]:
    """Load all trades from all files (efficiently)"""
    all_trades = []
    
    if not os.path.exists(TRADES_DIR):
        return all_trades
    
    # Get all trade files sorted by date (newest first)
    files = sorted([f for f in os.listdir(TRADES_DIR) if f.startswith('trades_') and f.endswith('.json')], reverse=True)
    
    for file in files:
        file_path = os.path.join(TRADES_DIR, file)
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                all_trades.extend(data.get('trades', []))
        except:
            continue
    
    return all_trades


def _save_trade(trade: Dict):
    """Save a single trade to file"""
    file_path = _get_trades_file_path()
    
    data = {'trades': []}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
        except:
            pass
    
    data['trades'].append(trade)
    
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)


def _update_trade(trade_id: str, updates: Dict):
    """Update an existing trade"""
    # Search in all files
    if not os.path.exists(TRADES_DIR):
        return
    
    for file in os.listdir(TRADES_DIR):
        if not (file.startswith('trades_') and file.endswith('.json')):
            continue
        
        file_path = os.path.join(TRADES_DIR, file)
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            updated = False
            for trade in data.get('trades', []):
                if trade.get('id') == trade_id:
                    trade.update(updates)
                    updated = True
                    break
            
            if updated:
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)
                return
        except:
            continue


def generate_trade_id() -> str:
    """Generate unique trade ID"""
    return f"TRD{datetime.now().strftime('%Y%m%d%H%M%S')}{os.urandom(2).hex().upper()}"


def execute_paper_trade(signal_data: Dict) -> Optional[Dict]:
    """
    Execute a paper trade based on VCP signal
    
    Args:
        signal_data: VCP signal data with ticker, price, targets, stop_loss
    
    Returns:
        Trade record or None if trade not executed
    """
    ticker = signal_data.get('ticker')
    if not ticker:
        return None
    
    with _cache_lock:
        # Check if already have active trade for this ticker
        if ticker in _active_trades_cache:
            return None
        
        entry_price = signal_data.get('price', 0)
        if entry_price <= 0:
            return None
        
        # Calculate quantity based on capital per trade
        quantity = int(CAPITAL_PER_TRADE / entry_price)
        if quantity < 1:
            quantity = 1
        
        # Get target and stop loss
        target_str = signal_data.get('target', '')
        stop_loss = signal_data.get('stop_loss', 0)
        
        # Parse target range
        target_low = entry_price * 1.15  # Default 15%
        target_high = entry_price * 1.40  # Default 40%
        
        if target_str and '₹' in target_str:
            try:
                parts = target_str.replace('₹', '').split('-')
                if len(parts) == 2:
                    target_low = float(parts[0].strip())
                    target_high = float(parts[1].strip())
            except:
                pass
        
        trade = {
            'id': generate_trade_id(),
            'ticker': ticker,
            'signal_type': signal_data.get('tag', 'VCP'),
            'entry_price': round(entry_price, 2),
            'quantity': quantity,
            'target_low': round(target_low, 2),
            'target_high': round(target_high, 2),
            'stop_loss': round(stop_loss, 2),
            'entry_time': datetime.now().isoformat(),
            'status': 'ACTIVE',
            'exit_price': None,
            'exit_time': None,
            'exit_reason': None,
            'pnl_amount': 0,
            'pnl_percent': 0,
            'days_held': 0,
            'score_at_entry': signal_data.get('score', 0),
            'resistance_at_entry': signal_data.get('resistance', 0)
        }
        
        # Save to file
        _save_trade(trade)
        
        # Add to cache
        _active_trades_cache[ticker] = trade
        
        print(f"[PAPER TRADE] Entered {ticker} @ ₹{entry_price} Qty: {quantity} Target: ₹{target_low}-{target_high} SL: ₹{stop_loss}")
        
        return trade


def check_and_update_trades(current_market_data: Dict[str, float]):
    """
    Check active trades against current market prices and update P&L
    Call this periodically with current prices
    
    Args:
        current_market_data: Dict of ticker -> current_price
    """
    with _cache_lock:
        trades_to_close = []
        
        for ticker, trade in list(_active_trades_cache.items()):
            if trade['status'] != 'ACTIVE':
                continue
            
            current_price = current_market_data.get(ticker)
            if not current_price:
                continue
            
            entry_price = trade['entry_price']
            target_low = trade['target_low']
            target_high = trade['target_high']
            stop_loss = trade['stop_loss']
            
            exit_trade = None
            exit_reason = None
            exit_price = None
            
            # Check stop loss
            if current_price <= stop_loss:
                exit_trade = True
                exit_reason = 'STOP_LOSS'
                exit_price = stop_loss
            
            # Check target 1 (book partial profit)
            elif current_price >= target_low and current_price < target_high:
                # Check if we already booked partial profit
                if not trade.get('partial_exit_done'):
                    # Book 50% at first target
                    trade['partial_exit_done'] = True
                    trade['partial_exit_price'] = current_price
                    trade['partial_exit_time'] = datetime.now().isoformat()
                    trade['partial_pnl'] = round((current_price - entry_price) * trade['quantity'] * 0.5, 2)
                    print(f"[PAPER TRADE] Partial profit booked for {ticker} @ ₹{current_price}")
                    _update_trade(trade['id'], {
                        'partial_exit_done': True,
                        'partial_exit_price': current_price,
                        'partial_exit_time': trade['partial_exit_time'],
                        'partial_pnl': trade['partial_pnl']
                    })
            
            # Check target 2 (full exit)
            elif current_price >= target_high:
                exit_trade = True
                exit_reason = 'TARGET_HIT'
                exit_price = target_high
            
            # Time-based exit (max 30 days)
            entry_time = datetime.fromisoformat(trade['entry_time'])
            days_held = (datetime.now() - entry_time).days
            
            if days_held >= 30 and not exit_trade:
                exit_trade = True
                exit_reason = 'TIME_EXIT'
                exit_price = current_price
            
            if exit_trade:
                # Calculate final P&L
                quantity = trade['quantity']
                
                # If partial exit was done, calculate remaining 50%
                if trade.get('partial_exit_done'):
                    remaining_qty = quantity * 0.5
                    partial_pnl = trade.get('partial_pnl', 0)
                    remaining_pnl = round((exit_price - entry_price) * remaining_qty, 2)
                    total_pnl = round(partial_pnl + remaining_pnl, 2)
                else:
                    total_pnl = round((exit_price - entry_price) * quantity, 2)
                
                pnl_percent = round(((exit_price - entry_price) / entry_price) * 100, 2)
                
                updates = {
                    'status': 'CLOSED',
                    'exit_price': round(exit_price, 2),
                    'exit_time': datetime.now().isoformat(),
                    'exit_reason': exit_reason,
                    'pnl_amount': total_pnl,
                    'pnl_percent': pnl_percent,
                    'days_held': days_held
                }
                
                trade.update(updates)
                _update_trade(trade['id'], updates)
                
                trades_to_close.append({
                    'ticker': ticker,
                    'pnl': total_pnl,
                    'reason': exit_reason
                })
                
                print(f"[PAPER TRADE] Closed {ticker} @ ₹{exit_price} Reason: {exit_reason} P&L: ₹{total_pnl} ({pnl_percent}%)")
        
        # Remove closed trades from cache
        for closed in trades_to_close:
            if closed['ticker'] in _active_trades_cache:
                del _active_trades_cache[closed['ticker']]
        
        return trades_to_close


def get_active_trades() -> List[Dict]:
    """Get all currently active trades"""
    with _cache_lock:
        return list(_active_trades_cache.values())


def get_trade_history(limit: int = 100, days: int = 30) -> List[Dict]:
    """
    Get trade history with filters
    
    Args:
        limit: Max number of trades to return
        days: Only return trades from last N days
    """
    all_trades = _load_all_trades()
    
    # Filter by date
    cutoff_date = datetime.now() - timedelta(days=days)
    filtered = []
    
    for trade in all_trades:
        try:
            entry_time = datetime.fromisoformat(trade.get('entry_time', ''))
            if entry_time >= cutoff_date:
                filtered.append(trade)
        except:
            continue
    
    # Sort by entry time (newest first) and limit
    filtered.sort(key=lambda x: x.get('entry_time', ''), reverse=True)
    return filtered[:limit]


def get_performance_stats(days: int = 30) -> Dict:
    """
    Get paper trading performance statistics
    
    Args:
        days: Analysis period in days
    
    Returns:
        Performance statistics dictionary
    """
    trades = get_trade_history(limit=10000, days=days)
    
    if not trades:
        return {
            'total_trades': 0,
            'win_rate': 0,
            'total_pnl': 0,
            'avg_pnl_per_trade': 0,
            'avg_holding_days': 0,
            'best_trade': None,
            'worst_trade': None,
            'active_trades': len(get_active_trades())
        }
    
    closed_trades = [t for t in trades if t['status'] == 'CLOSED']
    
    if not closed_trades:
        return {
            'total_trades': len(trades),
            'win_rate': 0,
            'total_pnl': 0,
            'avg_pnl_per_trade': 0,
            'avg_holding_days': 0,
            'best_trade': None,
            'worst_trade': None,
            'active_trades': len(get_active_trades())
        }
    
    wins = [t for t in closed_trades if t.get('pnl_amount', 0) > 0]
    losses = [t for t in closed_trades if t.get('pnl_amount', 0) <= 0]
    
    total_pnl = sum(t.get('pnl_amount', 0) for t in closed_trades)
    avg_pnl = total_pnl / len(closed_trades)
    avg_days = sum(t.get('days_held', 0) for t in closed_trades) / len(closed_trades)
    
    # Find best and worst trades
    best = max(closed_trades, key=lambda x: x.get('pnl_amount', 0))
    worst = min(closed_trades, key=lambda x: x.get('pnl_amount', 0))
    
    return {
        'total_trades': len(trades),
        'closed_trades': len(closed_trades),
        'active_trades': len(get_active_trades()),
        'win_rate': round((len(wins) / len(closed_trades)) * 100, 2) if closed_trades else 0,
        'total_pnl': round(total_pnl, 2),
        'avg_pnl_per_trade': round(avg_pnl, 2),
        'avg_holding_days': round(avg_days, 1),
        'winning_trades': len(wins),
        'losing_trades': len(losses),
        'best_trade': {
            'ticker': best['ticker'],
            'pnl': best['pnl_amount'],
            'pnl_percent': best['pnl_percent']
        },
        'worst_trade': {
            'ticker': worst['ticker'],
            'pnl': worst['pnl_amount'],
            'pnl_percent': worst['pnl_percent']
        }
    }


def load_active_trades_from_disk():
    """Load active trades from disk on startup"""
    with _cache_lock:
        _active_trades_cache.clear()
        all_trades = _load_all_trades()
        
        for trade in all_trades:
            if trade.get('status') == 'ACTIVE':
                ticker = trade.get('ticker')
                if ticker:
                    _active_trades_cache[ticker] = trade
        
        print(f"[PAPER TRADING] Loaded {len(_active_trades_cache)} active trades from disk")


# Initialize on module load
load_active_trades_from_disk()

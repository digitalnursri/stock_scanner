// centralized script to handle surgical DOM updates without fetching the API over and over
const LivePriceManager = {
    init: function(socket) {
        if (!socket) {
            console.error("No global socket provided to LivePriceManager");
            return;
        }

        socket.on('market_data_update', function (data) {
            if (data.live_prices && Object.keys(data.live_prices).length > 0) {
                // We have a direct payload of prices that changed.
                // Apply them directly to the DOM avoiding full API JSON re-fetch.
                
                // Find all live price cells on the page using a specific data attribute
                // (e.g., <td data-live-ticker="RELIANCE">...</td>)
                const priceElements = document.querySelectorAll('[data-live-ticker]');
                
                priceElements.forEach(el => {
                    const ticker = el.getAttribute('data-live-ticker');
                    const info = data.live_prices[ticker];
                    
                    if (info && info.p && info.p !== info.prev) {
                        // Update the text to the new price
                        
                        const prefix = el.getAttribute('data-live-prefix') || '₹';
                        const suffix = el.getAttribute('data-live-suffix') || '';
                        
                        // Check if we need to embed the ticker name as well (used in seasonal screener)
                        if (el.classList.contains('ticker-link-with-price')) {
                            el.innerHTML = `${ticker} (${prefix}${info.p}${suffix})`;
                        } else {
                            el.innerHTML = `${prefix}${info.p}${suffix}`;
                        }
                        
                        // Apply flash animation
                        let priceClass = info.p > info.prev ? 'flash-up-text' : 'flash-down-text';
                        el.classList.add(priceClass);
                        
                        // Also apply to parent row if specified
                        const row = el.closest('tr');
                        if (row && el.hasAttribute('data-flash-row')) {
                            let rowClass = info.p > info.prev ? 'flash-up' : 'flash-down';
                            row.classList.add(rowClass);
                            setTimeout(() => row.classList.remove(rowClass), 1500);
                        }
                        
                        // Remove flash text class after animation
                        setTimeout(() => {
                            el.classList.remove(priceClass);
                        }, 1500);
                    }
                });
            } else if (data.signal === 'refresh' && !data.live_prices) {
                // Fallback to old behavior: if the backend only sent a refresh signal
                // tell the page to run its local fetch method.
                if (typeof window.fetchResults === 'function') {
                    window.fetchResults(true);
                } else if (typeof window.loadScreenerData === 'function') {
                    window.loadScreenerData(true);
                } else if (typeof window.loadScannerData === 'function') {
                    window.loadScannerData(true);
                }
            }
        });
    }
};

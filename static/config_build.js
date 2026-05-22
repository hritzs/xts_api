// ════════════════════════════════════════════════════════════════════════════
// CONFIG HELPERS
// ════════════════════════════════════════════════════════════════════════════

/**
 * Fetches the latest IDV for the given symbol from the backend,
 * using a daily cache in localStorage to avoid redundant requests.
 * @param {string} symbol The stock symbol (e.g., "NIFTY").
 */
async function fetchAndSetLatestIdv(symbol) {
    const idvInput = document.getElementById('config-idv');
    if (!idvInput) return;

    const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
    const cacheKey = 'latestIdvCache';

    try {
        const cachedData = JSON.parse(localStorage.getItem(cacheKey));
        // Check if cache exists, is for today, and has the IDV for the symbol
        if (cachedData && cachedData.date === today && cachedData.idvMap && cachedData.idvMap[symbol]) {
            idvInput.value = cachedData.idvMap[symbol];
            return; // Exit if we successfully used the cache
        }
    } catch (e) {
        console.warn("No valid IDV cache found, fetching from server.");
    }

    try {
        const response = await fetch('/api/latest-idv');
        const result = await response.json();

        if (result.success && result.data) {
            const idvMap = result.data;
            localStorage.setItem(cacheKey, JSON.stringify({ date: today, idvMap: idvMap }));
            if (idvMap[symbol]) idvInput.value = idvMap[symbol];
        }
    } catch (error) {
        console.error('Error fetching latest IDV:', error);
    }
}

/**
 * Reads all configuration values from the UI form fields.
 * @returns {object} The configuration object.
 */
function getUiConfig() {
    const ceStrikeVal = document.getElementById('config-ce-strike').value;
    const peStrikeVal = document.getElementById('config-pe-strike').value;

    // --- MODIFIED: Convert "Qty Per Call" to "Lots Per Call" for the backend ---
    const lotSize = window._chainATMTokens?.lot_size;
    const qtyPerCallVal = document.getElementById('auto_lots_per_call').value;
    let lotsPerCall = 1;

    if (qtyPerCallVal) {
        const qtyPerCall = parseInt(qtyPerCallVal);
        if (lotSize && lotSize > 0 && qtyPerCall > 0) {
            lotsPerCall = Math.ceil(qtyPerCall / lotSize) || 1;
        } else if (qtyPerCall > 0) {
            lotsPerCall = qtyPerCall; // Fallback to treating input as lots if lot size is unavailable
        }
    }

    return {
        symbol: document.getElementById('config-symbol').value,
        size: parseInt(document.getElementById('config-size').value),
        ce_strike_price: ceStrikeVal ? parseInt(ceStrikeVal) : null,
        pe_strike_price: peStrikeVal ? parseInt(peStrikeVal) : null,
        idv: parseFloat(document.getElementById('config-idv').value),
        idv_divisor: parseFloat(document.getElementById('config-idv-divisor').value),
        straddle_filter: parseFloat(document.getElementById('config-straddle-filter').value),
        straddle_stop_loss_pct: parseFloat(document.getElementById('config-straddle-stop-pct').value),
        entry_time: document.getElementById('config-entry-time').value,
        exit_time: document.getElementById('config-exit-time').value,
        hedge_div: parseFloat(document.getElementById('config-hedge-div').value),
        straddle_div: parseFloat(document.getElementById('config-straddle-div').value),
        roll_straddle_div: parseFloat(document.getElementById('config-roll-straddle-div').value),
        hedge_frac: parseFloat(document.getElementById('config-hedge-frac').value),
        sl_bps: parseFloat(document.getElementById('config-sl-bps').value),
        hedge_monitor_interval: parseFloat(document.getElementById('config-hedge-interval').value) || 60.0,
        sl_monitor_interval: parseFloat(document.getElementById('config-sl-interval').value) || 60.0,
        roll_monitor_interval: parseFloat(document.getElementById('config-roll-interval').value) || 60.0,
        hedge_start_time: document.getElementById('config-hedge-start-time').value || null,
        sl_start_time: document.getElementById('config-sl-start-time').value || null,
        roll_start_time: document.getElementById('config-roll-start-time').value || null,
        buy_buffer: parseInt(document.getElementById('config-buy-buffer').value),
        sell_buffer: parseInt(document.getElementById('config-sell-buffer').value),
        order_lots_per_call: lotsPerCall
    };
}

/**
 * Populates the UI form fields from a configuration object.
 * @param {object} config The configuration object.
 */
function setUiConfig(config) {
    document.getElementById('config-symbol').value = config.symbol;
    document.getElementById('config-size').value = config.size;
    document.getElementById('config-ce-strike').value = config.ce_strike_price || '';
    document.getElementById('config-pe-strike').value = config.pe_strike_price || '';
    document.getElementById('config-idv').value = config.idv;
    document.getElementById('config-idv-divisor').value = config.idv_divisor;
    document.getElementById('config-straddle-filter').value = config.straddle_filter;
    document.getElementById('config-straddle-stop-pct').value = config.straddle_stop_loss_pct || 1.0;
    // Time-sensitive fields are now handled by setUiDefaults to always provide fresh values.
    // document.getElementById('config-entry-time').value = config.entry_time;
    // document.getElementById('config-exit-time').value = config.exit_time;
    document.getElementById('config-hedge-div').value = config.hedge_div;
    document.getElementById('config-straddle-div').value = config.straddle_div;
    document.getElementById('config-roll-straddle-div').value = config.roll_straddle_div;
    document.getElementById('config-hedge-frac').value = config.hedge_frac;
    document.getElementById('config-sl-bps').value = config.sl_bps;
    document.getElementById('config-sl-interval').value = config.sl_monitor_interval || 60.0;
    document.getElementById('config-hedge-interval').value = config.hedge_monitor_interval || 60.0;
    document.getElementById('config-roll-interval').value = config.roll_monitor_interval || 60.0;
    // document.getElementById('config-hedge-start-time').value = config.hedge_start_time || '';
    // document.getElementById('config-sl-start-time').value = config.sl_start_time || '';
    // document.getElementById('config-roll-start-time').value = config.roll_start_time || '';
    // Load buffer values, taking absolute value for compatibility with old configs
    document.getElementById('config-buy-buffer').value = config.buy_buffer != null ? Math.abs(config.buy_buffer) : 2;
    document.getElementById('config-sell-buffer').value = config.sell_buffer != null ? Math.abs(config.sell_buffer) : 2;
    
    // --- MODIFIED: Convert saved "Lots Per Call" back to "Qty Per Call" for UI display ---
    if (config.order_lots_per_call !== undefined) {
        const lotSize = window._chainATMTokens?.lot_size;
        let displayQty = config.order_lots_per_call;
        if (lotSize && lotSize > 0) {
            displayQty = config.order_lots_per_call * lotSize;
        }
        document.getElementById('auto_lots_per_call').value = displayQty;
    }
}

/**
 * Sets the default time values in the UI form fields on page load.
 */
function setUiDefaults() {
    const now = new Date();
    // Add 1 minute and round down to the start of that minute
    now.setMinutes(now.getMinutes() + 1);
    now.setSeconds(0);
    now.setMilliseconds(0);

    const nextMinuteStr = now.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });

    // Determine default exit time
    // Standard market exit is 15:27:00
    let exitTimeStr = '15:27:00';
    
    const marketExit = new Date();
    marketExit.setHours(15, 27, 0, 0);

    // If the proposed entry time (now) is at or after the standard market exit,
    // default the exit time to 1 hour after entry for testing.
    if (now >= marketExit) {
        const testExit = new Date(now);
        testExit.setHours(testExit.getHours() + 1);
        exitTimeStr = testExit.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
    }

    // Always set time-sensitive fields to fresh defaults, overriding any saved values.
    document.getElementById('config-entry-time').value = nextMinuteStr;
    document.getElementById('config-hedge-start-time').value = nextMinuteStr;
    document.getElementById('config-sl-start-time').value = nextMinuteStr;
    document.getElementById('config-roll-start-time').value = nextMinuteStr;
    document.getElementById('config-exit-time').value = exitTimeStr;

    // --- MODIFIED: Always set these specific defaults on page load, overriding any saved values for these fields. ---
    // This ensures the form starts with a consistent, safe baseline for these critical parameters.
    document.getElementById('config-size').value = 77;
    document.getElementById('config-hedge-div').value = 57;
    document.getElementById('config-straddle-div').value = 4;
    document.getElementById('config-roll-straddle-div').value = 0.2;

    // Auto-set buffer based on initial symbol state
    const symbol = document.getElementById('config-symbol').value.toUpperCase();
    const defaultBuffer = symbol.includes('SENSEX') ? 6 : 2;
    document.getElementById('config-buy-buffer').value = defaultBuffer;
    document.getElementById('config-sell-buffer').value = defaultBuffer;
}

// ════════════════════════════════════════════════════════════════════════════
// CONFIG BUILD
// ════════════════════════════════════════════════════════════════════════════

document.getElementById('btn-config-build').addEventListener('click', async () => {
    let config = getUiConfig();

    // --- MODIFIED: Convert approximate quantity to lots ---
    const lotSize = window._chainATMTokens?.lot_size;
    if (!lotSize || lotSize <= 0) {
        showNotification('Lot size not available. Please fetch option chain first.', 'error');
        return;
    }

    const approxQuantity = config.size; // User input from 'size' field is now quantity
    if (isNaN(approxQuantity) || approxQuantity <= 0) {
        showNotification('Please enter a valid approximate quantity.', 'error');
        return;
    }

    // Round to nearest multiple of lot size and then calculate lots
    const roundedQuantity = Math.round(approxQuantity / lotSize) * lotSize;
    const calculatedLots = roundedQuantity / lotSize;

    if (calculatedLots <= 0) {
        showNotification('Calculated lots are zero. Please enter a larger quantity.', 'error');
        return;
    }
    config.size = calculatedLots; // Update config with calculated lots for the backend
    
    // --- MODIFIED: Handle optional strikes and add 2% validation ---
    const spotPriceEl = document.getElementById('chain-spot-value');
    const atmStrikeEl = document.getElementById('chain-atm-value');

    if (!spotPriceEl || !spotPriceEl.textContent || !atmStrikeEl || !atmStrikeEl.textContent) {
        showNotification('Spot price or ATM strike not available. Please fetch option chain first.', 'error');
        return;
    }

    const spotPrice = parseFloat(spotPriceEl.textContent.replace('₹', ''));
    const atmStrike = parseInt(atmStrikeEl.textContent);

    if (isNaN(spotPrice) || isNaN(atmStrike)) {
        showNotification('Could not parse spot price or ATM strike.', 'error');
        return;
    }

    // If one strike is provided, use ATM for the other.
    if (config.ce_strike_price && !config.pe_strike_price) {
        config.pe_strike_price = atmStrike;
        document.getElementById('config-pe-strike').value = atmStrike;
    } else if (!config.ce_strike_price && config.pe_strike_price) {
        config.ce_strike_price = atmStrike;
        document.getElementById('config-ce-strike').value = atmStrike;
    }

    // Validate strikes against 1% range of spot price, only if custom strikes are used.
    if (config.ce_strike_price || config.pe_strike_price) {
        const lowerBound = spotPrice * 0.99;
        const upperBound = spotPrice * 1.01;

        if (config.ce_strike_price && (config.ce_strike_price < lowerBound || config.ce_strike_price > upperBound)) {
            showNotification(`CE Strike ${config.ce_strike_price} is outside the 1% range of the spot price (₹${spotPrice.toFixed(2)}).`, 'error');
            return;
        }
        if (config.pe_strike_price && (config.pe_strike_price < lowerBound || config.pe_strike_price > upperBound)) {
            showNotification(`PE Strike ${config.pe_strike_price} is outside the 1% range of the spot price (₹${spotPrice.toFixed(2)}).`, 'error');
            return;
        }
    }

    // Add backend-specific defaults
    config.roll_flag_check_interval = 60;

    const statusDiv = document.getElementById('config-status-content');
    statusDiv.innerHTML = '<div class="placeholder">🔄 Starting automated build...</div>';

    try {
        const response = await fetch('/api/straddle/config-build', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });

        const data = await response.json();

        if (data.success) {
            statusDiv.innerHTML = `
                <div class="log-entry log-success">
                    <span class="log-level">SUCCESS</span>
                    <span class="log-message">${data.message} for ${config.symbol} @ ${config.entry_time}.</span>
                </div>
            `;
        } else {
            statusDiv.innerHTML = `
                <div class="log-entry log-error">
                    <span class="log-level">ERROR</span>
                    <span class="log-message">Build Failed: ${data.error}</span>
                </div>
            `;
        }
    } catch (error) {
        statusDiv.innerHTML = `
            <div class="log-entry log-error">
                <span class="log-level">ERROR</span>
                <span class="log-message">Network Error: ${error.message}</span>
            </div>
        `;
    }
});

// ════════════════════════════════════════════════════════════════════════════
// CONFIG BUILD EVENT HANDLERS
// ════════════════════════════════════════════════════════════════════════════

function handleConfigBuildSuccess(data) {
    const statusDiv = document.getElementById('config-status-content');
    statusDiv.innerHTML = `
        <div class="log-entry log-success">
            <span class="log-level">SUCCESS</span>
            <span class="log-message">Position Built! UID: ${data.trade_uid} at ${new Date(data.timestamp).toLocaleTimeString()}</span>
        </div>
    `;
    showNotification(`✅ Position Built: ${data.trade_uid}`, 'success');
    fetchStraddles();
}

function handleConfigBuildFailed(data) {
    const statusDiv = document.getElementById('config-status-content');
    statusDiv.innerHTML = `
        <div class="log-entry log-error">
            <span class="log-level">FAILED</span>
            <span class="log-message">Build Failed: ${data.reason} at ${new Date(data.timestamp).toLocaleTimeString()}</span>
        </div>
    `;
    showNotification(`❌ Build Failed: ${data.reason}`, 'error');
}

// ════════════════════════════════════════════════════════════════════════════
// CONFIG SAVE/LOAD
// ════════════════════════════════════════════════════════════════════════════

document.getElementById('btn-save-config').addEventListener('click', () => {
    const config = getUiConfig();
    localStorage.setItem('tradingConfig', JSON.stringify(config));
    showNotification('💾 Configuration saved!', 'success');
});

document.getElementById('btn-load-config').addEventListener('click', () => {
    const configStr = localStorage.getItem('tradingConfig');

    if (!configStr) {
        showNotification('❌ No saved configuration found.', 'error');
        return;
    }

    try {
        const config = JSON.parse(configStr);
        setUiConfig(config);
        showNotification('📂 Configuration loaded.', 'success');
    } catch (error) {
        showNotification('❌ Failed to load configuration.', 'error');
    }
});

// ════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ════════════════════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
    // Attempt to load a saved config first. If it exists, it will populate the fields.
    const savedConfig = localStorage.getItem('tradingConfig');
    if (savedConfig) {
        try {
            setUiConfig(JSON.parse(savedConfig));
        } catch (e) {
            console.error("Failed to parse saved config:", e);
        }
    }
    // After attempting to load, set defaults for any fields that are still empty.
    setUiDefaults();

    // NEW: Fetch latest IDV for the initial symbol
    const initialSymbol = document.getElementById('config-symbol').value;
    fetchAndSetLatestIdv(initialSymbol);

    // Auto-update buffers based on Symbol selection
    document.getElementById('config-symbol').addEventListener('change', function() {
        const symbol = this.value.toUpperCase();
        const isSensex = symbol.includes('SENSEX');
        const defaultBuffer = isSensex ? 6 : 2;
        
        document.getElementById('config-buy-buffer').value = defaultBuffer;
        document.getElementById('config-sell-buffer').value = defaultBuffer;

        // NEW: Fetch latest IDV for the selected symbol
        fetchAndSetLatestIdv(symbol);
    });
});
// ════════════════════════════════════════════════════════════════════════════
// CONFIG HELPERS
// ════════════════════════════════════════════════════════════════════════════

/**
 * Reads all configuration values from the UI form fields.
 * @returns {object} The configuration object.
 */
function getUiConfig() {
    return {
        symbol: document.getElementById('config-symbol').value,
        size: parseInt(document.getElementById('config-size').value),
        idv: parseFloat(document.getElementById('config-idv').value),
        idv_divisor: parseFloat(document.getElementById('config-idv-divisor').value),
        straddle_filter: parseFloat(document.getElementById('config-straddle-filter').value),
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
        sell_buffer: parseInt(document.getElementById('config-sell-buffer').value)
    };
}

/**
 * Populates the UI form fields from a configuration object.
 * @param {object} config The configuration object.
 */
function setUiConfig(config) {
    document.getElementById('config-symbol').value = config.symbol;
    document.getElementById('config-size').value = config.size;
    document.getElementById('config-idv').value = config.idv;
    document.getElementById('config-idv-divisor').value = config.idv_divisor;
    document.getElementById('config-straddle-filter').value = config.straddle_filter;
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
}

// ════════════════════════════════════════════════════════════════════════════
// CONFIG BUILD
// ════════════════════════════════════════════════════════════════════════════

document.getElementById('btn-config-build').addEventListener('click', async () => {
    const config = getUiConfig();
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
});
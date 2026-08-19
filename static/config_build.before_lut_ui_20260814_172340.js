// ════════════════════════════════════════════════════════════════════════════
// CONFIG HELPERS
// ════════════════════════════════════════════════════════════════════════════

const CONFIG_STORAGE_KEY = 'tradingConfig';
const SCORE_POLL_INTERVAL_MS = 3000;
const SCORE_PREVIEW_DEBOUNCE_MS = 300;

let scorePollTimer = null;
let scorePreviewDebounceTimer = null;
let scorePreviewAbortController = null;

function getEl(id) {
    return document.getElementById(id);
}

function setValue(id, value) {
    const el = getEl(id);
    if (!el) {
        console.warn(`Missing element for setValue: #${id}`);
        return;
    }
    el.value = value ?? '';
}

function setText(id, value) {
    const el = getEl(id);
    if (!el) {
        console.warn(`Missing element for setText: #${id}`);
        return;
    }

    const finalValue = value === null || value === undefined || value === '' ? '--' : String(value);

    if (
        el instanceof HTMLInputElement ||
        el instanceof HTMLTextAreaElement ||
        el instanceof HTMLSelectElement
    ) {
        el.value = finalValue;
    } else {
        el.textContent = finalValue;
    }
}

function toNum(value, fallback = null) {
    if (value === '' || value === null || value === undefined) return fallback;
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function normalizeVolValue(value, fallback = null) {
    const n = toNum(value, fallback);
    if (n === null || n === undefined || !Number.isFinite(n)) return fallback;
    if (n <= 0) return fallback;
    return n > 1 ? n / 100 : n;
}

function formatNumber(value, digits = 2) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '--';
    return n.toFixed(digits);
}

function displayValue(value, formatter = null) {
    if (value === null || value === undefined || value === '') return '--';
    if (formatter) return formatter(value);
    return value;
}

function getTodayYMD() {
    return new Date().toISOString().split('T')[0];
}

function parseTimeToSeconds(value) {
    if (!value) return null;
    const parts = value.split(':').map(Number);
    if (parts.length < 2) return null;
    const h = parts[0] ?? '';
    const m = parts[1] ?? '';
    const s = parts[2] ?? '';
    return h * 3600 + m * 60 + s;
}

function isEntryTimeValid(value) {
    const sec = parseTimeToSeconds(value);
    if (sec === null) return false;
    return sec <= parseTimeToSeconds('13:30:00');
}

function enforceEntryCutoff() {
    const entryInput = getEl('config-entry-time');
    if (!entryInput) return true;

    const value = entryInput.value;
    if (!value) return true;

    if (!isEntryTimeValid(value)) {
        entryInput.value = '13:30:00';
        if (typeof showNotification === 'function') {
            showNotification('Entry time cannot be later than 13:15:00.', 'error');
        }
        return false;
    }

    return true;
}

function updateScoreBanner(message, type = 'neutral') {
    const banner = getEl('score-status-banner');
    if (!banner) {
        console.warn('Missing element: #score-status-banner');
        return;
    }

    const styles = {
        neutral: { bg: '#222', color: '#ddd', border: '#444' },
        success: { bg: '#12351f', color: '#7CFC98', border: '#2b7a46' },
        error: { bg: '#3b1616', color: '#ff9b9b', border: '#884040' },
        warning: { bg: '#3a2c12', color: '#ffd27f', border: '#8a6a2f' },
        info: { bg: '#16263b', color: '#9fd0ff', border: '#355d88' }
    };

    const style = styles[type] || styles.neutral;
    banner.textContent = message;
    banner.style.background = style.bg;
    banner.style.color = style.color;
    banner.style.border = `1px solid ${style.border}`;
}

function clearScoreFields() {
    [
        // New Fields
        'score-live-iv',
        'score-adj-iv',
        'score-live-straddle',
        'score-adj-idv',
        'score-prev-straddle',
        'score-iv-idv-ratio',
        'score-straddle-ratio',
        'score-norm-og-gap',
        'score-adj-iv-chg',
        'score-dte-bucket',
        'score-iv-ratio-bucket',
        'score-straddle-ratio-bucket',
        'score-build-iv-bucket',
        'score-norm-og-gap-bucket',
        'score-adj-iv-chg-bucket',
        'score-prev-day-adj-iv',
        'score-og-gap-pct',
        'score-future-price-ref',
        'score-synthetic-price-ref',
        'score-price-ref-source',
        'score-decision-1',
        'score-decision-2',
        'score-size-multiplier'
    ].forEach(id => setText(id, '--'));
}

function resetScoreDisplay() {
    clearScoreFields();
    updateScoreBanner('Waiting for score...', 'neutral');
}

function buildScoreBannerMessage(score) {
    const sellAllowed = score?.sell_allowed;
    const scoreAvailable = score?.score_available;
    const manualLoadRequired = score?.manual_load_required;
    const warnings = Array.isArray(score?.warnings) ? score.warnings : [];

    if (sellAllowed === true && scoreAvailable) {
        const sizePct = (score.size_multiplier * 100).toFixed(0);
        return {
            message: `PASS | LUTs allow trade with ${sizePct}% size.`,
            type: 'success'
        };
    }

    if (scoreAvailable === true && sellAllowed === false) {
        return {
            message: `LUT BLOCK | Conditions not met | Sell Not Allowed`,
            type: 'warning'
        };
    }

    if (manualLoadRequired) {
        return {
            message: score.manual_load_message || 'Reference data missing. Manual load required before final score is available.',
            type: 'warning'
        };
    }

    if (warnings.length > 0) {
        return {
            message: `Partial score available | ${warnings[0]}`,
            type: 'info'
        };
    }

    return {
        message: `Awaiting valid conditions...`,
        type: 'info'
    };
}

function renderScoreData(score) {
    if (!score) {
        resetScoreDisplay();
        return;
    }

    console.log('RENDER SCORE DATA', score);

    // Update with new fields
    setText('score-live-iv', displayValue(score.live_iv, v => formatNumber(v, 6)));
    setText('score-live-iv-decimal', displayValue(score.live_iv, v => formatNumber(v / 100.0, 6)));
    setText('score-adj-iv', displayValue(score.adj_iv, v => formatNumber(v, 6)));
    setText('score-live-straddle', displayValue(score.live_straddle, v => formatNumber(v, 2)));
    setText('score-adj-idv', displayValue(score.adj_idv, v => formatNumber(v, 6)));
    setText('score-prev-straddle', displayValue(score.prev_day_straddle, v => formatNumber(v, 2)));
    setText('score-prev-day-adj-iv', displayValue(score.prev_day_adj_iv, v => formatNumber(v, 6)));
    setText('score-iv-idv-ratio', displayValue(score.iv_idv_ratio, v => formatNumber(v, 4)));
    setText('score-straddle-ratio', displayValue(score.straddle_ratio, v => formatNumber(v, 4)));
    setText('score-og-gap-pct', displayValue(score.og_gap_pct, v => `${(v * 100).toFixed(2)}%`));
    setText('score-norm-og-gap', displayValue(score.norm_og_gap, v => formatNumber(v, 6)));
    setText('score-adj-iv-chg', displayValue(score.adj_iv_chg, v => formatNumber(v, 6))); // Show as decimal

    // Update with new bucket fields
    const payload = score.lut_payload || {};
    setText('score-dte-bucket', displayValue(payload.DTE));
    setText('score-iv-ratio-bucket', displayValue(payload.IV_Ratio));
    setText('score-straddle-ratio-bucket', displayValue(payload.Straddle_Ratio));
    setText('score-build-iv-bucket', displayValue(payload.Build_IV));
    setText('score-norm-og-gap-bucket', displayValue(payload.Norm_OG_Gap));
    setText('score-adj-iv-chg-bucket', displayValue(payload.Adj_IV_Chg));
    setText('score-future-price-ref', displayValue(score.future_price_ref, v => formatNumber(v, 2)));
    setText('score-synthetic-price-ref', displayValue(score.synthetic_price_ref, v => formatNumber(v, 2)));
    setText('score-price-ref-source', displayValue(score.price_ref_source));

    // Update dual-LUT decision table
    setText('score-decision-1', displayValue(score.decision_1));
    setText('score-decision-2', displayValue(score.decision_2));
    setText('score-size-multiplier', displayValue(score.size_multiplier, v => `${(v * 100).toFixed(0)}%`));
}

function triggerScorePreviewDebounced(delay = SCORE_PREVIEW_DEBOUNCE_MS) {
    if (scorePreviewDebounceTimer) {
        clearTimeout(scorePreviewDebounceTimer);
    }

    scorePreviewDebounceTimer = setTimeout(() => {
        fetchLiveScorePreview({ silent: true });
        restartScorePolling();
    }, delay);
}

async function fetchLatestIdvValue() {
    return null;
}

function getUiConfig() {
    const ceStrikeEl = getEl('config-ce-strike');
    const peStrikeEl = getEl('config-pe-strike');
    const ceStrikeVal = ceStrikeEl ? ceStrikeEl.value : '';
    const peStrikeVal = peStrikeEl ? peStrikeEl.value : '';

    const lotSize = window._chainATMTokens?.lot_size;
    const qtyPerCallEl = getEl('auto_lots_per_call');
    const qtyPerCallVal = qtyPerCallEl ? qtyPerCallEl.value : '';
    let lotsPerCall = 1;

    if (qtyPerCallVal) {
        const qtyPerCall = parseInt(qtyPerCallVal, 10);
        if (lotSize && lotSize > 0 && qtyPerCall > 0) {
            lotsPerCall = Math.ceil(qtyPerCall / lotSize) || 1;
        } else if (qtyPerCall > 0) {
            lotsPerCall = qtyPerCall;
        }
    }

    return {
        symbol: getEl('config-symbol')?.value || 'NIFTY',
        size: parseInt(getEl('config-size')?.value, 10),
        ce_strike_price: ceStrikeVal ? parseInt(ceStrikeVal, 10) : null,
        pe_strike_price: peStrikeVal ? parseInt(peStrikeVal, 10) : null,
        entry_time: getEl('config-entry-time')?.value || '',
        exit_time: getEl('config-exit-time')?.value || '',
        hedge_div: toNum(getEl('config-hedge-div')?.value, 57),
        straddle_div: toNum(getEl('config-straddle-div')?.value, 4),
        roll_straddle_div: toNum(getEl('config-roll-straddle-div')?.value, 0.001),
        hedge_frac: toNum(getEl('config-hedge-frac')?.value, 1.0),
        sl_bps: toNum(getEl('config-sl-bps')?.value, 14),
        hedge_monitor_interval: toNum(getEl('config-hedge-interval')?.value, 60.0),
        sl_monitor_interval: toNum(getEl('config-sl-interval')?.value, 60.0),
        roll_monitor_interval: toNum(getEl('config-roll-interval')?.value, 60.0),
        roll_flag_check_interval: 60,
        hedge_start_time: getEl('config-hedge-start-time')?.value || null,
        sl_start_time: getEl('config-sl-start-time')?.value || null,
        roll_start_time: getEl('config-roll-start-time')?.value || null,
        buy_buffer: parseInt(getEl('config-buy-buffer')?.value, 10),
        sell_buffer: parseInt(getEl('config-sell-buffer')?.value, 10),
        straddle_stop_loss_pct: toNum(getEl('config-straddle-stop-pct')?.value, 1.0),
        manual_latest_idv: toNum(getEl('config-manual-latest-idv')?.value, null),
        manual_historical_idv: toNum(getEl('config-manual-historical-idv')?.value, null),
        manual_prev_day_straddle: toNum(getEl('config-manual-prev-day-straddle')?.value, null),
        tp_points: toNum(getEl('config-tp-points')?.value, null),
        tp_bps: toNum(getEl('config-tp-bps')?.value, null), // New TP BPS field
        manual_spot_price: toNum(getEl('config-manual-spot-price')?.value, null),
        order_lots_per_call: lotsPerCall,
        straddle_price_drop_trigger: toNum(getEl('config-straddle-price-drop-trigger')?.value, null),
        exit_at_straddle: toNum(getEl('config-exit-at-straddle')?.value, null), // New
        straddle_price_drop_pct_sqf: toNum(getEl('config-straddle-price-drop-pct-sqf')?.value, null), // New
    };
}

function setUiConfig(config) {
    if (!config) return;

    setValue('config-symbol', config.symbol ?? 'NIFTY');
    setValue('config-size', config.size ?? 77);
    setValue('config-ce-strike', config.ce_strike_price || '');
    setValue('config-pe-strike', config.pe_strike_price || '');
    setValue('config-straddle-stop-pct', config.straddle_stop_loss_pct ?? 1.0);
    setValue('config-hedge-div', config.hedge_div ?? 57);
    setValue('config-straddle-div', config.straddle_div ?? 4);
    setValue('config-roll-straddle-div', config.roll_straddle_div ?? 0.001);
    setValue('config-hedge-frac', config.hedge_frac ?? 1.0);
    setValue('config-sl-bps', config.sl_bps ?? 14);
    setValue('config-sl-interval', config.sl_monitor_interval ?? 60.0);
    setValue('config-hedge-interval', config.hedge_monitor_interval ?? 60.0);
    setValue('config-roll-interval', config.roll_monitor_interval ?? 60.0);
    setValue('config-buy-buffer', config.buy_buffer != null ? Math.abs(config.buy_buffer) : 2);
    setValue('config-sell-buffer', config.sell_buffer != null ? Math.abs(config.sell_buffer) : 2);
    setValue('config-entry-time', config.entry_time ?? '');
    setValue('config-exit-time', config.exit_time ?? '');
    setValue('config-hedge-start-time', config.hedge_start_time ?? '');
    setValue('config-sl-start-time', config.sl_start-time ?? '');
    setValue('config-roll-start-time', config.roll_start_time ?? '');
    setValue('config-manual-latest-idv', config.manual_latest_idv ?? '');
    setValue('config-manual-historical-idv', config.manual_historical_idv ?? '');
    setValue('config-manual-prev-day-straddle', config.manual_prev_day_straddle ?? '');
    setValue('config-tp-points', config.tp_points ?? '');
    setValue('config-tp-bps', config.tp_bps ?? ''); // New TP BPS field
    setValue('config-manual-spot-price', config.manual_spot_price ?? '');
    setValue('config-straddle-price-drop-trigger', config.straddle_price_drop_trigger ?? '');
    setValue('config-exit-at-straddle', config.exit_at_straddle ?? ''); // New
    setValue('config-straddle-price-drop-pct-sqf', config.straddle_price_drop_pct_sqf ?? ''); // New

    if (config.order_lots_per_call !== undefined) {
        const lotSize = window._chainATMTokens?.lot_size;
        let displayQty = config.order_lots_per_call;
        if (lotSize && lotSize > 0) {
            displayQty = config.order_lots_per_call * lotSize;
        }
        setValue('auto_lots_per_call', displayQty);
    }
}

function setUiDefaults() {
    const now = new Date();
    now.setMinutes(now.getMinutes() + 1);
    now.setSeconds(0);
    now.setMilliseconds(0);

    const hh = String(now.getHours()).padStart(2, '0');
    const mm = String(now.getMinutes()).padStart(2, '0');
    const ss = String(now.getSeconds()).padStart(2, '0');
    let nextMinuteStr = `${hh}:${mm}:${ss}`;

    if (!isEntryTimeValid(nextMinuteStr)) {
        nextMinuteStr = '13:30:00';
    }

    let exitTimeStr = '15:27:00';

    const marketExit = new Date();
    marketExit.setHours(15, 27, 0, 0);

    if (now >= marketExit) {
        const testExit = new Date(now);
        testExit.setHours(testExit.getHours() + 1);
        const eh = String(testExit.getHours()).padStart(2, '0');
        const em = String(testExit.getMinutes()).padStart(2, '0');
        const es = String(testExit.getSeconds()).padStart(2, '0');
        exitTimeStr = `${eh}:${em}:${es}`;
    }

    if (!getEl('config-entry-time')?.value) setValue('config-entry-time', nextMinuteStr);
    if (!getEl('config-hedge-start-time')?.value) setValue('config-hedge-start-time', nextMinuteStr);
    if (!getEl('config-sl-start-time')?.value) setValue('config-sl-start-time', nextMinuteStr);
    if (!getEl('config-roll-start-time')?.value) setValue('config-roll-start-time', nextMinuteStr);
    if (!getEl('config-exit-time')?.value) setValue('config-exit-time', exitTimeStr);

    if (!getEl('config-size')?.value) setValue('config-size', 77);
    if (!getEl('config-hedge-div')?.value) setValue('config-hedge-div', 57);
    if (!getEl('config-straddle-div')?.value) setValue('config-straddle-div', 4);
    if (!getEl('config-roll-straddle-div')?.value) setValue('config-roll-straddle-div', 0.001);
    if (!getEl('config-straddle-stop-pct')?.value) setValue('config-straddle-stop-pct', 1.0);

    const symbol = (getEl('config-symbol')?.value || 'NIFTY').toUpperCase();
    const defaultBuffer = symbol.includes('SENSEX') ? 6 : 2;

    if (!getEl('config-buy-buffer')?.value) setValue('config-buy-buffer', defaultBuffer);
    if (!getEl('config-sell-buffer')?.value) setValue('config-sell-buffer', defaultBuffer);
}

async function fetchLiveScorePreview(options = {}) {
    const { silent = false } = options;
    const symbolEl = getEl('config-symbol');
    if (!symbolEl) return null;

    const config = getUiConfig();
    console.log('CONFIG SCORE PAYLOAD', config);

    try {
        if (scorePreviewAbortController) {
            scorePreviewAbortController.abort();
        }
        scorePreviewAbortController = new AbortController();

        const response = await fetch('/api/straddle/config-score-preview', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config),
            signal: scorePreviewAbortController.signal
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();
        console.log('CONFIG SCORE RESPONSE', data);

        
        // ------------------------------------------------------------------
        // Support BOTH response formats:
        //
        // Old:
        // {
        //     success: true,
        //     score: {...}
        // }
        //
        // New:
        // {
        //     symbol: "...",
        //     decision: "...",
        //     ...
        // }
        // ------------------------------------------------------------------

        const score =
            (data && data.score)
                ? data.score
                : data;

        if (
            score &&
            typeof score === "object" &&
            (
                score.symbol ||
                score.decision ||
                score.sell_allowed !== undefined
            )
        ) {

            renderScoreData(score);

            const bannerInfo = buildScoreBannerMessage(score);

            if (bannerInfo) {
                updateScoreBanner(
                    bannerInfo.message,
                    bannerInfo.type
                );
            }

            return score;
        }

        clearScoreFields();

        updateScoreBanner(
            data?.error ||
            data?.message ||
            "Score preview unavailable.",
            "warning"
        );

        return data;

    } catch (error) {
        if (error.name === 'AbortError') {
            return null;
        }

        console.error('Score preview error:', error);

        if (!silent) {
            clearScoreFields();
            updateScoreBanner(`Score preview error: ${error.message}`, 'error');
        }

        return {
            success: false,
            error: error.message
        };
    }
}

function startScorePolling() {
    stopScorePolling();
    fetchLiveScorePreview({ silent: true });
    scorePollTimer = setInterval(() => {
        fetchLiveScorePreview({ silent: true });
    }, SCORE_POLL_INTERVAL_MS);
}
function stopScorePolling() {
    if (scorePollTimer) {
        clearInterval(scorePollTimer);
        scorePollTimer = null;
    }
    if (scorePreviewDebounceTimer) {
        clearTimeout(scorePreviewDebounceTimer);
        scorePreviewDebounceTimer = null;
    }
    if (scorePreviewAbortController) {
        scorePreviewAbortController.abort();
        scorePreviewAbortController = null;
    }
}

function restartScorePolling() {
    stopScorePolling();
    startScorePolling();
}

// ════════════════════════════════════════════════════════════════════════════
// CONFIG BUILD
// ════════════════════════════════════════════════════════════════════════════

const configBuildBtn = getEl('btn-config-build');
if (configBuildBtn) {
    configBuildBtn.addEventListener('click', async () => {
        let config = getUiConfig();

        // Enforce entry cutoff, still block if entry time is invalid
        if (!enforceEntryCutoff()) {
            return;
        }

        // Validate lot size
        const lotSize = window._chainATMTokens?.lot_size;
        if (!lotSize || lotSize <= 0) {
            if (typeof showNotification === 'function') {
                showNotification('Lot size not available. Please fetch option chain first.', 'error');
            }
            return;
        }

        // Validate approx quantity
        const approxQuantity = config.size;
        if (isNaN(approxQuantity) || approxQuantity <= 0) {
            if (typeof showNotification === 'function') {
                showNotification('Please enter a valid approximate quantity.', 'error');
            }
            return;
        }

        // Convert approx quantity to lots
        const roundedQuantity = Math.round(approxQuantity / lotSize) * lotSize;
        const calculatedLots = roundedQuantity / lotSize;

        if (calculatedLots <= 0) {
            if (typeof showNotification === 'function') {
                showNotification('Calculated lots are zero. Please enter a larger quantity.', 'error');
            }
            return;
        }

        config.size = calculatedLots;

        // Read spot and ATM from option-chain section
        const spotPriceEl = getEl('chain-synfut-value');
        const atmStrikeEl = getEl('chain-atm-value');

        if (!spotPriceEl || !spotPriceEl.textContent || !atmStrikeEl || !atmStrikeEl.textContent) {
            if (typeof showNotification === 'function') {
                showNotification('Spot price or ATM strike not available. Please fetch option chain first.', 'error');
            }
            return;
        }

        const spotPrice = parseFloat(spotPriceEl.textContent.replace('₹', '').replace(/,/g, ''));
        const atmStrike = parseInt(atmStrikeEl.textContent, 10);

        if (isNaN(spotPrice) || isNaN(atmStrike)) {
            if (typeof showNotification === 'function') {
                showNotification('Could not parse spot price or ATM strike.', 'error');
            }
            return;
        }

        // Auto-fill missing CE/PE strikes with ATM
        if (config.ce_strike_price && !config.pe_strike_price) {
            config.pe_strike_price = atmStrike;
            setValue('config-pe-strike', atmStrike);
        } else if (!config.ce_strike_price && config.pe_strike_price) {
            config.ce_strike_price = atmStrike;
            setValue('config-ce-strike', atmStrike);
        }

        // Validate CE/PE strikes within ±1% of spot
        if (config.ce_strike_price || config.pe_strike_price) {
            const lowerBound = spotPrice * 0.99;
            const upperBound = spotPrice * 1.01;

            if (config.ce_strike_price && (config.ce_strike_price < lowerBound || config.ce_strike_price > upperBound)) {
                if (typeof showNotification === 'function') {
                    showNotification(
                        `CE Strike ${config.ce_strike_price} is outside the 1% range of spot (₹${spotPrice.toFixed(2)}).`,
                        'error'
                    );
                }
                return;
            }

            if (config.pe_strike_price && (config.pe_strike_price < lowerBound || config.pe_strike_price > upperBound)) {
                if (typeof showNotification === 'function') {
                    showNotification(
                        `PE Strike ${config.pe_strike_price} is outside the 1% range of spot (₹${spotPrice.toFixed(2)}).`,
                        'error'
                    );
                }
                return;
            }
        }

        // Fixed roll flag interval
        config.roll_flag_check_interval = 60;

        const statusDiv = getEl('config-status-content');
        if (statusDiv) {
            statusDiv.innerHTML = '<div class="placeholder">🔄 Starting automated build...</div>';
        }

        try {
            // 1) Get a score preview for information only
            const previewData = await fetchLiveScorePreview();

            if (previewData?.success && previewData?.score) {
                renderScoreData(previewData.score);
            }

            // 2) Always call backend to start config-based build
            const response = await fetch('/api/straddle/config-build', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const data = await response.json();

            if (data.success) {
                if (statusDiv) {
                    statusDiv.innerHTML = `
                        <div class="log-entry log-success">
                            <span class="log-level">SUCCESS</span>
                            <span class="log-message">${data.message} for ${config.symbol} @ ${config.entry_time}.</span>
                        </div>
                    `;
                }
                updateScoreBanner('Automated build started. Monitoring live score...', 'info');
                startScorePolling();
            } else {
                stopScorePolling();
                if (statusDiv) {
                    statusDiv.innerHTML = `
                        <div class="log-entry log-error">
                            <span class="log-level">ERROR</span>
                            <span class="log-message">Build Failed: ${data.error || data.message || 'Unknown error'}</span>
                        </div>
                    `;
                }
                updateScoreBanner(`Build start failed: ${data.error || data.message || 'Unknown error'}`, 'error');
            }
        } catch (error) {
            console.error('Config build error:', error);
            stopScorePolling();
            const statusDiv = getEl('config-status-content');
            if (statusDiv) {
                statusDiv.innerHTML = `
                    <div class="log-entry log-error">
                        <span class="log-level">ERROR</span>
                        <span class="log-message">Network Error: ${error.message}</span>
                    </div>
                `;
            }
            updateScoreBanner(`Network error: ${error.message}`, 'error');
        }
    });
}

// ════════════════════════════════════════════════════════════════════════════
// CONFIG BUILD EVENT HANDLERS
// ════════════════════════════════════════════════════════════════════════════

function handleConfigBuildSuccess(data) {
    const statusDiv = getEl('config-status-content');
    if (statusDiv) {
        statusDiv.innerHTML = `
            <div class="log-entry log-success">
                <span class="log-level">SUCCESS</span>
                <span class="log-message">Position Built! UID: ${data.trade_uid} at ${new Date(data.timestamp).toLocaleTimeString()}</span>
            </div>
        `;
    }
    if (typeof showNotification === 'function') {
        showNotification(`✅ Position Built: ${data.trade_uid}`, 'success');
    }
    updateScoreBanner(`Position built successfully. UID: ${data.trade_uid}`, 'success');
    stopScorePolling();
    if (typeof fetchStraddles === 'function') {
        fetchStraddles();
    }
}

function handleConfigBuildFailed(data) {
    const statusDiv = getEl('config-status-content');
    if (statusDiv) {
        statusDiv.innerHTML = `
            <div class="log-entry log-error">
                <span class="log-level">FAILED</span>
                <span class="log-message">Build Failed: ${data.reason} at ${new Date(data.timestamp).toLocaleTimeString()}</span>
            </div>
        `;
    }
    if (typeof showNotification === 'function') {
        showNotification(`❌ Build Failed: ${data.reason}`, 'error');
    }
    updateScoreBanner(`Build failed: ${data.reason}`, 'error');
    stopScorePolling();
}

// ════════════════════════════════════════════════════════════════════════════
// MANUAL BUILD
// ════════════════════════════════════════════════════════════════════════════

const manualBuildBtn = getEl('btn-manual-build');
if (manualBuildBtn) {
    manualBuildBtn.addEventListener('click', async () => {
        const symbol = getEl('manual-symbol')?.value;
        const lots = parseInt(getEl('manual-lots')?.value, 10);
        const deltaNeutral = getEl('manual-delta-neutral')?.checked;
        const orderLotsPerCall = parseInt(getEl('manual_lots_per_call')?.value, 10) || 1;

        const resultDiv = getEl('manual-build-result');
        if (resultDiv) {
            resultDiv.innerHTML = '<p>🔄 Building straddle...</p>';
        }

        try {
            const response = await fetch('/api/straddle/sell', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    symbol: symbol,
                    lots: lots,
                    delta_neutral: deltaNeutral,
                    order_lots_per_call: orderLotsPerCall
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const data = await response.json();

            if (data.success) {
                if (resultDiv) {
                    resultDiv.innerHTML = `
                        <div style="background: #d4edda; padding: 15px; border-radius: 5px; border: 1px solid #c3e6cb;">
                            <h3 style="color: #155724;">✅ Straddle Built Successfully!</h3>
                            <p><strong>Trade UID:</strong> ${data.trade_uid}</p>
                            <p><strong>Strike:</strong> ${data.data.strike}</p>
                            <p><strong>CE Quantity:</strong> ${data.data.ce_quantity}</p>
                            <p><strong>PE Quantity:</strong> ${data.data.pe_quantity}</p>
                            <p><strong>Net Delta:</strong> ${data.data.net_delta?.toFixed(2) || 'N/A'}</p>
                        </div>
                    `;
                }
            } else {
                if (resultDiv) {
                    resultDiv.innerHTML = `
                        <div style="background: #f8d7da; padding: 15px; border-radius: 5px; border: 1px solid #f5c6cb;">
                            <h3 style="color: #721c24;">❌ Build Failed</h3>
                            <p>${data.error || data.message || 'Unknown error'}</p>
                        </div>
                    `;
                }
            }
        } catch (error) {
            console.error('Manual build error:', error);
            if (resultDiv) {
                resultDiv.innerHTML = `
                    <div style="background: #f8d7da; padding: 15px; border-radius: 5px;">
                        <h3 style="color: #721c24;">❌ Error</h3>
                        <p>${error.message}</p>
                    </div>
                `;
            }
        }
    });
}

// ════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ════════════════════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', async () => {
    // save/load removed: always start from defaults

    setUiDefaults();
    enforceEntryCutoff();
    resetScoreDisplay();

    const symbolEl = getEl('config-symbol');
    const entryEl = getEl('config-entry-time');

    if (symbolEl) {
        symbolEl.addEventListener('change', function () {
            const symbol = (this.value || '').toUpperCase();
            const isSensex = symbol.includes('SENSEX');
            const defaultBuffer = isSensex ? 6 : 2;

            setValue('config-buy-buffer', defaultBuffer);
            setValue('config-sell-buffer', defaultBuffer);

            triggerScorePreviewDebounced();
        });
    }

    if (entryEl) {
        entryEl.addEventListener('change', () => {
            enforceEntryCutoff();
            triggerScorePreviewDebounced();
        });
    }

    [
        'config-symbol',
        'config-size',
        'config-ce-strike',
        'config-pe-strike',
        'config-entry-time',
        'config-exit-time',
        'config-hedge-div',
        'config-straddle-div',
        'config-roll-straddle-div',
        'config-hedge-frac',
        'config-sl-bps',
        'config-hedge-interval',
        'config-sl-interval',
        'config-roll-interval',
        'config-hedge-start-time',
        'config-sl-start-time',
        'config-roll-start-time',
        'config-buy-buffer',
        'config-sell-buffer',
        'config-straddle-stop-pct',
        'config-manual-latest-idv',
        'config-manual-historical-idv',
        'config-manual-prev-day-straddle',
        'config-tp-points',
        'config-tp-bps', // New TP BPS field
        'config-manual-spot-price',
        'config-straddle-price-drop-trigger', // New
        'config-exit-at-straddle', // New
        'config-straddle-price-drop-pct-sqf', // New
    ].forEach(id => {
        const el = getEl(id);
        if (el) {
            el.addEventListener('change', () => triggerScorePreviewDebounced());
            el.addEventListener('input', () => triggerScorePreviewDebounced());
        } else {
            console.warn(`Missing config input element: #${id}`);
        }
    });

    await fetchLiveScorePreview({ silent: true });
    startScorePolling();
});
  
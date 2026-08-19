// ════════════════════════════════════════════════════════════════════════════
// websocket.js — Real-time WebSocket client
//
// Rule: no DOMContentLoaded here. init.js calls connectWebSocket().
// Rule: option_chain_update should keep the visible chain snapshot in sync.
//       It may patch header + row values in place, but should not do a full
//       table rebuild unless explicitly requested elsewhere.
//
// IMPORTANT ARCHITECTURE RULE:
// - option_chain_update is the ONLY authoritative live writer for the
//   option-chain UI state.
// - price_update / chain_header_update / chain_quote_update may still be used
//   by non-chain widgets, but they must NOT mutate option-chain table/header
//   state directly.
// ════════════════════════════════════════════════════════════════════════════

let straddleChannel = null;
try {
    straddleChannel = new BroadcastChannel('straddle_updates');
} catch (e) {
    console.warn('⚠️ BroadcastChannel unavailable in this browser/context:', e);
}

let ws = null;
let _reconnectTimer = null;
let _reconnectDelay = 1500;
const _RECONNECT_MAX = 30000;
const _RECONNECT_FACTOR = 1.5;

// Debug switch
window.__CHAIN_DEBUG = true;

// ════════════════════════════════════════════════════════════════════════════
// CONNECTION
// ════════════════════════════════════════════════════════════════════════════

function connectWebSocket() {
    if (ws && (
        ws.readyState === WebSocket.OPEN ||
        ws.readyState === WebSocket.CONNECTING
    )) {
        return;
    }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    console.log('🔌 Connecting to WebSocket:', wsUrl);
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log('✅ WebSocket connected');

        if (typeof updateStatus === 'function') {
            updateStatus('ws-status', 'LIVE', true);
        }

        _reconnectDelay = 1500;

        if (_reconnectTimer) {
            clearTimeout(_reconnectTimer);
            _reconnectTimer = null;
        }

        if (typeof handleLogMessage === 'function') {
            handleLogMessage({
                level: 'SUCCESS',
                message: 'Real-time connection established.',
                timestamp: new Date().toISOString()
            });
        }

        // Explicit resync after reconnect to restore missed state.
        const sym = _selectedSymbol() || window._currentChainSymbol || 'NIFTY';
        setTimeout(() => {
            if (typeof fetchOptionChain === 'function') {
                console.log('[WS-RESYNC-fetchOptionChain]', { symbol: sym });
                fetchOptionChain(sym);
            }
        }, 250);
    };

    ws.onmessage = (event) => {
        try {
            const parsed = JSON.parse(event.data);
            handleWebSocketMessage(parsed);
        } catch (e) {
            console.error('❌ WS parse error:', e, event?.data);
        }
    };

    ws.onerror = (error) => {
        console.error('❌ WebSocket error:', error);

        if (typeof updateStatus === 'function') {
            updateStatus('ws-status', 'ERROR', false);
        }
    };

    ws.onclose = () => {
        console.warn('🔴 WebSocket disconnected');

        if (typeof updateStatus === 'function') {
            updateStatus('ws-status', 'OFFLINE', false);
        }

        ws = null;

        if (typeof handleLogMessage === 'function') {
            handleLogMessage({
                level: 'ERROR',
                message: `Connection lost. Reconnecting in ${(_reconnectDelay / 1000).toFixed(1)}s...`,
                timestamp: new Date().toISOString()
            });
        }

        if (!_reconnectTimer) {
            const jitter = Math.floor(Math.random() * 300);
            const delayWithJitter = Math.min(_reconnectDelay + jitter, _RECONNECT_MAX);

            _reconnectTimer = setTimeout(() => {
                _reconnectTimer = null;
                _reconnectDelay = Math.min(_reconnectDelay * _RECONNECT_FACTOR, _RECONNECT_MAX);
                connectWebSocket();
            }, delayWithJitter);
        }
    };
}

// ════════════════════════════════════════════════════════════════════════════
// HELPERS
// ════════════════════════════════════════════════════════════════════════════

function _selectedSymbol() {
    return (document.getElementById('symbol')?.value || '').toUpperCase();
}

function _sameSymbol(a, b) {
    return String(a || '').toUpperCase() === String(b || '').toUpperCase();
}

function _safeNum(v, d = 0) {
    const n = Number(v);
    return Number.isFinite(n) ? n : d;
}

function _f2ws(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n.toFixed(2) : '--';
}

function _selectedSymbolMatches(data) {
    const selected = _selectedSymbol();
    if (!selected) return true;
    if (!data?.symbol) return true;
    return _sameSymbol(selected, data.symbol);
}

// Fallback local merge helper retained for resilience, but the preferred path
// is to delegate to applyOptionChainSnapshotPatch() from option_chain.js.
function _mergeChainSnapshot(incoming) {
    if (!incoming || typeof incoming !== 'object') return;

    const current = window._lastChainData || {};
    const merged = { ...current, ...incoming };

    if (Array.isArray(incoming.chain)) {
        if (incoming.is_full_snapshot === true) {
            merged.chain = incoming.chain;
        } else if (typeof _mergeRowsByStrike === 'function') {
            merged.chain = _mergeRowsByStrike(current.chain || [], incoming.chain);
        } else {
            merged.chain = incoming.chain;
        }
    } else if (Array.isArray(current.chain)) {
        merged.chain = current.chain;
    }

    window._lastChainData = merged;
}

function _refreshATMTokensFromSnapshot() {
    const chainData = window._lastChainData;
    if (!chainData?.chain || !chainData?.atm) return;

    const atmRow = chainData.chain.find(r => Number(r.strike) === Number(chainData.atm));
    if (!atmRow?.ce_token || !atmRow?.pe_token) return;

    window._chainATMTokens = {
        atm: Number(chainData.atm),
        ce_token: String(atmRow.ce_token),
        pe_token: String(atmRow.pe_token),
        gap: Number(chainData.gap ?? 50),
        lot_size: Number(atmRow.ce_lot_size ?? chainData.lot_size ?? 50),
        ce_ltp_snap: Number(atmRow.ce_ltp || 0),
        pe_ltp_snap: Number(atmRow.pe_ltp || 0),
    };
}

function _seedLastPricesFromChainRows(chainRows) {
    if (!Array.isArray(chainRows)) return;
    window._lastPrices = window._lastPrices || {};

    chainRows.forEach(row => {
        if (row?.ce_token != null && row?.ce_ltp != null) {
            window._lastPrices[String(row.ce_token)] = Number(row.ce_ltp);
        }
        if (row?.pe_token != null && row?.pe_ltp != null) {
            window._lastPrices[String(row.pe_token)] = Number(row.pe_ltp);
        }
    });
}

function _patchHeaderFromSnapshot(chainData) {
    if (!chainData) return;

    const spotEl = document.getElementById('chain-spot-value');
    const synFutEl = document.getElementById('chain-synfut-value');
    const atmEl = document.getElementById('chain-atm-value');
    const expiryEl = document.getElementById('chain-expiry-value');
    const varEl = document.getElementById('chain-var-value');

    const spot = _safeNum(chainData.synthetic_spot, 0);
    let syn = _safeNum(chainData.synthetic_spot, 0);

    const atm = Number(chainData.atm ?? 0);
    const atmRow = Array.isArray(chainData.chain)
        ? chainData.chain.find(r => Number(r.strike) === atm)
        : null;

    if (atm > 0 && atmRow) {
        const ce = _safeNum(atmRow.ce_ltp, 0);
        const pe = _safeNum(atmRow.pe_ltp, 0);
        if (ce > 0 && pe > 0) {
            syn = atm + ce - pe;
        }
    }

    if (spotEl) spotEl.textContent = spot > 0 ? `₹${spot.toFixed(2)}` : '--';
    if (synFutEl) synFutEl.textContent = syn > 0 ? `₹${syn.toFixed(2)}` : '--';
    if (atmEl) atmEl.textContent = atm || '--';
    if (expiryEl) expiryEl.textContent = chainData.expiry ?? '--';

    if (varEl) {
        if (_safeNum(chainData.var_pts, 0) > 0) {
            varEl.textContent = chainData.var_pct
                ? `±${Number(chainData.var_pts).toFixed(1)} pts (${Number(chainData.var_pct).toFixed(2)}%)`
                : `±${Number(chainData.var_pts).toFixed(1)} pts`;
        } else {
            varEl.textContent = '--';
        }
    }
}

function _debugDom24150(tag) {
    const tr = document.querySelector('#option-chain-container tr[data-strike="24150"]');
    if (!tr) {
        console.log(tag, 'DOM row 24150 not found');
        return;
    }

    const tds = tr.querySelectorAll('td');
    console.log(tag, {
        ce_bid:   tds[0]?.textContent?.trim(),
        ce_ask:   tds[1]?.textContent?.trim(),
        ce_theta: tds[2]?.textContent?.trim(),
        ce_vega:  tds[3]?.textContent?.trim(),
        ce_iv:    tds[4]?.textContent?.trim(),
        ce_ltp:   tds[5]?.textContent?.trim(),
        strike:   tds[6]?.textContent?.trim(),
        pe_ltp:   tds[7]?.textContent?.trim(),
        pe_iv:    tds[8]?.textContent?.trim(),
        pe_vega:  tds[9]?.textContent?.trim(),
        pe_theta: tds[10]?.textContent?.trim(),
        pe_bid:   tds[11]?.textContent?.trim(),
        pe_ask:   tds[12]?.textContent?.trim(),
    });
}

function _debug24150(prefix, chainData) {
    if (!chainData?.chain) return;

    const symbol = String(chainData.symbol || '').toUpperCase();
    if (symbol !== 'NIFTY') return;

    const row = chainData.chain.find(r => Number(r.strike) === 24150);
    if (!row) return;

    console.log(prefix, {
        symbol,
        spot: chainData.synthetic_spot,
        syn: chainData.synthetic_spot,
        atm: chainData.atm,
        published_at: chainData.published_at,
        row24150: {
            ce_bid: row.ce_bid,
            ce_ask: row.ce_ask,
            ce_ltp: row.ce_ltp,
            pe_ltp: row.pe_ltp,
            pe_bid: row.pe_bid,
            pe_ask: row.pe_ask,
            ce_iv: row.ce_iv,
            pe_iv: row.pe_iv,
            ce_vega: row.ce_vega,
            pe_vega: row.pe_vega,
            ce_theta: row.ce_theta,
            pe_theta: row.pe_theta,
        }
    });
}

function _patchSingleRow(rowData) {
    if (!rowData) return;

    if (window.__CHAIN_DEBUG && Number(rowData?.strike) === 24150) {
        console.log('[PATCH-SINGLE-ROW-24150-IN]', structuredClone(rowData));
    }

    const rowEl = document.querySelector(
        `#option-chain-container tr[data-strike="${rowData.strike}"]`
    );
    if (!rowEl) return;

    const cells = rowEl.querySelectorAll('td');
    if (!cells || cells.length < 13) return;

    cells[0].textContent = _f2ws(rowData.ce_bid);
    cells[1].textContent = _f2ws(rowData.ce_ask);

    cells[2].textContent = _f2ws(rowData.ce_theta);
    cells[2].className = typeof _thetaCls === 'function' ? _thetaCls(rowData.ce_theta) : '';

    cells[3].textContent = _f2ws(rowData.ce_vega);

    cells[4].textContent = `${_f2ws(rowData.ce_iv)}%`;
    cells[4].className = typeof _ivCls === 'function' ? _ivCls(rowData.ce_iv) : '';

    const ceCell = cells[5];
    const ceSpan = ceCell.querySelector('span');
    if (ceSpan) ceSpan.textContent = rowData.ce_ltp != null ? `₹${Number(rowData.ce_ltp).toFixed(2)}` : '--';
    if (rowData.ce_ltp != null) ceCell.setAttribute('data-prev-price', Number(rowData.ce_ltp));
    if (rowData.ce_token != null) ceCell.setAttribute('data-token', String(rowData.ce_token));

    cells[6].textContent = rowData.strike ?? '--';

    const peCell = cells[7];
    const peSpan = peCell.querySelector('span');
    if (peSpan) peSpan.textContent = rowData.pe_ltp != null ? `₹${Number(rowData.pe_ltp).toFixed(2)}` : '--';
    if (rowData.pe_ltp != null) peCell.setAttribute('data-prev-price', Number(rowData.pe_ltp));
    if (rowData.pe_token != null) peCell.setAttribute('data-token', String(rowData.pe_token));

    cells[8].textContent = `${_f2ws(rowData.pe_iv)}%`;
    cells[8].className = typeof _ivCls === 'function' ? _ivCls(rowData.pe_iv) : '';

    cells[9].textContent = _f2ws(rowData.pe_vega);

    cells[10].textContent = _f2ws(rowData.pe_theta);
    cells[10].className = typeof _thetaCls === 'function' ? _thetaCls(rowData.pe_theta) : '';

    cells[11].textContent = _f2ws(rowData.pe_bid ?? rowData.pe_bid_price);
    cells[12].textContent = _f2ws(rowData.pe_ask ?? rowData.pe_ask_price);

    if (window.__CHAIN_DEBUG && Number(rowData?.strike) === 24150) {
        _debugDom24150('[PATCH-SINGLE-ROW-24150-OUT-DOM]');
    }
}

function _patchTableFromSnapshot(chainData) {
    if (!chainData?.chain || !Array.isArray(chainData.chain)) return;

    chainData.chain.forEach(row => _patchSingleRow(row));

    const atm = Number(chainData.atm ?? 0);
    if (atm > 0) {
        document.querySelectorAll('#option-chain-container tr[data-strike]').forEach(row => {
            const strike = Number(row.getAttribute('data-strike'));
            if (strike === atm) row.classList.add('atm-row');
            else row.classList.remove('atm-row');
        });
    }
}

// ════════════════════════════════════════════════════════════════════════════
// HANDLERS
// ════════════════════════════════════════════════════════════════════════════

function handlePriceUpdate(data) {
    if (!data) return;

    if (window.__CHAIN_DEBUG) {
        console.log('[WS-price_update-raw]', data);
        _debug24150('[BEFORE-price_update-MEM]', window._lastChainData);
        _debugDom24150('[BEFORE-price_update-DOM]');
    }

    // Non-chain widgets may still depend on updatePrice(token, ltp).
    if (typeof updatePrice === 'function') {
        for (const [token, ltp] of Object.entries(data)) {
            updatePrice(parseInt(token, 10), ltp);
        }
    }

    // IMPORTANT:
    // Do NOT call handleChainPriceUpdate(data) here.
    // option_chain_update is the only live writer for option-chain UI state.

    if (window.__CHAIN_DEBUG) {
        _debug24150('[AFTER-price_update-MEM]', window._lastChainData);
        _debugDom24150('[AFTER-price_update-DOM]');
    }
}

function handleChainHeader(msg) {
    if (!msg) return;

    // Keep legacy spot widgets alive if they exist.
    const el =
        document.getElementById('spot-price-value') ??
        document.getElementById('live-spot');

    const spot = Number(msg?.spot ?? msg?.data?.spot ?? 0);
    if (el && Number.isFinite(spot) && spot > 0) {
        el.textContent = `₹${spot.toFixed(2)}`;
    }

    // IMPORTANT:
    // Do NOT call handleChainHeaderUpdate(msg) here.
    // option_chain_update must own the option-chain header state.
}

function handleOptionChainUpdate(data) {
    if (!data) return;

    const selected = _selectedSymbol();
    if (selected && data.symbol && !_sameSymbol(data.symbol, selected)) {
        return;
    }

    if (window.__CHAIN_DEBUG) {
        console.log('[WS-option_chain_update-raw]', {
            published_at: data?.published_at,
            symbol: data?.symbol,
            is_full_snapshot: data?.is_full_snapshot,
            row24150: data?.chain?.find?.(r => Number(r.strike) === 24150)
        });
        _debug24150('[BEFORE-option_chain_update-MEM]', window._lastChainData);
        _debugDom24150('[BEFORE-option_chain_update-DOM]');
    }

    if (typeof _snapshotIsStale === 'function' && _snapshotIsStale(data, 'WS')) {
        return;
    }

    // Preferred path: delegate to option_chain.js single-source-of-truth handler.
    if (typeof applyOptionChainSnapshotPatch === 'function') {
        const applied = applyOptionChainSnapshotPatch(data);

        if (typeof updateLiveScoreMonitorFromChain === 'function') {
            updateLiveScoreMonitorFromChain(window._lastChainData || data);
        }

        if (window.__CHAIN_DEBUG) {
            console.log('[WS-option_chain_update-applied]', applied);
            _debug24150('[AFTER-option_chain_update-MEM]', window._lastChainData);
            _debugDom24150('[AFTER-option_chain_update-DOM]');
        }

        return;
    }

    // Fallback path only if option_chain.js helper isn't available.
    if (typeof _mergeChainSnapshot === 'function') {
        _mergeChainSnapshot(data);
    } else {
        window._lastChainData = {
            ...(window._lastChainData || {}),
            ...data,
            chain: Array.isArray(data.chain)
                ? data.chain
                : (window._lastChainData?.chain || [])
        };
    }

    const chainData = window._lastChainData;
    if (!chainData) return;

    if (typeof _adoptSnapshotVersion === 'function') {
        _adoptSnapshotVersion(chainData);
    } else if (data.published_at) {
        window._chainLastPublishedAt = data.published_at;
    }

    if (chainData.symbol) {
        window._currentChainSymbol = String(chainData.symbol).toUpperCase();
    }

    if (chainData.fut_token != null) {
        const div = document.getElementById('option-chain-container');
        if (div) div.setAttribute('data-fut-token', chainData.fut_token);
    }

    if (typeof _seedLastPricesFromChainRows === 'function') {
        _seedLastPricesFromChainRows(data.chain || []);
    } else if (Array.isArray(data.chain)) {
        window._lastPrices = window._lastPrices || {};
        data.chain.forEach(row => {
            if (row?.ce_token != null && row?.ce_ltp != null) {
                window._lastPrices[String(row.ce_token)] = Number(row.ce_ltp);
            }
            if (row?.pe_token != null && row?.pe_ltp != null) {
                window._lastPrices[String(row.pe_token)] = Number(row.pe_ltp);
            }
        });
    }

    if (chainData.fut_token != null && chainData.fut_ltp != null) {
        window._lastPrices = window._lastPrices || {};
        window._lastPrices[String(chainData.fut_token)] = Number(chainData.fut_ltp);
    }

    if (typeof _refreshATMTokensFromSnapshot === 'function') {
        _refreshATMTokensFromSnapshot();
    } else if (chainData?.chain && chainData?.atm) {
        const atmRow = chainData.chain.find(r => Number(r.strike) === Number(chainData.atm));
        if (atmRow?.ce_token && atmRow?.pe_token) {
            window._chainATMTokens = {
                ...(window._chainATMTokens || {}),
                atm: Number(chainData.atm),
                ce_token: String(atmRow.ce_token),
                pe_token: String(atmRow.pe_token),
                gap: Number(chainData.gap ?? window._chainATMTokens?.gap ?? 50),
                lot_size: Number(atmRow.ce_lot_size ?? chainData.lot_size ?? window._chainATMTokens?.lot_size ?? 50),
                ce_ltp_snap: Number(atmRow.ce_ltp || 0),
                pe_ltp_snap: Number(atmRow.pe_ltp || 0),
            };
        }
    }

    if (typeof _patchHeaderFromSnapshot === 'function') {
        _patchHeaderFromSnapshot(chainData);
    }

    if (Array.isArray(data.chain) && data.chain.length > 0) {
        if (typeof _patchTableFromSnapshot === 'function') {
            _patchTableFromSnapshot(chainData);
        } else {
            data.chain.forEach(row => {
                const tr = document.querySelector(
                    `#option-chain-container tr[data-strike="${row.strike}"]`
                );
                if (!tr) return;

                const tds = tr.querySelectorAll('td');
                if (!tds || tds.length < 13) return;

                tds[0].textContent = _f2ws(row.ce_bid);
                tds[1].textContent = _f2ws(row.ce_ask);

                tds[2].textContent = _f2ws(row.ce_theta);
                tds[2].className = typeof _thetaCls === 'function' ? _thetaCls(row.ce_theta) : '';

                tds[3].textContent = _f2ws(row.ce_vega);

                tds[4].textContent = `${_f2ws(row.ce_iv)}%`;
                tds[4].className = typeof _ivCls === 'function' ? _ivCls(row.ce_iv) : '';

                const ceCell = tds[5];
                const ceSpan = ceCell.querySelector('span');
                if (ceSpan) ceSpan.textContent = row.ce_ltp != null ? `₹${Number(row.ce_ltp).toFixed(2)}` : '--';
                if (row.ce_ltp != null) ceCell.setAttribute('data-prev-price', Number(row.ce_ltp));
                if (row.ce_token != null) ceCell.setAttribute('data-token', String(row.ce_token));

                tds[6].textContent = row.strike ?? '--';

                const peCell = tds[7];
                const peSpan = peCell.querySelector('span');
                if (peSpan) peSpan.textContent = row.pe_ltp != null ? `₹${Number(row.pe_ltp).toFixed(2)}` : '--';
                if (row.pe_ltp != null) peCell.setAttribute('data-prev-price', Number(row.pe_ltp));
                if (row.pe_token != null) peCell.setAttribute('data-token', String(row.pe_token));

                tds[8].textContent = `${_f2ws(row.pe_iv)}%`;
                tds[8].className = typeof _ivCls === 'function' ? _ivCls(row.pe_iv) : '';

                tds[9].textContent = _f2ws(row.pe_vega);

                tds[10].textContent = _f2ws(row.pe_theta);
                tds[10].className = typeof _thetaCls === 'function' ? _thetaCls(row.pe_theta) : '';

                tds[11].textContent = _f2ws(row.pe_bid ?? row.pe_bid_price);
                tds[12].textContent = _f2ws(row.pe_ask ?? row.pe_ask_price);
            });

            const atm = Number(chainData.atm ?? 0);
            if (atm > 0) {
                document.querySelectorAll('#option-chain-container tr[data-strike]').forEach(row => {
                    const strike = Number(row.getAttribute('data-strike'));
                    if (strike === atm) row.classList.add('atm-row');
                    else row.classList.remove('atm-row');
                });
            }
        }
    }

    if (typeof updateStraddlePremium === 'function') {
        updateStraddlePremium();
    }

    if (window.__CHAIN_DEBUG) {
        _debug24150('[AFTER-option_chain_update-MEM]', chainData);
        _debugDom24150('[AFTER-option_chain_update-DOM]');
    }

    if (typeof _debug24150 === 'function') {
        _debug24150('[UI-WS-option_chain_update]', chainData);
    }
}

// ════════════════════════════════════════════════════════════════════════════
// MESSAGE ROUTER
// ════════════════════════════════════════════════════════════════════════════

function handleWebSocketMessage(data) {
    if (!data || !data.type) return;

    switch (data.type) {
        case 'price_update':
            handlePriceUpdate(data.data);
            break;

        case 'chain_header_update':
            handleChainHeader(data);
            break;

        case 'option_chain_update':
            handleOptionChainUpdate(data.data);
            break;

        case 'chain_quote_update':
            // IMPORTANT:
            // Ignore for option-chain UI single-source-of-truth.
            // If some non-chain widget needs this later, route it there only.
            if (window.__CHAIN_DEBUG) {
                console.debug('[WS-chain_quote_update-ignored-for-chain-ui]', data.data);
            }
            break;

        case 'pnl_update':
            if (typeof updatePnLDisplay === 'function') {
                updatePnLDisplay(data.data);
            }
            break;

        case 'pnl_batch_update':
            if (typeof handlePnlBatchUpdate === 'function') {
                handlePnlBatchUpdate(data.data);
            }
            break;

        case 'straddle_update':
            straddleChannel?.postMessage(data.data);
            if (typeof handleStraddleUpdate === 'function') {
                handleStraddleUpdate(data.data);
            }
            break;

        case 'straddle_full_update':
            if (typeof handleStraddleFullUpdate === 'function') {
                handleStraddleFullUpdate(data.data);
            } else if (typeof handleStraddleUpdate === 'function') {
                handleStraddleUpdate(data.data);
            }
            break;

        case 'straddle_placed':
            if (typeof showNotification === 'function') {
                showNotification(`Straddle placed: ${data.trade_uid}`, 'success');
            }
            if (typeof fetchStraddles === 'function') {
                fetchStraddles();
            }
            break;

        case 'straddle_closed':
            if (typeof showNotification === 'function') {
                showNotification(`Straddle closed: ${data.trade_uid}`, 'success');
            }
            if (typeof fetchStraddles === 'function') {
                fetchStraddles();
            }
            break;

        case 'log_message':
            if (typeof handleLogMessage === 'function') {
                handleLogMessage(data.data);
            }
            break;

        case 'config_build_success':
            if (typeof handleConfigBuildSuccess === 'function') {
                handleConfigBuildSuccess(data);
            }
            break;

        case 'config_build_failed':
            if (typeof handleConfigBuildFailed === 'function') {
                handleConfigBuildFailed(data);
            }
            break;

        case 'xts_socket_status':
            if (typeof updateStatus === 'function') {
                updateStatus(
                    'socket-status',
                    data?.data?.connected ? 'XTS LIVE' : 'XTS OFF',
                    Boolean(data?.data?.connected)
                );
            }
            break;

        case 'ping':
            break;

        default:
            console.debug('⚠️ Unknown WS message type:', data.type, data);
    }
}

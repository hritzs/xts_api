// ════════════════════════════════════════════════════════════════════════════
// websocket.js — Real-time WebSocket client
//
// Rule: no DOMContentLoaded here. init.js calls connectWebSocket().
// Rule: option_chain_update does NOT rebuild the table — only updates
//       header values. Full rebuilds happen only via fetchOptionChain().
// ════════════════════════════════════════════════════════════════════════════

const straddleChannel   = new BroadcastChannel('straddle_updates');

let ws                  = null;
let _reconnectTimer     = null;
let _reconnectDelay     = 1500;
const _RECONNECT_MAX    = 30000;
const _RECONNECT_FACTOR = 1.5;


// ════════════════════════════════════════════════════════════════════════════
// CONNECTION
// ════════════════════════════════════════════════════════════════════════════

function connectWebSocket() {
    if (ws && (ws.readyState === WebSocket.OPEN ||
               ws.readyState === WebSocket.CONNECTING)) return;

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl    = `${protocol}//${window.location.host}/ws`;

    console.log('🔌 Connecting to WebSocket:', wsUrl);
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log('✅ WebSocket connected');
        updateStatus('ws-status', 'LIVE', true);
        _reconnectDelay = 1500;

        if (_reconnectTimer) { clearTimeout(_reconnectTimer); _reconnectTimer = null; }

        if (typeof handleLogMessage === 'function')
            handleLogMessage({
                level:     'SUCCESS',
                message:   'Real-time connection established.',
                timestamp: new Date().toISOString()
            });
    };

    ws.onmessage = (event) => {
        try {
            handleWebSocketMessage(JSON.parse(event.data));
        } catch (e) {
            console.error('❌ WS parse error:', e);
        }
    };

    ws.onerror = (error) => {
        console.error('❌ WebSocket error:', error);
        updateStatus('ws-status', 'ERROR', false);
    };

    ws.onclose = () => {
        console.warn('🔴 WebSocket disconnected');
        updateStatus('ws-status', 'OFFLINE', false);
        ws = null;

        if (typeof handleLogMessage === 'function')
            handleLogMessage({
                level:     'ERROR',
                message:   `Connection lost. Reconnecting in ${(_reconnectDelay / 1000).toFixed(1)}s...`,
                timestamp: new Date().toISOString()
            });

        if (!_reconnectTimer) {
            _reconnectTimer = setTimeout(() => {
                _reconnectTimer = null;
                _reconnectDelay = Math.min(_reconnectDelay * _RECONNECT_FACTOR, _RECONNECT_MAX);
                connectWebSocket();
            }, _reconnectDelay);
        }
    };
}


// ════════════════════════════════════════════════════════════════════════════
// HANDLERS
// ════════════════════════════════════════════════════════════════════════════

function handlePriceUpdate(data) {
    if (!data) return;
    // Update global price cache first
    if (typeof updatePrice === 'function') {
        for (const [token, ltp] of Object.entries(data))
            updatePrice(parseInt(token), ltp);
    }
    // Then update chain cells (reads its own _lastPrices — no conflict)
    if (typeof handleChainPriceUpdate === 'function')
        handleChainPriceUpdate(data);
}

function handleChainHeader(msg) {
    if (typeof handleChainHeaderUpdate === 'function')
        handleChainHeaderUpdate(msg);

    // Update any generic spot display outside the chain panel
    const el = document.getElementById('spot-price-value')
            || document.getElementById('live-spot');
    if (el && msg.spot > 0) el.textContent = `₹${msg.spot.toFixed(2)}`;
}

function handleOptionChainUpdate(data) {
    // ✅ Safe path: update header values only — NEVER rebuild the table here.
    // Full rebuilds only happen via loadOptionChain() → fetchOptionChain().
    // This prevents the flicker caused by two competing DOM rewrites.
    if (!data) return;

    const selected = document.getElementById('symbol')?.value || '';
    if (selected && data.symbol &&
        data.symbol.toUpperCase() !== selected.toUpperCase()) return;

    // Delegate header-only update to option_chain.js handler
    if (data.fut_ltp || data.synthetic_spot || data.atm) {
        if (typeof handleChainHeaderUpdate === 'function') {
            handleChainHeaderUpdate({
                symbol:  data.symbol,
                spot:    data.fut_ltp,
                syn_fut: data.synthetic_spot || data.fut_ltp,
                atm:     data.atm,
                expiry:  data.expiry,
            });
        }
    }

    if (typeof updateStraddlePremium === 'function') updateStraddlePremium();
}


// ════════════════════════════════════════════════════════════════════════════
// MESSAGE ROUTER
// ════════════════════════════════════════════════════════════════════════════

function handleWebSocketMessage(data) {
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

        case 'pnl_update':
            if (typeof updatePnLDisplay === 'function') updatePnLDisplay(data.data);
            break;

        case 'pnl_batch_update':
            if (typeof handlePnlBatchUpdate === 'function') handlePnlBatchUpdate(data.data);
            break;

        case 'straddle_update':
            straddleChannel.postMessage(data.data);
            if (typeof handleStraddleUpdate === 'function') handleStraddleUpdate(data.data);
            break;

        case 'straddle_full_update':
            if (typeof handleStraddleFullUpdate === 'function')
                handleStraddleFullUpdate(data.data);
            else if (typeof handleStraddleUpdate === 'function')
                handleStraddleUpdate(data.data);
            break;

        case 'straddle_placed':
            if (typeof showNotification === 'function')
                showNotification(`Straddle placed: ${data.trade_uid}`, 'success');
            if (typeof fetchStraddles === 'function') fetchStraddles();
            break;

        case 'straddle_closed':
            if (typeof showNotification === 'function')
                showNotification(`Straddle closed: ${data.trade_uid}`, 'success');
            if (typeof fetchStraddles === 'function') fetchStraddles();
            break;

        case 'log_message':
            if (typeof handleLogMessage === 'function') handleLogMessage(data.data);
            break;

        case 'config_build_success':
            if (typeof handleConfigBuildSuccess === 'function') handleConfigBuildSuccess(data);
            break;

        case 'config_build_failed':
            if (typeof handleConfigBuildFailed === 'function') handleConfigBuildFailed(data);
            break;

        case 'xts_socket_status':
            updateStatus(
                'socket-status',
                data.data.connected ? 'XTS LIVE' : 'XTS OFF',
                data.data.connected
            );
            break;

        case 'ping':
            break;

        default:
            console.debug('⚠️ Unknown WS message type:', data.type);
    }
}
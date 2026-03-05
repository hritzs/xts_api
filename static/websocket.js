// ════════════════════════════════════════════════════════════════════════════
// BROADCAST CHANNEL for inter-tab communication
const straddleChannel = new BroadcastChannel('straddle_updates');
// ════════════════════════════════════════════════════════════════════════════
// WEBSOCKET CONNECTION
// ════════════════════════════════════════════════════════════════════════════
let reconnectInterval = null;

function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    console.log('🔌 Connecting to WebSocket:', wsUrl);

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log('✅ WebSocket connected');
        updateStatus('ws-status', 'LIVE', true);

        if (reconnectInterval) {
            clearInterval(reconnectInterval);
            reconnectInterval = null;
        }
        handleLogMessage({level: 'SUCCESS', message: 'Real-time connection established.', timestamp: new Date().toISOString()});
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            handleWebSocketMessage(data);
        } catch (error) {
            console.error('❌ WebSocket message error:', error);
        }
    };

    ws.onerror = (error) => {
        console.error('❌ WebSocket error:', error);
        updateStatus('ws-status', 'ERROR', false);
    };

    ws.onclose = () => {
        console.log('🔴 WebSocket disconnected');
        updateStatus('ws-status', 'OFFLINE', false);
        handleLogMessage({level: 'ERROR', message: 'Real-time connection lost. Reconnecting...', timestamp: new Date().toISOString()});

        if (!reconnectInterval) {
            reconnectInterval = setInterval(() => {
                console.log('🔄 Attempting to reconnect...');
                connectWebSocket();
            }, 5000);
        }
    };
}

function handlePriceUpdate(data) {
    // data is now expected to be a dictionary of {token: ltp, ...}
    if (typeof updatePrice === 'function' && data) {
        for (const [token, ltp] of Object.entries(data)) {
            // The token from JSON key is a string, needs to be parsed.
            updatePrice(parseInt(token), ltp);
        }
    }
}

function handleOptionChainUpdate(data) {
    if (typeof displayOptionChain === 'function') displayOptionChain(data);
    if (typeof renderOptionChain === 'function') renderOptionChain(data);
}

// ════════════════════════════════════════════════════════════════════════════
// WEBSOCKET MESSAGE HANDLER
// ════════════════════════════════════════════════════════════════════════════

function handleWebSocketMessage(data) {

    switch(data.type) {
        case 'price_update':
            handlePriceUpdate(data.data); // Pass the nested data object
            break;
        case 'pnl_update':
            updatePnLDisplay(data.data);
            break;
        case 'straddle_update':
            // Post to channel for other tabs/windows (like the details popup)
            straddleChannel.postMessage(data.data);
            // Handle update in the current tab
            handleStraddleUpdate(data.data);
            break;
        case 'straddle_full_update':
            // This is the main update from the snapshotter. It contains all live data.
            handleStraddleUpdate(data.data);
            break;
        case 'pnl_batch_update':
            if (typeof handlePnlBatchUpdate === 'function') {
                handlePnlBatchUpdate(data.data);
            }
            break;
        case 'straddle_placed':
            showNotification(`Straddle placed: ${data.trade_uid}`, 'success');
            fetchStraddles();
            break;
        case 'straddle_closed':
            showNotification(`Straddle closed: ${data.trade_uid}`, 'success');
            fetchStraddles();
            break;
        case 'log_message':
            handleLogMessage(data.data);
            break;
        case 'config_build_success':
            handleConfigBuildSuccess(data);
            break;
        case 'config_build_failed':
            handleConfigBuildFailed(data);
            break;
        case 'xts_socket_status':
            updateStatus('socket-status', data.data.connected ? 'XTS LIVE' : 'XTS OFF', data.data.connected);
            break;
        case 'option_chain_update':
            handleOptionChainUpdate(data.data);
            break;
        case 'ping':
            // Server heartbeat, ignore.
            break;
        default:
            console.log('Unknown message type:', data.type);
    }
}

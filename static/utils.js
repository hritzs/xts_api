// ════════════════════════════════════════════════════════════════════════════
// UTILITY FUNCTIONS
// ════════════════════════════════════════════════════════════════════════════

function updateStatus(elementId, text, status) {
    const element = document.getElementById(elementId);
    if (element) {
        element.textContent = text;
        element.className = `status-indicator ${status}`;
    }
}

function showNotification(title, message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: ${type === 'success' ? '#d4edda' : type === 'error' ? '#f8d7da' : type === 'warning' ? '#fff3cd' : '#d1ecf1'};
        border: 2px solid ${type === 'success' ? '#c3e6cb' : type === 'error' ? '#f5c6cb' : type === 'warning' ? '#ffc107' : '#bee5eb'};
        color: ${type === 'success' ? '#155724' : type === 'error' ? '#721c24' : type === 'warning' ? '#856404' : '#0c5460'};
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 5px 20px rgba(0,0,0,0.3);
        z-index: 10000;
        max-width: 400px;
        animation: slideIn 0.3s;
    `;

    notification.innerHTML = `
        <h4 style="margin: 0 0 10px 0;">${title}</h4>
        <p style="margin: 0;">${message}</p>
    `;

    document.body.appendChild(notification);

    // Auto remove after 5 seconds
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s';
        setTimeout(() => notification.remove(), 300);
    }, 5000);
}

// Add CSS animations
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from { transform: translateX(400px); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    @keyframes slideOut {
        from { transform: translateX(0); opacity: 1; }
        to { transform: translateX(400px); opacity: 0; }
    }
`;
document.head.appendChild(style);

// ════════════════════════════════════════════════════════════════════════════
// TIME DISPLAY
// ════════════════════════════════════════════════════════════════════════════

function updateTimeDisplay() {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('en-IN', {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    });
    document.getElementById('time-display').textContent = `⏰ ${timeStr}`;
}

setInterval(updateTimeDisplay, 1000);

// ════════════════════════════════════════════════════════════════════════════
// HEALTH CHECK
// ════════════════════════════════════════════════════════════════════════════

async function checkHealth() {
    try {
        const response = await fetch('/health');
        const data = await response.json();

        updateStatus('db-status', `💾 DB: ${data.db_status}`, data.db_status === 'connected' ? 'connected' : 'error');
        updateStatus('event-bus-status', `🚌 EventBus: ${data.event_bus}`, data.event_bus === 'active' ? 'connected' : 'error');

        if (data.socket_connected && ws && ws.readyState === WebSocket.OPEN) {
            updateStatus('socket-status', '🟢 Connected', 'connected');
        }
    } catch (error) {
        console.error('Health check error:', error);
    }
}

setInterval(checkHealth, 10000);

// ════════════════════════════════════════════════════════════════════════════
// PRICE UPDATE
// ════════════════════════════════════════════════════════════════════════════

function updateOptionChainPrice(token, ltp) {
    // Find and update LTP cells in the option chain table
    const table = document.querySelector('#option-chain-display table');
    if (!table) return;

    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 11) {
            // Check CE LTP (index 3) - the token is in the symbol cell (index 4)
            const ceSymbolCell = cells[4];
            const ceLtpCell = cells[3];
            if (ceSymbolCell && ceSymbolCell.textContent && ceSymbolCell.textContent.includes(`NIFTY${token}`)) {
                ceLtpCell.textContent = `₹${ltp.toFixed(2)}`;
            }

            // Check PE LTP (index 7) - the token is in the symbol cell (index 6)
            const peSymbolCell = cells[6];
            const peLtpCell = cells[7];
            if (peSymbolCell && peSymbolCell.textContent && peSymbolCell.textContent.includes(`NIFTY${token}`)) {
                peLtpCell.textContent = `₹${ltp.toFixed(2)}`;
            }
        }
    });
}

function handleStraddlePlaced(data) {
    console.log('Straddle placed:', data.trade_uid);
    showNotification('✅ STRADDLE PLACED', `Trade: ${data.trade_uid}`, 'success');
    fetchStraddles();
}

function handlePriceUpdate(data) {
    const token = data.token;
    const ltp = parseFloat(data.ltp);
    const ltpFormatted = ltp.toFixed(2);

    // Determine price change direction
    const prevPrice = previousPrices[token];
    let priceClass = '';
    if (prevPrice !== undefined) {
        if (ltp > prevPrice) {
            priceClass = 'price-up';
        } else if (ltp < prevPrice) {
            priceClass = 'price-down';
        }
    }
    previousPrices[token] = ltp;

    // Update spot price if it's the future token
    const displayDiv = document.getElementById('option-chain-display');
    const futToken = displayDiv.getAttribute('data-fut-token');
    if (futToken && parseInt(futToken) === token) {
        const spotElement = document.getElementById('spot-price');
        if (spotElement) {
            const span = spotElement.querySelector('span');
            if (span) {
                span.className = priceClass;
                span.textContent = `₹${ltpFormatted}`;
            }
        }
    }

    // Update CE cells
    document.querySelectorAll(`[data-ce-token="${token}"] span`)
        .forEach(span => {
            span.className = priceClass;
            span.textContent = `₹${ltpFormatted}`;
        });

    // Update PE cells
    document.querySelectorAll(`[data-pe-token="${token}"] span`)
        .forEach(span => {
            span.className = priceClass;
            span.textContent = `₹${ltpFormatted}`;
        });
}

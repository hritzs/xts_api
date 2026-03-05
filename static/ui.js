function updateTimeDisplay() {
    const now = new Date();
    const timeString = now.toLocaleTimeString('en-GB');
    document.getElementById('clock').textContent = timeString;
}

function updateStatus(elementId, text, isConnected) {
    const el = document.getElementById(elementId);
    if (el) {
        const dot = el.querySelector('.dot');
        const textEl = el.querySelector('.text');
        
        if (isConnected) {
            el.classList.remove('disconnected');
            el.classList.add('connected');
        } else {
            el.classList.remove('connected');
            el.classList.add('disconnected');
        }
        textEl.textContent = text;
    }
}

function updatePnLDisplay(data) {
    if (!data) return;

    const totalPnl = data.total_pnl || 0;
    const pnlValueEl = document.getElementById('total-pnl-value');
    
    pnlValueEl.textContent = `₹${totalPnl.toFixed(2)}`;
    pnlValueEl.className = 'summary-value';
    if (totalPnl > 0) pnlValueEl.classList.add('positive');
    else if (totalPnl < 0) pnlValueEl.classList.add('negative');
    else pnlValueEl.classList.add('neutral');

    const activeTrades = data.straddles ? data.straddles.length : 0;
    document.getElementById('active-trades-count').textContent = activeTrades;

    let totalPremium = 0;
    let totalNetDelta = 0;
    const straddles = data.straddles || [];

    straddles.forEach(s => {
        totalPremium += s.total_premium || 0;
        totalNetDelta += s.live_net_delta || s.net_delta || 0; // Assuming live_net_delta is preferred
    });

    document.getElementById('total-premium-received').textContent = `₹${totalPremium.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    
    const netDeltaEl = document.getElementById('total-net-delta');
    netDeltaEl.textContent = totalNetDelta.toFixed(2);
    netDeltaEl.className = 'summary-value';
    if (totalNetDelta > 1) netDeltaEl.classList.add('positive');
    else if (totalNetDelta < -1) netDeltaEl.classList.add('negative');
    else netDeltaEl.classList.add('neutral');
}

function handleLogMessage(data) {
    const logPanel = document.getElementById('log-panel-content');
    if (!logPanel) return;

    const logEntry = document.createElement('div');
    logEntry.className = `log-entry log-${data.level.toLowerCase()}`;

    const timestamp = new Date(data.timestamp).toLocaleTimeString('en-GB');

    logEntry.innerHTML = `
        <span class="log-timestamp">${timestamp}</span>
        <span class="log-level">${data.level}</span>
        <span class="log-message">${data.message}</span>
    `;

    logPanel.appendChild(logEntry);
    // Auto-scroll to the bottom
    logPanel.scrollTop = logPanel.scrollHeight;
}

function showNotification(message, type = 'success') {
    const notif = document.createElement('div');
    notif.className = `notification ${type}`;
    notif.textContent = message;
    document.body.appendChild(notif);
    
    setTimeout(() => notif.remove(), 4000);
}

function switchMainTab(tabName) {
    // Deactivate all tabs
    document.querySelectorAll('.main-tab-content').forEach(tab => tab.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));

    // Activate the selected tab
    const tabView = document.getElementById(`${tabName}-view`);
    if (tabView) {
        tabView.classList.add('active');
    } else {
        console.error(`Tab content view not found for: ${tabName}-view`);
    }
    document.querySelector(`.tab-btn[onclick="switchMainTab('${tabName}')"]`).classList.add('active');

    // Fetch data if needed
    if (tabName === 'portfolio') {
        fetchStraddles(true); // Pass a flag to render in portfolio view
        fetchOrders();
    }
}
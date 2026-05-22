// ════════════════════════════════════════════════════════════════════════════
// ui.js — Shared UI utilities only. No business logic.
// ════════════════════════════════════════════════════════════════════════════

// ── Clock ─────────────────────────────────────────────────────────────────
function updateTimeDisplay() {
    const el = document.getElementById('clock');
    if (el) el.textContent = new Date().toLocaleTimeString('en-GB');
}

// ── Status indicators (uses .dot + .text spans in HTML) ───────────────────
function updateStatus(elementId, text, isConnected) {
    const el = document.getElementById(elementId);
    if (!el) return;
    el.classList.toggle('connected',    !!isConnected);
    el.classList.toggle('disconnected', !isConnected);
    const textEl = el.querySelector('.text');
    if (textEl) textEl.textContent = text;
}

// ── Notifications ─────────────────────────────────────────────────────────
function showNotification(message, type = 'success') {
    const colors = {
        success: { bg: '#d4edda', border: '#c3e6cb', text: '#155724' },
        error:   { bg: '#f8d7da', border: '#f5c6cb', text: '#721c24' },
        warning: { bg: '#fff3cd', border: '#ffc107', text: '#856404' },
        info:    { bg: '#d1ecf1', border: '#bee5eb', text: '#0c5460' },
    };
    const c = colors[type] || colors.info;

    const notif = document.createElement('div');
    notif.style.cssText = `
        position:fixed; top:20px; right:20px; z-index:10000;
        background:${c.bg}; border:2px solid ${c.border}; color:${c.text};
        padding:15px 20px; border-radius:8px; max-width:380px;
        box-shadow:0 4px 15px rgba(0,0,0,0.3);
        font-family:'Inter',sans-serif; font-size:14px;
        animation:slideIn 0.3s ease;
    `;
    notif.textContent = message;
    document.body.appendChild(notif);
    setTimeout(() => {
        notif.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => notif.remove(), 300);
    }, 4000);
}

// Inject notification animations once
const _notifStyle = document.createElement('style');
_notifStyle.textContent = `
    @keyframes slideIn  { from { transform:translateX(420px); opacity:0; } to { transform:translateX(0); opacity:1; } }
    @keyframes slideOut { from { transform:translateX(0); opacity:1; } to { transform:translateX(420px); opacity:0; } }
`;
document.head.appendChild(_notifStyle);

// ── PnL summary bar ───────────────────────────────────────────────────────
function updatePnLDisplay(data) {
    if (!data) return;

    const totalPnl = data.total_pnl || 0;
    const pnlEl = document.getElementById('total-pnl-value');
    if (pnlEl) {
        pnlEl.textContent = `₹${totalPnl.toFixed(2)}`;
        pnlEl.className = 'summary-value ' +
            (totalPnl > 0 ? 'positive' : totalPnl < 0 ? 'negative' : 'neutral');
    }

    const straddles = data.straddles || [];
    const activeEl  = document.getElementById('active-trades-count');
    if (activeEl) activeEl.textContent = straddles.length;

    let totalPremium = 0;
    let totalNetDelta = 0;
    straddles.forEach(s => {
        totalPremium  += s.total_premium   || 0;
        totalNetDelta += s.live_net_delta  || s.net_delta || 0;
    });

    const premEl = document.getElementById('total-premium-received');
    if (premEl) premEl.textContent =
        `₹${totalPremium.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

    const deltaEl = document.getElementById('total-net-delta');
    if (deltaEl) {
        deltaEl.textContent = totalNetDelta.toFixed(2);
        deltaEl.className = 'summary-value ' +
            (totalNetDelta > 1 ? 'positive' : totalNetDelta < -1 ? 'negative' : 'neutral');
    }
}

// ── Event log ─────────────────────────────────────────────────────────────
function handleLogMessage(data) {
    const panel = document.getElementById('log-panel-content');
    if (!panel || !data) return;

    const entry = document.createElement('div');
    entry.className = `log-entry log-${(data.level || 'info').toLowerCase()}`;
    entry.innerHTML = `
        <span class="log-timestamp">${new Date(data.timestamp).toLocaleTimeString('en-GB')}</span>
        <span class="log-level">${data.level}</span>
        <span class="log-message">${data.message}</span>
    `;
    panel.appendChild(entry);
    panel.scrollTop = panel.scrollHeight;
}

// ── Tab switching ─────────────────────────────────────────────────────────
function switchMainTab(tabName) {
    document.querySelectorAll('.main-tab-content').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));

    const view = document.getElementById(`${tabName}-view`);
    if (view) view.classList.add('active');

    const btn = document.querySelector(`.tab-btn[onclick="switchMainTab('${tabName}')"]`);
    if (btn) btn.classList.add('active');

    if (tabName === 'portfolio') {
        if (typeof fetchStraddles === 'function') fetchStraddles(true);
        if (typeof fetchOrders   === 'function') fetchOrders();
    }
}

// ── Global price cache (read by straddles.js / option_chain.js) ──────────
window._globalPrices = {};
function updatePrice(token, ltp) {
    window._globalPrices[String(token)] = ltp;
}
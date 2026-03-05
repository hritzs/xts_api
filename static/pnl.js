// ════════════════════════════════════════════════════════════════════════════
// PNL
// ════════════════════════════════════════════════════════════════════════════

async function fetchPnL() {
    try {
        const response = await fetch('/api/pnl');
        const data = await response.json();

        if (data.success && data.data) {
            updatePnLDisplay(data.data);
        }
    } catch (error) {
        console.error('PnL fetch error:', error);
    }
}

function updatePnLDisplay(pnlData) {
    const totalPnl = pnlData.total_pnl || 0;
    const realizedPnl = pnlData.realized_pnl || 0;
    const unrealizedPnl = pnlData.unrealized_pnl || 0;

    document.getElementById('total-pnl').textContent = `₹${totalPnl.toFixed(2)}`;
    document.getElementById('total-pnl').className = `pnl-value ${totalPnl >= 0 ? 'positive' : 'negative'}`;

    document.getElementById('realized-pnl').textContent = `₹${realizedPnl.toFixed(2)}`;
    document.getElementById('realized-pnl').className = `pnl-value ${realizedPnl >= 0 ? 'positive' : 'negative'}`;

    document.getElementById('unrealized-pnl').textContent = `₹${unrealizedPnl.toFixed(2)}`;
    document.getElementById('unrealized-pnl').className = `pnl-value ${unrealizedPnl >= 0 ? 'positive' : 'negative'}`;
}

function handlePnLUpdate(data) {
    updatePnLDisplay(data);
}

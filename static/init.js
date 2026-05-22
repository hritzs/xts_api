// ════════════════════════════════════════════════════════════════════════════
// init.js — SOLE entry point for all startup logic.
//
// Rule: no other JS file may call connectWebSocket() or loadOptionChain()
//       from a DOMContentLoaded handler. Everything starts here.
// ════════════════════════════════════════════════════════════════════════════

async function initializeDashboard() {
    console.log('🚀 Dashboard initializing...');

    // 1. Clock
    updateTimeDisplay();
    setInterval(updateTimeDisplay, 1000);

    // 2. WebSocket — connect first so seed chain_header_update
    //    arrives as soon as the chain fetch completes
    connectWebSocket();

    // 3. Initial data — single authoritative call for each
    fetchStraddles();
    loadOptionChain();   // ← only call site for option chain init

    // 4. Periodic refresh for non-WS data only
    setInterval(() => {
        const isPortfolio = document.getElementById('portfolio-view')
            ?.classList.contains('active');
        if (isPortfolio) fetchOrders();
    }, 15000);

    console.log('✅ Dashboard ready');
}

// Single DOMContentLoaded for the entire app
document.addEventListener('DOMContentLoaded', initializeDashboard);
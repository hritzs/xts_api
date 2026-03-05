// ════════════════════════════════════════════════════════════════════════════
// INITIALIZATION
// ════════════════════════════════════════════════════════════════════════════
async function initializeDashboard() {
    console.log('🚀 Dashboard initialized');

    // Connect WebSocket
    connectWebSocket();

    // Update time immediately
    updateTimeDisplay();
    setInterval(updateTimeDisplay, 1000);

    // Load active straddles
    fetchStraddles();
    
    // Load option chain
    loadOptionChain();

    // Auto-refresh non-WebSocket data (like historical orders) less frequently
    setInterval(() => {
        const isPortfolio = document.getElementById('portfolio-view').classList.contains('active');
        // fetchStraddles(isPortfolio); // This is now handled by straddle_full_update
        // loadOptionChain(); // This is now handled by option_chain_update
        if (isPortfolio) fetchOrders(); // Keep this for historical orders, which don't have a dedicated push update
    }, 15000); // every 15 seconds
}

document.addEventListener('DOMContentLoaded', initializeDashboard);

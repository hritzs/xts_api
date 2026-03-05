// ════════════════════════════════════════════════════════════════════════════
// TAB NAVIGATION
// ════════════════════════════════════════════════════════════════════════════

document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const tabName = btn.dataset.tab;

        // Update tab buttons
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');

        // Update tab content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.remove('active');
        });
        document.getElementById(tabName).classList.add('active');

        // Load data for specific tabs
        if (tabName === 'positions') {
            fetchPositions();
        } else if (tabName === 'straddles') {
            fetchStraddles();
        } else if (tabName === 'orders') {
            fetchOrders();
        } else if (tabName === 'pnl') {
            fetchPnL();
        }
    });
});

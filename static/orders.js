async function fetchOrders() {
    const displayDiv = document.getElementById('portfolio-orders-display');
    try {
        const response = await fetch('/api/orders');
        const data = await response.json();

        if (data.success) {
            displayOrders(data.orders);
        } else {
            displayDiv.innerHTML = `<div class="placeholder">❌ ${data.error}</div>`;
        }
    } catch (error) {
        displayDiv.innerHTML = `<div class="placeholder">❌ ${error.message}</div>`;
    }
}

function displayOrders(orders) {
    const displayDiv = document.getElementById('portfolio-orders-display');

    if (!orders || orders.length === 0) {
        displayDiv.innerHTML = '<div class="placeholder">No orders found for today</div>';
        return;
    }

    let html = '<table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Avg. Price</th><th>Status</th><th>UID</th></tr></thead><tbody>';

    orders.forEach(order => {
        const statusClass = order.order_status === 'FILLED' ? 'positive' : (order.order_status === 'REJECTED' || order.order_status === 'CANCELLED') ? 'negative' : 'neutral';

        html += `
            <tr>
                <td>${new Date(order.formatted_time || order.order_generated_datetime).toLocaleTimeString()}</td>
                <td>${order.trading_symbol}</td>
                <td>${order.order_side}</td>
                <td>${order.order_quantity}</td>
                <td>₹${(order.order_avg_price || order.order_price || 0).toFixed(2)}</td>
                <td class="${statusClass}">${order.order_status}</td>
                <td>${order.order_unique_id || '-'}</td>
            </tr>
        `;
    });

    html += '</tbody></table>';
    displayDiv.innerHTML = html;
}
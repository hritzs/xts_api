// ════════════════════════════════════════════════════════════════════════════
// POSITIONS
// ════════════════════════════════════════════════════════════════════════════

document.getElementById('btn-refresh-positions').addEventListener('click', fetchPositions);

async function fetchPositions() {
    const displayDiv = document.getElementById('positions-display');
    displayDiv.innerHTML = '<p>🔄 Fetching positions...</p>';

    try {
        const response = await fetch('/api/positions');
        const data = await response.json();

        if (data.success) {
            displayPositions(data.positions);
        } else {
            displayDiv.innerHTML = `<p style="color: red;">❌ ${data.error}</p>`;
        }
    } catch (error) {
        displayDiv.innerHTML = `<p style="color: red;">❌ ${error.message}</p>`;
    }
}

function displayPositions(positions) {
    const displayDiv = document.getElementById('positions-display');

    if (!positions || positions.length === 0) {
        displayDiv.innerHTML = '<p>No positions found</p>';
        return;
    }

    let html = '<table><thead><tr><th>Symbol</th><th>Side</th><th>Qty</th><th>Avg Price</th><th>LTP</th><th>PnL</th></tr></thead><tbody>';

    positions.forEach(pos => {
        const pnl = (pos.LTP - pos.AveragePrice) * pos.Quantity * (pos.OrderSide === 'BUY' ? 1 : -1);
        const pnlClass = pnl >= 0 ? 'positive' : 'negative';

        html += `
            <tr>
                <td>${pos.TradingSymbol}</td>
                <td>${pos.OrderSide}</td>
                <td>${pos.Quantity}</td>
                <td>₹${pos.AveragePrice?.toFixed(2)}</td>
                <td>₹${pos.LTP?.toFixed(2)}</td>
                <td class="${pnlClass}">₹${pnl.toFixed(2)}</td>
            </tr>
        `;
    });

    html += '</tbody></table>';
    displayDiv.innerHTML = html;
}

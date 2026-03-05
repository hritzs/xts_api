// ════════════════════════════════════════════════════════════════════════════
// OPTION CHAIN
// ════════════════════════════════════════════════════════════════════════════

async function fetchOptionChain() {
    const symbol = document.getElementById('chain-symbol')?.value || 'NIFTY';
    const displayDiv = document.getElementById('option-chain-display');

    try {
        const response = await fetch(`/api/option-chain/${symbol}`);
        const data = await response.json();

        if (data.success && data.data) {
            displayOptionChain(data.data);
        }
    } catch (error) {
        console.error('Option chain fetch error:', error);
    }
}

document.getElementById('btn-fetch-chain').addEventListener('click', async () => {
    const symbol = document.getElementById('chain-symbol').value;
    const displayDiv = document.getElementById('option-chain-display');

    displayDiv.innerHTML = '<p>🔄 Fetching option chain...</p>';

    try {
        const response = await fetch(`/api/option-chain/${symbol}`);
        const data = await response.json();

        if (data.success && data.data) {
            displayOptionChain(data.data);
        } else {
            displayDiv.innerHTML = `<p style="color: red;">❌ ${data.error || 'Failed to fetch'}</p>`;
        }
    } catch (error) {
        displayDiv.innerHTML = `<p style="color: red;">❌ ${error.message}</p>`;
    }
});

function displayOptionChain(chainData) {
    const displayDiv = document.getElementById('option-chain-display');
    displayDiv.setAttribute('data-fut-token', chainData.fut_token);

    // Initialize future price for tracking
    if (chainData.fut_token && chainData.fut_ltp) {
        previousPrices[chainData.fut_token] = chainData.fut_ltp;
    }

    let html = `
        <div style="margin-bottom: 20px;">
            <h3>📊 ${chainData.symbol} Option Chain</h3>
            <p id="spot-price"><strong>Spot:</strong> <span>₹${chainData.fut_ltp?.toFixed(2)}</span></p>
            <p><strong>ATM:</strong> ${chainData.atm}</p>
            <p><strong>Expiry:</strong> ${chainData.expiry}</p>
        </div>
        <table>
            <thead>
                <tr>
                    <th colspan="5" style="background: #28a745;">CALL</th>
                    <th style="background: #ffc107;">Strike</th>
                    <th colspan="5" style="background: #dc3545;">PUT</th>
                </tr>
                <tr>
                    <th>Delta</th>
                    <th>Gamma</th>
                    <th>IV</th>
                    <th>LTP</th>
                    <th>Symbol</th>
                    <th>Strike</th>
                    <th>Symbol</th>
                    <th>LTP</th>
                    <th>IV</th>
                    <th>Gamma</th>
                    <th>Delta</th>
                </tr>
            </thead>
            <tbody>
    `;

    chainData.chain.forEach(row => {
        const atmClass = row.is_atm ? 'style="background: #fff3cd; font-weight: bold;"' : '';

        // Initialize previous prices for color tracking
        if (row.ce_token && row.ce_ltp) {
            previousPrices[row.ce_token] = row.ce_ltp;
        }
        if (row.pe_token && row.pe_ltp) {
            previousPrices[row.pe_token] = row.pe_ltp;
        }

        html += `
            <tr ${atmClass}>
                <td>${row.ce_delta?.toFixed(3) || '-'}</td>
                <td>${row.ce_gamma?.toFixed(4) || '-'}</td>
                <td>${row.ce_iv?.toFixed(2) || '-'}</td>
                <td data-token="${row.ce_token}"><span>₹${row.ce_ltp?.toFixed(2) || '-'}</span></td>
                <td style="font-size: 11px;">${row.ce_symbol || '-'}</td>
                <td style="text-align: center; font-weight: bold;">${row.strike}</td>
                <td style="font-size: 11px;">${row.pe_symbol || '-'}</td>
                <td data-token="${row.pe_token}"><span>₹${row.pe_ltp?.toFixed(2) || '-'}</span></td>
                <td>${row.pe_iv?.toFixed(2) || '-'}</td>
                <td>${row.pe_gamma?.toFixed(4) || '-'}</td>
                <td>${row.pe_delta?.toFixed(3) || '-'}</td>
            </tr>
        `;
    });

    html += '</tbody></table>';
    displayDiv.innerHTML = html;
}

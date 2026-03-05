// ════════════════════════════════════════════════════════════════════════════
// MANUAL BUILD
// ════════════════════════════════════════════════════════════════════════════

document.getElementById('btn-manual-build').addEventListener('click', async () => {
    const symbol = document.getElementById('manual-symbol').value;
    const lots = parseInt(document.getElementById('manual-lots').value);
    const deltaNeutral = document.getElementById('manual-delta-neutral').checked;

    const resultDiv = document.getElementById('manual-build-result');
    resultDiv.innerHTML = '<p>🔄 Building straddle...</p>';

    try {
        const response = await fetch('/api/straddle/sell', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                symbol: symbol,
                lots: lots,
                delta_neutral: deltaNeutral
            })
        });

        const data = await response.json();

        if (data.success) {
            resultDiv.innerHTML = `
                <div style="background: #d4edda; padding: 15px; border-radius: 5px; border: 1px solid #c3e6cb;">
                    <h3 style="color: #155724;">✅ Straddle Built Successfully!</h3>
                    <p><strong>Trade UID:</strong> ${data.trade_uid}</p>
                    <p><strong>Strike:</strong> ${data.data.strike}</p>
                    <p><strong>CE Quantity:</strong> ${data.data.ce_quantity}</p>
                    <p><strong>PE Quantity:</strong> ${data.data.pe_quantity}</p>
                    <p><strong>Net Delta:</strong> ${data.data.net_delta?.toFixed(2) || 'N/A'}</p>
                </div>
            `;
        } else {
            resultDiv.innerHTML = `
                <div style="background: #f8d7da; padding: 15px; border-radius: 5px; border: 1px solid #f5c6cb;">
                    <h3 style="color: #721c24;">❌ Build Failed</h3>
                    <p>${data.error}</p>
                </div>
            `;
        }
    } catch (error) {
        resultDiv.innerHTML = `
            <div style="background: #f8d7da; padding: 15px; border-radius: 5px;">
                <h3 style="color: #721c24;">❌ Error</h3>
                <p>${error.message}</p>
            </div>
        `;
    }
});

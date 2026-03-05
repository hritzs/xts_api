// ════════════════════════════════════════════════════════════════════════════
// STYLE INJECTION
// ════════════════════════════════════════════════════════════════════════════
// We inject the 'yellow' class style directly here to ensure it's available.
// This avoids potential issues with file load order or other browser behaviors
// that might block styles added in separate utility files.
(function() {
    // Check if the style already exists to avoid duplicates
    if (document.getElementById('dynamic-color-styles')) return;

    const style = document.createElement('style');
    style.id = 'dynamic-color-styles';
    style.textContent = `
        .yellow {
            color: #b58500 !important; /* Dark yellow for good readability */
            font-weight: bold;
        }
        /* Modal Styles */
        .modal-overlay {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.6);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 1000;
        }
        .modal-content {
            background-color: #2c2c2c;
            padding: 20px;
            border-radius: 8px;
            width: 90%;
            max-width: 600px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.3);
        }
        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #444;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }
        .modal-header h2 {
            margin: 0;
            font-size: 1.2em;
        }
        .modal-header .close-button, .modal-body .close-button {
            background: none;
            border: none;
            font-size: 1.5rem;
            cursor: pointer;
            color: #aaa;
        }
        .form-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }
        .form-actions {
            margin-top: 20px;
            text-align: right;
        }
        .form-actions .btn {
            margin-left: 10px;
        }
        .form-group input {
            padding: 8px; border: 1px solid #555; background-color: #333;
            color: #fff; border-radius: 4px;
        }
    `;
    document.head.appendChild(style);
})();
// ════════════════════════════════════════════════════════════════════════════
// STRADDLES
// ════════════════════════════════════════════════════════════════════════════
let previousPrices = {};
let optionChainData = {};
let priceMap = {};
async function fetchStraddles(isPortfolioView = false) {
    const displayDivId = isPortfolioView ? 'portfolio-positions-display' : 'straddles-display';
    const displayDiv = document.getElementById(displayDivId);
    if (isPortfolioView) {
        displayDiv.classList.add('scrollable'); // Ensure scrollable class is present for list view
    }
    try {
        const response = await fetch('/api/straddles');
        const data = await response.json();

        if (data.success) {
            if (isPortfolioView) {
                displayPortfolioPositions(data.straddles);
            } else {
                displayStraddles(data.straddles);
            }
        } else {
            displayDiv.innerHTML = `<div class="placeholder">❌ ${data.error}</div>`;
        }
    } catch (error) {
        displayDiv.innerHTML = `<div class="placeholder">❌ ${error.message}</div>`;
    }
}
function displayStraddles(straddles) {
    const displayDiv = document.getElementById('straddles-display');
    if (!straddles || straddles.length === 0) {
        displayDiv.innerHTML = '<div class="placeholder">No active positions</div>';
        return;
    }

    let html = '<table><thead><tr><th>UID</th><th>Symbol</th><th>Strike</th><th>CE Qty</th><th>PE Qty</th><th>Status</th><th>Net Δ</th><th>PnL</th><th>Actions</th></tr></thead><tbody>';

    straddles.forEach(straddle => {
        const is_building = straddle.status === 'BUILDING';
        const statusClass = is_building ? 'yellow' : ((straddle.status === 'ACTIVE' || straddle.status === 'FILLED') ? 'positive' : 'neutral');
        const pnlClass = (straddle.live_pnl || 0) >= 0 ? 'positive' : 'negative';
        const tradeUid = straddle.trade_uid || straddle.straddle_id;
        const live_net_delta = (straddle.live_net_delta || straddle.net_delta || 0);

        html += `
            <tr data-trade-uid="${tradeUid}" onclick="showStraddleDetails('${tradeUid}', this)" style="cursor: pointer;">
                <td>${tradeUid.slice(-6)}</td>
                <td>${straddle.symbol}</td>
                <td>${straddle.strike}</td>
                <td>${straddle.ce_quantity}</td>
                <td>${straddle.pe_quantity}</td>
                <td class="${statusClass}">${straddle.status}</td>
                <td class="net-delta-cell">${live_net_delta.toFixed(2)}</td>
                <td class="pnl-cell ${pnlClass}">₹${(straddle.live_pnl || 0).toFixed(2)}</td>
                <td class="actions-cell">
                     ${(straddle.status === 'ACTIVE' || straddle.status === 'FILLED') ?
                        `<button class="btn-icon" title="Partial Square Off" onclick="event.stopPropagation(); partialSquareOffStraddle('${tradeUid}')">✂️</button>
                         <button class="btn-icon" title="Full Square Off" onclick="event.stopPropagation(); squareOffStraddle('${tradeUid}', ${live_net_delta})">❌</button>` :
                         ''}
                </td>
            </tr>
        `;
    });

    html += '</tbody></table>';
    displayDiv.innerHTML = html;
}

function displayPortfolioPositions(straddles) {
    const displayDiv = document.getElementById('portfolio-positions-display');
    if (!straddles || straddles.length === 0) {
        displayDiv.innerHTML = '<div class="placeholder">No active positions</div>';
        return;
    }

    // This header removes 'CE Entry'/'PE Entry' and adds 'Pts Out'/'Pts Allowed'
    let html = '<table><thead><tr><th>UID</th><th>Symbol</th><th>Strike</th><th>Status</th><th>CE Qty</th><th>CE LTP</th><th>PE Qty</th><th>PE LTP</th><th>Net Δ</th><th>Pts Out</th><th>Pts Allowed</th><th>Unrealized</th><th>Realized</th><th>PnL/Straddle</th><th>Actions</th></tr></thead><tbody>';

    straddles.forEach(straddle => {
        const isClosed = straddle.status.toUpperCase().startsWith('CLOSED');
        const isPending = straddle.status === 'PENDING';
        const isBuilding = straddle.status === 'BUILDING';
        const activeLikeStatuses = ['ACTIVE', 'FILLED', 'PARTIAL-SQF', 'HEDGING', 'ROLLING'];
        
        const statusClass = isPending ? 'yellow' : (isBuilding ? 'yellow' : (activeLikeStatuses.includes(straddle.status) ? 'positive' : (isClosed ? 'grey' : 'neutral')));
        const tradeUid = straddle.trade_uid || straddle.straddle_id;

        const unrealized_pnl = isClosed || isPending ? 0 : (straddle.unrealized_pnl || 0);
        const realized_pnl = isClosed ? (straddle.realized_pnl || 0) : (straddle.realized_pnl || 0);

        const pnl_per_lot = straddle.pnl_per_lot || 0;
        const pnlPerLotDisplay = isClosed || isPending ? '—' : (straddle.pnl_per_lot || 0).toFixed(2);

        // Zero out live data for closed trades
        const net_delta_display = isClosed || isPending ? '—' : (straddle.live_net_delta || straddle.net_delta || 0).toFixed(2);
        const ce_ltp_display = isClosed || isPending ? '—' : `₹${(priceMap[straddle.ce_token] || straddle.ce_ltp || 0).toFixed(2)}`;
        const pe_ltp_display = isClosed || isPending ? '—' : `₹${(priceMap[straddle.pe_token] || straddle.pe_ltp || 0).toFixed(2)}`;

        const pts_out = isClosed || isPending ? 0 : (straddle.pts_out || 0);
        const points_allowed = isClosed || isPending ? 0 : (straddle.points_allowed); // Can be null from API
        const pts_out_display = pts_out.toFixed(2);
        const pts_allowed_display = (points_allowed !== null && !isPending) ? points_allowed.toFixed(2) : '—'; // Display dash for infinity or pending
        let ptsOutClass = '';
        if (!isClosed && points_allowed > 0) {
            const ratio = pts_out / points_allowed;
            if (ratio >= 1.0) {
                ptsOutClass = 'negative';
            } else if (ratio >= 0.5) {
                ptsOutClass = 'yellow';
            } else {
                ptsOutClass = 'positive';
            }
        }

        const unrealizedPnlClass = unrealized_pnl >= 0 ? 'positive' : 'negative';
        const realizedPnlClass = realized_pnl >= 0 ? 'positive' : 'negative';
        const pnlPerLotClass = pnl_per_lot >= 0 ? 'positive' : 'negative';

        // --- NEW: Dynamic Actions Cell ---
        // Always show the details button.
        let actionsHtml = `<button class="btn-icon" title="View Details" onclick="showStraddleDetails('${tradeUid}', this.closest('tr'))">ℹ️</button>`;
        const cancellable_statuses = ['SQUARING-OFF', 'PARTIAL-SQF', 'HEDGING', 'ROLLING', 'BUILDING'];
        if (cancellable_statuses.includes(straddle.status.toUpperCase())) {
            // For actions in progress, add a cancel button.
            actionsHtml += `<button class="btn-icon btn-danger" title="Cancel Action" onclick="event.stopPropagation(); cancelTradeAction('${tradeUid}')">🚫</button>`;
        } else if (isPending) {
            // For pending trades, allow modification and cancellation directly from the main row.
            actionsHtml += `<button class="btn-icon" title="Modify Config" onclick="event.stopPropagation(); showModifyConfigModal('${tradeUid}')">⚙️</button>`;
            actionsHtml += `<button class="btn-icon btn-danger" title="Cancel Scheduled Build" onclick="event.stopPropagation(); cancelTradeAction('${tradeUid}')">🚫</button>`;
        }
        // --- END NEW ---

        html += `
            <tr data-trade-uid="${tradeUid}">
                <td>${tradeUid.slice(-6)}</td>
                <td>${straddle.symbol}</td>
                <td class="strike-cell">${straddle.strike}</td>
                <td class="${statusClass}">${straddle.status}</td>
                <td>${straddle.ce_quantity || '—'}</td>
                <td data-token="${straddle.ce_token}">${ce_ltp_display}</td>
                <td>${straddle.pe_quantity || '—'}</td>
                <td data-token="${straddle.pe_token}">${pe_ltp_display}</td>
                <td class="net-delta-cell">${net_delta_display}</td>
                <td class="pts-out-cell ${ptsOutClass}">${pts_out_display}</td>
                <td class="pts-allowed-cell">${pts_allowed_display}</td>
                <td class="unrealized-pnl-cell ${unrealizedPnlClass}">₹${unrealized_pnl.toFixed(2)}</td>
                <td class="realized-pnl-cell ${realizedPnlClass}">₹${realized_pnl.toFixed(2)}</td>
                <td class="pnl-per-lot-cell ${pnlPerLotClass}">₹${pnlPerLotDisplay}</td>
                <td>${actionsHtml}</td>
            </tr>`;
    });
    html += '</tbody></table>';
    displayDiv.innerHTML = html;
}

function handlePnlBatchUpdate(updates) {
    if (!updates || !Array.isArray(updates)) return;

    updates.forEach(update => {
        const tradeUid = update.trade_uid;
        if (!tradeUid) return;

        // --- Update PnL and Delta in both views ---
        const rows = document.querySelectorAll(`tr[data-trade-uid="${tradeUid}"]`);
        rows.forEach(row => {
            // Check if trade is closed. Portfolio view status is 4th, main straddles view is 6th.
            const statusCell = row.querySelector('td:nth-child(4), td:nth-child(6)');
            if (statusCell && statusCell.textContent.toUpperCase().startsWith('CLOSED')) {
                return;
            }

            // Update Delta
            const deltaCell = row.querySelector('.net-delta-cell');
            if (deltaCell && update.live_net_delta !== undefined) {
                deltaCell.textContent = update.live_net_delta.toFixed(2);
            }

            // Update PnL (main view)
            const pnlCell = row.querySelector('.pnl-cell');
            if (pnlCell && update.live_pnl !== undefined) {
                const pnl = update.live_pnl;
                pnlCell.textContent = `₹${pnl.toFixed(2)}`;
                pnlCell.className = `pnl-cell ${pnl >= 0 ? 'positive' : 'negative'}`;
            }

            // Update Unrealized PnL (portfolio view)
            const unrealizedPnlCell = row.querySelector('.unrealized-pnl-cell');
            if (unrealizedPnlCell && update.unrealized_pnl !== undefined) {
                const unrealized_pnl = update.unrealized_pnl;
                unrealizedPnlCell.textContent = `₹${unrealized_pnl.toFixed(2)}`;
                unrealizedPnlCell.className = `unrealized-pnl-cell ${unrealized_pnl >= 0 ? 'positive' : 'negative'}`;
            }

            // Update PnL per Straddle (portfolio view)
            const pnlPerLotCell = row.querySelector('.pnl-per-lot-cell');
            if (pnlPerLotCell && update.pnl_per_lot !== undefined) {
                const pnl_per_lot = update.pnl_per_lot;
                pnlPerLotCell.textContent = `₹${pnl_per_lot.toFixed(2)}`;
                pnlPerLotCell.className = `pnl-per-lot-cell ${pnl_per_lot >= 0 ? 'positive' : 'negative'}`;
            }

            // Update Pts Out and Pts Allowed (portfolio view)
            const ptsOutCell = row.querySelector('.pts-out-cell');
            if (ptsOutCell && update.pts_out !== undefined) {
                const pts_out = update.pts_out;
                const points_allowed = update.points_allowed; // number or null

                ptsOutCell.textContent = pts_out.toFixed(2);

                const ptsAllowedCell = row.querySelector('.pts-allowed-cell');
                if (ptsAllowedCell) {
                    ptsAllowedCell.textContent = (points_allowed !== null) ? points_allowed.toFixed(2) : '—';
                }

                // Update color class for pts-out-cell
                let ptsOutClass = '';
                if (points_allowed && points_allowed > 0) { // null check is implicit
                    const ratio = pts_out / points_allowed;
                    if (ratio >= 1.0) { ptsOutClass = 'negative'; }
                    else if (ratio >= 0.5) { ptsOutClass = 'yellow'; }
                    else { ptsOutClass = 'positive'; }
                }
                ptsOutCell.className = `pts-out-cell ${ptsOutClass}`;
            }
        });

        // --- Update LTPs for all positions in this trade ---
        if (update.position_ltps) {
            for (const [token, ltp] of Object.entries(update.position_ltps)) {
                // The token from JSON key is a string, needs to be parsed.
                updatePrice(parseInt(token), ltp);
            }
        }
    });
}

function handleStraddleUpdate(data) {
    const tradeUid = data.trade_uid;
    if (!tradeUid) { return; }

    const mainRow = document.querySelector(`#straddles-display tr[data-trade-uid="${tradeUid}"]`);
    if (mainRow) {
        // Check if the trade is closed. If so, don't update live data.
        const statusCell = mainRow.querySelector('td:nth-child(6)'); // Assuming status is the 6th column
        if (statusCell && statusCell.textContent.toUpperCase().startsWith('CLOSED')) {
            return; // Do not update PnL/Delta for closed trades
        }

        const deltaCell = mainRow.querySelector('.net-delta-cell');
        if (deltaCell) {
            deltaCell.textContent = (data.live_net_delta || 0).toFixed(2);
        }

        const pnlCell = mainRow.querySelector('.pnl-cell');
        if (pnlCell) {
            const pnl = data.live_pnl || 0;
            pnlCell.textContent = `₹${pnl.toFixed(2)}`;
            pnlCell.className = `pnl-cell ${pnl >= 0 ? 'positive' : 'negative'}`;
        }
    }

    // Also update the portfolio view if it exists
    const portfolioRow = document.querySelector(`#portfolio-positions-display tr[data-trade-uid="${tradeUid}"]`);
    if (portfolioRow) {
        // Check if the trade is closed. If so, don't update live data.
        const statusCell = portfolioRow.querySelector('td:nth-child(4)'); // Assuming status is 4th column
        if (statusCell && statusCell.textContent.toUpperCase().startsWith('CLOSED')) {
            return; // Do not update PnL/Delta for closed trades
        }

        const deltaCell = portfolioRow.querySelector('.net-delta-cell');
        if (deltaCell) deltaCell.textContent = (data.live_net_delta || 0).toFixed(2);

        // Update strike if it changed (e.g., after a roll)
        const strikeCell = portfolioRow.querySelector('.strike-cell');
        if (strikeCell && data.strike) {
            strikeCell.textContent = data.strike;
        }

        const unrealizedPnlCell = portfolioRow.querySelector('.unrealized-pnl-cell');
        if (unrealizedPnlCell) {
            const unrealized_pnl = data.unrealized_pnl || 0;
            unrealizedPnlCell.textContent = `₹${unrealized_pnl.toFixed(2)}`;
            unrealizedPnlCell.className = `unrealized-pnl-cell ${unrealized_pnl >= 0 ? 'positive' : 'negative'}`;
        }

        const realizedPnlCell = portfolioRow.querySelector('.realized-pnl-cell');
        if (realizedPnlCell) {
            const realized_pnl = data.realized_pnl || 0;
            realizedPnlCell.textContent = `₹${realized_pnl.toFixed(2)}`;
            realizedPnlCell.className = `realized-pnl-cell ${realized_pnl >= 0 ? 'positive' : 'negative'}`;
        }


        const ptsOutCell = portfolioRow.querySelector('.pts-out-cell');
        if (ptsOutCell) {
            const pts_out = data.pts_out || 0;
            const points_allowed = data.points_allowed || 0;
            ptsOutCell.textContent = pts_out.toFixed(2);

            let ptsOutClass = 'positive';
            if (points_allowed > 0) {
                const ratio = pts_out / points_allowed;
                if (ratio >= 1.0) {
                    ptsOutClass = 'negative';
                } else if (ratio >= 0.5) {
                    ptsOutClass = 'yellow';
                }
            }
            ptsOutCell.className = `pts-out-cell ${ptsOutClass}`;
        }

        const ptsAllowedCell = portfolioRow.querySelector('.pts-allowed-cell');
        if (ptsAllowedCell) ptsAllowedCell.textContent = (data.points_allowed || 0).toFixed(2);

        const pnlPerLotCell = portfolioRow.querySelector('.pnl-per-lot-cell');
        if (pnlPerLotCell) {
            pnlPerLotCell.textContent = `₹${(data.pnl_per_lot || 0.0).toFixed(2)}`;
            pnlPerLotCell.className = `pnl-per-lot-cell ${(data.pnl_per_lot || 0) >= 0 ? 'positive' : 'negative'}`;
        }
    }
}

function showStraddleDetails(tradeUid, clickedRow) {
    if (!clickedRow) {
        console.warn(`Could not find row for trade UID: ${tradeUid}`);
        return;
    }

    // Check if details row already exists and toggle it
    const existingDetailsRow = document.getElementById(`details-${tradeUid}`);
    if (existingDetailsRow) {
        existingDetailsRow.remove();
        clickedRow.classList.remove('details-open');
        return;
    }

    // Close any other open details rows in the same container (table)
    const parentTable = clickedRow.closest('table');
    if (parentTable) {
        parentTable.querySelectorAll('.details-row').forEach(row => row.remove());
        parentTable.querySelectorAll('.details-open').forEach(row => row.classList.remove('details-open'));
    }

    // Fetch data and create the dropdown for the clicked row
    fetch('/api/straddles')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                const straddle = data.straddles.find(s => (s.trade_uid || s.straddle_id) === tradeUid);
                if (straddle) {
                    clickedRow.classList.add('details-open');
                    const detailsHtml = createDetailsHtml(straddle);

                    const detailsRow = document.createElement('tr');
                    detailsRow.id = `details-${tradeUid}`;
                    detailsRow.className = 'details-row';

                    const detailsCell = document.createElement('td');
                    detailsCell.colSpan = clickedRow.cells.length;
                    detailsCell.innerHTML = detailsHtml;

                    detailsRow.appendChild(detailsCell);
                    clickedRow.parentNode.insertBefore(detailsRow, clickedRow.nextSibling);
                }
            }
        });
}

function createDetailsHtml(straddle) {
    const isClosed = straddle.status.toUpperCase().startsWith('CLOSED');
    const isPending = straddle.status === 'PENDING';
    const tradeUid = straddle.trade_uid || straddle.straddle_id;
    const total_pnl = isClosed || isPending ? (straddle.realized_pnl || 0) : (straddle.live_pnl || 0);
    const realized_pnl = isClosed ? (straddle.realized_pnl || 0) : (straddle.realized_pnl || 0);
    const unrealized_pnl = isClosed || isPending ? 0 : (straddle.unrealized_pnl || 0);

    const net_delta = isClosed || isPending ? 0 : (straddle.live_net_delta || straddle.net_delta || 0);
    const net_gamma = isClosed || isPending ? 0 : (straddle.net_gamma || 0);
    const net_theta = isClosed || isPending ? 0 : (straddle.net_theta || 0);
    const net_vega = isClosed || isPending ? 0 : (straddle.net_vega || 0);
    const points_out = isClosed || isPending ? 0 : (Math.abs(net_gamma) > 1e-6 ? Math.abs(net_delta) / Math.abs(net_gamma) : 0);
    const points_allowed = isClosed || isPending ? 0 : (straddle.points_allowed || 0);

    let ptsOutClass = '';
    if (!isClosed && points_allowed > 0) {
        const ratio = points_out / points_allowed;
        if (ratio >= 1.0) {
            ptsOutClass = 'negative';
        } else if (ratio >= 0.5) {
            ptsOutClass = 'yellow';
        } else {
            ptsOutClass = 'positive';
        }
    }

    const entry_spot = straddle.entry_spot || 'N/A';
    // Break-even is complex with multi-leg positions, so we show the initial entry spot instead.

    // PnL & Risk Table
    const pnlHtml = `
        <div class="details-metrics">
            <div class="metric"><span>Total PnL</span><strong class="${total_pnl >= 0 ? 'positive' : 'negative'}">₹${total_pnl.toFixed(2)}</strong></div>
            <div class="metric"><span>Unrealized PnL</span><strong class="${unrealized_pnl >= 0 ? 'positive' : 'negative'}">₹${unrealized_pnl.toFixed(2)}</strong></div>
            <div class="metric"><span>Realized PnL</span><strong class="${realized_pnl >= 0 ? 'positive' : 'negative'}">₹${realized_pnl.toFixed(2)}</strong></div>
            <div class="metric"><span>Points Out</span><strong class="${ptsOutClass}">${points_out.toFixed(2)}</strong></div>
            <div class="metric"><span>Points Allowed</span><strong>${points_allowed.toFixed(2)}</strong></div>
            <div class="metric"><span>Roll Trigger</span><strong>₹${(straddle.roll_trigger_price || 0).toFixed(2)}</strong></div>
            <div class="metric"><span>Entry Spot</span><strong>${entry_spot !== 'N/A' ? parseFloat(entry_spot).toFixed(2) : 'N/A'}</strong></div>
        </div>
    `;

    // Greeks Table
    const greeksHtml = `
        <div class="details-metrics">
            <div class="metric"><span>Net Delta</span><strong>${(net_delta).toFixed(2)}</strong></div>
            <div class="metric"><span>Net Gamma</span><strong>${(net_gamma).toFixed(4)}</strong></div>
            <div class="metric"><span>Net Theta</span><strong>${(net_theta).toFixed(2)}</strong></div>
            <div class="metric"><span>Net Vega</span><strong>${(net_vega).toFixed(2)}</strong></div>
        </div>
    `;

    // Monitors Table
    let monitorsHtml = '<div class="placeholder-small">No monitor data</div>';
    if (straddle.monitors) {
        const slStartTime = straddle.monitors.sl.start_time !== 'Trade Start' ? ` | Start: ${straddle.monitors.sl.start_time}` : '';
        // For pending trades, show the config instead of live status
        if (isPending) {
            const config = straddle.monitors; // The pending trade stores config under 'monitors' key
            monitorsHtml = `
                <div class="details-metrics">
                    <div class="metric"><span>Entry Time</span><strong>${config.sl.start_time || 'N/A'}</strong></div>
                    <div class="metric"><span>Exit Time</span><strong>${config.square_off.exit_time || 'N/A'}</strong></div>
                    <div class="metric"><span>SL BPS</span><strong>${config.sl.sl_bps || 'N/A'}</strong></div>
                </div>
                <div class="placeholder-small">This trade is scheduled. Other monitor values will be calculated on entry.</div>
            `;
        } else {
        const hedgeStartTime = straddle.monitors.hedge.start_time !== 'Trade Start' ? ` | Start: ${straddle.monitors.hedge.start_time}` : '';
        const rollStartTime = straddle.monitors.roll.start_time !== 'Trade Start' ? ` | Start: ${straddle.monitors.roll.start_time}` : '';

        monitorsHtml = `
            <table class="details-table minimal">
                <tbody>
                    <tr>
                        <td>Stop-Loss</td>
                        <td><span class="status-dot ${straddle.monitors.sl.running ? 'active' : ''}"></span> ${straddle.monitors.sl.running ? 'Running' : 'Stopped'}</td>
                        <td>SL: ${straddle.monitors.sl.sl_points.toFixed(2)} pts (${straddle.monitors.sl.sl_bps} BPS) | Interval: ${straddle.monitors.sl.interval}s${slStartTime}</td>
                    </tr>
                    ${straddle.monitors.tp ? `
                    <tr>
                        <td>Take-Profit</td>
                        <td><span class="status-dot ${straddle.monitors.tp.triggered ? 'triggered' : (straddle.monitors.tp.running ? 'active' : '')}"></span> ${straddle.monitors.tp.triggered ? 'Triggered' : (straddle.monitors.tp.running ? 'Running' : 'Stopped')}</td>
                        <td>TP: ₹${(straddle.tp_threshold_points || 0).toFixed(2)} pts (SL x${straddle.monitors.tp.tp_sl_multiplier}) | SQF: ${straddle.monitors.tp.tp_sqf_percentage}%</td>
                    </tr>
                    ` : ''}
                    <tr>
                        <td>Hedge</td>
                        <td><span class="status-dot ${straddle.monitors.hedge.running ? 'active' : ''}"></span> ${straddle.monitors.hedge.running ? 'Running' : 'Stopped'}</td>
                        <td>H-Div: ${straddle.monitors.hedge.hedge_div} | S-Div: ${straddle.monitors.hedge.straddle_div} | Interval: ${straddle.monitors.hedge.interval}s${hedgeStartTime}</td>
                    </tr>
                    <tr>
                        <td>Roll</td>
                        <td><span class="status-dot ${straddle.monitors.roll.running ? 'active' : ''}"></span> ${straddle.monitors.roll.running ? 'Running' : 'Stopped'}</td>
                        <td>Roll Div: ${straddle.monitors.roll.roll_straddle_div} | Interval: ${straddle.monitors.roll.interval}s${rollStartTime}</td>
                    </tr>
                    <tr>
                        <td>Square-Off</td>
                        <td><span class="status-dot ${straddle.monitors.square_off.running ? 'active' : ''}"></span> ${straddle.monitors.square_off.running ? 'Running' : 'Stopped'}</td>
                        <td>Exit Time: ${straddle.monitors.square_off.exit_time}</td>
                    </tr>
                </tbody>
            </table>
        `;
        }
    }

    // Events Table
    let eventsHtml = '<div class="placeholder-small">No recent events</div>';
    if (straddle.events && straddle.events.length > 0) {
        eventsHtml = `
            <table class="details-table minimal">
                <thead><tr><th>Time</th><th>Event</th><th>Priority</th></tr></thead>
                <tbody>
                    ${straddle.events.map(evt => `
                        <tr>
                            <td>${evt.timestamp}</td>
                            <td>${evt.type}</td>
                            <td>${evt.priority}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
    }

    // Position Details Table
    let positionsHtml = '<div class="placeholder-small">No live positions found.</div>';
    if (isClosed) {
        positionsHtml = '<div class="placeholder-small">Trade is closed. Final PnL is realized.</div>';
    } else if (isPending) {
        positionsHtml = '<div class="placeholder-small">Trade is scheduled and has not entered yet.</div>';
    } else if (straddle.live_positions && straddle.live_positions.length > 0) {
        positionsHtml = `
            <table class="details-table">
                <thead><tr><th>Leg</th><th>Strike</th><th>Action</th><th>Qty</th><th>Entry</th><th>LTP</th><th>PnL</th><th>IV</th><th>Delta</th></tr></thead>
                <tbody>
                    ${straddle.live_positions.map(pos => `
                        <tr>
                            <td>${pos.option_type}</td>
                            <td>${pos.strike}</td>
                            <td class="${pos.action === 'BUY' ? 'positive' : 'negative'}">${pos.action}</td>
                            <td>${pos.quantity}</td>
                            <td>₹${(pos.entry_price || 0).toFixed(2)}</td>
                            <td>₹${(pos.ltp || 0).toFixed(2)}</td>
                            <td class="${pos.pnl >= 0 ? 'positive' : 'negative'}">₹${pos.pnl.toFixed(2)}</td>
                            <td>${(pos.iv * 100).toFixed(2)}%</td>
                            <td>${(pos.delta || 0).toFixed(2)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
    }

    // Manual Actions
    let manualActionsHtml = '<div class="placeholder-small">Trade is closed.</div>';
    if (!isClosed) {
        manualActionsHtml = `
            <div class="details-actions" style="display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; padding: 10px;">
                ${isPending ? `
                    <button class="btn btn-primary" onclick="event.stopPropagation(); showModifyConfigModal('${tradeUid}')" title="Modify scheduled trade parameters">Modify Config</button>
                    <button class="btn btn-danger" onclick="event.stopPropagation(); cancelTradeAction('${tradeUid}')">Cancel Build</button>
                ` : `
                <button class="btn btn-primary" onclick="event.stopPropagation(); showModifyConfigModal('${tradeUid}')" title="Modify live trade parameters">Modify Config</button>
                <button class="btn btn-secondary" onclick="manualHedge('${tradeUid}')" title="Execute a hedge to neutralize delta">Hedge Now</button>
                <button class="btn btn-secondary" onclick="manualRoll('${tradeUid}')" title="Roll position to the current ATM strike">Roll Now</button>
                <button class="btn btn-info" onclick="manualVerify('${tradeUid}')" title="Force a sync with the broker's order book">Sync Trade</button>
                <button class="btn btn-warning" onclick="partialSquareOffStraddle('${tradeUid}')">Partial Exit</button>
                <button class="btn btn-danger" onclick="squareOffStraddle('${tradeUid}', ${net_delta})">Full Exit</button>
                `}
            </div>
        `;
    }

    return `
    <div class="details-container">
        <div class="details-grid-complex">
            <div class="details-card"><div class="details-card-header">PnL & Risk</div><div class="details-card-body">${pnlHtml}</div></div>
            <div class="details-card"><div class="details-card-header">Net Greeks</div><div class="details-card-body">${greeksHtml}</div></div>
            <div class="details-card"><div class="details-card-header">Monitor Status</div><div class="details-card-body">${monitorsHtml}</div></div>
            <div class="details-card"><div class="details-card-header">Recent Events</div><div class="details-card-body">${eventsHtml}</div></div>
            <div class="details-card large-card"><div class="details-card-header">Position Details</div><div class="details-card-body">${positionsHtml}</div></div>
            <div class="details-card"><div class="details-card-header">Manual Actions</div><div class="details-card-body">${manualActionsHtml}</div></div>
        </div>
    </div>
    `;
}

async function loadOptionChain() {
    const symbol = document.getElementById('symbol').value;
    const container = document.getElementById('option-chain-container');

    try {
        const response = await fetch(`/api/option-chain/${symbol}?strike_range=10`);
        const result = await response.json();
        
        if (result.success && result.data) {
            optionChainData = result.data;
            renderOptionChain();
            updateStraddlePremium();
        } else {
            container.innerHTML = `<div class="placeholder">❌ Error loading chain: ${result.error || 'Unknown error'}</div>`;
        }
    } catch (error) {
        container.innerHTML = `<div class="placeholder">❌ Network error loading chain.</div>`;
        console.error('Load chain error:', error);
    }
}

function renderOptionChain() {
    const container = document.getElementById('option-chain-container');
    const headerSpot = document.getElementById('chain-header-spot');
    const headerAtm = document.getElementById('chain-header-atm');
    const headerExpiry = document.getElementById('chain-header-expiry');

    if (!optionChainData || !optionChainData.chain) {
        container.innerHTML = '<div class="placeholder">No option chain data</div>';
        return;
    }

    headerSpot.textContent = `Spot: ₹${optionChainData.fut_ltp.toFixed(2)}`; // Always display the synthetic fut_ltp
    headerAtm.textContent = `ATM: ${optionChainData.atm}`;
    headerExpiry.textContent = `Expiry: ${optionChainData.expiry} (DTE: ${optionChainData.dte.toFixed(2)})`;

    let tableHtml = `<table class="option-chain-table"><thead><tr>
        <th class="calls-side">Theta</th>
        <th class="calls-side">IV</th>
        <th class="calls-side">Delta</th>
        <th class="calls-side">LTP</th>
        <th class="strike-col">Strike</th>
        <th class="puts-side">LTP</th>
        <th class="puts-side">Delta</th>
        <th class="puts-side">IV</th>
        <th class="puts-side">Theta</th>
    </tr></thead><tbody>`;

    optionChainData.chain.forEach(row => {
        const ceLtp = priceMap[row.ce_token] || row.ce_ltp || 0;
        const peLtp = priceMap[row.pe_token] || row.pe_ltp || 0;

        tableHtml += `<tr class="${row.is_atm ? 'atm-row' : ''}">
            <td class="calls-side">${row.ce_theta.toFixed(2)}</td>
            <td class="calls-side">${row.ce_iv.toFixed(2)}%</td>
            <td class="calls-side">${row.ce_delta.toFixed(4)}</td>
            <td class="calls-side" data-token="${row.ce_token}">₹${ceLtp.toFixed(2)}</td>
            <td class="strike-col">${row.strike}</td>
            <td class="puts-side" data-token="${row.pe_token}">₹${peLtp.toFixed(2)}</td>
            <td class="puts-side">${row.pe_delta.toFixed(4)}</td>
            <td class="puts-side">${row.pe_iv.toFixed(2)}%</td>
            <td class="puts-side">${row.pe_theta.toFixed(2)}</td>
        </tr>`;
    });

    tableHtml += '</tbody></table>';
    container.innerHTML = tableHtml;
}

function updateStraddlePremium() {
    if (!optionChainData || !optionChainData.chain) return;

    const atmRow = optionChainData.chain.find(r => r.is_atm);
    if (!atmRow) return;

    const ceLtp = priceMap[atmRow.ce_token] || atmRow.ce_ltp || 0;
    const peLtp = priceMap[atmRow.pe_token] || atmRow.pe_ltp || 0;
    const straddle = ceLtp + peLtp;

    const lots = parseInt(document.getElementById('lots').value) || 1;
    const lotSize = atmRow.ce_lot_size || 1;
    const total = straddle * lots * lotSize;

    document.getElementById('atm-strike').value = atmRow.strike;
    document.getElementById('straddle-premium').value = `₹${straddle.toFixed(2)}`;
    document.getElementById('total-premium').value = `₹${total.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function updatePrice(token, ltp) {
    priceMap[token] = ltp;

    // Update option chain header if it's the future token
    if (optionChainData && token === optionChainData.fut_token) {
        // This is for the Build tab's chain
        const headerSpotEl = document.getElementById('chain-header-spot');
        if (headerSpotEl) {
            headerSpotEl.textContent = `Spot: ₹${ltp.toFixed(2)}`;
        }
        // This is for the Option Chain tab
        const spotPriceSpan = document.querySelector('#spot-price span');
        if (spotPriceSpan) {
            spotPriceSpan.textContent = `₹${ltp.toFixed(2)}`;
        }
    }

    const cells = document.querySelectorAll(`[data-token="${token}"]`);
    cells.forEach(cell => {
        const row = cell.closest('tr');
        // Check if this cell is part of a trade row in the portfolio view and if that trade is closed.
        if (row && row.dataset.tradeUid) {
            // Portfolio view status is 4th, main straddles view is 6th
            const statusCell = row.querySelector('td:nth-child(4), td:nth-child(6)');
            if (statusCell && statusCell.textContent.toUpperCase().startsWith('CLOSED')) {
                return; // Skip updating LTP for closed trades
            }
        }

        // Find the span if it exists (for option chain), otherwise use the cell itself (for portfolio).
        const targetElement = cell.querySelector('span') || cell;

        const prevPrice = previousPrices[token] || ltp;
        const flashClass = ltp > prevPrice ? 'ltp-flash-up' : (ltp < prevPrice ? 'ltp-flash-down' : '');

        targetElement.textContent = `₹${ltp.toFixed(2)}`;

        // Always apply the flash effect to the parent cell for consistent styling.
        if (flashClass) {
            cell.classList.add(flashClass);
            setTimeout(() => cell.classList.remove(flashClass), 700);
        }
    });
    previousPrices[token] = ltp;
    updateStraddlePremium();
}

async function showModifyConfigModal(tradeUid) {
    // Prevent event from bubbling up to the row click handler
    event.stopPropagation();

    // Fetch the latest data for the trade to ensure we have the config
    const response = await fetch('/api/straddles');
    const data = await response.json();
    if (!data.success) {
        showNotification('❌ Could not fetch trade data to modify.', 'error');
        return;
    }
    const straddle = data.straddles.find(s => (s.trade_uid || s.straddle_id) === tradeUid);
    if (!straddle || !straddle.monitors) {
        showNotification('❌ Monitor configuration not available for this trade.', 'error');
        return;
    }

    // Create modal structure
    const modalOverlay = document.createElement('div');
    modalOverlay.id = 'modify-config-modal-overlay';
    modalOverlay.className = 'modal-overlay';
    
    const modalContent = document.createElement('div');
    modalContent.className = 'modal-content';

    // Get current config values, providing defaults if they don't exist
    const config = straddle.monitors;
    const sl_bps = config.sl.sl_bps || 14;
    const sl_start_time = config.sl.start_time || '12:32:00';
    const hedge_div = config.hedge.hedge_div || 76;
    const straddle_div = config.hedge.straddle_div || 40;
    const hedge_start_time = config.hedge.start_time || '12:32:00';
    const roll_straddle_div = config.roll.roll_straddle_div || 2;
    const roll_start_time = config.roll.start_time || '12:32:00';
    const exit_time = config.square_off.exit_time || '15:27:00';

    modalContent.innerHTML = `
        <div class="modal-header">
            <h2>Modify Config for ${tradeUid}</h2>
            <button class="close-button">&times;</button>
        </div>
        <div class="modal-body">
            <form id="modify-config-form">
                <div class="form-grid">
                    <div class="form-group">
                        <label for="sl_bps">SL (BPS)</label>
                        <input type="number" id="sl_bps" name="sl_bps" value="${sl_bps}" step="0.1">
                    </div>
                    <div class="form-group">
                        <label for="sl_start_time">SL Start Time</label>
                        <input type="text" id="sl_start_time" name="sl_start_time" value="${sl_start_time}" placeholder="HH:MM:SS">
                    </div>
                    <div class="form-group">
                        <label for="hedge_div">Hedge Div</label>
                        <input type="number" id="hedge_div" name="hedge_div" value="${hedge_div}">
                    </div>
                    <div class="form-group">
                        <label for="straddle_div">Straddle Div</label>
                        <input type="number" id="straddle_div" name="straddle_div" value="${straddle_div}">
                    </div>
                    <div class="form-group">
                        <label for="hedge_start_time">Hedge Start Time</label>
                        <input type="text" id="hedge_start_time" name="hedge_start_time" value="${hedge_start_time}" placeholder="HH:MM:SS">
                    </div>
                    <div class="form-group">
                        <label for="roll_straddle_div">Roll Div</label>
                        <input type="number" id="roll_straddle_div" name="roll_straddle_div" value="${roll_straddle_div}">
                    </div>
                    <div class="form-group">
                        <label for="roll_start_time">Roll Start Time</label>
                        <input type="text" id="roll_start_time" name="roll_start_time" value="${roll_start_time}" placeholder="HH:MM:SS">
                    </div>
                    <div class="form-group">
                        <label for="exit_time">Exit Time</label>
                        <input type="text" id="exit_time" name="exit_time" value="${exit_time}" placeholder="HH:MM:SS">
                    </div>
                </div>
                <div class="form-actions">
                    <button type="submit" class="btn btn-primary">Update Config</button>
                    <button type="button" class="btn btn-secondary close-button">Cancel</button>
                </div>
            </form>
        </div>
    `;

    modalOverlay.appendChild(modalContent);
    document.body.appendChild(modalOverlay);

    // Add event listeners
    const form = document.getElementById('modify-config-form');
    form.addEventListener('submit', (e) => updateTradeConfig(e, tradeUid));

    modalOverlay.querySelectorAll('.close-button').forEach(btn => {
        btn.addEventListener('click', () => {
            modalOverlay.remove();
        });
    });

    // Close on overlay click
    modalOverlay.addEventListener('click', (e) => {
        if (e.target === modalOverlay) {
            modalOverlay.remove();
        }
    });
}

async function updateTradeConfig(event, tradeUid) {
    event.preventDefault();
    const form = document.getElementById('modify-config-form');
    const formData = new FormData(form);
    const newConfig = {};
    for (const [key, value] of formData.entries()) {
        // Convert numeric fields from string to number
        if (!isNaN(value) && value.trim() !== '') {
            newConfig[key] = Number(value);
        } else {
            newConfig[key] = value;
        }
    }

    if (!confirm('Are you sure you want to update the live configuration for this trade? The monitors will be restarted.')) {
        return;
    }

    try {
        showNotification(`Updating config for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/update-config/${tradeUid}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(newConfig)
        });
        const data = await response.json();

        if (data.success) {
            showNotification('✅ CONFIG UPDATED', data.message, 'success');
            document.getElementById('modify-config-modal-overlay').remove();
            // Force a refresh to show new monitor status
            setTimeout(() => fetchStraddles(true), 1000);
        } else {
            showNotification('❌ UPDATE FAILED', data.error || data.detail, 'error');
        }
    } catch (error) {
        showNotification('❌ ERROR', error.message, 'error');
    }
}

async function cancelTradeAction(tradeUid) {
    if (!confirm(`Cancel the ongoing action for trade ${tradeUid}?\n\nThis will attempt to stop the current operation (e.g., square-off). The trade status will be reverted to ACTIVE.`)) {
        return;
    }
    try {
        showNotification(`Requesting cancellation for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/cancel-action/${tradeUid}`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            showNotification('✅ CANCELLATION REQUESTED', data.message, 'success');
            // The backend will revert status, and the UI will update via websocket.
            // We can force a refresh for immediate feedback.
            setTimeout(() => fetchStraddles(true), 1000);
        } else {
            showNotification('❌ CANCELLATION FAILED', data.error || data.detail, 'error');
        }
    } catch (error) {
        showNotification('❌ ERROR', error.message, 'error');
    }
}

async function manualVerify(tradeUid) {
    if (!confirm(`Manually sync trade ${tradeUid} with the broker's order book?\n\nThis will fetch all orders from the broker and update the database. Use this if you suspect a discrepancy.`)) {
        return;
    }
    try {
        showNotification(`Requesting manual sync for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/manual-verify/${tradeUid}`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            showNotification('✅ SYNC COMPLETE', data.message, 'success');
            // Force a refresh of the portfolio view to show the updated state
            setTimeout(() => fetchStraddles(true), 1000);
        } else {
            showNotification('❌ SYNC FAILED', data.error || data.detail, 'error');
        }
    } catch (error) {
        showNotification('❌ ERROR', error.message, 'error');
    }
}

async function manualHedge(tradeUid) {
    if (!confirm(`Manually trigger a HEDGE for trade ${tradeUid}?\n\nThis will check conditions and hedge if needed.`)) {
        return;
    }
    try {
        showNotification(`Requesting manual hedge check for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/manual-hedge/${tradeUid}`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            showNotification('✅ HEDGE REQUESTED', data.message, 'success');
        } else {
            showNotification('❌ HEDGE FAILED', data.error || data.detail, 'error');
        }
    } catch (error) {
        showNotification('❌ ERROR', error.message, 'error');
    }
}

async function manualRoll(tradeUid) {
    if (!confirm(`Manually trigger a ROLL for trade ${tradeUid}?\n\nThis will check conditions and roll if needed.`)) {
        return;
    }
    try {
        showNotification(`Requesting manual roll check for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/manual-roll/${tradeUid}`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            showNotification('✅ ROLL REQUESTED', data.message, 'success');
        } else {
            showNotification('❌ ROLL FAILED', data.error || data.detail, 'error');
        }
    } catch (error) {
        showNotification('❌ ERROR', error.message, 'error');
    }
}

async function partialSquareOffStraddle(tradeUid) {
    const percentage = prompt(`Enter percentage of CURRENT position to square off for ${tradeUid}:`, "25");
    if (percentage === null || percentage === "" || isNaN(percentage) || percentage <= 0 || percentage > 100) {
        if (percentage !== null) { // Don't show error if user cancelled
            showNotification('Invalid percentage. Please enter a number between 1 and 100.', 'error');
        }
        return;
    }

    const percentageValue = parseFloat(percentage);

    if (!confirm(`Partially square off ${percentageValue}% of trade ${tradeUid}?`)) {
        return;
    }

    try {
        showNotification(`Requesting partial square-off for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/partial-square-off/${tradeUid}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ percentage: percentageValue })
        });
        const data = await response.json();

        if (data.success) {
            showNotification('✅ PARTIAL SQF QUEUED', data.message, 'success');
            // No need to refresh immediately, wait for updates via websocket
        } else {
            showNotification('❌ PARTIAL SQF FAILED', data.error || data.detail, 'error');
        }
    } catch (error) {
        showNotification('❌ ERROR', error.message, 'error');
    }
}

async function squareOffStraddle(tradeUid, netDelta) {
    const confirmationMessage = `Square off trade ${tradeUid}?\n\n` +
                              `Current Net Delta: ${netDelta !== undefined ? netDelta.toFixed(2) : 'N/A'}`;

    if (!confirm(confirmationMessage)) {
        return;
    }

    try {
        showNotification(`Requesting square-off for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/square-off/${tradeUid}`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            showNotification('✅ SQUARE-OFF SUCCESS', `Trade ${tradeUid} closed.`, 'success');
            // Refresh both views to update status everywhere
            fetchStraddles(true);
            fetchStraddles(false);
        } else {
            showNotification('❌ SQUARE-OFF FAILED', data.error, 'error');
        }
    } catch (error) {
        showNotification('❌ ERROR', error.message, 'error');
    }
}

async function sellStraddle() {
    const symbol = document.getElementById('symbol').value;
    const lots = parseInt(document.getElementById('lots').value);

    if (!optionChainData.chain) {
        showNotification('Please wait for option chain to load.', 'error');
        return;
    }
    if (!confirm(`Place ${symbol} ATM Straddle (${lots} lot)?`)) return;

    try {
        showNotification(`Placing ${lots} lot straddle for ${symbol}...`, 'info');
        const response = await fetch('/api/straddle/sell', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({symbol, lots, delta_neutral: true})
        });
        const result = await response.json();
        
        if (result.success) {
            showNotification(`✅ Straddle order placed: ${result.trade_uid}`, 'success');
            setTimeout(() => {
                fetchStraddles();
            }, 2000);
        } else {
            showNotification(`❌ Error: ${result.error}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Network error: ${error.message}`, 'error');
    }
}

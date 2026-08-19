// ════════════════════════════════════════════════════════════════════════════
// straddles.js
// ════════════════════════════════════════════════════════════════════════════

function roundQuantity(inputId) {
    const inputEl = document.getElementById(inputId);
    if (!inputEl) return;

    const approxQuantity = parseInt(inputEl.value);
    const lotSize = window._chainATMTokens?.lot_size;

    if (!lotSize || lotSize <= 0 || isNaN(approxQuantity) || approxQuantity <= 0) {
        return;
    }

    const roundedQuantity = Math.round(approxQuantity / lotSize) * lotSize;

    if (roundedQuantity > 0) {
        // Only update if the value is different to avoid cursor jumping
        // and to prevent re-triggering the oninput event in a loop.
        if (String(inputEl.value) !== String(roundedQuantity)) {
            inputEl.value = roundedQuantity;
        }
    }

    // Also trigger premium update if it exists for the terminal view
    if (inputId === 'lots' && typeof updateStraddlePremium === 'function') {
        updateStraddlePremium();
    }
}

let qtyUpdateTimeout = null;
function debouncedRoundQuantity(inputId) {
    clearTimeout(qtyUpdateTimeout);
    // When user is typing, debounce the rounding action.
    qtyUpdateTimeout = setTimeout(() => roundQuantity(inputId), 600);
}

// ── Style injection ───────────────────────────────────────────────────────────
(function () {
    if (document.getElementById('dynamic-color-styles')) return;
    const style = document.createElement('style');
    style.id = 'dynamic-color-styles';
    style.textContent = `
        .yellow { color: #b58500 !important; font-weight: bold; }
        .modal-overlay {
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background-color: rgba(0,0,0,0.6);
            display: flex; justify-content: center; align-items: center; z-index: 1000;
        }
        .modal-content {
            background-color: #2c2c2c; padding: 20px; border-radius: 8px;
            width: 90%; max-width: 600px; box-shadow: 0 5px 15px rgba(0,0,0,0.3);
        }
        .modal-header {
            display: flex; justify-content: space-between; align-items: center;
            border-bottom: 1px solid #444; padding-bottom: 10px; margin-bottom: 20px;
        }
        .modal-header h2 { margin: 0; font-size: 1.2em; }
        .modal-header .close-button, .modal-body .close-button {
            background: none; border: none; font-size: 1.5rem; cursor: pointer; color: #aaa;
        }
        .form-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .form-actions { margin-top: 20px; text-align: right; }
        .form-actions .btn { margin-left: 10px; }
        .form-group input {
            padding: 8px; border: 1px solid #555;
            background-color: #333; color: #fff; border-radius: 4px;
        }
        .btn-danger  { background: #dc3545; color: #fff; }
        .btn-warning { background: #ffc107; color: #212529; }
        .btn-info    { background: #17a2b8; color: #fff; }
        .btn-success { background: #28a745; color: #fff; }
    `;
    document.head.appendChild(style);
})();

// ── Globals ───────────────────────────────────────────────────────────────────
let previousPrices = {};
let priceMap       = {};
// NOTE: optionChainData intentionally removed — chain state lives in option_chain.js

const CANCELLABLE_STATUSES = ['SQUARING-OFF', 'PARTIAL-SQF', 'HEDGING', 'ROLLING', 'BUILDING'];
const ACTIVE_LIKE_STATUSES = ['ACTIVE', 'FILLED'];
const ALL_ACTIVE_STATUSES  = ['ACTIVE', 'FILLED', 'PARTIAL-SQF', 'HEDGING', 'ROLLING'];


// ════════════════════════════════════════════════════════════════════════════
// SHARED HELPERS
// ════════════════════════════════════════════════════════════════════════════

function _buildActionsHtml(tradeUid, statusRaw, liveNetDelta) {
    const statusUpper  = (statusRaw || '').toUpperCase();
    const isPending    = statusUpper === 'PENDING';
    const isActiveLike = ACTIVE_LIKE_STATUSES.includes(statusUpper);
    const delta        = typeof liveNetDelta === 'number' ? liveNetDelta : 0;

    let html = `<button class="btn-icon" title="View Details"
        onclick="event.stopPropagation(); showStraddleDetails('${tradeUid}', this.closest('tr'))">ℹ️</button>`;

    if (CANCELLABLE_STATUSES.includes(statusUpper)) {
        html += `<button class="btn-icon btn-danger" title="Cancel Action"
            onclick="event.stopPropagation(); cancelTradeAction('${tradeUid}')">🚫</button>`;
    } else if (isPending) {
        html += `<button class="btn-icon" title="Modify Config"
            onclick="event.stopPropagation(); showModifyConfigModal('${tradeUid}')">⚙️</button>`;
        html += `<button class="btn-icon btn-danger" title="Cancel Scheduled Build"
            onclick="event.stopPropagation(); cancelTradeAction('${tradeUid}')">🚫</button>`;
    } else if (isActiveLike) {
        html += `<button class="btn-icon" title="Partial Exit"
            onclick="event.stopPropagation(); partialSquareOffStraddle('${tradeUid}')">✂️</button>`;
        html += `<button class="btn-icon btn-danger" title="Full Exit"
            onclick="event.stopPropagation(); squareOffStraddle('${tradeUid}', ${delta})">❌</button>`;
    }
    return html;
}

function _statusClass(statusRaw) {
    const s = (statusRaw || '').toUpperCase();
    if (s === 'PENDING' || s === 'BUILDING') return 'yellow';
    if (ALL_ACTIVE_STATUSES.includes(s)) return 'positive';
    if (s.startsWith('CLOSED')) return 'neutral';
    return 'neutral';
}


// ════════════════════════════════════════════════════════════════════════════
// PRICE CACHE
// Syncs to window._lastPrices so option_chain.js can read live prices.
// ════════════════════════════════════════════════════════════════════════════

function updatePrice(token, ltp) {
    priceMap[token] = ltp;

    window._lastPrices = window._lastPrices || {};
    window._lastPrices[String(token)] = ltp;

    const cells = document.querySelectorAll(`[data-token="${token}"]`);
    cells.forEach(cell => {
        const row = cell.closest('tr');
        if (row && row.dataset.tradeUid) {
            const statusCell = row.querySelector('td:nth-child(4), td:nth-child(6)');
            if (statusCell && statusCell.textContent.toUpperCase().startsWith('CLOSED')) return;
        }

        const target     = cell.querySelector('span') || cell;
        const prev       = previousPrices[token] || ltp;
        const flashClass = ltp > prev ? 'ltp-flash-up' : (ltp < prev ? 'ltp-flash-down' : '');

        target.textContent = `₹${ltp.toFixed(2)}`;
        if (flashClass) {
            cell.classList.add(flashClass);
            setTimeout(() => cell.classList.remove(flashClass), 700);
        }
    });

    previousPrices[token] = ltp;
    // NOTE: updateStraddlePremium() removed — does not exist here.
    //       option_chain.js manages its own premium display via window._lastPrices.
}


// ════════════════════════════════════════════════════════════════════════════
// FETCH + RENDER STRADDLES
// ════════════════════════════════════════════════════════════════════════════

async function fetchStraddles(isPortfolioView = false) {
    const divId      = isPortfolioView ? 'portfolio-positions-display' : 'straddles-display';
    const displayDiv = document.getElementById(divId);
    if (!displayDiv) return;
    if (isPortfolioView) displayDiv.classList.add('scrollable');

    try {
        const response = await fetch('/api/straddles');
        const data     = await response.json();
        if (data.success) {
            isPortfolioView
                ? displayPortfolioPositions(data.straddles)
                : displayStraddles(data.straddles);
        } else {
            displayDiv.innerHTML = `<div class="placeholder">❌ ${data.error}</div>`;
        }
    } catch (error) {
        displayDiv.innerHTML = `<div class="placeholder">❌ ${error.message}</div>`;
    }
}

// ── Terminal compact view ─────────────────────────────────────────────────────

function displayStraddles(straddles) {
    const displayDiv = document.getElementById('straddles-display');
    if (!displayDiv) return;

    if (!straddles || straddles.length === 0) {
        displayDiv.innerHTML = '<div class="placeholder">No active positions</div>';
        return;
    }

    let html = `<table><thead><tr>
        <th>UID</th><th>Symbol</th><th>Strike</th><th>CE Qty</th><th>PE Qty</th>
        <th>Status</th><th>Net Δ</th><th>PnL</th><th>Actions</th>
    </tr></thead><tbody>`;

    straddles.forEach(straddle => {
        const statusUpper  = (straddle.status || '').toUpperCase();
        const statusClass  = statusUpper === 'BUILDING' ? 'yellow'
            : (ACTIVE_LIKE_STATUSES.includes(statusUpper) ? 'positive' : 'neutral');
        const pnlClass     = (straddle.live_pnl || 0) >= 0 ? 'positive' : 'negative';
        const tradeUid     = straddle.trade_uid || straddle.straddle_id;
        const liveNetDelta = straddle.live_net_delta || straddle.net_delta || 0;

        html += `
            <tr data-trade-uid="${tradeUid}"
                onclick="showStraddleDetails('${tradeUid}', this)"
                style="cursor:pointer;">
                <td>${tradeUid.slice(-6)}</td>
                <td>${straddle.symbol}</td>
                <td>${straddle.strike}</td>
                <td>${straddle.ce_quantity}</td>
                <td>${straddle.pe_quantity}</td>
                <td class="${statusClass}">${straddle.status}</td>
                <td class="net-delta-cell">${liveNetDelta.toFixed(2)}</td>
                <td class="pnl-cell ${pnlClass}">₹${(straddle.live_pnl || 0).toFixed(2)}</td>
                <td class="actions-cell">
                    ${ACTIVE_LIKE_STATUSES.includes(statusUpper) ? `
                        <button class="btn-icon" title="Partial Square Off"
                            onclick="event.stopPropagation(); partialSquareOffStraddle('${tradeUid}')">✂️</button>
                        <button class="btn-icon" title="Full Square Off"
                            onclick="event.stopPropagation(); squareOffStraddle('${tradeUid}', ${liveNetDelta})">❌</button>
                    ` : ''}
                </td>
            </tr>`;
    });

    html += '</tbody></table>';
    displayDiv.innerHTML = html;
}

// ── Portfolio detailed view ───────────────────────────────────────────────────

function displayPortfolioPositions(straddles) {
    const displayDiv = document.getElementById('portfolio-positions-display');
    if (!displayDiv) return;

    if (!straddles || straddles.length === 0) {
        displayDiv.innerHTML = '<div class="placeholder">No active positions</div>';
        return;
    }

    let html = `<table><thead><tr>
        <th>UID</th><th>Symbol</th><th>Strike</th><th>Status</th>
        <th>CE Qty</th><th>CE LTP</th><th>PE Qty</th><th>PE LTP</th>
        <th>Net Δ</th><th>Pts Out</th><th>Pts Allowed</th>
        <th>Unrealized</th><th>Realized</th><th>PnL/Straddle</th><th>Actions</th>
    </tr></thead><tbody>`;

    straddles.forEach(straddle => {
        const statusUpper  = (straddle.status || '').toUpperCase();
        const isClosed     = statusUpper.startsWith('CLOSED');
        const isPending    = statusUpper === 'PENDING';
        const tradeUid     = straddle.trade_uid || straddle.straddle_id;
        const liveNetDelta = straddle.live_net_delta || straddle.net_delta || 0;

        const unrealized_pnl   = isClosed || isPending ? 0 : (straddle.unrealized_pnl || 0);
        const realized_pnl     = straddle.realized_pnl || 0;
        const pnl_per_lot      = straddle.pnl_per_lot  || 0;
        const pnlPerLotDisplay = isClosed || isPending ? '—' : pnl_per_lot.toFixed(2);
        const net_delta_display = isClosed || isPending ? '—' : liveNetDelta.toFixed(2);

        const ce_ltp_display = isClosed || isPending
            ? '—' : `₹${(priceMap[straddle.ce_token || (straddle.live_positions?.find(p => p.option_type === "CE")?.token)] || straddle.ce_ltp || 0).toFixed(2)}`;
        const pe_ltp_display = isClosed || isPending
            ? '—' : `₹${(priceMap[straddle.pe_token || (straddle.live_positions?.find(p => p.option_type === "PE")?.token)] || straddle.pe_ltp || 0).toFixed(2)}`;

        const pts_out        = isClosed || isPending ? 0 : (straddle.pts_out || 0);
        const points_allowed = isClosed || isPending ? null : straddle.points_allowed;
        const pts_allowed_display = (points_allowed !== null && points_allowed !== undefined)
            ? points_allowed.toFixed(2) : '—';

        let ptsOutClass = '';
        if (!isClosed && !isPending && points_allowed > 0) {
            const ratio = pts_out / points_allowed;
            ptsOutClass = ratio >= 1.0 ? 'negative' : ratio >= 0.5 ? 'yellow' : 'positive';
        }

        const unrealizedPnlClass = unrealized_pnl >= 0 ? 'positive' : 'negative';
        const realizedPnlClass   = realized_pnl   >= 0 ? 'positive' : 'negative';
        const pnlPerLotClass     = pnl_per_lot     >= 0 ? 'positive' : 'negative';
        const actionsHtml        = _buildActionsHtml(tradeUid, straddle.status, liveNetDelta);

        html += `
            <tr data-trade-uid="${tradeUid}"
                onclick="showStraddleDetails('${tradeUid}', this)"
                style="cursor:pointer;">
                <td>${tradeUid.slice(-6)}</td>
                <td>${straddle.symbol}</td>
                <td class="strike-cell">${straddle.strike}</td>
                <td class="${_statusClass(straddle.status)}">${straddle.status}</td>
                <td>${straddle.ce_quantity || '—'}</td>
                <td data-token="${straddle.ce_token}">${ce_ltp_display}</td>
                <td>${straddle.pe_quantity || '—'}</td>
                <td data-token="${straddle.pe_token}">${pe_ltp_display}</td>
                <td class="net-delta-cell">${net_delta_display}</td>
                <td class="pts-out-cell ${ptsOutClass}">${pts_out.toFixed(2)}</td>
                <td class="pts-allowed-cell">${pts_allowed_display}</td>
                <td class="unrealized-pnl-cell ${unrealizedPnlClass}">₹${unrealized_pnl.toFixed(2)}</td>
                <td class="realized-pnl-cell ${realizedPnlClass}">₹${realized_pnl.toFixed(2)}</td>
                <td class="pnl-per-lot-cell ${pnlPerLotClass}">₹${pnlPerLotDisplay}</td>
                <td class="actions-cell">${actionsHtml}</td>
            </tr>`;
    });

    html += '</tbody></table>';
    displayDiv.innerHTML = html;
}


// ════════════════════════════════════════════════════════════════════════════
// LIVE UPDATE HANDLERS  (called by websocket.js)
// ════════════════════════════════════════════════════════════════════════════

function handlePnlBatchUpdate(updates) {
    if (!updates || !Array.isArray(updates)) return;

    updates.forEach(update => {
        const tradeUid = update.trade_uid;
        if (!tradeUid) return;

        document.querySelectorAll(`tr[data-trade-uid="${tradeUid}"]`).forEach(row => {
            const statusCell = row.querySelector('td:nth-child(4), td:nth-child(6)');
            if (statusCell && statusCell.textContent.toUpperCase().startsWith('CLOSED')) return;

            const deltaCell = row.querySelector('.net-delta-cell');
            if (deltaCell && update.live_net_delta !== undefined)
                deltaCell.textContent = update.live_net_delta.toFixed(2);

            const pnlCell = row.querySelector('.pnl-cell');
            if (pnlCell && update.live_pnl !== undefined) {
                const pnl = update.live_pnl;
                pnlCell.textContent = `₹${pnl.toFixed(2)}`;
                pnlCell.className   = `pnl-cell ${pnl >= 0 ? 'positive' : 'negative'}`;
            }

            const unrealizedPnlCell = row.querySelector('.unrealized-pnl-cell');
            if (unrealizedPnlCell && update.unrealized_pnl !== undefined) {
                const v = update.unrealized_pnl;
                unrealizedPnlCell.textContent = `₹${v.toFixed(2)}`;
                unrealizedPnlCell.className   = `unrealized-pnl-cell ${v >= 0 ? 'positive' : 'negative'}`;
            }

            const pnlPerLotCell = row.querySelector('.pnl-per-lot-cell');
            if (pnlPerLotCell && update.pnl_per_lot !== undefined) {
                const v = update.pnl_per_lot;
                pnlPerLotCell.textContent = `₹${v.toFixed(2)}`;
                pnlPerLotCell.className   = `pnl-per-lot-cell ${v >= 0 ? 'positive' : 'negative'}`;
            }

            const ptsOutCell = row.querySelector('.pts-out-cell');
            if (ptsOutCell && update.pts_out !== undefined) {
                const pts_out     = update.pts_out;
                const pts_allowed = update.points_allowed;

                ptsOutCell.textContent = pts_out.toFixed(2);

                const ptsAllowedCell = row.querySelector('.pts-allowed-cell');
                if (ptsAllowedCell)
                    ptsAllowedCell.textContent = (pts_allowed !== null && pts_allowed !== undefined)
                        ? pts_allowed.toFixed(2) : '—';

                let cls = '';
                if (pts_allowed && pts_allowed > 0) {
                    const ratio = pts_out / pts_allowed;
                    cls = ratio >= 1.0 ? 'negative' : ratio >= 0.5 ? 'yellow' : 'positive';
                }
                ptsOutCell.className = `pts-out-cell ${cls}`;
            }
        });

        if (update.position_ltps) {
            for (const [token, ltp] of Object.entries(update.position_ltps))
                updatePrice(parseInt(token), ltp);
        }
    });
}

// ── handleStraddleUpdate  (row-level in-place update) ────────────────────────

function handleStraddleUpdate(data) {
    const tradeUid = data.trade_uid;
    if (!tradeUid) return;

    const num = (v, fallback = 0) => {
        const n = Number(v);
        return Number.isFinite(n) ? n : fallback;
    };

    const setTextIfPresent = (id, value, formatter = v => String(v)) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (value === null || value === undefined || value === '') {
            el.textContent = '--';
            return;
        }
        el.textContent = formatter(value);
    };

    const setClassIfPresent = (id, className) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.className = className;
    };

    // ── Terminal table ────────────────────────────────────────────────────────
    const mainRow = document.querySelector(`#straddles-display tr[data-trade-uid="${tradeUid}"]`);
    if (mainRow) {
        const statusCell = mainRow.querySelector('td:nth-child(6)');
        if (statusCell && statusCell.textContent.toUpperCase().startsWith('CLOSED')) return;

        const deltaCell = mainRow.querySelector('.net-delta-cell');
        if (deltaCell) {
            deltaCell.textContent = num(data.live_net_delta ?? data.net_delta).toFixed(2);
        }

        const pnlCell = mainRow.querySelector('.pnl-cell');
        if (pnlCell) {
            const pnl = num(data.live_pnl ?? data.total_pnl);
            pnlCell.textContent = `₹${pnl.toFixed(2)}`;
            pnlCell.className = `pnl-cell ${pnl >= 0 ? 'positive' : 'negative'}`;
        }

        if (data.position_ltps) {
            for (const [token, ltp] of Object.entries(data.position_ltps)) {
                updatePrice(parseInt(token, 10), ltp);
            }
        }
    }

    // ── Portfolio table ───────────────────────────────────────────────────────
    const portfolioRow = document.querySelector(
        `#portfolio-positions-display tr[data-trade-uid="${tradeUid}"]`
    );

    if (portfolioRow) {
        const statusCell = portfolioRow.querySelector('td:nth-child(4)');
        if (statusCell && statusCell.textContent.toUpperCase().startsWith('CLOSED')) return;

        const deltaCell = portfolioRow.querySelector('.net-delta-cell');
        if (deltaCell) {
            deltaCell.textContent = num(data.live_net_delta ?? data.net_delta).toFixed(2);
        }

        const strikeCell = portfolioRow.querySelector('.strike-cell');
        if (strikeCell && data.strike !== null && data.strike !== undefined) {
            strikeCell.textContent = data.strike;
        }

        if (data.status && statusCell) {
            const prevStatus = statusCell.textContent;
            statusCell.textContent = data.status;
            statusCell.className = _statusClass(data.status);

            if (data.status !== prevStatus) {
                const actionsCell = portfolioRow.querySelector('.actions-cell');
                if (actionsCell) {
                    actionsCell.innerHTML = _buildActionsHtml(
                        tradeUid,
                        data.status,
                        num(data.live_net_delta ?? data.net_delta)
                    );
                }
            }
        }

        const unrealizedPnlCell = portfolioRow.querySelector('.unrealized-pnl-cell');
        if (unrealizedPnlCell) {
            const v = num(data.unrealized_pnl);
            unrealizedPnlCell.textContent = `₹${v.toFixed(2)}`;
            unrealizedPnlCell.className = `unrealized-pnl-cell ${v >= 0 ? 'positive' : 'negative'}`;
        }

        const realizedPnlCell = portfolioRow.querySelector('.realized-pnl-cell');
        if (realizedPnlCell) {
            const v = num(data.realized_pnl);
            realizedPnlCell.textContent = `₹${v.toFixed(2)}`;
            realizedPnlCell.className = `realized-pnl-cell ${v >= 0 ? 'positive' : 'negative'}`;
        }

        const ptsOutCell = portfolioRow.querySelector('.pts-out-cell');
        if (ptsOutCell) {
            const ptsOut = num(data.pts_out);
            const pointsAllowed = data.points_allowed ?? null;
            ptsOutCell.textContent = ptsOut.toFixed(2);

            let cls = 'positive';
            if (pointsAllowed !== null && Number(pointsAllowed) > 0) {
                const ratio = ptsOut / Number(pointsAllowed);
                cls = ratio >= 1.0 ? 'negative' : ratio >= 0.5 ? 'yellow' : 'positive';
            }
            ptsOutCell.className = `pts-out-cell ${cls}`;
        }

        const ptsAllowedCell = portfolioRow.querySelector('.pts-allowed-cell');
        if (ptsAllowedCell) {
            const pa = data.points_allowed;
            ptsAllowedCell.textContent =
                (pa !== null && pa !== undefined) ? num(pa).toFixed(2) : '∞';
        }

        const pnlPerLotCell = portfolioRow.querySelector('.pnl-per-lot-cell');
        if (pnlPerLotCell) {
            const v = data.pnl_per_straddle ?? data.pnl_per_lot ?? null;
            if (v !== null && v !== undefined) {
                const pnlPerLot = num(v);
                pnlPerLotCell.textContent = `₹${pnlPerLot.toFixed(2)}`;
                pnlPerLotCell.className = `pnl-per-lot-cell ${pnlPerLot >= 0 ? 'positive' : 'negative'}`;
            }
        }

        if (data.position_ltps) {
            for (const [token, ltp] of Object.entries(data.position_ltps)) {
                updatePrice(parseInt(token, 10), ltp);
            }
        }
    }

    // ── Spot / synthetic spot header ──────────────────────────────────────────
    if (data.spot_price !== null && data.spot_price !== undefined) {
        const el = document.getElementById('chain-spot-value');
        if (el) el.textContent = `₹${num(data.spot_price).toFixed(2)}`;
    }

    if (data.synthetic_spot !== null && data.synthetic_spot !== undefined) {
        const el = document.getElementById('chain-synfut-value');
        if (el) el.textContent = `₹${num(data.synthetic_spot).toFixed(2)}`;
    }

    // ── Live score monitor via websocket ──────────────────────────────────────
    const score = data.score || {};
    setTextIfPresent('score-live-iv', score.live_iv, v => num(v).toFixed(6));
    setTextIfPresent('score-adj-iv', score.adj_iv, v => num(v).toFixed(6));
    setTextIfPresent('score-live-straddle', score.live_straddle, v => num(v).toFixed(2));
    setTextIfPresent('score-adj-idv', score.adj_idv, v => num(v).toFixed(6));
    setTextIfPresent('score-prev-straddle', score.prev_day_straddle, v => num(v).toFixed(2));
    setTextIfPresent('score-prev-day-adj-iv', score.prev_day_adj_iv, v => num(v).toFixed(6));
    setTextIfPresent('score-iv-idv-ratio', score.iv_idv_ratio, v => num(v).toFixed(4));
    setTextIfPresent('score-straddle-ratio', score.straddle_ratio, v => num(v).toFixed(4));
    setTextIfPresent('score-og-gap-pct', score.og_gap_pct, v => `${(v * 100).toFixed(2)}%`);
    setTextIfPresent('score-norm-og-gap', score.norm_og_gap, v => num(v).toFixed(6));
    setTextIfPresent('score-adj-iv-chg', data.adj_iv_chg, v => num(v).toFixed(6));

    // Buckets
    const lut_payload = score.lut_payload || {};
    setTextIfPresent('score-build-iv-bucket', lut_payload.Build_IV);
    setTextIfPresent('score-dte-bucket', lut_payload.DTE);
    setTextIfPresent('score-iv-ratio-bucket', lut_payload.IV_Ratio);
    setTextIfPresent('score-straddle-ratio-bucket', lut_payload.Straddle_Ratio);
    setTextIfPresent('score-adj-iv-chg-bucket', lut_payload.Adj_IV_Chg);
    setTextIfPresent('score-norm-og-gap-bucket', lut_payload.Norm_OG_Gap);

    // Raw values for gap calc
    setTextIfPresent('score-future-price-ref', score.future_price_ref, v => num(v).toFixed(2));
    setTextIfPresent('score-synthetic-price-ref', score.synthetic_price_ref, v => num(v).toFixed(2));
    setTextIfPresent('score-price-ref-source', score.price_ref_source);

    // Status Banner
    const banner = document.getElementById('score-status-banner');
    if (banner) {
        const bannerInfo = buildScoreBannerMessage(score);
        if (bannerInfo) {
            updateScoreBanner(bannerInfo.message, bannerInfo.type);
        }
    }
}

// ── handleStraddleFullUpdate ──────────────────────────────────────────────────

function handleStraddleFullUpdate(data) {
    fetchStraddles(false);
    const isPortfolio = document.getElementById('portfolio-view')
        ?.classList.contains('active');
    if (isPortfolio) fetchStraddles(true);
}


// ════════════════════════════════════════════════════════════════════════════
// ORDER ACTIONS
// ════════════════════════════════════════════════════════════════════════════

function _calculateLotsPerCallFromQty() {
    const lotSize = window._chainATMTokens?.lot_size;
    const qtyPerCallVal = document.getElementById('manual_lots_per_call').value;

    if (!qtyPerCallVal) return 1; // Default to 1 lot if empty

    const qtyPerCall = parseInt(qtyPerCallVal);
    if (isNaN(qtyPerCall) || qtyPerCall <= 0) return 1;

    if (lotSize && lotSize > 0) {
        // Convert quantity per call to lots per call, rounding up.
        const lots = Math.ceil(qtyPerCall / lotSize);
        return lots > 0 ? lots : 1; // Ensure at least 1 lot
    }

    // Fallback: if no lot size is available, assume the user entered lots directly.
    // This maintains behavior if the option chain hasn't been fetched.
    return qtyPerCall;
}
function updateLiveScoreMonitorFromChain(chainData) {
    if (!chainData) return;

    const num = (v, fallback = 0) => {
        const n = Number(v);
        return Number.isFinite(n) ? n : fallback;
    };

    const setField = (id, value, formatter = v => String(v)) => {
        const el = document.getElementById(id);
        if (!el) return;

        const out = (value === null || value === undefined || value === '')
            ? '--'
            : formatter(value);

        if ('value' in el) el.value = out;
        else el.textContent = out;
    };

    const atm = Number(chainData.atm ?? 0);
    const row = Array.isArray(chainData.chain)
        ? chainData.chain.find(r => Number(r.strike) === atm)
        : null;

    if (!row) return;

    const ceIv = num(row.ce_iv);
    const peIv = num(row.pe_iv);
    const liveIv = (ceIv > 0 || peIv > 0) ? ((ceIv + peIv) / 2.0) : 0.0;
    const liveIvDecimal = liveIv > 0 ? liveIv / 100.0 : 0.0;

    const ceLtp = num(row.ce_ltp);
    const peLtp = num(row.pe_ltp);
    const liveStraddle = ceLtp + peLtp;

    setField('score-live-iv', liveIv, v => num(v).toFixed(6));
    setField('score-live-iv-decimal', liveIvDecimal, v => num(v).toFixed(6));
    setField('score-live-straddle', liveStraddle, v => num(v).toFixed(2));

    if (typeof previewBuildScore === 'function') {
        previewBuildScore();
    } else if (typeof calculateBuildScorePreview === 'function') {
        calculateBuildScorePreview();
    }
}

function _calculateLotsFromQuantity(approxQuantity) {
    const lotSize = window._chainATMTokens?.lot_size;
    if (!lotSize || lotSize <= 0) {
        showNotification('Lot size not available. Please fetch option chain first.', 'error');
        return null;
    }
    if (isNaN(approxQuantity) || approxQuantity <= 0) {
        showNotification('Please enter a valid approximate quantity.', 'error');
        return null;
    }

    // Round to nearest multiple of lot size and then calculate lots
    const roundedQuantity = Math.round(approxQuantity / lotSize) * lotSize;
    const calculatedLots = roundedQuantity / lotSize;

    if (calculatedLots <= 0) {
        showNotification('Calculated lots are zero. Please enter a larger quantity.', 'error');
        return null;
    }
    return calculatedLots;
}

async function sellStraddle() {
    const symbol      = document.getElementById('symbol').value;
    const approxQuantity = parseInt(document.getElementById('lots').value);
    const lotsPerCall = _calculateLotsPerCallFromQty();
    
    const lots = _calculateLotsFromQuantity(approxQuantity);
    if (lots === null) return;

    // NO chain guard — backend computes ATM strike server-side
    if (!confirm(`Place ${symbol} ATM Straddle (${lots} lots)?\n(Approx. Qty: ${approxQuantity} -> Rounded Qty: ${lots * (window._chainATMTokens?.lot_size || 0)})`)) return;

    try {
        showNotification(`Placing ${lots} lot ATM straddle for ${symbol}...`, 'info');
        const response = await fetch('/api/straddle/sell', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({

                symbol,
                lots,

                delta_neutral: true,
                order_lots_per_call: lotsPerCall,

                entry_at_straddle:
                    document.getElementById("entry-at-straddle").value === ""
                    ? null
                    : Number(document.getElementById("entry-at-straddle").value),

                exit_at_straddle:
                    document.getElementById("exit-at-straddle").value === ""
                    ? null
                    : Number(document.getElementById("exit-at-straddle").value)

            })
        });
        const result = await response.json();
        if (result.success) {
            showNotification(`✅ Straddle placed: ${result.trade_uid}`, 'success');
            setTimeout(() => fetchStraddles(false), 250);
            setTimeout(() => fetchStraddles(true),  250);
        } else {
            showNotification(`❌ Error: ${result.detail || result.error}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Network error: ${error.message}`, 'error');
    }
}

async function sellCustomStraddle() {
    const symbol       = document.getElementById('symbol').value;
    const approxQuantity = parseInt(document.getElementById('lots').value);
    const lotsPerCall = _calculateLotsPerCallFromQty();
    const deltaNeutral = document.getElementById('custom-delta-neutral').checked;

    const lots = _calculateLotsFromQuantity(approxQuantity);
    if (lots === null) return;

    // --- MODIFIED: Handle optional strikes and add 2% validation ---
    const ceStrikeVal = document.getElementById('custom-ce-strike').value;
    const peStrikeVal = document.getElementById('custom-pe-strike').value;

    let ceStrike = ceStrikeVal ? parseInt(ceStrikeVal) : null;
    let peStrike = peStrikeVal ? parseInt(peStrikeVal) : null;

    const spotPriceEl = document.getElementById('chain-synfut-value');
    const atmStrikeEl = document.getElementById('chain-atm-value');

    if (!spotPriceEl || !spotPriceEl.textContent || !atmStrikeEl || !atmStrikeEl.textContent) {
        showNotification('Spot price or ATM strike not available. Please fetch option chain first.', 'error');
        return;
    }

    const spotPrice = parseFloat(spotPriceEl.textContent.replace('₹', ''));
    const atmStrike = parseInt(atmStrikeEl.textContent);

    if (isNaN(spotPrice) || isNaN(atmStrike)) {
        showNotification('Could not parse spot price or ATM strike.', 'error');
        return;
    }

    // If one or both strikes are empty, use ATM.
    if (!ceStrike && !peStrike) {
        ceStrike = atmStrike;
        peStrike = atmStrike;
        document.getElementById('custom-ce-strike').value = atmStrike;
        document.getElementById('custom-pe-strike').value = atmStrike;
    } else {
        if (!ceStrike) {
            ceStrike = atmStrike;
            document.getElementById('custom-ce-strike').value = atmStrike;
        }
        if (!peStrike) {
            peStrike = atmStrike;
            document.getElementById('custom-pe-strike').value = atmStrike;
        }
    }

    // Validate strikes against 1% range of spot price
    const lowerBound = spotPrice * 0.99;
    const upperBound = spotPrice * 1.01;

    if (ceStrike < lowerBound || ceStrike > upperBound) {
        showNotification(`CE Strike ${ceStrike} is outside the 1% range of the spot price (₹${spotPrice.toFixed(2)}).`, 'error');
        return;
    }
    if (peStrike < lowerBound || peStrike > upperBound) {
        showNotification(`PE Strike ${peStrike} is outside the 1% range of the spot price (₹${spotPrice.toFixed(2)}).`, 'error');
        return;
    }

    if (!confirm(
        `Place custom ${symbol} trade?\nCE: ${ceStrike}\nPE: ${peStrike}\nLots: ${lots}\nDelta Neutral: ${deltaNeutral}\n(Approx. Qty: ${approxQuantity} -> Rounded Qty: ${lots * (window._chainATMTokens?.lot_size || 0)})`
    )) return;

    try {
        showNotification(`Placing custom trade for ${symbol}...`, 'info');
        const response = await fetch('/api/straddle/custom-sell', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                symbol,
                lots,
                delta_neutral:       deltaNeutral,
                order_lots_per_call: lotsPerCall,
                ce_strike_price:     ceStrike,
                pe_strike_price:     peStrike,
                product_type:        'MIS'
            })
        });
        const result = await response.json();
        if (result.success) {
            showNotification(`✅ Custom trade placed: ${result.trade_uid}`, 'success');
            setTimeout(() => fetchStraddles(false), 250);
            setTimeout(() => fetchStraddles(true),  250);
        } else {
            showNotification(`❌ Error: ${result.detail || result.error}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Network error: ${error.message}`, 'error');
    }
}

async function squareOffStraddle(tradeUid, netDelta) { 
    if (!confirm( 
        `Square off trade ${tradeUid}?\n\nCurrent Net Delta: ${netDelta !== undefined ? netDelta.toFixed(2) : 'N/A'}` 
    )) return; 

    try {
        showNotification(`Requesting square-off for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/square-off/${tradeUid}`, { method: 'POST' });
        const data     = await response.json();
        if (data.success) {
            showNotification(`✅ Trade ${tradeUid} closed.`, 'success');
            fetchStraddles(false);
            fetchStraddles(true);
        } else {
            showNotification(`❌ Square-off failed: ${data.error}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Error: ${error.message}`, 'error');
    }
}

async function partialSquareOffStraddle(tradeUid) {
    const percentage = prompt(
        `Enter % of original position to square off for ${tradeUid}:\n(e.g., 25 = close 25%)`,
        '25'
    );
    if (percentage === null || percentage === '' ||
        isNaN(percentage) || percentage <= 0 || percentage > 100) {
        if (percentage !== null)
            showNotification('Invalid percentage. Enter a number between 1 and 100.', 'warning');
        return;
    }
    const pct = parseFloat(percentage);
    if (!confirm(`Partially square off ${pct}% of trade ${tradeUid}?`)) return;

    try {
        showNotification(`Requesting partial square-off for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/partial-square-off/${tradeUid}`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ percentage: pct })
        });
        const data = await response.json();
        if (data.success) {
            showNotification(`✅ Partial SQF queued: ${data.message}`, 'success');
        } else {
            showNotification(`❌ Partial SQF failed: ${data.error || data.detail}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Error: ${error.message}`, 'error');
    }
}

async function cancelTradeAction(tradeUid) {
    if (!confirm(
        `Cancel the ongoing action for trade ${tradeUid}?\n\nStatus will revert to ACTIVE.`
    )) return;
    try {
        showNotification(`Requesting cancellation for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/cancel-action/${tradeUid}`, { method: 'POST' });
        const data     = await response.json();
        if (data.success) {
            showNotification(`✅ Cancellation requested: ${data.message}`, 'success');
            setTimeout(() => fetchStraddles(true), 250);
        } else {
            showNotification(`❌ Cancellation failed: ${data.error || data.detail}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Error: ${error.message}`, 'error');
    }
}

async function manualVerify(tradeUid) {
    if (!confirm(
        `Manually sync trade ${tradeUid} with the broker's order book?\n\nThis will fetch all orders and update the database.`
    )) return;
    try {
        showNotification(`Requesting manual sync for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/manual-verify/${tradeUid}`, { method: 'POST' });
        const data     = await response.json();
        if (data.success) {
            showNotification(`✅ Sync complete: ${data.message}`, 'success');
            setTimeout(() => fetchStraddles(true), 250);
        } else {
            showNotification(`❌ Sync failed: ${data.error || data.detail}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Error: ${error.message}`, 'error');
    }
}

async function manualHedge(tradeUid) {
    if (!confirm(
        `Manually trigger a HEDGE for trade ${tradeUid}?\n\nThis will check conditions and hedge if needed.`
    )) return;
    try {
        showNotification(`Requesting manual hedge for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/manual-hedge/${tradeUid}`, { method: 'POST' });
        const data     = await response.json();
        if (data.success) {
            showNotification(`✅ Hedge requested: ${data.message}`, 'success');
        } else {
            showNotification(`❌ Hedge failed: ${data.error || data.detail}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Error: ${error.message}`, 'error');
    }
}

async function manualRoll(tradeUid) {
    if (!confirm(
        `Manually trigger a ROLL for trade ${tradeUid}?\n\nThis will check conditions and roll if needed.`
    )) return;
    try {
        showNotification(`Requesting manual roll for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/manual-roll/${tradeUid}`, { method: 'POST' });
        const data     = await response.json();
        if (data.success) {
            showNotification(`✅ Roll requested: ${data.message}`, 'success');
        } else {
            showNotification(`❌ Roll failed: ${data.error || data.detail}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Error: ${error.message}`, 'error');
    }
}


// ════════════════════════════════════════════════════════════════════════════
// DETAILS PANEL
// ════════════════════════════════════════════════════════════════════════════

async function showStraddleDetails(tradeUid, clickedRow) {
    if (!clickedRow) return;

    const existing = document.getElementById(`details-${tradeUid}`);
    if (existing) {
        existing.remove();
        clickedRow.classList.remove('details-open');
        return;
    }

    const parentTable = clickedRow.closest('table');
    if (parentTable) {
        parentTable.querySelectorAll('.details-row').forEach(r => r.remove());
        parentTable.querySelectorAll('.details-open').forEach(r => r.classList.remove('details-open'));
    }

    try {
                    const [snapshotRes, straddlesRes] = await Promise.all([
                fetch(`/api/snapshot/${tradeUid}`),
                fetch('/api/straddles')
            ]);

            let snapshotData = {};
            if (snapshotRes.ok) {
                snapshotData = await snapshotRes.json();
            } else if (snapshotRes.status === 404) {
                console.warn("Snapshot 404 for " + tradeUid + ", using fallback object.");
                snapshotData = { total_pnl: 0, unrealized_pnl: 0, realized_pnl: 0, net_delta: 0, pnl_per_straddle: 0, live_positions: [], events: [] };
            } else {
                throw new Error("Snapshot failed with status " + snapshotRes.status);
            }
        const straddlesData = await straddlesRes.json();

        if (!straddlesData.success)
            throw new Error('Failed to fetch base straddles data');

        const baseStraddle = straddlesData.straddles.find(
            s => (s.trade_uid || s.straddle_id) === tradeUid
        );
        if (!baseStraddle)
            throw new Error(`Base data for ${tradeUid} not found`);

        const straddle = {
            ...baseStraddle,
            ...snapshotData,
            live_pnl:       snapshotData.total_pnl,
            unrealized_pnl: snapshotData.unrealized_pnl,
            realized_pnl:   snapshotData.realized_pnl,
            live_net_delta: snapshotData.net_delta,
            pnl_per_lot:    snapshotData.pnl_per_straddle,
        };

        clickedRow.classList.add('details-open');

        const detailsRow     = document.createElement('tr');
        detailsRow.id        = `details-${tradeUid}`;
        detailsRow.className = 'details-row';

        const detailsCell      = document.createElement('td');
        detailsCell.colSpan    = clickedRow.cells.length;
        detailsCell.innerHTML  = createDetailsHtml(straddle);

        detailsRow.appendChild(detailsCell);
        clickedRow.parentNode.insertBefore(detailsRow, clickedRow.nextSibling);

    } catch (error) {
        console.error(`Failed to show details for ${tradeUid}:`, error);
        showNotification(`❌ Could not load details for ${tradeUid}.`, 'error');
        clickedRow.classList.remove('details-open');
    }
}

function createDetailsHtml(straddle) {
    const statusUpper = (straddle.status || '').toUpperCase();
    const isClosed    = statusUpper.startsWith('CLOSED');
    const isPending   = statusUpper === 'PENDING';
    const tradeUid    = straddle.trade_uid || straddle.straddle_id;

    const total_pnl      = isClosed || isPending ? (straddle.realized_pnl || 0) : (straddle.live_pnl || 0);
    const realized_pnl   = straddle.realized_pnl   || 0;
    const unrealized_pnl = isClosed || isPending ? 0 : (straddle.unrealized_pnl || 0);

    const net_delta = isClosed || isPending ? 0 : (straddle.live_net_delta || straddle.net_delta || 0);
    const net_gamma = isClosed || isPending ? 0 : (straddle.net_gamma || 0);
    const net_theta = isClosed || isPending ? 0 : (straddle.net_theta || 0);
    const net_vega  = isClosed || isPending ? 0 : (straddle.net_vega  || 0);

    const points_out = isClosed || isPending ? 0
        : (Math.abs(net_gamma) > 1e-6 ? Math.abs(net_delta) / Math.abs(net_gamma) : 0);
    const points_allowed = isClosed || isPending ? 0 : (straddle.points_allowed || 0);
    const entry_spot = straddle.synthetic_spot ?? straddle.reference_spot ?? 'N/A';

    let ptsOutClass = '';
    if (!isClosed && points_allowed > 0) {
        const ratio = points_out / points_allowed;
        ptsOutClass = ratio >= 1.0 ? 'negative' : ratio >= 0.5 ? 'yellow' : 'positive';
    }

    const pnlHtml = `
        <div class="details-metrics">
            <div class="metric"><span>Total PnL</span>
                <strong class="${total_pnl >= 0 ? 'positive' : 'negative'}">₹${total_pnl.toFixed(2)}</strong></div>
            <div class="metric"><span>Unrealized PnL</span>
                <strong class="${unrealized_pnl >= 0 ? 'positive' : 'negative'}">₹${unrealized_pnl.toFixed(2)}</strong></div>
            <div class="metric"><span>Realized PnL</span>
                <strong class="${realized_pnl >= 0 ? 'positive' : 'negative'}">₹${realized_pnl.toFixed(2)}</strong></div>
            <div class="metric"><span>Points Out</span>
                <strong class="${ptsOutClass}">${points_out.toFixed(2)}</strong></div>
            <div class="metric"><span>Points Allowed</span>
                <strong>${points_allowed.toFixed(2)}</strong></div>
            <div class="metric"><span>Roll Trigger</span>
                <strong>₹${(straddle.roll_trigger_price || 0).toFixed(2)}</strong></div>
            <div class="metric"><span>Entry Spot</span>
                <strong>${entry_spot !== 'N/A' ? parseFloat(entry_spot).toFixed(2) : 'N/A'}</strong></div>
        </div>`;

    const greeksHtml = `
        <div class="details-metrics">
            <div class="metric"><span>Net Delta</span><strong>${net_delta.toFixed(2)}</strong></div>
            <div class="metric"><span>Net Gamma</span><strong>${net_gamma.toFixed(4)}</strong></div>
            <div class="metric"><span>Net Theta</span><strong>${net_theta.toFixed(2)}</strong></div>
            <div class="metric"><span>Net Vega</span><strong>${net_vega.toFixed(2)}</strong></div>
        </div>`;

    let monitorsHtml = '<div class="placeholder-small">No monitor configuration found</div>';
    if (straddle.config && Object.keys(straddle.config).length > 0) {
        const cfg = straddle.config;
        if (isPending) {
            monitorsHtml = `
                <div class="details-metrics">
                    <div class="metric"><span>Entry Time</span><strong>${cfg.entry_time || 'N/A'}</strong></div>
                    <div class="metric"><span>Exit Time</span><strong>${cfg.exit_time || 'N/A'}</strong></div>
                    <div class="metric"><span>SL BPS</span><strong>${cfg.sl_bps || 'N/A'}</strong></div>
                </div>
                <div class="placeholder-small">Scheduled — monitor values calculated on entry.</div>`;
        } else {
            const isRunning      = !isClosed && !isPending && statusUpper !== 'ERROR';
            const slStartTime    = cfg.sl_start_time    ? ` | Start: ${cfg.sl_start_time}`    : '';
            const hedgeStartTime = cfg.hedge_start_time ? ` | Start: ${cfg.hedge_start_time}` : '';
            const rollStartTime  = cfg.roll_start_time  ? ` | Start: ${cfg.roll_start_time}`  : '';
            const slPoints       = straddle.sl_points || 0;

            monitorsHtml = `
                <table class="details-table minimal"><tbody>
                    <tr><td>Stop-Loss</td>
                        <td><span class="status-dot ${isRunning ? 'active' : ''}"></span>${isRunning ? 'Running' : 'Stopped'}</td>
                        <td>SL: ${slPoints.toFixed(2)} pts (${cfg.sl_bps || 'N/A'} BPS)${slStartTime}</td></tr>
                    <tr><td>Hedge</td>
                        <td><span class="status-dot ${isRunning ? 'active' : ''}"></span>${isRunning ? 'Running' : 'Stopped'}</td>
                        <td>H-Div: ${cfg.hedge_div || 'N/A'} | S-Div: ${cfg.straddle_div || 'N/A'}${hedgeStartTime}</td></tr>
                    <tr><td>Roll</td>
                        <td><span class="status-dot ${isRunning ? 'active' : ''}"></span>${isRunning ? 'Running' : 'Stopped'}</td>
                        <td>Roll Div: ${cfg.roll_straddle_div || 'N/A'}${rollStartTime}</td></tr>
                    <tr><td>Square-Off</td>
                        <td><span class="status-dot ${isRunning ? 'active' : ''}"></span>${isRunning ? 'Running' : 'Stopped'}</td>
                        <td>Exit Time: ${cfg.exit_time || 'N/A'}</td></tr>
                </tbody></table>`;
        }
    }

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
                        </tr>`).join('')}
                </tbody>
            </table>`;
    }

    let positionsHtml = '<div class="placeholder-small">No live positions found.</div>';
    if (isClosed) {
        positionsHtml = '<div class="placeholder-small">Trade is closed. Final PnL is realized.</div>';
    } else if (isPending) {
        positionsHtml = '<div class="placeholder-small">Trade is scheduled and has not entered yet.</div>';
    } else if (straddle.live_positions && straddle.live_positions.length > 0) {
        positionsHtml = `
            <table class="details-table">
                <thead><tr>
                    <th>Leg</th><th>Strike</th><th>Action</th><th>Qty</th>
                    <th>Entry</th><th>LTP</th><th>PnL</th><th>IV</th><th>Delta</th>
                </tr></thead>
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
                            <td>${(pos.iv || 0).toFixed(2)}%</td>
                            <td>${(pos.delta || 0).toFixed(2)}</td>
                        </tr>`).join('')}
                </tbody>
            </table>`;
    }

    let manualActionsHtml = '<div class="placeholder-small">Trade is closed.</div>';
    if (!isClosed) {
        manualActionsHtml = `
            <div style="display:flex;flex-wrap:wrap;gap:10px;justify-content:center;padding:10px;">
                ${isPending ? `
                    <button class="btn btn-primary"
                        onclick="event.stopPropagation(); showModifyConfigModal('${tradeUid}')">Modify Config</button>
                    <button class="btn btn-danger"
                        onclick="event.stopPropagation(); cancelTradeAction('${tradeUid}')">Cancel Build</button>
                ` : `
                    <button class="btn btn-primary"
                        onclick="event.stopPropagation(); showModifyConfigModal('${tradeUid}')">Modify Config</button>
                    <button class="btn btn-secondary"
                        onclick="manualHedge('${tradeUid}')">Hedge Now</button>
                    <button class="btn btn-secondary"
                        onclick="manualRoll('${tradeUid}')">Roll Now</button>
                    <button class="btn btn-info"
                        onclick="manualVerify('${tradeUid}')">Sync Trade</button>
                    <button class="btn btn-warning"
                        onclick="partialSquareOffStraddle('${tradeUid}')">Partial Exit</button>
                    <button class="btn btn-danger"
                        onclick="squareOffStraddle('${tradeUid}', ${net_delta})">Full Exit</button>
                `}
            </div>`;
    }

    return `
        <div class="details-container">
            <div class="details-grid-complex">
                <div class="details-card">
                    <div class="details-card-header">PnL & Risk</div>
                    <div class="details-card-body">${pnlHtml}</div>
                </div>
                <div class="details-card">
                    <div class="details-card-header">Net Greeks</div>
                    <div class="details-card-body">${greeksHtml}</div>
                </div>
                <div class="details-card">
                    <div class="details-card-header">Monitor Status</div>
                    <div class="details-card-body">${monitorsHtml}</div>
                </div>
                <div class="details-card">
                    <div class="details-card-header">Recent Events</div>
                    <div class="details-card-body">${eventsHtml}</div>
                </div>
                <div class="details-card large-card">
                    <div class="details-card-header">Position Details</div>
                    <div class="details-card-body">${positionsHtml}</div>
                </div>
                <div class="details-card">
                    <div class="details-card-header">Manual Actions</div>
                    <div class="details-card-body">${manualActionsHtml}</div>
                </div>
            </div>
        </div>`;
}


// ════════════════════════════════════════════════════════════════════════════
// MODIFY CONFIG MODAL
// ════════════════════════════════════════════════════════════════════════════

async function showModifyConfigModal(tradeUid) {
    event.stopPropagation();
    const response = await fetch('/api/straddles');
    const data     = await response.json();
    if (!data.success) {
        showNotification('❌ Could not fetch trade data to modify.', 'error');
        return;
    }
    const straddle = data.straddles.find(s => (s.trade_uid || s.straddle_id) === tradeUid);
    if (!straddle || !straddle.config) {
        showNotification('❌ Monitor configuration not available for this trade.', 'error');
        return;
    }

    const cfg              = straddle.config;
    const statusUpper      = (straddle.status || '').toUpperCase();
    const isPending        = statusUpper === 'PENDING';
    const modalOverlay     = document.createElement('div');
    modalOverlay.id        = 'modify-config-modal-overlay';
    modalOverlay.className = 'modal-overlay';

    const modalContent     = document.createElement('div');
    modalContent.className = 'modal-content';

    modalContent.innerHTML = `
        <div class="modal-header">
            <h2>Modify Config — ${tradeUid}</h2>
            <button class="close-button">&times;</button>
        </div>
        <div class="modal-body">
            <form id="modify-config-form">
                <div class="form-grid">
                    <div class="form-group"><label>Size (Lots)</label>
                        <input type="number" name="size"              value="${cfg.size || straddle.lots || ''}"></div>
                    <div class="form-group"><label>Lots Per Call</label>
                        <input type="number" name="order_lots_per_call" value="${cfg.order_lots_per_call || 1}"></div>
                    <div class="form-group"><label>SL (BPS)</label>
                        <input type="number" name="sl_bps"            value="${cfg.sl_bps            || 14}"       step="0.1"></div>
                    <div class="form-group"><label>SL Start Time</label>
                        <input type="text"   name="sl_start_time"     value="${cfg.sl_start_time     || ''}"       placeholder="HH:MM:SS"></div>
                    <div class="form-group"><label>Hedge Div</label>
                        <input type="number" name="hedge_div"         value="${cfg.hedge_div         || 76}"></div>
                    <div class="form-group"><label>Straddle Div</label>
                        <input type="number" name="straddle_div"      value="${cfg.straddle_div      || 40}"></div>
                    <div class="form-group"><label>Hedge Start Time</label>
                        <input type="text"   name="hedge_start_time"  value="${cfg.hedge_start_time  || ''}"       placeholder="HH:MM:SS"></div>
                    <div class="form-group"><label>Roll Div</label>
                        <input type="number" name="roll_straddle_div" value="${cfg.roll_straddle_div || 2}"></div>
                    <div class="form-group"><label>Roll Start Time</label>
                        <input type="text"   name="roll_start_time"   value="${cfg.roll_start_time   || ''}"       placeholder="HH:MM:SS"></div>
                    <div class="form-group"><label>Exit Time</label>
                        <input type="text"   name="exit_time"         value="${cfg.exit_time         || '15:27:00'}" placeholder="HH:MM:SS"></div>
                    <div class="form-group"><label>Buy Buffer</label>
                        <input type="number" name="buy_buffer"        value="${cfg.buy_buffer        || 2}"></div>
                    <div class="form-group"><label>Sell Buffer</label>
                        <input type="number" name="sell_buffer"       value="${cfg.sell_buffer       || 2}"></div>
                    <div class="form-group"><label>Hedge Fraction</label>
                        <input type="number" name="hedge_frac"        value="${cfg.hedge_frac        || 1.0}" step="0.1"></div>
                    <div class="form-group"><label>Drop Trigger (pts)</label>
                        <input type="number" name="straddle_price_drop_trigger" value="${cfg.exit_at_straddle ?? ''}" step="0.5"></div>

<div class="config-item">
<label>Exit At Straddle</label>
<input
    type="number"
    name="exit_at_straddle"
    value="${cfg.straddle_price_drop_trigger ?? ''}"
    step="0.05">
</div>
                    <div class="form-group"><label>Price Drop % SQF</label>
                        <input type="number" name="straddle_price_drop_pct_sqf" value="${cfg.straddle_price_drop_pct_sqf || 0}" step="1" min="0" max="100"></div>


                </div>
                ${!isPending ? `
                <div style="margin-top: 15px; color: #ffc107; font-size: 0.85em; text-align: center;">
                    ⚠️ Note: Changing "Size" on an active trade only updates the configuration for future automated actions (like rolling). It does not automatically scale your current live open positions.
                </div>` : ''}
                <div class="form-actions">
                    <button type="submit" class="btn btn-primary">Update Config</button>
                    <button type="button" class="btn btn-secondary close-button">Cancel</button>
                </div>
            </form>
        </div>`;

    modalOverlay.appendChild(modalContent);
    document.body.appendChild(modalOverlay);

    document.getElementById('modify-config-form')
        .addEventListener('submit', e => updateTradeConfig(e, tradeUid));
    modalOverlay.querySelectorAll('.close-button')
        .forEach(btn => btn.addEventListener('click', () => modalOverlay.remove()));
    modalOverlay.addEventListener('click', e => {
        if (e.target === modalOverlay) modalOverlay.remove();
    });
}

async function updateTradeConfig(event, tradeUid) {
    event.preventDefault();
    const formData  = new FormData(document.getElementById('modify-config-form'));
    const newConfig = {};
    for (const [key, value] of formData.entries())
        newConfig[key] = (!isNaN(value) && value.trim() !== '') ? Number(value) : value;

    if (!confirm('Update live config for this trade? Monitors will restart.')) return;

    try {
        showNotification(`Updating config for ${tradeUid}...`, 'info');
        const response = await fetch(`/api/straddle/update-config/${tradeUid}`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(newConfig)
        });
        const data = await response.json();
        if (data.success) {
            showNotification(`✅ Config updated: ${data.message}`, 'success');
            document.getElementById('modify-config-modal-overlay')?.remove();
            setTimeout(() => fetchStraddles(true), 250);
        } else {
            showNotification(`❌ Update failed: ${data.error || data.detail}`, 'error');
        }
    } catch (error) {
        showNotification(`❌ Error: ${error.message}`, 'error');
    }
}
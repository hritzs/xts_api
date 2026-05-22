// ════════════════════════════════════════════════════════════════════════════
// option_chain.js
// Rule: never self-initialises. init.js calls loadOptionChain().
// ════════════════════════════════════════════════════════════════════════════

window._currentChainSymbol = null;
window._chainATMTokens     = null;   // { atm, ce_token, pe_token, gap, lot_size, ce_ltp_snap, pe_ltp_snap }
window._lastPrices         = {};     // token(string) → ltp
window._lastChainData      = null;   // full chain snapshot
window._chainRendering     = false;  // render lock


// ── Inject option-chain styles ────────────────────────────────────────────────
(function () {
    if (document.getElementById('option-chain-styles')) return;
    const s = document.createElement('style');
    s.id = 'option-chain-styles';
    s.textContent = `
        /* ── Table base ────────────────────────────────────────── */
        #option-chain-container table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
            font-family: 'Roboto Mono', monospace;
        }

        /* ── Headers ─────────────────────────────────────────────*/
        #option-chain-container thead th {
            padding: 8px 10px;
            text-align: center;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            border-bottom: 2px solid #333;
            position: sticky;
            top: 0;
            z-index: 2;
            background: #1a1a2e;
            color: #888;
        }
        /* CE headers: cols 1-4 */
        #option-chain-container thead th:nth-child(-n+4) {
            background: #162416;
            color: #6ea86e;
        }
        /* PE headers: cols 6-9 */
        #option-chain-container thead th:nth-child(n+6) {
            background: #241616;
            color: #a86e6e;
        }
        /* Strike header: col 5 */
        #option-chain-container thead th:nth-child(5) {
            background: #16162a;
            color: #7090c0;
        }

        /* ── Body rows ───────────────────────────────────────────*/
        #option-chain-container tbody tr {
            border-bottom: 1px solid #222;
            transition: background 0.1s;
        }
        #option-chain-container tbody tr:hover td {
            background: #2a2a3e !important;
        }

        /* CE cells: cols 1-4 */
        #option-chain-container tbody td:nth-child(-n+4) {
            background: #131f13;
            color: #b8d4b8;
            text-align: right;
            padding: 6px 10px;
        }
        /* PE cells: cols 6-9 */
        #option-chain-container tbody td:nth-child(n+6) {
            background: #1f1313;
            color: #d4b8b8;
            text-align: left;
            padding: 6px 10px;
        }
        /* Strike cell: col 5 */
        #option-chain-container tbody td:nth-child(5) {
            background: #161626;
            color: #8090b0;
            font-weight: 700;
            text-align: center;
            padding: 6px 12px;
            border-left:  1px solid #2a2a3a;
            border-right: 1px solid #2a2a3a;
            font-size: 13px;
        }
        /* LTP cells bold */
        #option-chain-container tbody td:nth-child(4),
        #option-chain-container tbody td:nth-child(6) {
            font-weight: 600;
        }

        /* ── ATM row — subtle amber, NEVER white ─────────────────*/
        #option-chain-container tbody tr.atm-row td:nth-child(-n+4) {
            background: #1e2a14 !important;
        }
        #option-chain-container tbody tr.atm-row td:nth-child(5) {
            background: #2a2210 !important;
            color: #ffc107 !important;
            font-size: 14px !important;
        }
        #option-chain-container tbody tr.atm-row td:nth-child(n+6) {
            background: #2a1414 !important;
        }
        #option-chain-container tbody tr.atm-row {
            border-top:    1px solid rgba(255, 193, 7, 0.35);
            border-bottom: 1px solid rgba(255, 193, 7, 0.35);
        }

        /* ── Price flash ─────────────────────────────────────────*/
        .chain-flash-up   {
            background: rgba(40, 167, 69, 0.5) !important;
            transition: background 0.08s;
        }
        .chain-flash-down {
            background: rgba(220, 53, 69, 0.5) !important;
            transition: background 0.08s;
        }

        /* ── IV colour bands ─────────────────────────────────────*/
        .iv-low  { color: #6ec96e !important; }
        .iv-mid  { color: #ffc107 !important; }
        .iv-high { color: #e07070 !important; }

        /* ── Theta colour bands ──────────────────────────────────*/
        .theta-low  { color: #6ec96e !important; }
        .theta-mid  { color: #ffc107 !important; }
        .theta-high { color: #e07070 !important; }

        /* ── Chain header info ───────────────────────────────────*/
        .chain-header-info {
            display: flex;
            gap: 18px;
            align-items: center;
            font-size: 13px;
            color: #888;
            flex-wrap: wrap;
        }
        .chain-header-info span { white-space: nowrap; }
        #chain-spot-value   { color: #7fbfff; font-weight: 600; }
        #chain-synfut-value { color: #b07fff; font-weight: 600; }
        #chain-atm-value    { color: #ffc107; font-weight: 600; }
        #chain-expiry-value { color: #888; }
        #chain-var-value    { color: #ff9f43; font-weight: 600; }
    `;
    document.head.appendChild(s);
})();



// ════════════════════════════════════════════════════════════════════════════
// STRADDLE PREMIUM
// Fills: ATM Strike / Straddle Premium / Total Premium
// ════════════════════════════════════════════════════════════════════════════

function updateStraddlePremium() {
    if (!window._chainATMTokens) return;

    const {
        atm,
        ce_token, pe_token,
        lot_size,
        ce_ltp_snap, pe_ltp_snap
    } = window._chainATMTokens;

    const prices = window._lastPrices || {};

    const ce_ltp = prices[String(ce_token)] || ce_ltp_snap || 0;
    const pe_ltp = prices[String(pe_token)] || pe_ltp_snap || 0;

    const straddle = ce_ltp + pe_ltp;
    const lots     = parseInt(document.getElementById('lots')?.value) || 1;
    const total    = straddle * lots * (lot_size || 50);

    const atmEl  = document.getElementById('atm-strike');
    const premEl = document.getElementById('straddle-premium');
    const totEl  = document.getElementById('total-premium');

    if (atmEl)  atmEl.value  = atm ?? '--';
    if (premEl) premEl.value = straddle > 0
        ? `₹${straddle.toFixed(2)}`
        : '--';
    if (totEl)  totEl.value  = total > 0
        ? `₹${total.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
        : '--';
}



// ════════════════════════════════════════════════════════════════════════════
// ATM CENTERING
// ════════════════════════════════════════════════════════════════════════════

function scrollAtmToCenter() {
    const div    = document.getElementById('option-chain-container');
    const atmRow = div?.querySelector('tr.atm-row');
    if (!div || !atmRow) return;

    // Walk up to find the actual scrollable ancestor
    let container = div;
    let el = div.parentElement;
    while (el && el !== document.body) {
        const overflow = getComputedStyle(el).overflowY;
        if (overflow === 'auto' || overflow === 'scroll') { container = el; break; }
        el = el.parentElement;
    }

    // scrollIntoView works well when the container is the viewport scroller;
    // manual scrollTop is more reliable for inner scroll boxes.
    if (container === document.body || container === div) {
        atmRow.scrollIntoView({ block: 'center', behavior: 'smooth' });
    } else {
        const containerMid = container.clientHeight / 2;
        const rowTop       = atmRow.offsetTop - container.offsetTop;
        const rowMid       = atmRow.offsetHeight / 2;
        container.scrollTo({ top: rowTop - containerMid + rowMid, behavior: 'smooth' });
    }
}



// ════════════════════════════════════════════════════════════════════════════
// ENTRY POINTS
// ════════════════════════════════════════════════════════════════════════════

function loadOptionChain() {
    const sym = document.getElementById('symbol')?.value || 'NIFTY';
    fetchOptionChain(sym);
}

document.getElementById('btn-fetch-chain')?.addEventListener('click', () => {
    const sym = document.getElementById('symbol')?.value || 'NIFTY';
    const div = document.getElementById('option-chain-container');
    if (div) div.innerHTML = '<div class="placeholder">🔄 Fetching option chain...</div>';
    fetchOptionChain(sym);
});



// ════════════════════════════════════════════════════════════════════════════
// LIVE HEADER UPDATE  (in-place, no table rebuild)
// ════════════════════════════════════════════════════════════════════════════

function handleChainHeaderUpdate(msg) {
    const { symbol, spot, syn_fut, atm, expiry, var_pts, var_pct } = msg;

    const displayed = window._currentChainSymbol || '';
    if (displayed && symbol &&
        symbol.toUpperCase() !== displayed.toUpperCase()) return;

    const spotEl   = document.getElementById('chain-spot-value');
    const synFutEl = document.getElementById('chain-synfut-value');
    const atmEl    = document.getElementById('chain-atm-value');
    const expiryEl = document.getElementById('chain-expiry-value');
    const varEl    = document.getElementById('chain-var-value');

    if (spotEl   && spot    > 0) spotEl.textContent   = `₹${spot.toFixed(2)}`;
    if (synFutEl && syn_fut > 0) synFutEl.textContent = `₹${syn_fut.toFixed(2)}`;
    if (atmEl    && atm)         atmEl.textContent     = atm;
    if (expiryEl && expiry)      expiryEl.textContent  = expiry;

    // VaR — update only when backend provides it
    if (varEl && var_pts > 0) {
        varEl.textContent = var_pct
            ? `±${var_pts.toFixed(1)} pts (${var_pct.toFixed(2)}%)`
            : `±${var_pts.toFixed(1)} pts`;
    }

    const atmInput = document.getElementById('atm-strike');
    if (atmInput && atm) atmInput.value = atm;

    // Re-highlight ATM row and re-center if strike shifted
    if (atm) {
        const prevAtm = window._chainATMTokens?.atm;

        document.querySelectorAll(
            '#option-chain-container tr[data-strike]'
        ).forEach(row => {
            row.style.background = '';
            row.style.fontWeight = '';
            const s = parseInt(row.getAttribute('data-strike'));
            s === atm
                ? row.classList.add('atm-row')
                : row.classList.remove('atm-row');
        });

        // Re-center only when ATM strike actually changes
        if (atm !== prevAtm) {
            requestAnimationFrame(scrollAtmToCenter);
        }

        // Refresh ATM token cache if strike shifted
        if (window._chainATMTokens && atm !== prevAtm) {
            const chainData = window._lastChainData;
            if (chainData?.chain) {
                const newAtmRow = chainData.chain.find(r => r.strike === atm);
                if (newAtmRow?.ce_token && newAtmRow?.pe_token) {
                    window._chainATMTokens = {
                        ...window._chainATMTokens,
                        atm:         atm,
                        ce_token:    String(newAtmRow.ce_token),
                        pe_token:    String(newAtmRow.pe_token),
                        ce_ltp_snap: newAtmRow.ce_ltp || 0,
                        pe_ltp_snap: newAtmRow.pe_ltp || 0,
                    };
                    updateStraddlePremium();
                }
            }
        }
    }
}



// ════════════════════════════════════════════════════════════════════════════
// LIVE PRICE UPDATE  (flash + Syn.Fut recompute + premium refresh)
// ════════════════════════════════════════════════════════════════════════════

function handleChainPriceUpdate(prices) {
    let atmTouched = false;

    Object.entries(prices).forEach(([token, ltp]) => {
        const tokenStr = String(token);
        window._lastPrices[tokenStr] = ltp;

        if (window._chainATMTokens) {
            const { ce_token, pe_token } = window._chainATMTokens;
            if (tokenStr === String(ce_token) || tokenStr === String(pe_token))
                atmTouched = true;
        }

        document.querySelectorAll(
            `#option-chain-container [data-token="${tokenStr}"]`
        ).forEach(cell => {
            const span = cell.querySelector('span');
            if (!span) return;

            const prev = parseFloat(cell.getAttribute('data-prev-price') || ltp);
            const val  = parseFloat(ltp);
            span.textContent = `₹${val.toFixed(2)}`;

            if (val > prev) {
                cell.classList.add('chain-flash-up');
                setTimeout(() => cell.classList.remove('chain-flash-up'), 600);
            } else if (val < prev) {
                cell.classList.add('chain-flash-down');
                setTimeout(() => cell.classList.remove('chain-flash-down'), 600);
            }
            cell.setAttribute('data-prev-price', ltp);
        });
    });

    // Recompute Syn.Fut client-side
    if (window._chainATMTokens) {
        const { atm, ce_token, pe_token, gap } = window._chainATMTokens;
        const ce_p = prices[ce_token]
                  ?? prices[parseInt(ce_token)]
                  ?? window._lastPrices[String(ce_token)];
        const pe_p = prices[pe_token]
                  ?? prices[parseInt(pe_token)]
                  ?? window._lastPrices[String(pe_token)];

        if (ce_p > 0 && pe_p > 0) {
            const syn = atm + ce_p - pe_p;
            if (Math.abs(syn - atm) <= gap * 2) {
                const el = document.getElementById('chain-synfut-value');
                if (el) el.textContent = `₹${syn.toFixed(2)}`;
            }
        }
    }

    if (atmTouched) updateStraddlePremium();
}



// ════════════════════════════════════════════════════════════════════════════
// FETCH
// ════════════════════════════════════════════════════════════════════════════

async function fetchOptionChain(symbol = null) {
    if (window._chainRendering) return;

    const sym = symbol
        || document.getElementById('symbol')?.value
        || 'NIFTY';

    const div = document.getElementById('option-chain-container');

    try {
        const res  = await fetch(`/api/option-chain/${sym}`);
        const data = await res.json();
        if (data.success && data.data) {
            displayOptionChain(data.data);

            // --- MODIFIED: Re-round quantity fields now that lot size is known ---
            if (typeof roundQuantity === 'function') {
                roundQuantity('lots');
                roundQuantity('config-size');
                roundQuantity('manual_lots_per_call');
                roundQuantity('auto_lots_per_call');
            }
        } else {
            if (div) div.innerHTML =
                `<div class="placeholder">❌ ${data.error || 'Chain unavailable'}</div>`;
        }
    } catch (err) {
        console.error('Option chain fetch error:', err);
        if (div) div.innerHTML =
            `<div class="placeholder">❌ ${err.message}</div>`;
    }
}



// ════════════════════════════════════════════════════════════════════════════
// RENDER  — full rebuild on fresh fetch only
// ════════════════════════════════════════════════════════════════════════════

function displayOptionChain(chainData) {
    const div = document.getElementById('option-chain-container');
    if (!div) return;

    window._chainRendering = true;
    window._lastChainData  = chainData;

    if (chainData.symbol)
        window._currentChainSymbol = chainData.symbol.toUpperCase();

    if (chainData.fut_token != null)
        div.setAttribute('data-fut-token', chainData.fut_token);

    // Cache ATM tokens
    const atmRow = (chainData.chain || []).find(r => r.strike === chainData.atm);
    if (atmRow?.ce_token && atmRow?.pe_token) {
        window._chainATMTokens = {
            atm:         chainData.atm,
            ce_token:    String(atmRow.ce_token),
            pe_token:    String(atmRow.pe_token),
            gap:         chainData.gap         ?? 50,
            lot_size:    atmRow.ce_lot_size    ?? chainData.lot_size ?? 50,
            ce_ltp_snap: atmRow.ce_ltp         || 0,
            pe_ltp_snap: atmRow.pe_ltp         || 0,
        };
    }

    // Seed _lastPrices from snapshot
    (chainData.chain || []).forEach(row => {
        if (row.ce_token != null && row.ce_ltp)
            window._lastPrices[String(row.ce_token)] = row.ce_ltp;
        if (row.pe_token != null && row.pe_ltp)
            window._lastPrices[String(row.pe_token)] = row.pe_ltp;
    });
    if (chainData.fut_token && chainData.fut_ltp)
        window._lastPrices[String(chainData.fut_token)] = chainData.fut_ltp;

    // Compute initial Syn.Fut
    let synFutVal = null;
    if (atmRow?.ce_ltp > 0 && atmRow?.pe_ltp > 0)
        synFutVal = chainData.atm + atmRow.ce_ltp - atmRow.pe_ltp;

    // ── Header ────────────────────────────────────────────────────────────
    const spotEl   = document.getElementById('chain-spot-value');
    const synFutEl = document.getElementById('chain-synfut-value');
    const atmEl    = document.getElementById('chain-atm-value');
    const expiryEl = document.getElementById('chain-expiry-value');
    const varEl    = document.getElementById('chain-var-value');

    if (spotEl)   spotEl.textContent   = chainData.fut_ltp  ? `₹${chainData.fut_ltp.toFixed(2)}` : '--';
    if (synFutEl) synFutEl.textContent = synFutVal           ? `₹${synFutVal.toFixed(2)}`         : '--';
    if (atmEl)    atmEl.textContent    = chainData.atm       ?? '--';
    if (expiryEl) expiryEl.textContent = chainData.expiry    ?? '--';
    if (varEl)    varEl.textContent    = '--';   // backend will fill on next Greeks tick

    // ── Table ─────────────────────────────────────────────────────────────
    let html = `
        <table>
            <thead>
                <tr>
                    <th>Theta</th>
                    <th>Vega</th>
                    <th>IV %</th>
                    <th>CE LTP</th>
                    <th>Strike</th>
                    <th>PE LTP</th>
                    <th>IV %</th>
                    <th>Vega</th>
                    <th>Theta</th>
                </tr>
            </thead>
            <tbody>`;

    (chainData.chain || []).forEach(row => {
        const isATM  = row.strike === chainData.atm;
        const atmCls = isATM ? 'atm-row' : '';

        const ceAttr = row.ce_token != null ? `data-token="${row.ce_token}"` : '';
        const peAttr = row.pe_token != null ? `data-token="${row.pe_token}"` : '';

        html += `
            <tr class="${atmCls}" data-strike="${row.strike}">
                <td class="${_thetaCls(row.ce_theta)}">${_f2(row.ce_theta)}</td>
                <td>${_f2(row.ce_vega)}</td>
                <td class="${_ivCls(row.ce_iv)}">${_f2(row.ce_iv)}%</td>
                <td ${ceAttr} data-prev-price="${row.ce_ltp ?? 0}">
                    <span>₹${row.ce_ltp?.toFixed(2) ?? '--'}</span>
                </td>
                <td>${row.strike}</td>
                <td ${peAttr} data-prev-price="${row.pe_ltp ?? 0}">
                    <span>₹${row.pe_ltp?.toFixed(2) ?? '--'}</span>
                </td>
                <td class="${_ivCls(row.pe_iv)}">${_f2(row.pe_iv)}%</td>
                <td>${_f2(row.pe_vega)}</td>
                <td class="${_thetaCls(row.pe_theta)}">${_f2(row.pe_theta)}</td>
            </tr>`;
    });

    html += '</tbody></table>';
    div.innerHTML = html;

    updateStraddlePremium();

    // Center ATM row after paint
    requestAnimationFrame(() => {
        scrollAtmToCenter();
        window._chainRendering = false;
    });
}



// ════════════════════════════════════════════════════════════════════════════
// COLOUR HELPERS
// ════════════════════════════════════════════════════════════════════════════

function _f2(v) {
    return (v != null && !isNaN(v)) ? parseFloat(v).toFixed(2) : '--';
}

function _ivCls(iv) {
    if (iv == null) return '';
    if (iv < 14)    return 'iv-low';
    if (iv < 20)    return 'iv-mid';
    return 'iv-high';
}

function _thetaCls(theta) {
    if (theta == null) return '';
    if (theta < -1)    return 'theta-high';
    if (theta < -0.3)  return 'theta-mid';
    return 'theta-low';
}
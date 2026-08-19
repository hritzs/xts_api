// ════════════════════════════════════════════════════════════════════════════
// option_chain.js
// Rule: never self-initialises. init.js calls loadOptionChain().
// Rule: fetch builds full table; websocket patches visible snapshot in place.
// Rule: ignore stale fetch responses and stale websocket chain snapshots.
// Rule: option_chain_update is the only live writer for option-chain state.
// ════════════════════════════════════════════════════════════════════════════

window._currentChainSymbol = null;
window._chainATMTokens = null;        // { atm, ce_token, pe_token, gap, lot_size, ce_ltp_snap, pe_ltp_snap }
window._lastPrices = {};              // token(string) → ltp
window._lastChainData = null;         // full visible chain snapshot
window._chainRendering = false;       // render lock

window._chainSnapshotVersion = 0;     // monotonic UI version
window._chainLastPublishedAt = null;  // ISO timestamp from backend
window._chainFetchSeq = 0;            // latest fetch request id
window._chainAppliedFetchSeq = 0;     // latest applied fetch request id

// ── Inject option-chain styles ──────────────────────────────────────────────
(function () {
    if (document.getElementById('option-chain-styles')) return;

    const s = document.createElement('style');
    s.id = 'option-chain-styles';
    s.textContent = `
        #option-chain-container table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
            font-family: 'Roboto Mono', monospace;
        }

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

        #option-chain-container thead th:nth-child(-n+6) {
            background: #162416;
            color: #6ea86e;
        }

        #option-chain-container thead th:nth-child(n+8) {
            background: #241616;
            color: #a86e6e;
        }

        #option-chain-container thead th:nth-child(7) {
            background: #16162a;
            color: #7090c0;
        }

        #option-chain-container tbody td.quote-cell {
            font-size: 12px;
            color: #aaa;
        }

        #option-chain-container tbody td.ce-bid,
        #option-chain-container tbody td.ce-ask {
            text-align: right;
        }

        #option-chain-container tbody td.pe-bid,
        #option-chain-container tbody td.pe-ask {
            text-align: left;
        }

        #option-chain-container tbody tr {
            border-bottom: 1px solid #222;
            transition: background 0.1s;
        }

        #option-chain-container tbody tr:hover td {
            background: #2a2a3e !important;
        }

        #option-chain-container tbody td:nth-child(-n+6) {
            background: #131f13;
            color: #b8d4b8;
            text-align: right;
            padding: 6px 10px;
        }

        #option-chain-container tbody td:nth-child(n+8) {
            background: #1f1313;
            color: #d4b8b8;
            text-align: left;
            padding: 6px 10px;
        }

        #option-chain-container tbody td:nth-child(7) {
            background: #161626;
            color: #8090b0;
            font-weight: 700;
            text-align: center;
            padding: 6px 12px;
            border-left: 1px solid #2a2a3a;
            border-right: 1px solid #2a2a3a;
            font-size: 13px;
        }

        #option-chain-container tbody td:nth-child(6),
        #option-chain-container tbody td:nth-child(8) {
            font-weight: 600;
        }

        #option-chain-container tbody tr.atm-row td:nth-child(-n+6) {
            background: #1e2a14 !important;
        }

        #option-chain-container tbody tr.atm-row td:nth-child(7) {
            background: #2a2210 !important;
            color: #ffc107 !important;
            font-size: 14px !important;
        }

        #option-chain-container tbody tr.atm-row td:nth-child(n+8) {
            background: #2a1414 !important;
        }

        #option-chain-container tbody tr.atm-row {
            border-top: 1px solid rgba(255, 193, 7, 0.35);
            border-bottom: 1px solid rgba(255, 193, 7, 0.35);
        }

        .chain-flash-up {
            background: rgba(40, 167, 69, 0.5) !important;
            transition: background 0.08s;
        }

        .chain-flash-down {
            background: rgba(220, 53, 69, 0.5) !important;
            transition: background 0.08s;
        }

        .iv-low  { color: #6ec96e !important; }
        .iv-mid  { color: #ffc107 !important; }
        .iv-high { color: #e07070 !important; }

        .theta-low  { color: #6ec96e !important; }
        .theta-mid  { color: #ffc107 !important; }
        .theta-high { color: #e07070 !important; }

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

function _mergeRowsByStrike(currentRows = [], incomingRows = []) {
    const byStrike = new Map();

    currentRows.forEach(r => byStrike.set(String(r.strike), { ...r }));
    incomingRows.forEach(r => {
        const k = String(r.strike);
        byStrike.set(k, { ...(byStrike.get(k) || {}), ...r });
    });

    return Array.from(byStrike.values()).sort((a, b) => Number(a.strike) - Number(b.strike));
}

// ════════════════════════════════════════════════════════════════════════════
// HELPERS
// ════════════════════════════════════════════════════════════════════════════

function _symUpper(v) {
    return String(v || '').toUpperCase();
}

function _sameSym(a, b) {
    return _symUpper(a) === _symUpper(b);
}

function _num(v, d = 0) {
    const n = Number(v);
    return Number.isFinite(n) ? n : d;
}

function _isoMs(iso) {
    if (!iso) return 0;
    const t = Date.parse(iso);
    return Number.isFinite(t) ? t : 0;
}

function _isNewerPublishedAt(nextIso, currIso) {
    const a = _isoMs(nextIso);
    const b = _isoMs(currIso);
    if (!a && !b) return false;
    if (a && !b) return true;
    return a > b;
}

function _adoptSnapshotVersion(incoming) {
    window._chainSnapshotVersion += 1;
    if (incoming?.published_at) {
        window._chainLastPublishedAt = incoming.published_at;
    }
}

function _snapshotIsStale(incoming, source = 'unknown') {
    if (!incoming) return true;

    const selected = document.getElementById('symbol')?.value || window._currentChainSymbol || '';
    if (selected && incoming.symbol && !_sameSym(selected, incoming.symbol)) {
        console.debug(`[CHAIN-${source}] ignore: symbol mismatch`, {
            selected,
            incoming: incoming.symbol
        });
        return true;
    }

    const incomingTs = incoming.published_at;
    const currentTs = window._chainLastPublishedAt;

    if (incomingTs && currentTs && _isoMs(incomingTs) < _isoMs(currentTs)) {
        console.warn(`[CHAIN-${source}] stale snapshot ignored`, {
            incomingTs,
            currentTs,
            symbol: incoming.symbol
        });
        return true;
    }

    return false;
}

function _seedLastPricesFromSnapshot(chainData) {
    (chainData?.chain || []).forEach(row => {
        if (row.ce_token != null && row.ce_ltp != null) {
            window._lastPrices[String(row.ce_token)] = Number(row.ce_ltp);
        }
        if (row.pe_token != null && row.pe_ltp != null) {
            window._lastPrices[String(row.pe_token)] = Number(row.pe_ltp);
        }
    });

    if (chainData?.fut_token != null && chainData?.fut_ltp != null) {
        window._lastPrices[String(chainData.fut_token)] = Number(chainData.fut_ltp);
    }
}

function _rebuildATMTokensFromData(chainData) {
    const atmRow = (chainData?.chain || []).find(r => Number(r.strike) === Number(chainData.atm));
    if (atmRow?.ce_token && atmRow?.pe_token) {
        window._chainATMTokens = {
            atm: Number(chainData.atm),
            ce_token: String(atmRow.ce_token),
            pe_token: String(atmRow.pe_token),
            gap: Number(chainData.gap ?? 50),
            lot_size: Number(atmRow.ce_lot_size ?? chainData.lot_size ?? 50),
            ce_ltp_snap: Number(atmRow.ce_ltp || 0),
            pe_ltp_snap: Number(atmRow.pe_ltp || 0),
        };
    }
}

function _computeDisplayedSynFut(chainData) {
    const atm = Number(chainData?.atm ?? 0);
    const atmRow = (chainData?.chain || []).find(r => Number(r.strike) === atm);

    if (atm > 0 && atmRow?.ce_ltp > 0 && atmRow?.pe_ltp > 0) {
        return atm + Number(atmRow.ce_ltp) - Number(atmRow.pe_ltp);
    }

    return Number(chainData?.synthetic_spot ?? 0) || null;
}

function _patchHeaderFromChainData(chainData) {
    const spotEl = document.getElementById('chain-spot-value');
    const synFutEl = document.getElementById('chain-synfut-value');
    const atmEl = document.getElementById('chain-atm-value');
    const expiryEl = document.getElementById('chain-expiry-value');
    const varEl = document.getElementById('chain-var-value');

    const synFutVal = _computeDisplayedSynFut(chainData);

    if (spotEl) spotEl.textContent = chainData?.synthetic_spot ? ('\u20B9' + Number(chainData.synthetic_spot).toFixed(2)) : '--';
    if (synFutEl) synFutEl.textContent = synFutVal ? `₹${Number(synFutVal).toFixed(2)}` : '--';
    if (atmEl) atmEl.textContent = chainData?.atm ?? '--';
    if (expiryEl) expiryEl.textContent = chainData?.expiry ?? '--';

    if (varEl) {
        if (_num(chainData?.var_pts) > 0) {
            varEl.textContent = chainData?.var_pct
                ? `±${Number(chainData.var_pts).toFixed(1)} pts (${Number(chainData.var_pct).toFixed(2)}%)`
                : `±${Number(chainData.var_pts).toFixed(1)} pts`;
        } else {
            varEl.textContent = '--';
        }
    }

    const atmInput = document.getElementById('atm-strike');
    if (atmInput && chainData?.atm) atmInput.value = chainData.atm;
}

function _mergeIncomingChainData(incoming) {
    const current = window._lastChainData || {};
    const merged = { ...current, ...incoming };

    if (Array.isArray(incoming.chain)) {
        merged.chain = incoming.is_full_snapshot === true
            ? incoming.chain
            : _mergeRowsByStrike(current.chain || [], incoming.chain);
    }

    return merged;
}

function _patchSingleChainRow(row) {
    const tr = document.querySelector(`#option-chain-container tr[data-strike="${row.strike}"]`);
    if (!tr) return;

    const tds = tr.querySelectorAll('td');
    if (!tds || tds.length < 13) return;

    tds[0].textContent = _f2(row.ce_bid ?? row.ce_bid_price);
    tds[1].textContent = _f2(row.ce_ask ?? row.ce_ask_price);

    tds[2].textContent = _f2(row.ce_theta);
    tds[2].className = _thetaCls(row.ce_theta);

    tds[3].textContent = _f2(row.ce_vega);

    tds[4].textContent = `${_f2(row.ce_iv)}%`;
    tds[4].className = _ivCls(row.ce_iv);

    const ceCell = tds[5];
    const ceSpan = ceCell.querySelector('span');
    if (ceSpan) ceSpan.textContent = row.ce_ltp != null ? `₹${Number(row.ce_ltp).toFixed(2)}` : '--';
    if (row.ce_token != null) ceCell.setAttribute('data-token', String(row.ce_token));
    ceCell.setAttribute('data-prev-price', row.ce_ltp ?? 0);

    tds[6].textContent = row.strike;

    const peCell = tds[7];
    const peSpan = peCell.querySelector('span');
    if (peSpan) peSpan.textContent = row.pe_ltp != null ? `₹${Number(row.pe_ltp).toFixed(2)}` : '--';
    if (row.pe_token != null) peCell.setAttribute('data-token', String(row.pe_token));
    peCell.setAttribute('data-prev-price', row.pe_ltp ?? 0);

    tds[8].textContent = `${_f2(row.pe_iv)}%`;
    tds[8].className = _ivCls(row.pe_iv);

    tds[9].textContent = _f2(row.pe_vega);

    tds[10].textContent = _f2(row.pe_theta);
    tds[10].className = _thetaCls(row.pe_theta);

    tds[11].textContent = _f2(row.pe_bid ?? row.pe_bid_price);
    tds[12].textContent = _f2(row.pe_ask ?? row.pe_ask_price);
}

function _patchVisibleTableRows(chainData) {
    if (!Array.isArray(chainData?.chain)) return;

    chainData.chain.forEach(_patchSingleChainRow);

    const newAtm = Number(chainData.atm ?? 0);
    let atmChanged = false;

    document.querySelectorAll('#option-chain-container tr[data-strike]').forEach(row => {
        const strike = Number(row.getAttribute('data-strike'));
        const wasAtm = row.classList.contains('atm-row');
        const isAtm = strike === newAtm;

        row.style.background = '';
        row.style.fontWeight = '';

        if (isAtm) row.classList.add('atm-row');
        else row.classList.remove('atm-row');

        if (wasAtm !== isAtm && isAtm) {
            atmChanged = true;
        }
    });

    if (atmChanged) {
        requestAnimationFrame(scrollAtmToCenter);
    }
}

function _debugRow24150(tag, chainData) {
    if (_symUpper(chainData?.symbol) !== 'NIFTY') return;
    const row = (chainData?.chain || []).find(r => Number(r.strike) === 24150);
    if (!row) return;

    console.log(tag, {
        published_at: chainData?.published_at,
        fut_ltp: chainData?.fut_ltp,
        synthetic_spot: chainData?.synthetic_spot,
        atm: chainData?.atm,
        row24150: {
            ce_bid: row.ce_bid,
            ce_ask: row.ce_ask,
            ce_ltp: row.ce_ltp,
            pe_ltp: row.pe_ltp,
            pe_bid: row.pe_bid,
            pe_ask: row.pe_ask,
            ce_iv: row.ce_iv,
            pe_iv: row.pe_iv,
            ce_theta: row.ce_theta,
            pe_theta: row.pe_theta,
            ce_vega: row.ce_vega,
            pe_vega: row.pe_vega,
        }
    });
}

// ════════════════════════════════════════════════════════════════════════════
// STRADDLE PREMIUM
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
    const lots = parseInt(document.getElementById('lots')?.value, 10) || 1;
    const total = straddle * lots * (lot_size || 50);

    const atmEl = document.getElementById('atm-strike');
    const premEl = document.getElementById('straddle-premium');
    const totEl = document.getElementById('total-premium');

    if (atmEl) atmEl.value = atm ?? '--';
    if (premEl) premEl.value = straddle > 0 ? `₹${straddle.toFixed(2)}` : '--';
    if (totEl) {
        totEl.value = total > 0
            ? `₹${total.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
            : '--';
    }
}

// ════════════════════════════════════════════════════════════════════════════
// ATM CENTERING
// ════════════════════════════════════════════════════════════════════════════

function scrollAtmToCenter() {
    const div = document.getElementById('option-chain-container');
    const atmRow = div?.querySelector('tr.atm-row');
    if (!div || !atmRow) return;

    let container = div;
    let el = div.parentElement;
    while (el && el !== document.body) {
        const overflow = getComputedStyle(el).overflowY;
        if (overflow === 'auto' || overflow === 'scroll') {
            container = el;
            break;
        }
        el = el.parentElement;
    }

    if (container === document.body || container === div) {
        atmRow.scrollIntoView({ block: 'center', behavior: 'smooth' });
    } else {
        const containerMid = container.clientHeight / 2;
        const rowTop = atmRow.offsetTop - container.offsetTop;
        const rowMid = atmRow.offsetHeight / 2;
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
// LEGACY LIVE MUTATORS
// Retained only for backward compatibility; no longer used by websocket.js
// for the option-chain UI single-source-of-truth path.
// ════════════════════════════════════════════════════════════════════════════

function handleChainHeaderUpdate(msg) {
    const { symbol, spot, syn_fut, atm, expiry, var_pts, var_pct } = msg || {};

    const displayed = window._currentChainSymbol || '';
    if (displayed && symbol && !_sameSym(symbol, displayed)) return;

    const spotEl = document.getElementById('chain-spot-value');
    const synFutEl = document.getElementById('chain-synfut-value');
    const atmEl = document.getElementById('chain-atm-value');
    const expiryEl = document.getElementById('chain-expiry-value');
    const varEl = document.getElementById('chain-var-value');

    if (spotEl && spot > 0) spotEl.textContent = `₹${Number(spot).toFixed(2)}`;
    if (synFutEl && syn_fut > 0) synFutEl.textContent = `₹${Number(syn_fut).toFixed(2)}`;
    if (atmEl && atm) atmEl.textContent = atm;
    if (expiryEl && expiry) expiryEl.textContent = expiry;

    if (varEl) {
        if (var_pts > 0) {
            varEl.textContent = var_pct
                ? `±${Number(var_pts).toFixed(1)} pts (${Number(var_pct).toFixed(2)}%)`
                : `±${Number(var_pts).toFixed(1)} pts`;
        } else {
            varEl.textContent = '--';
        }
    }

    const atmInput = document.getElementById('atm-strike');
    if (atmInput && atm) atmInput.value = atm;

    if (atm) {
        const prevAtm = window._chainATMTokens?.atm;

        document.querySelectorAll('#option-chain-container tr[data-strike]').forEach(row => {
            row.style.background = '';
            row.style.fontWeight = '';
            const s = parseInt(row.getAttribute('data-strike'), 10);
            s === Number(atm) ? row.classList.add('atm-row') : row.classList.remove('atm-row');
        });

        if (Number(atm) !== Number(prevAtm)) {
            requestAnimationFrame(scrollAtmToCenter);
        }

        if (window._chainATMTokens && Number(atm) !== Number(prevAtm)) {
            const chainData = window._lastChainData;
            if (chainData?.chain) {
                const newAtmRow = chainData.chain.find(r => Number(r.strike) === Number(atm));
                if (newAtmRow?.ce_token && newAtmRow?.pe_token) {
                    window._chainATMTokens = {
                        ...window._chainATMTokens,
                        atm: Number(atm),
                        ce_token: String(newAtmRow.ce_token),
                        pe_token: String(newAtmRow.pe_token),
                        ce_ltp_snap: newAtmRow.ce_ltp || 0,
                        pe_ltp_snap: newAtmRow.pe_ltp || 0,
                    };
                    updateStraddlePremium();
                }
            }
        }
    }
}

function applyChainQuotePatch(quotesByToken) {
    if (!window._lastChainData?.chain || !quotesByToken) return;

    const tokenMap = {};
    for (const [token, q] of Object.entries(quotesByToken)) {
        tokenMap[String(token)] = q;
    }

    window._lastChainData.chain = window._lastChainData.chain.map(row => {
        const ce = tokenMap[String(row.ce_token)];
        const pe = tokenMap[String(row.pe_token)];

        return {
            ...row,
            ...(ce ? {
                ce_bid: ce.bid_price,
                ce_ask: ce.ask_price,
                ce_bid_qty: ce.bid_qty,
                ce_ask_qty: ce.ask_qty,
                quote_ts: ce.quote_ts
            } : {}),
            ...(pe ? {
                pe_bid: pe.bid_price,
                pe_ask: pe.ask_price,
                pe_bid_qty: pe.bid_qty,
                pe_ask_qty: pe.ask_qty,
                quote_ts: pe.quote_ts
            } : {}),
        };
    });

    _patchVisibleTableRows(window._lastChainData);
}

function handleChainPriceUpdate(prices) {
    let atmTouched = false;

    Object.entries(prices || {}).forEach(([token, ltp]) => {
        const tokenStr = String(token);
        const val = Number(ltp);
        if (!Number.isFinite(val)) return;

        window._lastPrices[tokenStr] = val;

        if (window._chainATMTokens) {
            const { ce_token, pe_token } = window._chainATMTokens;
            if (tokenStr === String(ce_token) || tokenStr === String(pe_token)) atmTouched = true;
        }

        document.querySelectorAll(`#option-chain-container [data-token="${tokenStr}"]`).forEach(cell => {
            const span = cell.querySelector('span');
            if (!span) return;

            const prev = parseFloat(cell.getAttribute('data-prev-price') || val);
            span.textContent = `₹${val.toFixed(2)}`;

            if (val > prev) {
                cell.classList.add('chain-flash-up');
                setTimeout(() => cell.classList.remove('chain-flash-up'), 600);
            } else if (val < prev) {
                cell.classList.add('chain-flash-down');
                setTimeout(() => cell.classList.remove('chain-flash-down'), 600);
            }

            cell.setAttribute('data-prev-price', String(val));
        });
    });

    if (window._chainATMTokens) {
        const { atm, ce_token, pe_token, gap } = window._chainATMTokens;
        const ce_p = prices?.[ce_token]
                  ?? prices?.[parseInt(ce_token, 10)]
                  ?? window._lastPrices[String(ce_token)];
        const pe_p = prices?.[pe_token]
                  ?? prices?.[parseInt(pe_token, 10)]
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
    const reqId = ++window._chainFetchSeq;

    try {
        const url = `/api/option-chain/${encodeURIComponent(sym)}?ts=${Date.now()}`;
        const res = await fetch(url, {
            method: 'GET',
            cache: 'no-store',
            headers: {
                'Cache-Control': 'no-cache, no-store, max-age=0',
                'Pragma': 'no-cache'
            }
        });

        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }

        const data = await res.json();

        if (reqId < window._chainAppliedFetchSeq) {
            console.warn('[CHAIN-FETCH] stale fetch response ignored', {
                reqId,
                applied: window._chainAppliedFetchSeq,
                symbol: sym
            });
            return;
        }

        if (data.success && data.data) {
            if (_snapshotIsStale(data.data, 'FETCH')) return;

            window._chainAppliedFetchSeq = reqId;

            console.log('[UI-FETCH-option-chain]', data.data);
            _debugRow24150('[UI-FETCH-24150]', data.data);

            displayOptionChain(data.data);

            if (typeof roundQuantity === 'function') {
                roundQuantity('lots');
                roundQuantity('config-size');
                roundQuantity('manual_lots_per_call');
                roundQuantity('auto_lots_per_call');
            }
        } else {
            if (div) {
                div.innerHTML = `<div class="placeholder">❌ ${data.error || 'Chain unavailable'}</div>`;
            }
        }
    } catch (err) {
        console.error('Option chain fetch error:', err);
        if (div) {
            div.innerHTML = `<div class="placeholder">❌ ${err.message}</div>`;
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// RENDER — full rebuild on fetch only
// ════════════════════════════════════════════════════════════════════════════

function displayOptionChain(chainData) {
    const div = document.getElementById('option-chain-container');
    if (!div || !chainData) return;

    window._chainRendering = true;

    try {
        window._lastChainData = chainData;
        _adoptSnapshotVersion(chainData);

        if (chainData.symbol) {
            window._currentChainSymbol = chainData.symbol.toUpperCase();
        }

        if (chainData.fut_token != null) {
            div.setAttribute('data-fut-token', chainData.fut_token);
        }

        _rebuildATMTokensFromData(chainData);
        _seedLastPricesFromSnapshot(chainData);

        const synFutVal = _computeDisplayedSynFut(chainData);

        const spotEl = document.getElementById('chain-spot-value');
        const synFutEl = document.getElementById('chain-synfut-value');
        const atmEl = document.getElementById('chain-atm-value');
        const expiryEl = document.getElementById('chain-expiry-value');
        const varEl = document.getElementById('chain-var-value');

        if (spotEl) spotEl.textContent = chainData.synthetic_spot ? ('\u20B9' + Number(chainData.synthetic_spot).toFixed(2)) : '--';
        if (synFutEl) synFutEl.textContent = synFutVal ? `₹${Number(synFutVal).toFixed(2)}` : '--';
        if (atmEl) atmEl.textContent = chainData.atm ?? '--';
        if (expiryEl) expiryEl.textContent = chainData.expiry ?? '--';
        if (varEl) {
            varEl.textContent = _num(chainData.var_pts) > 0
                ? (chainData.var_pct
                    ? `±${Number(chainData.var_pts).toFixed(1)} pts (${Number(chainData.var_pct).toFixed(2)}%)`
                    : `±${Number(chainData.var_pts).toFixed(1)} pts`)
                : '--';
        }

        let html = `
            <table>
                <thead>
                    <tr>
                        <th>CE Bid</th>
                        <th>CE Ask</th>
                        <th>Theta</th>
                        <th>Vega</th>
                        <th>IV %</th>
                        <th>CE LTP</th>
                        <th>Strike</th>
                        <th>PE LTP</th>
                        <th>IV %</th>
                        <th>Vega</th>
                        <th>Theta</th>
                        <th>PE Bid</th>
                        <th>PE Ask</th>
                    </tr>
                </thead>
                <tbody>`;

        (chainData.chain || []).forEach(row => {
            const isATM = Number(row.strike) === Number(chainData.atm);
            const atmCls = isATM ? 'atm-row' : '';

            const ceAttr = row.ce_token != null ? `data-token="${String(row.ce_token)}"` : '';
            const peAttr = row.pe_token != null ? `data-token="${String(row.pe_token)}"` : '';

            html += `
                <tr class="${atmCls}" data-strike="${row.strike}">
                    <td class="quote-cell ce-bid">${_f2(row.ce_bid ?? row.ce_bid_price)}</td>
                    <td class="quote-cell ce-ask">${_f2(row.ce_ask ?? row.ce_ask_price)}</td>
                    <td class="${_thetaCls(row.ce_theta)}">${_f2(row.ce_theta)}</td>
                    <td>${_f2(row.ce_vega)}</td>
                    <td class="${_ivCls(row.ce_iv)}">${_f2(row.ce_iv)}%</td>
                    <td ${ceAttr} data-prev-price="${row.ce_ltp ?? 0}">
                        <span>${row.ce_ltp != null ? `₹${Number(row.ce_ltp).toFixed(2)}` : '--'}</span>
                    </td>
                    <td>${row.strike}</td>
                    <td ${peAttr} data-prev-price="${row.pe_ltp ?? 0}">
                        <span>${row.pe_ltp != null ? `₹${Number(row.pe_ltp).toFixed(2)}` : '--'}</span>
                    </td>
                    <td class="${_ivCls(row.pe_iv)}">${_f2(row.pe_iv)}%</td>
                    <td>${_f2(row.pe_vega)}</td>
                    <td class="${_thetaCls(row.pe_theta)}">${_f2(row.pe_theta)}</td>
                    <td class="quote-cell pe-bid">${_f2(row.pe_bid ?? row.pe_bid_price)}</td>
                    <td class="quote-cell pe-ask">${_f2(row.pe_ask ?? row.pe_ask_price)}</td>
                </tr>`;
        });

        html += '</tbody></table>';
        div.innerHTML = html;

        updateStraddlePremium();
        _debugRow24150('[UI-RENDER-24150]', chainData);

        requestAnimationFrame(scrollAtmToCenter);
    } finally {
        requestAnimationFrame(() => {
            window._chainRendering = false;
        });
    }
}

// ════════════════════════════════════════════════════════════════════════════
// WEBSOCKET PATCH ENTRY
// Call this from websocket.js when option_chain_update arrives.
// ════════════════════════════════════════════════════════════════════════════

function applyOptionChainSnapshotPatch(incoming) {
    if (!incoming) return false;
    if (_snapshotIsStale(incoming, 'WS')) return false;

    const merged = _mergeIncomingChainData(incoming);
    window._lastChainData = merged;
    _adoptSnapshotVersion(merged);

    if (merged.symbol) {
        window._currentChainSymbol = merged.symbol.toUpperCase();
    }

    const div = document.getElementById('option-chain-container');
    if (div && merged.fut_token != null) {
        div.setAttribute('data-fut-token', merged.fut_token);
    }

    _seedLastPricesFromSnapshot(merged);
    _rebuildATMTokensFromData(merged);
    _patchHeaderFromChainData(merged);

    if (Array.isArray(merged.chain) && merged.chain.length > 0) {
        _patchVisibleTableRows(merged);
    }

    updateStraddlePremium();
    _debugRow24150('[UI-WS-PATCH-24150]', merged);
    return true;
}

// ════════════════════════════════════════════════════════════════════════════
// COLOUR HELPERS
// ════════════════════════════════════════════════════════════════════════════

function _f2(v) {
    return (v != null && !isNaN(v)) ? parseFloat(v).toFixed(2) : '--';
}

function _ivCls(iv) {
    if (iv == null) return '';
    if (iv < 14) return 'iv-low';
    if (iv < 20) return 'iv-mid';
    return 'iv-high';
}

function _thetaCls(theta) {
    if (theta == null) return '';
    if (theta < -1) return 'theta-high';
    if (theta < -0.3) return 'theta-mid';
    return 'theta-low';
}




import asyncio
import sys
import re
from collections import defaultdict

try:
    import cred
    from Connect import XTSConnect
    from database.db_manager import Database
except ImportError as e:
    print("❌ CRITICAL ERROR: Could not import project modules.")
    print("   Make sure you run this script from the project root directory.")
    print(f"   Error details: {e}")
    sys.exit(1)

UID_PATTERN = re.compile(r'([a-zA-Z]{2}\d{12}[a-z]?)')

BUILD_CE = "BUILDCE"
BUILD_PE = "BUILDPE"
TEMP_HEDGE = "TEMPHEDGE"

EXECUTED_STATUSES = {
    "FILLED",
    "COMPLETE",
    "TRADED",
    "EXECUTED",
    "PARTIALLYFILLED",
    "PARTIAL",
}

IGNORE_STATUSES = {
    "CANCELLED",
    "CANCELED",
    "REJECTED",
}

TARGET_UID = "ny070726091800a"   # <- change if needed


def safe_int(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def safe_str(v):
    return str(v).strip() if v is not None else ""


def normalize_status(status: str) -> str:
    s = safe_str(status).upper()
    if s in {"FILLED", "COMPLETE", "TRADED", "EXECUTED"}:
        return "FILLED"
    if s in {"PARTIALLYFILLED", "PARTIAL"}:
        return "PARTIAL"
    if s in {"CANCELLED", "CANCELED"}:
        return "CANCELLED"
    if s == "REJECTED":
        return "REJECTED"
    return s or "UNKNOWN"


def extract_uid(text: str):
    if not text:
        return None
    m = UID_PATTERN.search(text)
    return m.group(1) if m else None


def classify_order_role(order: dict, ce_token: int, pe_token: int) -> str:
    explicit = safe_str(
        order.get("buildrole")
        or order.get("orderrole")
        or order.get("build_role")
        or order.get("order_role")
    ).upper()

    if explicit in {BUILD_CE, BUILD_PE, TEMP_HEDGE}:
        return explicit

    token = safe_int(order.get("token") or order.get("ExchangeInstrumentID"))
    if token == ce_token:
        return BUILD_CE
    if token == pe_token:
        return BUILD_PE
    return TEMP_HEDGE


def get_effective_exec_qty(order: dict) -> int:
    cumulative = safe_int(order.get("CumulativeQuantity"))
    if cumulative > 0:
        return cumulative

    order_qty = safe_int(order.get("OrderQuantity"))
    leaves_qty = safe_int(order.get("LeavesQuantity"))
    if order_qty > 0:
        computed = order_qty - leaves_qty
        if computed > 0:
            return computed

    filled = safe_int(order.get("FilledQuantity") or order.get("filledqty"))
    if filled > 0:
        return filled

    return 0


def get_side(order: dict) -> str:
    return safe_str(
        order.get("OrderSide")
        or order.get("orderside")
        or order.get("action")
        or order.get("TransactionType")
    ).upper()


def collapse_to_latest_by_appid(orders, ce_token, pe_token):
    collapsed = {}

    for order in orders:
        app_id = safe_str(order.get("AppOrderID"))
        if not app_id:
            continue

        token = safe_int(order.get("ExchangeInstrumentID"))
        side = get_side(order)
        status = normalize_status(order.get("OrderStatus"))
        qty = get_effective_exec_qty(order)
        role = classify_order_role(order, ce_token, pe_token)

        row = {
            "app_id": app_id,
            "token": token,
            "side": side,
            "status": status,
            "qty": qty,
            "role": role,
            "ouid": safe_str(order.get("OrderUniqueIdentifier")),
        }

        old = collapsed.get(app_id)
        if old is None:
            collapsed[app_id] = row
            continue

        rank = {"FILLED": 3, "PARTIAL": 2, "UNKNOWN": 1, "OPEN": 0}
        old_rank = rank.get(old["status"], 0)
        new_rank = rank.get(row["status"], 0)

        if row["qty"] > old["qty"] or (row["qty"] == old["qty"] and new_rank > old_rank):
            collapsed[app_id] = row

    return collapsed


def compute_single_trade_live_parity(collapsed, ce_token, pe_token):
    ce_net = 0
    pe_net = 0

    included_ce = []
    included_pe = []
    ignored = []

    for app_id, row in collapsed.items():
        status = row["status"]
        token = row["token"]
        side = row["side"]
        qty = row["qty"]
        role = row["role"]

        if status in IGNORE_STATUSES:
            ignored.append((app_id, "status"))
            continue

        if status not in EXECUTED_STATUSES:
            ignored.append((app_id, "not_executed"))
            continue

        if qty <= 0:
            ignored.append((app_id, "zero_exec_qty"))
            continue

        if role == TEMP_HEDGE:
            ignored.append((app_id, "hedge"))
            continue

        signed_qty = qty if side == "SELL" else -qty if side == "BUY" else 0
        if signed_qty == 0:
            ignored.append((app_id, "bad_side"))
            continue

        if role == BUILD_CE and token == ce_token:
            ce_net += signed_qty
            included_ce.append((app_id, side, qty, status))
        elif role == BUILD_PE and token == pe_token:
            pe_net += signed_qty
            included_pe.append((app_id, side, qty, status))
        else:
            ignored.append((app_id, "role_token_mismatch"))

    return {
        "ce_live_qty": max(0, ce_net),
        "pe_live_qty": max(0, pe_net),
        "included_ce": included_ce,
        "included_pe": included_pe,
        "ignored": ignored,
    }


async def main():
    print("\n" + "=" * 150)
    print("🕵️  SINGLE-TRADE EXECUTED PARITY CHECK")
    print("=" * 150)

    print("🔹 Connecting to Database...")
    db = Database()
    loop = asyncio.get_running_loop()

    trade = await loop.run_in_executor(None, db.get_straddle_by_id, TARGET_UID)
    if not trade:
        print(f"❌ Trade not found in DB: {TARGET_UID}")
        return

    ce_token = safe_int(trade.get("ce_token"))
    pe_token = safe_int(trade.get("pe_token"))
    lot_size = safe_int(trade.get("lot_size"))
    db_ce_qty = safe_int(trade.get("ce_quantity"))
    db_pe_qty = safe_int(trade.get("pe_quantity"))
    status = safe_str(trade.get("status"))

    print(f"   ✅ DB trade loaded: {TARGET_UID}")
    print(f"   ℹ️  DB row => status={status}, ce_qty={db_ce_qty}, pe_qty={db_pe_qty}, ce_token={ce_token}, pe_token={pe_token}, lot={lot_size}")

    print("🔹 Connecting to Broker...")
    xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
    login_resp = xt.interactive_login()

    if login_resp.get("type") != "success":
        print(f"❌ Login failed: {login_resp.get('description')}")
        return

    user_id = login_resp["result"].get("userID")
    client_id = getattr(cred, "clientID", None) or user_id
    xt.isInvestorClient = False

    print(f"   ✅ Broker login OK. User: {user_id}")
    if client_id != user_id:
        print(f"   ℹ️  Using Configured Client ID: {client_id}")

    print("🔹 Fetching broker order book...")
    order_book_resp = xt.get_order_book(clientID=client_id)
    if order_book_resp.get("type") != "success":
        print(f"❌ Failed to fetch order book: {order_book_resp}")
        return

    broker_orders = order_book_resp.get("result", [])
    print(f"   ✅ Total broker orders fetched: {len(broker_orders)}")

    print(f"🔹 Filtering only this trade UID: {TARGET_UID}")
    trade_orders = []

    for order in broker_orders:
        ouid = safe_str(order.get("OrderUniqueIdentifier"))
        app_id = safe_str(order.get("AppOrderID"))
        extracted = extract_uid(ouid)

        belongs = False
        if extracted == TARGET_UID:
            belongs = True
        elif extracted and TARGET_UID.startswith(extracted):
            belongs = True
        elif TARGET_UID in ouid:
            belongs = True

        if belongs:
            trade_orders.append(order)

    print(f"   ✅ Broker orders linked to this trade: {len(trade_orders)}")

    collapsed = collapse_to_latest_by_appid(trade_orders, ce_token, pe_token)
    result = compute_single_trade_live_parity(collapsed, ce_token, pe_token)

    broker_ce_qty = result["ce_live_qty"]
    broker_pe_qty = result["pe_live_qty"]

    diff_ce = db_ce_qty - broker_ce_qty
    diff_pe = db_pe_qty - broker_pe_qty

    diff_ce_lots = (diff_ce / lot_size) if lot_size > 0 else 0.0
    diff_pe_lots = (diff_pe / lot_size) if lot_size > 0 else 0.0

    print("\n" + "=" * 150)
    print(
        f"{'TRADE UID':<18} | {'LEG':<4} | {'TOKEN':<8} | {'LOT':<5} | {'DB QTY':<8} | {'BROKER EXEC':<12} | {'DIFF':<7} | {'DIFF LOTS':<10} | RESULT"
    )
    print("=" * 150)

    print(
        f"{TARGET_UID:<18} | {'CE':<4} | {ce_token:<8} | {lot_size:<5} | {db_ce_qty:<8} | {broker_ce_qty:<12} | {diff_ce:<+7} | {diff_ce_lots:<+10.2f} | {'✅ OK' if diff_ce == 0 else '❌ FAIL'}"
    )
    print(
        f"{'':<18} | {'PE':<4} | {pe_token:<8} | {lot_size:<5} | {db_pe_qty:<8} | {broker_pe_qty:<12} | {diff_pe:<+7} | {diff_pe_lots:<+10.2f} | {'✅ OK' if diff_pe == 0 else '❌ FAIL'}"
    )

    if result["included_ce"]:
        print(f"{'':<18} | {'':<4} | Included CE executed orders:")
        for app_id, side, qty, st in result["included_ce"]:
            print(f"{'':<18} | {'':<4} |   {app_id} | {side:<4} | qty={qty:<5} | status={st}")

    if result["included_pe"]:
        print(f"{'':<18} | {'':<4} | Included PE executed orders:")
        for app_id, side, qty, st in result["included_pe"]:
            print(f"{'':<18} | {'':<4} |   {app_id} | {side:<4} | qty={qty:<5} | status={st}")

    if result["ignored"]:
        print(f"{'':<18} | {'':<4} | Ignored orders:")
        for app_id, reason in result["ignored"][:50]:
            print(f"{'':<18} | {'':<4} |   {app_id} | {reason}")

    print("-" * 150)
    print(f"Collapsed unique AppOrderIDs for this trade: {len(collapsed)}")
    print(f"Executed CE live qty: {broker_ce_qty}")
    print(f"Executed PE live qty: {broker_pe_qty}")
    print("=" * 150 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Check cancelled by user.")
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


def _safe_int(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _safe_str(v):
    return str(v).strip() if v is not None else ""


def _extract_uid(ouid: str):
    if not ouid:
        return None
    m = UID_PATTERN.search(ouid)
    return m.group(1) if m else None


def _normalize_status(status: str):
    s = _safe_str(status).upper()
    if s in {"FILLED", "COMPLETE", "TRADED", "EXECUTED"}:
        return "FILLED"
    if s in {"CANCELLED", "CANCELED"}:
        return "CANCELLED"
    if s in {"REJECTED"}:
        return "REJECTED"
    if s in {"PARTIALLYFILLED", "PARTIAL"}:
        return "PARTIAL"
    return s or "UNKNOWN"


async def main():
    print("\n" + "=" * 120)
    print("🕵️  STANDALONE PARITY CHECKER (QTY ONLY, ORDER-LEVEL)")
    print("=" * 120)

    print("🔹 Connecting to Database...")
    try:
        db = Database()
        straddles = await asyncio.to_thread(db.get_todays_straddles)
        print(f"   ✅ DB Connected. Found {len(straddles)} trades in local DB for today.")
    except Exception as e:
        print(f"   ❌ DB Connection Failed: {e}")
        return

    straddles_by_uid = {}
    for s in straddles:
        uid = s.get("trade_uid") or s.get("straddle_id")
        if uid:
            straddles_by_uid[uid] = s

    print("🔹 Connecting to Broker (XTS Interactive)...")
    try:
        xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
        login_resp = xt.interactive_login()

        if login_resp.get("type") != "success":
            print(f"   ❌ Login Failed: {login_resp.get('description')}")
            return

        user_id = login_resp["result"].get("userID")
        client_id = getattr(cred, "clientID", None) or user_id

        print(f"   ✅ Login Successful. User: {user_id}")
        if client_id != user_id:
            print(f"   ℹ️  Using Configured Client ID: {client_id}")

        print("🔹 Fetching Broker Order Book...")
        xt.isInvestorClient = False
        order_book_resp = xt.get_order_book(clientID=client_id)

        if order_book_resp.get("type") != "success":
            print(f"   ❌ Failed to fetch order book: {order_book_resp}")
            return

        broker_orders = order_book_resp.get("result", [])
        print(f"   ✅ Fetched {len(broker_orders)} orders from broker.")

    except Exception as e:
        print(f"   ❌ Broker Connection/Fetch Failed: {e}")
        return

    print("🔹 Grouping broker orders by trade UID...")
    broker_trade_map = defaultdict(list)

    for order in broker_orders:
        ouid = _safe_str(order.get("OrderUniqueIdentifier"))
        extracted_uid = _extract_uid(ouid)
        if not extracted_uid:
            continue

        full_trade_uid = next((uid for uid in straddles_by_uid if uid == extracted_uid), None)
        if not full_trade_uid:
            full_trade_uid = next((uid for uid in straddles_by_uid if uid.startswith(extracted_uid)), None)

        broker_trade_map[full_trade_uid or extracted_uid].append(order)

    print(f"   ✅ Mapped broker orders to {len(broker_trade_map)} UID buckets.")

    print("\n" + "=" * 120)
    print(
        f"{'TRADE UID':<18} | {'STATUS':<10} | {'LEG':<4} | {'TOKEN':<8} | {'LOT':<5} | "
        f"{'DB QTY':<8} | {'BROKER QTY':<10} | {'DIFF':<7} | {'DIFF LOTS':<10} | {'RESULT'}"
    )
    print("=" * 120)

    mismatches_found = 0

    all_uids = set(straddles_by_uid.keys())
    all_uids.update(broker_trade_map.keys())

    for uid in sorted(all_uids):
        db_record = straddles_by_uid.get(uid)

        db_status = "UNKNOWN"
        db_ce_qty = 0
        db_pe_qty = 0
        ce_token = 0
        pe_token = 0
        lot_size = 0

        if db_record:
            db_status = _safe_str(db_record.get("status"))[:10]
            db_ce_qty = _safe_int(db_record.get("ce_quantity"))
            db_pe_qty = _safe_int(db_record.get("pe_quantity"))
            ce_token = _safe_int(db_record.get("ce_token"))
            pe_token = _safe_int(db_record.get("pe_token"))
            lot_size = _safe_int(db_record.get("lot_size"))

        real_ce_net = 0
        real_pe_net = 0
        ce_ids = []
        pe_ids = []

        orders = broker_trade_map.get(uid, [])

        # IMPORTANT: use each broker order once, not every trade-book fill row
        seen_app_ids = set()

        for order in orders:
            app_id = _safe_str(order.get("AppOrderID"))
            if not app_id or app_id in seen_app_ids:
                continue
            seen_app_ids.add(app_id)

            token = _safe_int(order.get("ExchangeInstrumentID"))
            side = _safe_str(order.get("OrderSide")).upper()
            status = _normalize_status(order.get("OrderStatus"))

            if side not in {"BUY", "SELL"}:
                continue

            qty = _safe_int(
                order.get("CumulativeQuantity")
                or order.get("LeavesQuantity")
                and (_safe_int(order.get("OrderQuantity")) - _safe_int(order.get("LeavesQuantity")))
                or order.get("OrderQuantity")
            )

            if qty <= 0:
                continue

            # For current position parity, ignore fully unfilled/non-filled states
            if status not in {"FILLED", "PARTIAL"}:
                continue

            signed_qty = qty if side == "SELL" else -qty

            if token == ce_token and ce_token != 0:
                real_ce_net += signed_qty
                ce_ids.append(app_id)
            elif token == pe_token and pe_token != 0:
                real_pe_net += signed_qty
                pe_ids.append(app_id)

        diff_ce = db_ce_qty - real_ce_net
        diff_pe = db_pe_qty - real_pe_net

        diff_ce_lots = (diff_ce / lot_size) if lot_size > 0 else 0
        diff_pe_lots = (diff_pe / lot_size) if lot_size > 0 else 0

        if ce_token > 0 or real_ce_net != 0:
            res_ce = "✅ OK" if diff_ce == 0 else "❌ FAIL"
            if diff_ce != 0:
                mismatches_found += 1
            print(
                f"{uid:<18} | {db_status:<10} | {'CE':<4} | {ce_token:<8} | {lot_size:<5} | "
                f"{db_ce_qty:<8} | {real_ce_net:<10} | {diff_ce:<+7} | {diff_ce_lots:<+10.2f} | {res_ce}"
            )
            if ce_ids:
                print(f"{'':<18} | {'':<10} | {'':<4} | Included CE AppOrderIDs: {', '.join(ce_ids)}")

        if pe_token > 0 or real_pe_net != 0:
            res_pe = "✅ OK" if diff_pe == 0 else "❌ FAIL"
            if diff_pe != 0:
                mismatches_found += 1
            print(
                f"{'':<18} | {'':<10} | {'PE':<4} | {pe_token:<8} | {lot_size:<5} | "
                f"{db_pe_qty:<8} | {real_pe_net:<10} | {diff_pe:<+7} | {diff_pe_lots:<+10.2f} | {res_pe}"
            )
            if pe_ids:
                print(f"{'':<18} | {'':<10} | {'':<4} | Included PE AppOrderIDs: {', '.join(pe_ids)}")

        if ce_token > 0 or pe_token > 0:
            print("-" * 120)

    print("\n" + "=" * 120)
    if mismatches_found == 0:
        print("✅ INTEGRITY CHECK PASSED: DB quantities match broker order-level net quantities.")
    else:
        print(f"❌ INTEGRITY CHECK FAILED: Found {mismatches_found} discrepancies.")
        print("   Positive Diff (+): DB has more than broker.")
        print("   Negative Diff (-): Broker has more than DB.")
    print("=" * 120 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Check cancelled by user.")
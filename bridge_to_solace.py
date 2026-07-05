"""
bridge_to_solace.py  —  The migration bridge.

Phase 1 (BRIDGE ACTIVE):
  Reads messages destructively from IBM MQ DEV.QUEUE.1
  Republishes them to Solace PubSub+ MIGRATED.APP.QUEUE

This is the "coexistence" phase described in the RFP:
  - MQ drains to zero (apps continue writing to MQ during transition)
  - Solace fills up (new consumers can switch incrementally)
  - Feature flag flipped per app team at their own pace

Phase 2 (FEATURE FLAG -> SOLACE):
  Demonstrates direct Solace publish, bypassing MQ entirely.
"""
import json, time, sys, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── IBM MQ (source) ───────────────────────────────────────────────────
MQ_BASE  = "https://localhost:9443/ibmmq/rest/v2"
MQ_AUTH  = ("app", "passw0rd")
QMGR     = "QM1"
MQ_QUEUE = "DEV.QUEUE.1"

# ── Solace PubSub+ (target) ───────────────────────────────────────────
SOLACE_REST  = "http://localhost:9000"
SOLACE_AUTH  = ("demo-client", "demo-pass")
SOL_QUEUE    = "MIGRATED.APP.QUEUE"
SEMP         = "http://localhost:8080/SEMP/v2"
SEMP_AUTH    = ("admin", "admin")

BANNER = "=" * 62

def mq_depth():
    """Return current depth via MQSC DISPLAY QSTATUS — the only reliable REST method."""
    try:
        r = requests.post(
            f"{MQ_BASE}/admin/action/qmgr/{QMGR}/mqsc",
            auth=MQ_ADMIN,
            headers={"Content-Type": "application/json",
                     "ibm-mq-rest-csrf-token": "bridge"},
            json={
                "type": "runCommandJSON",
                "command": "display",
                "qualifier": "qstatus",
                "name": MQ_QUEUE,
                "responseParameters": ["curdepth"]
            },
            verify=False,
            timeout=8
        )
        if r.status_code == 200:
            resp = r.json().get("commandResponse", [])
            if resp:
                return resp[0].get("parameters", {}).get("curdepth", "?")
    except Exception:
        pass
    return "?"

def solace_depth(queue_name):
    """Return current spooled message count in a Solace queue."""
    try:
        r = requests.get(
            f"{SEMP}/monitor/msgVpns/default/queues/{queue_name}",
            auth=SEMP_AUTH, timeout=6
        )
        if r.status_code == 200:
            return r.json().get("data", {}).get("spooledMsgCount", "?")
    except Exception:
        pass
    return "?"

def get_from_mq():
    """Destructive GET from IBM MQ (uses DELETE method in REST API)."""
    r = requests.delete(
        f"{MQ_BASE}/messaging/qmgr/{QMGR}/queue/{MQ_QUEUE}/message",
        auth=MQ_AUTH,
        headers={
            "ibm-mq-rest-csrf-token": "demo",
            "Accept": "application/json",
        },
        verify=False,
        timeout=10,
    )
    return r.status_code, r.text if r.status_code == 200 else None

def put_to_solace(body_str):
    """Publish to Solace via REST messaging API."""
    r = requests.post(
        f"{SOLACE_REST}/QUEUE/{SOL_QUEUE}",
        auth=SOLACE_AUTH,
        headers={
            "Content-Type": "application/json",
            "Solace-delivery-mode": "persistent",
        },
        data=body_str,
        timeout=10,
    )
    return r.status_code == 200, r.status_code

if __name__ == "__main__":
    print(f"\n{BANNER}")
    print(f"  MIGRATION BRIDGE  —  IBM MQ  ->  Solace PubSub+")
    print(f"  Source : {QMGR}/{MQ_QUEUE}  (IBM MQ)")
    print(f"  Target : MIGRATED.APP.QUEUE  (Solace PubSub+)")
    print(BANNER)

    print(f"\n  Feature Flag: BRIDGE ACTIVE  (rolling wave — coexistence phase)")
    print(f"  All new messages written to MQ are transparently bridged to Solace")
    print(f"  App teams switch their consumers to Solace at their own pace.\n")

    # ── Phase 1: drain MQ → Solace ────────────────────────────────────
    bridged = 0
    errors  = 0
    start   = time.time()

    while True:
        code, body = get_from_mq()

        if code == 200 and body:
            bridged += 1
            try:
                msg   = json.loads(body)
                label = f"{msg.get('type', 'Message'):<20}"
                ref   = msg.get("orderId") or msg.get("sku", "")
            except Exception:
                label, ref = "RawMessage         ", ""

            ok, sol_code = put_to_solace(body)
            bridge_ok = "[BRIDGED]" if ok else f"[SOL ERR {sol_code}]"
            if not ok:
                errors += 1

            print(f"  {bridge_ok}  #{bridged:02d}  {label}  {ref}")
            print(f"             IBM MQ depth: {mq_depth()}  |  Solace depth: {solace_depth(SOL_QUEUE)}")
            time.sleep(0.6)

        elif code == 204:
            # 204 = no message available (queue empty)
            elapsed = int(time.time() - start)
            print(f"\n  Queue empty after {elapsed}s")
            break

        else:
            print(f"  [ERROR] MQ GET returned HTTP {code}")
            errors += 1
            if errors >= 3:
                print("  Too many errors. Aborting bridge.")
                sys.exit(1)
            time.sleep(2)

    print(f"\n{BANNER}")
    print(f"  BRIDGE SUMMARY")
    print(f"  Messages bridged     : {bridged}")
    print(f"  Errors               : {errors}")
    print(f"  IBM MQ DEV.QUEUE.1   : EMPTY  (depth = {mq_depth()})")
    print(f"  Solace MIGRATED.QUEUE: {solace_depth(SOL_QUEUE)} messages spooled")
    print(BANNER)

    # ── Phase 2: feature-flag demo — produce direct-to-Solace ─────────
    input("\n  [ENTER] to demo Phase 2: direct Solace publish (feature flag = SOLACE)...")

    print(f"\n  Feature Flag: SOLACE  (legacy MQ completely bypassed)")
    print(f"  New events published directly into Solace PubSub+ Event Mesh\n")

    NEW_EVENTS = [
        {"type": "OrderCreated",    "orderId": "ORD-2001", "amount": 9999.00, "customer": "NewCustomer",  "note": "direct-to-solace"},
        {"type": "PaymentReceived", "orderId": "ORD-2001", "txnId":  "TXN-X01","status": "confirmed",     "note": "direct-to-solace"},
        {"type": "OrderShipped",    "orderId": "ORD-2001", "carrier": "DHL",   "trackingId": "TRK-X001",  "note": "direct-to-solace"},
    ]

    direct = 0
    for msg in NEW_EVENTS:
        ok, code = put_to_solace(json.dumps(msg))
        status = "[OK]" if ok else f"[ERR {code}]"
        print(f"  {status}  Direct -> Solace  {msg['type']:<20}  {msg.get('orderId','')}")
        if ok:
            direct += 1
        time.sleep(0.4)

    total = bridged + direct
    print(f"\n  Solace queue now holds: {solace_depth(SOL_QUEUE)} messages  (bridged + direct)")
    print(f"\n  Next: run  python verify_solace.py")
    print(BANNER)

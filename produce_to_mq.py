"""
produce_to_mq.py  —  Send 10 sample business messages to IBM MQ DEV.QUEUE.1
Simulates the LEGACY application (before migration).
"""
import json, time, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MQ_BASE = "https://localhost:9443/ibmmq/rest/v2"
MQ_AUTH = ("app", "passw0rd")
QMGR    = "QM1"
QUEUE   = "DEV.QUEUE.1"

# Sample business messages — realistic order/payment events
MESSAGES = [
    {"type": "OrderCreated",    "orderId": "ORD-1001", "amount": 1250.00, "customer": "Acme Corp",     "region": "EMEA"},
    {"type": "OrderCreated",    "orderId": "ORD-1002", "amount":  875.50, "customer": "TechStart Ltd",  "region": "APAC"},
    {"type": "PaymentReceived", "orderId": "ORD-1001", "txnId":  "TXN-9901", "status": "confirmed"},
    {"type": "OrderCreated",    "orderId": "ORD-1003", "amount": 4200.00, "customer": "Global Inc",     "region": "AMER"},
    {"type": "OrderShipped",    "orderId": "ORD-1001", "carrier": "FedEx",   "trackingId": "TRK-8812"},
    {"type": "PaymentReceived", "orderId": "ORD-1002", "txnId":  "TXN-9902", "status": "confirmed"},
    {"type": "OrderCreated",    "orderId": "ORD-1004", "amount":  320.00, "customer": "Beta Systems",   "region": "EMEA"},
    {"type": "InventoryAlert",  "sku": "SKU-5511",     "remaining": 3,    "threshold": 10},
    {"type": "OrderShipped",    "orderId": "ORD-1002", "carrier": "UPS",     "trackingId": "TRK-8813"},
    {"type": "OrderCreated",    "orderId": "ORD-1005", "amount": 6750.00, "customer": "Enterprise Co",  "region": "AMER"},
]

BANNER = "=" * 62

def put_to_mq(msg_dict):
    """PUT a JSON message to IBM MQ via REST API."""
    payload = json.dumps(msg_dict)
    r = requests.post(
        f"{MQ_BASE}/messaging/qmgr/{QMGR}/queue/{QUEUE}/message",
        auth=MQ_AUTH,
        headers={
            "Content-Type": "application/json",
            "ibm-mq-rest-csrf-token": "demo",   # required CSRF header (any value)
        },
        data=payload,
        verify=False,
        timeout=10,
    )
    return r.status_code in (200, 201), r.status_code

if __name__ == "__main__":
    print(f"\n{BANNER}")
    print(f"  IBM MQ PRODUCER  —  Queue: {QMGR}/{QUEUE}")
    print(f"  Simulating LEGACY application sending business events")
    print(BANNER)

    ok_count = 0
    for i, msg in enumerate(MESSAGES, 1):
        success, code = put_to_mq(msg)
        status = "OK " if success else f"ERR {code}"
        label  = f"{msg['type']:<20}"
        ref    = msg.get("orderId") or msg.get("sku", "")
        if success:
            ok_count += 1
            print(f"  [{i:02d}] {status}  {label}  {ref}")
        else:
            print(f"  [{i:02d}] {status}  {label}  {ref}  (check MQ is running)")
        time.sleep(0.3)

    print(f"\n  {ok_count}/{len(MESSAGES)} messages queued in IBM MQ  DEV.QUEUE.1")
    print(f"\n  -> Open https://localhost:9443/ibmmq/console and check DEV.QUEUE.1")
    print(f"     (admin / passw0rd)  — queue depth should show {ok_count}")
    print(f"\n  Next: run  python bridge_to_solace.py")
    print(BANNER)

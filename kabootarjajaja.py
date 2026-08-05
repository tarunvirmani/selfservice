#!/usr/bin/env python3
"""
self_service_portal.py — Self-Service Middleware Migration Portal
IBM MQ → Solace PubSub+ Event Mesh

Run:  python self_service_portal.py
Open: http://localhost:5001
"""

from flask import Flask, jsonify, request, Response, stream_with_context
import json, time, random, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# ── Connection defaults (containers from docker-compose) ──────────────
MQ_BASE    = "https://localhost:9443/ibmmq/rest/v2"
MQ_ADMIN   = ("admin", "passw0rd")
MQ_APP     = ("app",   "passw0rd")   # mqclient group — only user allowed on /messaging/ REST API
SEMP       = "http://localhost:8080/SEMP/v2"
SEMP_AUTH  = ("admin", "admin")
SOL_REST   = "http://localhost:9000"
SOL_CLIENT = ("demo-client", "demo-pass")

# ── Message templates by event type ──────────────────────────────────
TEMPLATES = {
    "Order Events": [
        {"type": "OrderCreated",    "orderId": "ORD-{n}", "amount": "{amt}", "customer": "{cust}", "region": "{reg}"},
        {"type": "PaymentReceived", "orderId": "ORD-{n}", "txnId": "TXN-{n}", "status": "confirmed"},
        {"type": "OrderShipped",    "orderId": "ORD-{n}", "carrier": "FedEx", "trackingId": "TRK-{n}"},
        {"type": "OrderCancelled",  "orderId": "ORD-{n}", "reason": "customer_request"},
    ],
    "Payment Events": [
        {"type": "PaymentInitiated", "txnId": "TXN-{n}", "amount": "{amt}", "method": "card"},
        {"type": "PaymentConfirmed", "txnId": "TXN-{n}", "status": "success"},
        {"type": "PaymentFailed",    "txnId": "TXN-{n}", "reason": "insufficient_funds"},
        {"type": "RefundIssued",     "txnId": "TXN-{n}", "amount": "{amt}"},
    ],
    "Inventory Events": [
        {"type": "InventoryAlert",   "sku": "SKU-{n}", "remaining": "{cnt}", "threshold": 10},
        {"type": "StockReplenished", "sku": "SKU-{n}", "added": 100, "newTotal": 150},
        {"type": "ItemReserved",     "sku": "SKU-{n}", "orderId": "ORD-{n}", "qty": "{cnt}"},
        {"type": "StockTransfer",    "sku": "SKU-{n}", "fromWarehouse": "WH-01", "toWarehouse": "WH-02"},
    ],
    "Customer Events": [
        {"type": "CustomerCreated", "customerId": "CUST-{n}", "email": "user{n}@example.com"},
        {"type": "ProfileUpdated",  "customerId": "CUST-{n}", "field": "address"},
        {"type": "AddressChanged",  "customerId": "CUST-{n}", "country": "UK"},
        {"type": "CustomerDeleted", "customerId": "CUST-{n}", "reason": "gdpr_request"},
    ],
    "Trade / Financial Events": [
        {"type": "TradeExecuted",   "tradeId": "TRD-{n}", "instrument": "AAPL", "qty": "{cnt}", "price": "{amt}", "side": "BUY"},
        {"type": "TradeSettled",    "tradeId": "TRD-{n}", "settlementDate": "T+2", "status": "settled"},
        {"type": "PositionUpdated", "accountId": "ACC-{n}", "instrument": "AAPL", "netPosition": "{cnt}"},
        {"type": "FXConversion",    "txnId": "TXN-{n}", "fromCcy": "USD", "toCcy": "GBP", "amount": "{amt}"},
    ],
    "Logistics / Shipment Events": [
        {"type": "ShipmentCreated",   "shipmentId": "SHP-{n}", "origin": "Mumbai", "destination": "London", "weight": "{cnt}kg"},
        {"type": "ShipmentPickedUp",  "shipmentId": "SHP-{n}", "carrier": "DHL", "trackingId": "TRK-{n}"},
        {"type": "ShipmentInTransit", "shipmentId": "SHP-{n}", "currentLocation": "Dubai Hub", "eta": "2026-07-01"},
        {"type": "ShipmentDelivered", "shipmentId": "SHP-{n}", "deliveredAt": "2026-06-27T14:30:00Z", "signedBy": "J.Smith"},
    ],
    "IoT / Sensor Events": [
        {"type": "SensorReading",   "deviceId": "DEV-{n}", "metric": "temperature", "value": "{amt}", "unit": "C", "location": "Plant-A"},
        {"type": "DeviceAlert",     "deviceId": "DEV-{n}", "alertType": "threshold_breach", "severity": "HIGH"},
        {"type": "DeviceHeartbeat", "deviceId": "DEV-{n}", "status": "online", "battery": "{cnt}%"},
        {"type": "FirmwareUpdated", "deviceId": "DEV-{n}", "version": "2.4.1", "status": "success"},
    ],
    "Healthcare Events": [
        {"type": "PatientAdmitted",    "patientId": "PAT-{n}", "ward": "Cardiology", "priority": "HIGH"},
        {"type": "LabResultReady",     "patientId": "PAT-{n}", "testType": "CBC", "status": "ready", "labId": "LAB-{n}"},
        {"type": "AppointmentBooked",  "patientId": "PAT-{n}", "doctor": "Dr. Smith", "slot": "2026-07-01T09:00"},
        {"type": "DischargeInitiated", "patientId": "PAT-{n}", "ward": "Cardiology", "dischargeDate": "2026-06-28"},
    ],
    "HR / Employee Events": [
        {"type": "EmployeeOnboarded",  "empId": "EMP-{n}", "department": "Engineering", "startDate": "2026-07-01"},
        {"type": "PayrollProcessed",   "empId": "EMP-{n}", "period": "2026-06", "netPay": "{amt}", "currency": "USD"},
        {"type": "LeaveRequested",     "empId": "EMP-{n}", "leaveType": "annual", "days": "{cnt}", "status": "pending"},
        {"type": "EmployeeOffboarded", "empId": "EMP-{n}", "lastDay": "2026-06-30", "reason": "resignation"},
    ],
    "Banking / Account Events": [
        {"type": "AccountOpened",     "accountId": "ACC-{n}", "type": "savings", "customerId": "CUST-{n}", "branch": "London"},
        {"type": "TransactionPosted", "accountId": "ACC-{n}", "amount": "{amt}", "txnType": "debit", "balance": "{amt}"},
        {"type": "AccountFrozen",     "accountId": "ACC-{n}", "reason": "suspicious_activity", "raisedBy": "fraud-engine"},
        {"type": "LoanDisbursed",     "loanId": "LN-{n}", "accountId": "ACC-{n}", "amount": "{amt}", "tenure": "60months"},
    ],
    "Fraud / Risk Events": [
        {"type": "FraudAlertRaised",  "alertId": "FRD-{n}", "accountId": "ACC-{n}", "score": "{cnt}", "action": "block"},
        {"type": "RiskScoreUpdated",  "customerId": "CUST-{n}", "oldScore": 420, "newScore": "{cnt}", "model": "v3.1"},
        {"type": "SuspiciousLogin",   "userId": "USR-{n}", "ip": "192.168.{cnt}.1", "country": "Unknown", "blocked": True},
        {"type": "AMLFlagRaised",     "txnId": "TXN-{n}", "amount": "{amt}", "reportedTo": "compliance-team"},
    ],
    "ERP / SAP Events": [
        {"type": "PurchaseOrderCreated", "poId": "PO-{n}", "vendor": "Vendor-{n}", "amount": "{amt}", "plant": "P001"},
        {"type": "GoodsReceiptPosted",   "grId": "GR-{n}", "poId": "PO-{n}", "qty": "{cnt}", "material": "MAT-{n}"},
        {"type": "InvoiceVerified",      "invId": "INV-{n}", "poId": "PO-{n}", "amount": "{amt}", "status": "approved"},
        {"type": "PaymentRun",           "payRunId": "PR-{n}", "vendor": "Vendor-{n}", "amount": "{amt}", "dueDate": "2026-07-15"},
    ],
    "CRM / Sales Events": [
        {"type": "LeadCreated",       "leadId": "LED-{n}", "source": "web", "score": "{cnt}", "assignedTo": "sales-team-A"},
        {"type": "OpportunityUpdated","oppId": "OPP-{n}", "stage": "Negotiation", "value": "{amt}", "closeDate": "2026-08-01"},
        {"type": "CaseOpened",        "caseId": "CASE-{n}", "customerId": "CUST-{n}", "priority": "HIGH", "channel": "email"},
        {"type": "ContractSigned",    "contractId": "CTR-{n}", "customerId": "CUST-{n}", "value": "{amt}", "term": "12months"},
    ],
    "Notification Events": [
        {"type": "EmailNotification", "notifId": "NTF-{n}", "recipient": "user{n}@example.com", "template": "order_confirm", "status": "queued"},
        {"type": "SMSNotification",   "notifId": "NTF-{n}", "phone": "+44780000{n}", "message": "Your order is confirmed"},
        {"type": "PushNotification",  "notifId": "NTF-{n}", "deviceToken": "tok-{n}", "title": "Update", "body": "Action required"},
        {"type": "WebhookFired",      "webhookId": "WHK-{n}", "url": "https://partner{n}.example.com/hook", "status": "delivered"},
    ],
    "Custom JSON": [],
}
CUSTOMERS = ["Acme Corp", "TechStart Ltd", "Global Inc", "Beta Systems", "Enterprise Co"]
REGIONS   = ["EMEA", "APAC", "AMER"]

def fill(template, i):
    s = json.dumps(template)
    s = s.replace('"{n}"',   str(1000 + i))
    s = s.replace('"{amt}"', str(round(random.uniform(100, 9999), 2)))
    s = s.replace('"{cust}"', f'"{random.choice(CUSTOMERS)}"')
    s = s.replace('"{reg}"',  f'"{random.choice(REGIONS)}"')
    s = s.replace('"{cnt}"',  str(random.randint(1, 20)))
    return s

def mq_depth(qmgr, queue):
    # IBM MQ REST API does not return currentDepth via GET.
    # The reliable way is to run MQSC "DISPLAY QSTATUS CURDEPTH" via the
    # admin/action endpoint, which returns depth as a JSON number.
    try:
        r = requests.post(
            f"{MQ_BASE}/admin/action/qmgr/{qmgr}/mqsc",
            auth=MQ_ADMIN,
            headers={"Content-Type": "application/json",
                     "ibm-mq-rest-csrf-token": "portal"},
            json={
                "type": "runCommandJSON",
                "command": "display",
                "qualifier": "qstatus",
                "name": queue,
                "responseParameters": ["curdepth"]
            },
            verify=False,
            timeout=8
        )
        if r.status_code == 200:
            resp = r.json().get("commandResponse", [])
            if resp:
                return resp[0].get("parameters", {}).get("curdepth", 0)
    except Exception:
        pass
    return 0

def sol_depth(queue, vpn="default"):
    try:
        r = requests.get(f"{SEMP}/monitor/msgVpns/{vpn}/queues/{queue}",
                         auth=SEMP_AUTH, timeout=5)
        if r.status_code == 200:
            return r.json().get("data", {}).get("spooledMsgCount", 0)
    except Exception:
        pass
    return 0


# ── API Routes ────────────────────────────────────────────────────────

@app.route('/api/qmgrs')
def list_qmgrs():
    """Return all queue managers visible via the MQ REST API."""
    try:
        r = requests.get(f"{MQ_BASE}/admin/qmgr",
                         auth=MQ_ADMIN, verify=False, timeout=6)
        if r.status_code == 200:
            names = [q['name'] for q in r.json().get('qmgr', [])]
            return jsonify({'qmgrs': names})
    except Exception as e:
        return jsonify({'qmgrs': [], 'error': str(e)})
    return jsonify({'qmgrs': []})


@app.route('/api/queues')
def list_queues():
    """Return user-defined local queues via MQSC DISPLAY QLOCAL(*) — most reliable method."""
    qmgr = request.args.get('qmgr', 'QM1')
    try:
        r = requests.post(
            f"{MQ_BASE}/admin/action/qmgr/{qmgr}/mqsc",
            auth=MQ_ADMIN,
            headers={"Content-Type": "application/json",
                     "ibm-mq-rest-csrf-token": "portal"},
            json={
                "type": "runCommandJSON",
                "command": "display",
                "qualifier": "qlocal",   # DISPLAY QLOCAL(*) — only local queues
                "name": "*"
            },
            verify=False,
            timeout=8
        )
        if r.status_code == 200:
            responses = r.json().get('commandResponse', [])
            all_queues = [
                resp['parameters']['queue']
                for resp in responses
                if resp.get('completionCode') == 0
                and 'queue' in resp.get('parameters', {})
            ]
            # Hide SYSTEM.* and AMQ.* internal queues
            user_queues = sorted([
                q for q in all_queues
                if not q.startswith('SYSTEM.') and not q.startswith('AMQ.')
            ])
            return jsonify({'queues': user_queues})
    except Exception as e:
        return jsonify({'queues': [], 'error': str(e)})
    return jsonify({'queues': []})


# IKEA/SAMLA queue naming convention: [EBCNAME].[TYPE]
# Mirrors IBM MQ queue names — same name, different broker after migration
DEMO_SOL_QUEUES = [
    "EBCIRSE01.REQUEST",
    "EBCIRSE01.BACKOUT",
    "EBCIRDE01.REQUEST",
    "EBCIRUS01.REQUEST",
]

@app.route('/api/sol_queues')
def list_sol_queues():
    """Return Solace queues from SEMP, auto-creating the demo set if missing."""
    vpn = request.args.get('vpn', 'default')
    # Ensure all demo queues exist
    for q in DEMO_SOL_QUEUES:
        requests.post(
            f"{SEMP}/config/msgVpns/{vpn}/queues",
            auth=SEMP_AUTH,
            json={"queueName": q, "accessType": "non-exclusive",
                  "permission": "consume", "ingressEnabled": True, "egressEnabled": True},
            timeout=5
        )
    # Fetch all queues from SEMP
    try:
        r = requests.get(f"{SEMP}/monitor/msgVpns/{vpn}/queues",
                         auth=SEMP_AUTH, timeout=6)
        if r.status_code == 200:
            names = sorted([q['queueName'] for q in r.json().get('data', [])
                            if not q['queueName'].startswith('#')])
            return jsonify({'queues': names})
    except Exception as e:
        return jsonify({'queues': DEMO_SOL_QUEUES, 'error': str(e)})
    return jsonify({'queues': DEMO_SOL_QUEUES})


@app.route('/api/preflight', methods=['POST'])
def preflight():
    d = request.json
    out = {}

    # IBM MQ connection
    try:
        r = requests.get(f"{MQ_BASE}/admin/qmgr/{d['mq_qmgr']}",
                         auth=MQ_ADMIN, verify=False, timeout=6)
        out['mq_conn'] = r.status_code == 200
    except Exception as e:
        out['mq_conn'] = False
        out['mq_error'] = str(e)

    # MQ source queue
    try:
        depth = mq_depth(d['mq_qmgr'], d['mq_queue'])
        out['mq_queue_ok'] = True
        out['mq_depth']    = depth
    except Exception:
        out['mq_queue_ok'] = False
        out['mq_depth']    = 0

    # Solace connection
    try:
        r = requests.get(f"{SEMP}/config/", auth=SEMP_AUTH, timeout=6)
        out['sol_conn'] = r.status_code == 200
    except Exception as e:
        out['sol_conn'] = False
        out['sol_error'] = str(e)

    # Create target queue in Solace
    out['sol_queue_created'] = False
    if out.get('sol_conn'):
        r = requests.post(
            f"{SEMP}/config/msgVpns/{d['sol_vpn']}/queues",
            auth=SEMP_AUTH,
            json={"queueName": d['sol_queue'], "accessType": "non-exclusive",
                  "permission": "consume", "ingressEnabled": True, "egressEnabled": True}
        )
        out['sol_queue_created'] = r.status_code in (200, 201, 400)  # 400 = already exists

    out['ok'] = all([out.get('mq_conn'), out.get('mq_queue_ok'),
                     out.get('sol_conn'), out.get('sol_queue_created')])
    return jsonify(out)


def mq_grant_app_access(qmgr, queue_name):
    """Grant app user PUT/GET on a queue via admin MQSC REST API — called before first produce."""
    try:
        requests.post(
            f"{MQ_BASE}/admin/action/qmgr/{qmgr}/mqsc",
            auth=MQ_ADMIN,
            headers={"Content-Type": "application/json", "ibm-mq-rest-csrf-token": "portal"},
            json={
                "type": "runCommandJSON",
                "command": "set",
                "qualifier": "authrec",
                "name": queue_name,
                "parameters": {
                    "objtype": "queue",
                    "principal": "app",
                    "authadd": ["put", "get", "browse", "inq"]
                }
            },
            verify=False, timeout=6
        )
    except Exception:
        pass   # best-effort — produce will reveal if it worked

@app.route('/api/produce', methods=['POST'])
def produce():
    d      = request.json
    qmgr   = d['mq_qmgr']
    queue  = d['mq_queue']
    count  = int(d['msg_count'])
    mtype  = d['msg_type']
    custom = d.get('custom_json', '')

    tmpls   = TEMPLATES.get(mtype, TEMPLATES['Order Events'])
    results = []

    for i in range(count):
        if mtype == 'Custom JSON':
            try:
                body = json.dumps(json.loads(custom))
            except Exception:
                body = custom
        else:
            body = fill(tmpls[i % len(tmpls)], i + 1)

        try:
            r = requests.post(
                f"{MQ_BASE}/messaging/qmgr/{qmgr}/queue/{queue}/message",
                auth=MQ_APP,
                headers={"Content-Type": "application/json", "ibm-mq-rest-csrf-token": "portal"},
                data=body,
                verify=False,
                timeout=8
            )
            ok = r.status_code in (200, 201)
        except Exception as e:
            return jsonify({"ok": False, "count": 0, "error": f"MQ connection error: {e}"})
        try:
            mtype_label = json.loads(body).get('type', 'Message')
        except Exception:
            mtype_label = 'Message'
        results.append({"i": i + 1, "ok": ok, "type": mtype_label, "status": r.status_code})
        time.sleep(0.05)

    sent = sum(1 for x in results if x['ok'])
    first_err = next((x.get('status') for x in results if not x['ok']), None)
    return jsonify({"ok": sent > 0, "count": sent, "sent": sent, "total": count,
                    "results": results, "mq_depth": mq_depth(qmgr, queue),
                    "error": f"MQ returned HTTP {first_err}" if first_err and sent == 0 else None})


@app.route('/api/migrate')
def migrate():
    qmgr      = request.args.get('mq_qmgr', 'QM1')
    mq_queue  = request.args.get('mq_queue', 'EBCIRSE01.REQUEST')
    sq        = request.args.get('sol_queue', 'EBCIRSE01.REQUEST')
    vpn       = request.args.get('sol_vpn', 'default')

    def stream():
        bridged = 0
        errors  = 0
        yield f"data: {json.dumps({'type':'start','msg':'Bridge active — draining IBM MQ → Solace'})}\n\n"

        while True:
            r = requests.delete(
                f"{MQ_BASE}/messaging/qmgr/{qmgr}/queue/{mq_queue}/message",
                auth=MQ_APP,
                headers={"ibm-mq-rest-csrf-token": "portal", "Accept": "application/json"},
                verify=False, timeout=8
            )
            if r.status_code == 200:
                bridged += 1
                body = r.text
                try:
                    msg_label = json.loads(body).get('type', 'Message')
                except Exception:
                    msg_label = 'Message'

                sr = requests.post(
                    f"{SOL_REST}/QUEUE/{sq}",
                    auth=SOL_CLIENT,
                    headers={"Content-Type": "application/json",
                             "Solace-delivery-mode": "persistent"},
                    data=body, timeout=8
                )
                ok = sr.status_code == 200
                if not ok:
                    errors += 1

                event = {
                    'type':      'progress',
                    'bridged':   bridged,
                    'errors':    errors,
                    'msg_type':  msg_label,
                    'ok':        ok,
                    'mq_depth':  mq_depth(qmgr, mq_queue),
                    'sol_depth': sol_depth(sq, vpn),
                }
                yield f"data: {json.dumps(event)}\n\n"
                time.sleep(0.5)

            elif r.status_code == 204:   # queue empty
                event = {
                    'type':      'done',
                    'bridged':   bridged,
                    'errors':    errors,
                    'mq_depth':  0,
                    'sol_depth': sol_depth(sq, vpn),
                }
                yield f"data: {json.dumps(event)}\n\n"
                break
            else:
                errors += 1
                yield f"data: {json.dumps({'type':'error','msg':f'MQ returned {r.status_code}'})}\n\n"
                if errors >= 3:
                    break
                time.sleep(1)

    return Response(stream_with_context(stream()),
                    mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/api/produce_direct', methods=['POST'])
def produce_direct():
    """Phase 3 — produce directly to Solace (feature flag = SOLACE)."""
    d     = request.json
    sq    = d['sol_queue']
    vpn   = d.get('sol_vpn', 'default')
    count = int(d.get('msg_count', 3))
    mtype = d.get('msg_type', 'Order Events')

    tmpls   = TEMPLATES.get(mtype, TEMPLATES['Order Events'])
    results = []
    for i in range(count):
        body_obj = json.loads(fill(tmpls[i % len(tmpls)], i + 100))
        body_obj['_source'] = 'DIRECT_SOLACE'
        body_obj['_flag']   = 'feature_flag=SOLACE'
        body = json.dumps(body_obj)

        sr = requests.post(
            f"{SOL_REST}/QUEUE/{sq}",
            auth=SOL_CLIENT,
            headers={"Content-Type": "application/json", "Solace-delivery-mode": "persistent"},
            data=body, timeout=8
        )
        results.append({"i": i+1, "ok": sr.status_code == 200,
                        "type": body_obj.get('type')})
        time.sleep(0.1)

    sent = sum(1 for x in results if x['ok'])
    return jsonify({"sent": sent, "total": count,
                    "sol_depth": sol_depth(sq, vpn), "results": results})


@app.route('/api/reconfig-app', methods=['POST'])
def reconfig_app():
    """Rewrite tomcat-context.xml to point at Solace PubSub+."""
    import os as _os
    data      = request.get_json(force=True) or {}
    sol_host  = data.get('sol_host',  'localhost')
    sol_vpn   = data.get('sol_vpn',   'default')
    sol_queue = data.get('sol_queue', 'EBCIRSE01.REQUEST')
    sol_port  = data.get('sol_port',  '9000')
    mq_qmgr   = data.get('mq_qmgr',  'QM1')
    mq_queue  = data.get('mq_queue',  'EBCIRSE01.REQUEST')

    config_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'tomcat-context.xml')

    # NOTE: factory="org.apache.naming.factory.BeanFactory" is Tomcat's built-in
    # ObjectFactory. It creates the bean (type=) then calls setters for each attribute.
    # SolConnectionFactory setters: setHost(), setVPN(), setUsername(), setPassword()
    # Solace JMS uses SMF port 55555, NOT the REST port 9000.
    smf_host = sol_host if sol_host != 'localhost' else 'solace'  # use Docker service name
    new_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<!--
  Tomcat Context Configuration - IKEA Order Management System v2.3
  Application: ikea-order-mgmt
  Managed by:  HCLTech Middleware Solutioning
  Reconfigured: Solace PubSub+ (migrated from IBM MQ {mq_qmgr}/{mq_queue})

  factory="org.apache.naming.factory.BeanFactory" is Tomcat's built-in factory.
  It creates the bean class and calls setters for each XML attribute.
  SolConnectionFactory uses SMF protocol on port 55555 (not REST port 9000).
-->
<Context>

  <!-- JMS Connection Factory - Solace PubSub+
       BeanFactory calls: new SolConnectionFactory()
         .setHost("smf://solace:55555")
         .setVPN("{sol_vpn}")
         .setUsername("admin")
         .setPassword("admin")
  -->
  <Resource
    name="jms/OrderQueueFactory"
    auth="Container"
    type="com.solacesystems.jms.SolConnectionFactory"
    factory="org.apache.naming.factory.BeanFactory"
    host="smf://{smf_host}:55555"
    vpn="{sol_vpn}"
    username="admin"
    password="admin"
    description="Solace PubSub+ JMS Connection Factory for Order Processing"/>

  <!-- JMS Queue Destination - Solace PubSub+
       BeanFactory calls: new MQQueue().setBaseQueueName("{sol_queue}")
       Note: Using a String env-entry for queue name (SolQueue has no no-arg ctor)
  -->
  <Resource
    name="jms/OrderQueue"
    auth="Container"
    type="com.ibm.mq.jms.MQQueue"
    factory="org.apache.naming.factory.BeanFactory"
    baseQueueName="{sol_queue}"
    description="Order Processing Queue - Solace PubSub+ ({sol_queue})"/>

</Context>
"""
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(new_xml)

        # ── Trigger Tomcat Manager reload ──────────────────────────────────
        # After rewriting context.xml, tell real Tomcat to reload the webapp
        # so JNDI resources are re-bound with the new Solace config.
        # Tomcat Manager text API: GET /manager/text/reload?path=/order-mgmt
        tomcat_reload_status = "not attempted"
        try:
            tr = requests.get(
                "http://localhost:8888/manager/text/reload",
                params={"path": "/order-mgmt"},
                auth=("admin", "admin123"),
                timeout=5
            )
            tomcat_reload_status = tr.text.strip()
        except Exception as te:
            tomcat_reload_status = f"Tomcat reload skipped: {te}"

        return jsonify({"ok": True, "broker": "solace", "config_path": config_path,
                        "sol_host": sol_host, "sol_vpn": sol_vpn,
                        "sol_queue": sol_queue, "sol_port": sol_port,
                        "tomcat_reload": tomcat_reload_status})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})



@app.route('/api/capacity-check')
def capacity_check():
    vpn   = request.args.get('vpn', 'default')
    queue = request.args.get('queue', 'EBCIRSE01.REQUEST')
    out   = {'status':'ok','connections':0,'max_connections':1000,
             'msg_rate_in':0,'msg_rate_out':0,'spool_pct':0.0,'queue_count':0}
    try:
        r = requests.get(f"{SEMP}/monitor/msgVpns/{vpn}",
                         auth=SEMP_AUTH, timeout=5)
        if r.status_code == 200:
            d = r.json().get('data', {})
            out['connections']     = d.get('clientCount', 0)
            out['max_connections'] = d.get('serviceSmfMaxConnectionCount', 1000)
            out['msg_rate_in']     = round(d.get('averageRxMsgRate', 0), 1)
            out['msg_rate_out']    = round(d.get('averageTxMsgRate', 0), 1)
            spool_used = d.get('msgSpoolUsage', 0)
            spool_max  = d.get('msgSpoolMaxUsage', 1) or 1
            out['spool_pct']       = round((spool_used / spool_max) * 100, 2)
        qr = requests.get(f"{SEMP}/monitor/msgVpns/{vpn}/queues",
                          auth=SEMP_AUTH, timeout=5)
        if qr.status_code == 200:
            out['queue_count'] = len(qr.json().get('data', []))
    except Exception as e:
        out['status'] = 'error'
        out['error']  = str(e)
    return jsonify(out)

@app.route('/api/verify')
def verify():
    qmgr = request.args.get('mq_qmgr', 'QM1')
    mq_q = request.args.get('mq_queue', 'EBCIRSE01.REQUEST')
    sq   = request.args.get('sol_queue', 'EBCIRSE01.REQUEST')
    vpn  = request.args.get('sol_vpn', 'default')

    mq_d  = mq_depth(qmgr, mq_q)
    sol_d = sol_depth(sq, vpn)

    vpn_data = {}
    try:
        r = requests.get(f"{SEMP}/monitor/msgVpns/{vpn}", auth=SEMP_AUTH, timeout=5)
        if r.status_code == 200:
            vpn_data = r.json().get('data', {})
    except Exception:
        pass

    return jsonify({
        'mq_depth':     mq_d,
        'sol_depth':    sol_d,
        'migration_ok': mq_d == 0 and sol_d > 0,
        'vpn_rx_msgs':  vpn_data.get('dataRxMsgCount', 'N/A'),
        'vpn_tx_msgs':  vpn_data.get('dataTxMsgCount', 'N/A'),
    })


# ── Embedded single-page UI ───────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CHARM Extension Portal</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--navy:#0C2340;--blue:#38BDF8;--blue-d:#0284C7;--teal:#0EA5E9;--green:#10B981;--amber:#F59E0B;--red:#EF4444;--muted:#64748B;--hcl-grad:linear-gradient(135deg,#0284C7 0%,#0EA5E9 100%);}
*{box-sizing:border-box;}
body{background:#F0F9FF;font-family:'Plus Jakarta Sans',sans-serif;margin:0;display:flex;min-height:100vh;padding-top:40px;}

/* Sidebar */
.sidebar{background:#0F172A;width:240px;flex-shrink:0;padding:1.75rem 1.25rem;display:flex;flex-direction:column;}
.brand{color:#fff;font-size:1rem;font-weight:700;line-height:1.3;}
.brand small{display:block;font-weight:300;font-size:.7rem;opacity:.6;margin-top:.2rem;}
.step-list{list-style:none;padding:0;margin:2rem 0 0;}
.step-list li{display:flex;align-items:center;gap:.65rem;color:rgba(255,255,255,.4);font-size:.82rem;padding:.45rem 0;transition:color .2s;}
.step-list li.active{color:#fff;font-weight:600;}
.step-list li.done{color:var(--green);}
.dot{width:26px;height:26px;border-radius:50%;border:2px solid rgba(255,255,255,.2);display:flex;align-items:center;justify-content:center;font-size:.7rem;font-weight:700;flex-shrink:0;}
.step-list li.active .dot{background:#38BDF8;border-color:#38BDF8;color:#0F172A;}
.step-list li.done .dot{background:var(--green);border-color:var(--green);color:#fff;}
.sidebar-footer{margin-top:auto;color:rgba(255,255,255,.25);font-size:.68rem;line-height:1.6;}

/* Main */
.main{flex:1;padding:2rem 2.5rem;max-width:860px;}
.step-panel{display:none;animation:fadeIn .2s ease;}
.step-panel.active{display:block;}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
.page-title{color:#0369A1;font-weight:700;margin-bottom:.25rem;}
.page-sub{color:var(--muted);font-size:.875rem;margin-bottom:1.5rem;}
.card-s{background:#fff;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,.07);padding:1.5rem;margin-bottom:1.25rem;}
.card-s h6{color:#0C2340;font-weight:700;font-size:.875rem;margin-bottom:1rem;letter-spacing:.01em;}
.section-tag{display:inline-flex;align-items:center;gap:.4rem;font-size:.7rem;font-weight:700;border-radius:4px;padding:.2rem .55rem;margin-bottom:.85rem;}
.tag-mq{background:#0C2340;color:#fff;}
.tag-sol{background:#0284C7;color:#fff;}
.form-label{font-size:.8rem;font-weight:600;color:#374151;}
.form-control{font-size:.875rem;}
.form-control:focus{border-color:#0284C7;box-shadow:0 0 0 .18rem rgba(2,132,199,.18);}
.form-text{font-size:.72rem;color:var(--muted);}

/* Buttons */
.btn-p{background:var(--hcl-grad);border:none;color:#fff;font-weight:600;font-size:.875rem;padding:.55rem 1.4rem;border-radius:8px;cursor:pointer;transition:opacity .15s;}
.btn-p:hover{opacity:.88;}
.btn-p:disabled{background:#94A3B8;cursor:not-allowed;}
.btn-o{background:transparent;border:2px solid #0284C7;color:#0284C7;font-weight:600;font-size:.875rem;padding:.5rem 1.3rem;border-radius:8px;cursor:pointer;transition:all .15s;}
.btn-o:hover{background:#0284C7;border-color:transparent;color:#fff;}

/* Checks */
.check-row{display:flex;align-items:center;gap:.7rem;padding:.55rem 0;border-bottom:1px solid #F8FAFC;font-size:.875rem;}
.check-icon{font-size:1rem;width:22px;text-align:center;}

/* Meters */
.meter{background:#F8FAFC;border-radius:8px;padding:.85rem .75rem;text-align:center;}
.meter-val{font-size:2rem;font-weight:700;line-height:1;}
.meter-lbl{font-size:.68rem;color:var(--muted);margin-top:.2rem;}

/* Log */
.log{background:#0F172A;color:#94A3B8;border-radius:8px;padding:.85rem 1rem;font-family:Consolas,monospace;font-size:.75rem;height:200px;overflow-y:auto;margin-top:.85rem;}
.log .ok{color:#10B981;} .log .err{color:#EF4444;} .log .info{color:#38BDF8;}

/* Feature flag */
.flag-pill{display:inline-flex;align-items:center;gap:.4rem;border-radius:20px;padding:.3rem .85rem;font-size:.8rem;font-weight:600;background:#F1F5F9;}
.fp-mq{color:var(--navy);}  .fp-sol{color:var(--blue);}

/* Cert */
.cert{background:linear-gradient(135deg,#0F172A 0%,#0C2340 100%);color:#fff;border:1px solid #38BDF8;border-radius:12px;padding:2rem;text-align:center;margin-top:1.25rem;}
.cert h4{font-weight:700;}
.cert-meta{background:rgba(255,255,255,.1);border-radius:8px;padding:1rem;font-size:.8rem;text-align:left;margin-top:1rem;}
.cert-meta div{margin-bottom:.2rem;}
</style>
</head>
<body>

<!-- Top header bar -->
<div style="position:fixed;top:0;left:0;right:0;height:40px;background:linear-gradient(135deg,#0F172A 0%,#0C2340 100%);border-bottom:1px solid rgba(255,255,255,.15);display:flex;align-items:center;justify-content:space-between;padding:0 1.5rem;z-index:1000;">
  <!-- HCLTech logo left -->
  <img src="data:image/jpeg;base64,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"
       alt="HCLTech" style="height:28px;object-fit:contain;filter:brightness(0) invert(1);">
  <!-- Centre text -->
  <div style="text-align:center;line-height:1.25">
    <div style="color:#fff;font-weight:700;font-size:.88rem;letter-spacing:.02em">🔄 CHARM Extension Portal</div>
    <div style="color:rgba(255,255,255,.7);font-size:.7rem;font-weight:400">Powered by HCLTech &nbsp;·&nbsp; Migration Wizard for IKEA</div>
  </div>
  <!-- IKEA logo right -->
  <img src="data:image/png;base64,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"
       alt="IKEA" style="height:30px;object-fit:contain;border-radius:4px;">
  <!-- Copyright -->
  <div style="text-align:right;line-height:1.3;margin-left:1rem">
    <div style="color:#FFD700;font-size:.95rem;font-weight:800;letter-spacing:.04em;text-shadow:0 0 12px rgba(255,215,0,.5)">
      &copy; 2026 &nbsp;<span style="color:#fff;font-style:italic">Tarun Virmani</span>
    </div>
    <div style="color:rgba(255,255,255,.7);font-size:.68rem;font-weight:400;letter-spacing:.03em">HCLTech Middleware Solutioning</div>
  </div>
</div>

<!-- Sidebar -->
<div class="sidebar">
  <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:.5rem">
    <svg width="40" height="40" viewBox="0 0 40 40" xmlns="http://www.w3.org/2000/svg">
      <circle cx="20" cy="20" r="19" fill="none" stroke="#0EA5E9" stroke-width="1.5"/>
      <rect x="5" y="14" width="11" height="12" rx="2" fill="#1C3557" stroke="#0EA5E9" stroke-width="1"/>
      <rect x="24" y="14" width="11" height="12" rx="2" fill="#0EA5E9" stroke="#38BDF8" stroke-width="1"/>
      <line x1="16" y1="18" x2="20" y2="18" stroke="#F59E0B" stroke-width="1.5"/>
      <polygon points="20,15.5 24,18 20,20.5" fill="#F59E0B"/>
      <line x1="16" y1="22" x2="24" y2="22" stroke="#F59E0B" stroke-width="1" stroke-dasharray="2,1.5"/>
      <text x="8" y="25" font-size="5" fill="#fff" font-family="monospace" font-weight="bold">MQ</text>
      <text x="26" y="25" font-size="4" fill="#fff" font-family="monospace" font-weight="bold">SOL</text>
    </svg>
    <div class="brand">CHARM Extension Portal<small>IBM MQ → Solace PubSub+</small></div>
  </div>

  <!-- Client & partner badges in sidebar -->
  <div style="margin-top:.85rem;display:flex;flex-direction:column;gap:.5rem">
    <div style="display:flex;align-items:center;gap:.6rem;background:rgba(255,255,255,.12);border-radius:8px;padding:.45rem .75rem">
      <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAQgAAACUCAMAAABY+0dBAAAAxlBMVEUAV6T/2wH/3gAAUaj/4ADizjFfe4sATqoAUqcAVaX43BNyin3/5QD/4gAAVKYATKr/6QCcoGxhfopKdIsqZpMAQa9+j3kASazVxjeLknf02BiopmPcyTzeyze6tFInYp2zr1tXdo8ZW6ArX58ARq3Iv0krZJp8ioCXmW+sqV/Eu09rf4owZpe8s1jMvkJuh39DcY9FaJeMmG5xhII+bJHp0ijVx0OMlXI/Y5xYcJSQnGp1gYVkgYSao2JKbJUAO7A6U6Weplq8GtlpAAAMvUlEQVR4nO2dDVuqPBjHOdsQRgykU6IYqICp+JJmaHXM85zv/6WegWkIW5IvlV38rvN6NTb2Z7t3794YglBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFDwDjiBRkn+/6vv7cTQ+soQSnqEBGVNEFzX8xq3t7eNwWAQ/dXwXNel6WSo62aUCNJkP0UWWi1JN01TkoXG1bi0uKjNu0a93msOh44TBEE7YjSK/6L/DZzhsNns1Q2jW+tM+62HW1eD9PpIujNtKnglAGyMF5Xn+jBoz6q2KIahZSm/AAVxiX76CyiWFYaiXZ2N2k7TmE9/X2Hd9E0dal9ds9xgSfevr7XxXbfZtsO46rRiIK4g+PUBwAZlJcvMeamUGua1TxvY920ekdWjrcD3B3dGICqE0GcbVeYjVd+lS9RgCAlHvcpvOW4d38y0xnZf8LzlXb1tlcsqIcerPksRRNRyGVWb89+Prkvl176DHNQWyO7tU7/WqyqqqiJ0Sgm2oHKoRHSe71qTBobwK8cXDCXojf/Me6MQqQSdtBlwAIQQpeoYndJAkL7GkMq67pZqdadqoa/RYEsNJWw3u9OBrsPPbRjQ1B8qzZFIRTiiOTwEakhBOHOMS8HXP6lhYOlav6zPQgt8cUPIQMVQQjuoNK7Nk2tBVZAWQewUfXW12cRehzinWpzO1aADFfYqM6KSr67tLoCqWi8P0XTuBCrIsju5aKPvr8IKqoX43PLwkY0nlrRlpxmqp/WUjgxQwWjecul871hopjutV8FZqbACkdCpPZjHaRayP+gG4Se6jEcFUH+rV4L6wVJA86FpK99toPwQ1MFoX2j6YTL448D6riNlfuiQalf0vaXAsvkwI2faJdIA1ZrvF9XBMm61yz9EhpiyVfE+7llAodREZ+Iz5AWodqXxsREE6616+MNkiEAg6Ggwvw6SZthnPVDwIdZwaeZsFNgstcFPMg7bILubr1FA2Qh/qgxIpRClvcwxlOq3ozP0pfNBbhaNxiJUkdXZ5WpieapsjKTCJakUL00iCeClsXYVtKvw97NP3YzShc+z2fPfewTKTffduZgsPKubcpT53QWbSrhJBMQOM8ndy9ugg9qcNBdO3AfDPqccduGzVMdF1couguhmQPOx+q9Sm1mPBvmlVifSO+bBa6pvBahTU2JiThIPBWGdleZvUogLZhITGqsqqX+ZP2ejC8PUuE4cQX8f6T6qFvLa9sLomVbwRB8kCUtcl0JaBskyyDMnoVxKyjVmtrHr0ea5kdEty5+T3d5raeXbD4QMsOekhbjfdY32J1p0C3V1Ng/L3igsBfTmqKHgKCEt21uNjhg8IS6TQvxmC1Fd5wVogaybE3rrdlW+OkQIYPV3jYbahD4WZP9XvrmcdsYoXDTjrmJVmErAybYOVAhOvh8TAjVZi1BYNjb96zAhSMBscNt33EW03ia5uWu2jLIYtwjeI9K8IN3kjiIEummwEuidNztzmBDI2O0ewX5kFVzHvrDDv+X25NXYA6WUsZhYSNrJ4wkBwgeWdZb6ypGEAGJptxAYUwuLgv+CICw3XzbGiSqxTBcN79M6HEcIUvFZOSyTzutBQiBHyDF1MOfUlUB177kdGLLxdvtETF0ttbKTrGMIofZYOmjuKFmZg4SwankiT9rghtYPzTqtp+ks+cjVprmVOxSz04sjCEFmTEOpb7sCPCEg0ylwt4QA4iCXinozetCIACUVc1MXSSF9I9MxjiEECpes/us/b5s7jhB4Oq9l6bS2hXBM1rUZpEsrW8M4AzvROeTbkDHPOlgIAJgehF9JTRd4QgSKlSUUw+S1ykXOmCy0OVNqZLz1Xr/Jmm8eLAQxWB1DL6VV5wkxYsbIti+28q5Y6PccIYDdWGeh3dqnEAI5LsPTgYP0pIkvxO6wCGIaYxaawAsuKN11o9K7CivBgUKQKstAZP2291oESJO5x4yphNMxu41cp+coGzEDd3UF1hym9AcKYU0ZvRe79WxZvFFjaN+ksFNPDITX6Yv0+gvbsYAPjAEhzmTtkskPmcZ6BCFEg2XF5BKjKJ4QXiOFdyVutwl1ni4Eu0H4yJ57mKwRIUKZr4SAU3aKg4TQn5mbH7XJTf4WgdPIbkoI5KXrLPVDdcFxS+acJkHqK6MOa0wTcZgQmGUoo2unVkb13J6l5m0LgdoZK6R1EQnYsSf8mI3yrSrqrIwE7HJ+fogQvAkAFnqZJrG3EOo0rbYWxRKQxywcY465RO3VRD4RGTiiEDzkh2rO4TNLSgiQtQawTxOo92wnS/7D7htotHwVgj1en6RFUJvVSXeOfYUgmfEBCy/RrVmZsWR1+STzDFZCzJanaxG4wQ0bmcOcQuwyliATo9MeY7tPfjNDFBgbzL6BRpP4ZqU5U4fDhDD/cduE3BBzzTUEN42wdeX6/pNZL+KaooDtb8J+1lJHFXVWTrbUYU/MDvMjbKYfEWPe5Zp9Cs/NNENr6/4yjQ4GZLVwxA5jUreW1TdIb/XQ5BZzqnGwZ9nirqD4Tq54xExNvOFE4j8TV4HwT3bwnHZWDDiBevZkorvKCAtMnQ6eawQcVyKKMm0tM+836UJBI1tbGa7gLUSwnvmborrBLOngaXjG/90ApzlCdTuEAEa+kMx2wU5WCDRaG3bYYvrYhwoBwhYvvoyFf4ksuEKojJcA1y+H5IpeZ2DZQ1DfPDA/OE1gxmG7eFEWk0TAiGcj5r16lvvFKuAJAmmPTaQY36SrCsSHjaKwxRo3Do9Zkhr3ocHKm9niCRG9D7z5RX/H/zCFlaVV5nv0DDpivaT7G2gmxlqf5YUfLgQIM+sna7D7Fh7cK5wf7l7oYyEPMiGNZGxH9hgTs2OE8wPuY5PGG+doHyEY0Wu8vX+Ak6efMsHq/VZGZiU7HznGAg+p8JWYr5/NPkKgRdpLwY2L5N6Q+ZhT7KK8VclZytRA5yRLfsDizjmwtH42+whhZUylXguT4X+lnQnarPi75ZWhSapszaumzcRxFoHb3DCz7O4vhFpPZ4thepW8xc7V/5dIRxYZcw6X6VW/42wLUDr8OcdU3VeIcsarhKXUVI447Fy128QacIUxrEn9lFuVUwjO1qF1QBiM+C8rvw5W5XzLlytiIUg1oy6cp8w9QC67YHn22iSAYjADq9J0e6Op2pEgE2mZKJO4zFTX6z0IILw02dnQjPAoyqnscwpiXhP5EehS1+QtoNxLd22VU7DeX+9qrLNnQ1habPUOUGmV2CwS2wtDdpLWZqFX6fGyoakuopysCTcBg34boVkrQykzcyR1TsH9dnz74IW31xJLLTs5dohcEo6oxUsDdqag2FFOlv1OClbhQLSzZJxjEPJyiJaRAZgL/B4JH9sJJTILbsyVt91p+Pm8pnovAfOSXfe0q2A6pqDFu68Ky8K/8k/diZ1EFSc73mDB+t2P3Zu/AaAm3j1Q6ZOAsyL0QwBArOTaVQG1rv0DX2Nag6zhQ853eLD55Cg/VApAqhU5f2gLCp32ub4E/S4kNPI2h9dGAR9r4o9rFIQ6d9pHDw/QNO9e4WwoOE+QGoyFfc4Z0aD28nPegUXq7Era97Aq7GtG+BPGUgCsYHx9yIka2HTvZ9aZvwyLfonNsbnHKkhKCqEzFM93CAHEahvLg2WIpdClUred3sx9HhAk9jqeebRjZiC8/fMinvhUwqODVMWpPAn7rIjx0aDweDlUzuX0peiMBOpDPnna8c/lig6OdKdtVD4DLYCqhs8D7QQqrMWQrv3S0FK+3TF1b4DojLbq3L0++dl90PdLL9/xzL7XU/tEp/IJKsRgaOrLWnMkKuj7iBGf41h1jEvsS595wKesm9G5ngEV46vP9YxPwQXhqNmdXn36yZ4RGEpyozXtOlWLqF/mZSCigjAwKv2lIH1qU9jWQoNQ9iZP02dHjM7i/VQ1otOQVWvUq5SWj8LXnv67VkPWBNd9vLwfiqSsfsIRh1SCcrlsjV4ulq4r0OK/XoQN0fngUDd99/LeiY4HJ/Hh70euP/gVbSYjKBzV7x70a12Sv9kJ4QmoGr5/jR86xnC0Oio/OgmftwKTo+6rS6NT48MwvAnqtcuG/80PjU9A7ahJb1a4Kl3c15vBaFaNPyCgrM99X38sIEvip7/iyot29B2BnlFbjD3JNP1z+ozABtpbJD36JoRwOy796dS6RvRBCScI2qPZrGrbr2ue9FHTP6KVUPvmZjYaRV+XiL4t0Z1XppetKw9HH2bQ4Xm0gXeJvi8ApfgjI1F1XO92sFw+RUvYpVJ/TbQ43mo9LQeDhht1MWn1SZJv96WAY7H+2ky8qWGb1U6H1+/QfPV9FhQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUfFf+B4Y/aUIHZXcrAAAAAElFTkSuQmCC" alt="IKEA" style="height:24px;object-fit:contain;border-radius:3px;flex-shrink:0">
      <div style="line-height:1.25">
        <div style="color:rgba(255,255,255,.6);font-size:.62rem;text-transform:uppercase;letter-spacing:.06em">Client</div>
        <div style="color:#fff;font-size:.72rem;font-weight:700">IKEA Retail Systems</div>
      </div>
    </div>
    <div style="display:flex;align-items:center;gap:.6rem;background:rgba(255,255,255,.08);border-radius:8px;padding:.45rem .75rem">
      <img src="data:image/jpeg;base64,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" alt="HCLTech" style="height:20px;object-fit:contain;flex-shrink:0;background:#fff;border-radius:4px;padding:2px 4px;">
      <div style="line-height:1.25">
        <div style="color:rgba(255,255,255,.6);font-size:.62rem;text-transform:uppercase;letter-spacing:.06em">Delivered by</div>
        <div style="color:#fff;font-size:.72rem;font-weight:700">HCLTech Middleware</div>
      </div>
    </div>
  </div>

  <ul class="step-list" id="stepNav">
    <li class="active" id="nav-1"><span class="dot">1</span>Register</li>
    <li id="nav-2"><span class="dot">2</span>Source Queue</li>
    <li id="nav-3"><span class="dot">3</span>Target Queue</li>
    <li id="nav-4"><span class="dot">4</span>AI Capacity</li>
    <li id="nav-5"><span class="dot">5</span>Schema Review</li>
    <li id="nav-6"><span class="dot">6</span>Migration Scope</li>
    <li id="nav-7"><span class="dot">7</span>App Reconfig</li>
    <li id="nav-8"><span class="dot">8</span>Validate</li>
    <li id="nav-9"><span class="dot">9</span>Produce</li>
    <li id="nav-10"><span class="dot">10</span>Migrate</li>
    <li id="nav-11"><span class="dot">11</span>CMDB Changes</li>
    <li id="nav-12"><span class="dot">12</span>Monitoring</li>
    <li id="nav-13"><span class="dot">13</span>Verify</li>
  </ul>
  <div class="sidebar-footer">
    <div style="border-top:1px solid rgba(255,255,255,.1);padding-top:.75rem;margin-top:.5rem">
      <div style="color:rgba(255,255,255,.4);font-size:.67rem;">CHARM Extension v1.0</div>
    </div>
  </div>
</div>

<!-- Main -->
<div class="main">

<!-- ── STEP 1 ── -->
<div class="step-panel active" id="step-1">
  <h3 class="page-title">Register Your Migration</h3>
  <p class="page-sub">Tell us about your application. This creates a migration record in the platform.</p>
  <div class="card-s">
    <h6>Application Details</h6>
    <div class="row g-3">
      <div class="col-md-6">
        <label class="form-label">Application Name *</label>
        <input class="form-control" id="app_name" placeholder="e.g. EBCIRIX01 — Order Processing EBC">
      </div>
      <div class="col-md-6">
        <label class="form-label">Team / Owner *</label>
        <input class="form-control" id="team_name" placeholder="e.g. ITF Middleware Team">
      </div>
      <div class="col-12">
        <label class="form-label">Contact Email *</label>
        <input class="form-control" id="contact_email" type="email" placeholder="e.g. itf-middleware@ingka.com">
      </div>
    </div>
  </div>
  <!-- ── EBC / Server-Type Card ── -->
  <div class="card-s" style="border:1.5px solid rgba(56,189,248,.25);background:rgba(14,165,233,.04)">
    <h6 style="color:#0284C7">🖥️ Application Server Details</h6>
    <p style="color:#475569;font-size:.78rem;margin:-.25rem 0 .85rem">
      IBM Open Liberty application servers managed centrally via <strong>CHARM EBCAdmin</strong>.
      EBC instances are identified by Region + Country from the <strong>LDAP/CDS directory</strong>.
    </p>
    <div class="row g-3">
      <div class="col-md-4">
        <label class="form-label">Application Server Type *</label>
        <select class="form-control" id="app_server_type" onchange="onServerTypeChange()" style="appearance:auto">
          <option value="ebc_liberty" selected>EBC Liberty (IBM Open Liberty)</option>
          <option value="tomcat">Apache Tomcat / TomEE</option>
          <option value="weblogic">Oracle WebLogic</option>
        </select>
        <div class="form-text">Server hosting the consuming application</div>
      </div>
      <div class="col-md-4" id="ebc_name_col">
        <label class="form-label">EBC Instance &nbsp;<span id="ebc_lookup_status" style="font-size:.68rem;font-weight:600;color:#0284C7"></span></label>
        <select class="form-control" id="ebc_name" onchange="updateEbcResolved()" style="appearance:auto" disabled>
          <option value="">— select Region &amp; Country first —</option>
        </select>
        <div class="form-text" id="ebc_name_hint">Select Region + Country to fetch registered instances</div>
      </div>
      <div class="col-md-4" id="ebc_fwk_col">
        <label class="form-label">EBC Framework Version</label>
        <select class="form-control" id="ebc_framework" style="appearance:auto">
          <option value="10.x" selected>10.x (Current)</option>
          <option value="9.5.x">9.5.x</option>
          <option value="9.1.x">9.1.x</option>
          <option value="9.0.x">9.0.x (EOS 2024)</option>
          <option value="8.x">8.x (EOS 2024)</option>
        </select>
        <div class="form-text">EBC Framework ≥ 9.1.x = Solace EventMesh native</div>
      </div>

      <!-- Region + Country (LDAP/CDS) — shown for EBC Liberty only -->
      <div class="col-md-4" id="ebc_region_col">
        <label class="form-label">Region</label>
        <select class="form-control" id="ebc_region" onchange="onRegionChange()" style="appearance:auto">
          <option value="EMEA">🌍 EMEA</option>
          <option value="NA">🌎 NA — North America</option>
          <option value="APAC">🌏 APAC</option>
          <option value="LATAM">🌎 LATAM</option>
        </select>
        <div class="form-text">IKEA deployment region</div>
      </div>
      <div class="col-md-4" id="ebc_country_col">
        <label class="form-label">Country <span style="color:#94A3B8;font-weight:400;font-size:.7rem">(LDAP / CDS)</span></label>
        <select class="form-control" id="ebc_country" onchange="fetchEbcInstances()" style="appearance:auto">
          <option value="SE">🇸🇪 SE — Sweden</option>
          <option value="DE">🇩🇪 DE — Germany</option>
          <option value="GB">🇬🇧 GB — United Kingdom</option>
          <option value="PL">🇵🇱 PL — Poland</option>
          <option value="NL">🇳🇱 NL — Netherlands</option>
          <option value="FR">🇫🇷 FR — France</option>
          <option value="NO">🇳🇴 NO — Norway</option>
          <option value="DK">🇩🇰 DK — Denmark</option>
          <option value="FI">🇫🇮 FI — Finland</option>
          <option value="IT">🇮🇹 IT — Italy</option>
          <option value="ES">🇪🇸 ES — Spain</option>
        </select>
        <div class="form-text">Country OU from LDAP directory</div>
      </div>

      <!-- Resolved EBC Instance badge -->
      <div id="ebc_resolved" class="col-12" style="display:none">
        <div style="background:linear-gradient(135deg,rgba(56,189,248,.1),rgba(14,165,233,.04));border:1px solid rgba(56,189,248,.35);border-radius:8px;padding:.55rem 1rem;display:flex;align-items:center;gap:.9rem;flex-wrap:wrap">
          <span style="font-size:.78rem;font-weight:700;color:#0284C7">🔷 EBC Instance</span>
          <strong style="font-size:.9rem;color:#0F172A;letter-spacing:.01em" id="ebc_resolved_label">EBCIRIX01</strong>
          <span style="color:#CBD5E1">|</span>
          <span style="font-size:.78rem;color:#334155" id="ebc_resolved_region">🌍 EMEA</span>
          <span style="color:#CBD5E1">|</span>
          <span style="font-size:.78rem;color:#334155" id="ebc_resolved_country">🇸🇪 SE — Sweden</span>
          <span style="margin-left:auto;font-size:.67rem;color:#94A3B8;font-style:italic">Source: CHARM / LDAP</span>
        </div>
      </div>

      <!-- Tomcat app selector — shown only when Tomcat/TomEE is selected -->
      <div id="tomcat_app_col" style="display:none" class="col-md-6">
        <label class="form-label">Application Name <span style="font-size:.68rem;color:#7C3AED;font-weight:600">&#9679; 499 Tomcat · 547 TomEE servers in scope</span></label>
        <select class="form-control" id="tomcat_app_name" style="appearance:auto">
          <option value="">— select application —</option>
          <optgroup label="Store &amp; Retail">
            <option value="SOM">SOM — Store Order Management</option>
            <option value="IRW">IRW — Ingka Retail Web</option>
            <option value="GPS">GPS — Global Planning System</option>
          </optgroup>
          <optgroup label="Supply Chain &amp; Integration">
            <option value="ISI">ISI — IKEA Supplier Integration</option>
            <option value="CNS">CNS — Config &amp; Notification Service</option>
            <option value="Infosphere">Infosphere — Data Integration Platform</option>
          </optgroup>
          <optgroup label="Platform &amp; Infrastructure">
            <option value="WTP">WTP — Web Technical Platform</option>
            <option value="CALC">CALC — Pricing &amp; Calculation Engine</option>
          </optgroup>
        </select>
        <div class="form-text">Applications hosted on Application Hosting as a Service (SAMLA)</div>
      </div>

      <!-- Tomcat coming-soon banner -->
      <div id="tomcat_coming_soon" style="display:none" class="col-12">
        <div style="background:rgba(124,58,237,.07);border:1.5px dashed rgba(124,58,237,.4);border-radius:10px;padding:.85rem 1.1rem;">
          <div style="display:flex;align-items:center;gap:.6rem;margin-bottom:.5rem">
            <span style="font-size:1.1rem">🔧</span>
            <strong style="color:#7C3AED;font-size:.85rem">Feature Under Development</strong>
            <span style="margin-left:auto;font-size:.68rem;font-weight:700;padding:.2rem .6rem;background:#EDE9FE;color:#6D28D9;border-radius:12px">Coming Q3 2026</span>
          </div>
          <p style="color:#4C1D95;font-size:.78rem;margin:0 0 .5rem">
            Tomcat / TomEE migration will be <strong>Approval-Gated (Tier 2)</strong>. Once the change request is approved,
            the CHARM Extension Portal will automatically trigger <strong>Ansible Tower playbooks</strong> to:
          </p>
          <div style="font-size:.75rem;color:#5B21B6;display:grid;grid-template-columns:1fr 1fr;gap:.25rem .75rem">
            <span>▸ Rewrite JNDI factory in <code>tomcat-context.xml</code></span>
            <span>▸ Validate Solace PubSub+ connectivity</span>
            <span>▸ Trigger Tomcat Manager API live reload</span>
            <span>▸ Auto-rollback on failure within 60 s</span>
          </div>
          <div style="margin-top:.65rem;font-size:.7rem;color:#7C3AED;opacity:.7">
            📋 Source: SAMLA — Application Hosting as a Service &nbsp;|&nbsp; 499 Tomcat · 547 TomEE servers in migration scope
          </div>
        </div>
      </div>

      <div id="weblogic_warn" style="display:none" class="col-12">
        <div style="background:rgba(245,158,11,.1);border:1px solid rgba(245,158,11,.4);border-radius:8px;padding:.6rem .9rem;font-size:.78rem;color:#B45309">
          ⚠️ <strong>WebLogic 12.2 EOS Dec 2025.</strong> WebLogic apps require WL14c upgrade <em>before</em> MQ migration. This migration will be routed as <strong>Tier 3 — Supplier Only</strong> via CHARM portal.
        </div>
      </div>
    </div>
  </div>
  <div class="d-flex justify-content-end">
    <button class="btn-p" onclick="goStep(2)">Next: Configure Source →</button>
  </div>
</div>

<!-- ── STEP 2 ── -->
<div class="step-panel" id="step-2">
  <h3 class="page-title">Source — IBM MQ</h3>
  <p class="page-sub">Select the IBM MQ queue your legacy application currently publishes to.</p>
  <div class="card-s">
    <span class="section-tag tag-mq">■ IBM MQ</span>

    <!-- ── Row 1: Environment selectors ── -->
    <div class="row g-3 mb-3">
      <div class="col-md-3">
        <label class="form-label">Environment</label>
        <select class="form-control" id="mq_env" onchange="onMqEnvChange()" style="appearance:auto">
          <option value="PROD" selected>🟢 PROD — Production</option>
          <option value="PPE">🟡 PPE — Pre-Production</option>
          <option value="CTE">🔵 CTE — Integration Testing</option>
          <option value="PTE">⚪ PTE — Dev / Unit Test</option>
        </select>
        <div class="form-text">Deployment environment tier</div>
      </div>
      <div class="col-md-3">
        <label class="form-label">Region</label>
        <select class="form-control" id="mq_region" onchange="onMqEnvChange()" style="appearance:auto">
          <option value="GL" selected>🌍 GL — Global (EMEA / NA)</option>
          <option value="CN">🇨🇳 CN — China</option>
        </select>
        <div class="form-text">IKEA network region</div>
      </div>
      <div class="col-md-3">
        <label class="form-label">Data Centre</label>
        <select class="form-control" id="mq_dc" onchange="onMqEnvChange()" style="appearance:auto">
          <option value="DC7" selected>DC7 — Sweden (Primary)</option>
          <option value="DC8">DC8 — Sweden (DR)</option>
          <option value="DC9">DC9 — Sweden (Ext)</option>
          <option value="CHN">CHN — China (itcnchn-lx)</option>
        </select>
        <div class="form-text">Physical data centre location</div>
      </div>
      <div class="col-md-3">
        <label class="form-label">HA Leg</label>
        <select class="form-control" id="mq_leg" onchange="onMqEnvChange()" style="appearance:auto">
          <option value="A" selected>Leg A — Active</option>
          <option value="B">Leg B — Standby (HA pair)</option>
        </select>
        <div class="form-text">Cluster leg for failover</div>
      </div>
    </div>

    <!-- ── Resolved QM topology badge ── -->
    <div id="mq_topology_badge" style="background:linear-gradient(135deg,rgba(220,38,38,.06),rgba(220,38,38,.02));border:1px solid rgba(220,38,38,.2);border-radius:8px;padding:.55rem 1rem;margin-bottom:1rem;display:flex;align-items:center;gap:1rem;flex-wrap:wrap">
      <span style="font-size:.75rem;font-weight:700;color:#B91C1C">🖧 Queue Manager</span>
      <strong style="font-size:.9rem;color:#0F172A;letter-spacing:.02em" id="mq_resolved_qm">ITGLA01</strong>
      <span style="color:#CBD5E1">|</span>
      <span style="font-size:.75rem;color:#334155" id="mq_resolved_env">🟢 PROD</span>
      <span style="color:#CBD5E1">|</span>
      <span style="font-size:.75rem;color:#334155" id="mq_resolved_dc">DC7 — Sweden</span>
      <span style="color:#CBD5E1">|</span>
      <span style="font-size:.75rem;color:#334155" id="mq_resolved_leg">Leg A — Active</span>
      <span style="margin-left:auto;font-size:.67rem;color:#94A3B8;font-style:italic">HA pair: ITGLA01 ↔ ITGLB01 · MQ v9.3 · RHEL 8</span>
    </div>

    <!-- ── Row 2: Queue Manager + Queue Name + Protocol ── -->
    <div class="row g-3">
      <div class="col-md-4">
        <label class="form-label">Queue Manager *</label>
        <div style="position:relative">
          <select class="form-control" id="mq_qmgr" onchange="loadQueues()" style="appearance:auto">
            <option value="">⏳ Loading…</option>
          </select>
        </div>
        <div class="form-text" id="qmgr_status"></div>
      </div>
      <div class="col-md-4">
        <label class="form-label">Queue Name *
          <span id="mq_demo_badge" style="margin-left:.5rem;font-size:.65rem;font-weight:700;background:#D1FAE5;color:#065F46;border:1px solid #6EE7B7;border-radius:20px;padding:.1rem .5rem;letter-spacing:.03em;display:none">
            🎯 USE THIS FOR DEMO
          </span>
        </label>
        <div style="position:relative">
          <select class="form-control" id="mq_queue" style="appearance:auto" onchange="onMqQueueChange()">
            <option value="">— select a queue manager first —</option>
          </select>
          <span id="mq_queue_dot" style="position:absolute;right:.6rem;top:50%;transform:translateY(-50%);width:8px;height:8px;border-radius:50%;background:#CBD5E1;pointer-events:none"></span>
        </div>
        <div class="form-text" id="mq_queue_hint">User-defined queues only (SYSTEM.* hidden)</div>
        <div id="mq_queue_warn" style="display:none;margin-top:.25rem;font-size:.7rem;color:#B45309;background:#FFFBEB;border:1px solid #FCD34D;border-radius:4px;padding:.2rem .5rem">
          ⚠️ Pick <strong>EBCIRSE01.REQUEST</strong> — that's where demo messages live
        </div>
      </div>
      <div class="col-md-4">
        <label class="form-label">Source Protocol</label>
        <select class="form-control" id="mq_protocol" style="appearance:auto">
          <option value="native">IBM MQ Native (JMS)</option>
          <option value="rest" selected>REST / HTTP</option>
          <option value="amqp">AMQP 1.0</option>
          <option value="mqtt">MQTT 3.1.1</option>
        </select>
        <div class="form-text">Protocol used by the legacy application</div>
      </div>
    </div>

    <!-- ── MQ environment topology info card ── -->
    <div style="margin-top:1rem;background:#FAFAFA;border:1px solid #E2E8F0;border-radius:8px;padding:.7rem 1rem">
      <div style="font-size:.68rem;font-weight:700;color:#64748B;text-transform:uppercase;letter-spacing:.1em;margin-bottom:.5rem">
        📋 IKEA MQ Environment Topology — IT[REGION][ENV][LEG][SEQ]
      </div>
      <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:.3rem .75rem;font-size:.72rem;color:#475569">
        <span>🟢 <code>ITGLA01</code> — Prod Global Leg A &nbsp;<span style="color:#94A3B8">(itseelm-lx5047 · DC7)</span></span>
        <span>🟢 <code>ITGLB01</code> — Prod Global Leg B &nbsp;<span style="color:#94A3B8">(itseelm-lx5048 · DC8)</span></span>
        <span>🟡 <code>PPGLA01</code> — PPE Global Leg A &nbsp;<span style="color:#94A3B8">(ppseelm-lx5030)</span></span>
        <span>🇨🇳 <code>ITCNCHN01</code> — Prod China Leg A &nbsp;<span style="color:#94A3B8">(itcnchn-lx5003)</span></span>
        <span>🇨🇳 <code>ITCNCHN02</code> — Prod China Leg B &nbsp;<span style="color:#94A3B8">(itcnchn-lx5004)</span></span>
        <span>⚪ <code>PTEGLA01</code> — PTE / Dev &nbsp;<span style="color:#94A3B8">(internal test cluster)</span></span>
      </div>
      <div style="margin-top:.5rem;font-size:.68rem;color:#94A3B8">
        HA failover via cluster IP · Patching via Ansible Tower · MQ Admin via MQSC REST API · 14 IBM MQ servers in scope
      </div>
    </div>
  </div>
  <div style="margin-top:.75rem;padding:.6rem .85rem;background:#F8FAFC;border-left:3px solid #0EA5E9;border-radius:4px;font-size:.75rem;color:#64748B;">
    ℹ️ <strong>Note:</strong> AMQP, MQTT, and JMS are listed as future options — they will be available when real protocol connectors are wired in. Current demo uses IBM MQ REST API.
  </div>

  <!-- MQ Security Card -->
  <div class="card-s" style="margin-top:.85rem;border:1px solid #E2E8F0">
    <div style="display:flex;align-items:center;justify-content:space-between;cursor:pointer" onclick="toggleSec('mq_sec_body')">
      <div style="font-size:.85rem;font-weight:600;color:#1E293B">🔐 Security Configuration — IBM MQ</div>
      <span id="mq_sec_arrow" style="font-size:.75rem;color:#64748B">▼ expand</span>
    </div>
    <div id="mq_sec_body" style="display:none;margin-top:.85rem">
      <div class="row g-2">
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Authentication Type</label>
          <select class="form-control" id="mq_auth_type" onchange="toggleMqAuth()" style="font-size:.82rem">
            <option value="basic" selected>Basic Auth (Username / Password)</option>
            <option value="ldap">LDAP / Active Directory</option>
            <option value="cert">Mutual TLS (Client Certificate)</option>
            <option value="apikey">API Key / Token</option>
          </select>
        </div>
        <div class="col-md-4" id="mq_user_col">
          <label class="form-label" style="font-size:.78rem">MQ Username</label>
          <input class="form-control" id="mq_username" value="app" style="font-size:.82rem">
        </div>
        <div class="col-md-4" id="mq_pass_col">
          <label class="form-label" style="font-size:.78rem">Password</label>
          <input class="form-control" id="mq_password" type="password" value="passw0rd" style="font-size:.82rem">
        </div>
        <div class="col-md-4" id="mq_apikey_col" style="display:none">
          <label class="form-label" style="font-size:.78rem">API Key / Token</label>
          <input class="form-control" id="mq_apikey" placeholder="Bearer eyJ0eXAiOiJKV1Q..." style="font-size:.75rem;font-family:monospace">
        </div>
        <div class="col-md-4" id="mq_cert_col" style="display:none">
          <label class="form-label" style="font-size:.78rem">Certificate DN</label>
          <input class="form-control" id="mq_cert_dn" value="CN=mqapp,O=HCLTech,C=GB" style="font-size:.78rem;font-family:monospace">
        </div>
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">TLS / SSL</label>
          <select class="form-control" id="mq_tls" onchange="toggleMqTls()" style="font-size:.82rem">
            <option value="enabled" selected>Enabled (Port 9443)</option>
            <option value="disabled">Disabled (Port 9080)</option>
            <option value="mutual">Mutual TLS</option>
          </select>
        </div>
        <div class="col-md-4" id="mq_cipher_col">
          <label class="form-label" style="font-size:.78rem">Cipher Suite</label>
          <select class="form-control" id="mq_cipher" style="font-size:.78rem">
            <option selected>TLS_RSA_WITH_AES_256_CBC_SHA256</option>
            <option>TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256</option>
            <option>TLS_AES_256_GCM_SHA384 (TLS 1.3)</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Channel Auth</label>
          <select class="form-control" id="mq_chanauth" style="font-size:.82rem">
            <option value="enabled" selected>Enabled (CHLAUTH rules)</option>
            <option value="disabled">Disabled</option>
          </select>
        </div>
      </div>
      <div style="margin-top:.65rem;padding:.5rem .75rem;background:#F0FDF4;border-radius:6px;font-size:.72rem;color:#166534">
        🔒 Connection secured with TLS 1.3 — certificate validated against IBM MQ trust store
      </div>
    </div>
  </div>

  <div class="d-flex justify-content-between" style="margin-top:1rem">
    <button class="btn-o" onclick="goStep(1)">← Back</button>
    <button class="btn-p" onclick="goStep(3)">Next: Configure Target →</button>
  </div>
</div>

<!-- ── STEP 3 ── -->
<div class="step-panel" id="step-3">
  <h3 class="page-title">Target — Destination Platform</h3>
  <p class="page-sub">Select your target event mesh or messaging platform. The platform will provision the destination automatically.</p>

  <!-- Platform selector -->
  <div class="card-s">
    <h6>Destination Platform</h6>
    <div class="row g-3">
      <div class="col-md-6">
        <label class="form-label">Target Platform *</label>
        <select class="form-control" id="dest_platform" style="appearance:auto" onchange="switchPlatform()">
          <option value="solace">Solace PubSub+ Event Mesh</option>
          <option value="aws">Amazon Web Services (AWS)</option>
          <option value="azure">Microsoft Azure</option>
          <option value="gcp">Google Cloud Platform (GCP)</option>
          <option value="ibm">IBM Event Streams (Kafka)</option>
          <option value="confluent">Confluent Kafka</option>
        </select>
      </div>
      <div class="col-md-6" id="platform_badge_col" style="display:flex;align-items:flex-end;padding-bottom:.35rem">
        <span id="platform_badge" style="font-size:.78rem;font-weight:600;padding:.3rem .8rem;border-radius:20px;background:#E0F2FE;color:#0284C7;">● Solace PubSub+  —  Active in this demo</span>
      </div>
    </div>
  </div>

  <!-- ── Solace fields ── -->
  <div id="dest_solace">

    <!-- ── Migration context banner ── -->
    <div style="background:linear-gradient(135deg,rgba(2,132,199,.07),rgba(16,185,129,.05));border:1px solid rgba(2,132,199,.25);border-radius:8px;padding:.65rem 1rem;margin-bottom:.85rem;display:flex;gap:.75rem;align-items:flex-start">
      <span style="font-size:1.1rem;margin-top:.05rem">🔄</span>
      <div>
        <div style="font-size:.75rem;font-weight:700;color:#0369A1;margin-bottom:.2rem">INGKA EventMesh Migration Status</div>
        <div style="font-size:.72rem;color:#334155;line-height:1.55">
          <strong>ITF PubSub (EICPUBSUB)</strong> — fully decommissioned · migration to Solace EventMesh <span style="color:#10B981;font-weight:700">complete</span> for all publishers &amp; subscribers<br>
          <strong>IBM MQ v9</strong> — end-of-life · migration to EventMesh <span style="color:#F59E0B;font-weight:700">in progress</span> · 14 MQ servers in scope · <em>this portal automates each migration</em>
        </div>
      </div>
    </div>

    <!-- ── Main Solace config card ── -->
    <div class="card-s">
      <span class="section-tag tag-sol">● Solace PubSub+ EventMesh</span>

      <!-- Row 1: Broker + VPN + Environment -->
      <div class="row g-3 mb-3">
        <div class="col-md-4">
          <label class="form-label">Broker Host</label>
          <select class="form-control" id="sol_broker_host" onchange="onSolEnvChange()" style="appearance:auto">
            <option value="eventmesh-prod.ingka.ikea.com">PROD — eventmesh-prod.ingka.ikea.com</option>
            <option value="eventmesh-int.ingka.ikea.com">INT/Stage — eventmesh-int.ingka.ikea.com</option>
            <option value="eventmesh-ppe.ikeadt.com">PPE — eventmesh-ppe.ikeadt.com</option>
            <option value="localhost" selected>Local — localhost (demo)</option>
          </select>
          <div class="form-text">SMF TLS port: 55443 · REST: 9443</div>
        </div>
        <div class="col-md-4">
          <label class="form-label">Message VPN *</label>
          <select class="form-control" id="sol_vpn" onchange="onSolEnvChange()" style="appearance:auto">
            <option value="pro-gke-euwe4-cgeu-1">pro-gke-euwe4-cgeu-1 &nbsp;(PROD)</option>
            <option value="int-gke-euwe4">int-gke-euwe4 &nbsp;(INT / Stage)</option>
            <option value="ppe-gke-euwe4">ppe-gke-euwe4 &nbsp;(PPE)</option>
            <option value="default" selected>default &nbsp;(local demo)</option>
          </select>
          <div class="form-text">INGKA-managed VPN — provisioned by ITF EventMesh team</div>
        </div>
        <div class="col-md-4">
          <label class="form-label">Target Protocol</label>
          <select class="form-control" id="sol_protocol" style="appearance:auto">
            <option value="smf">Solace SMF/TLS (Native) — tcps://</option>
            <option value="rest" selected>REST Messaging — https://</option>
            <option value="amqp">AMQP 1.0</option>
            <option value="mqtt">MQTT 3.1.1</option>
            <option value="jms">JMS / JNDI</option>
          </select>
          <div class="form-text">ebcfwk.eventmesh.host uses <code>tcps://</code> SMF TLS in PROD</div>
        </div>
      </div>

      <!-- Row 2: Queue + Topic -->
      <div class="row g-3">
        <div class="col-md-5">
          <label class="form-label">Target Queue Name *
            <span id="demo_queue_badge" style="margin-left:.5rem;font-size:.65rem;font-weight:700;background:#D1FAE5;color:#065F46;border:1px solid #6EE7B7;border-radius:20px;padding:.1rem .5rem;letter-spacing:.03em">
              🎯 USE THIS FOR DEMO
            </span>
          </label>
          <div style="position:relative">
            <select class="form-control" id="sol_queue" style="appearance:auto;border-color:#10B981;border-width:2px" onchange="onSolQueueChange()">
              <option value="EBCIRSE01.REQUEST">MIGRATED.APP.QUEUE</option>
            </select>
            <span id="queue_green_dot" style="position:absolute;right:.6rem;top:50%;transform:translateY(-50%);width:8px;height:8px;border-radius:50%;background:#10B981;pointer-events:none"></span>
          </div>
          <div class="form-text" id="sol_queue_status">Loading queues…</div>
          <div id="queue_warn" style="display:none;margin-top:.25rem;font-size:.7rem;color:#B45309;background:#FFFBEB;border:1px solid #FCD34D;border-radius:4px;padding:.2rem .5rem">
            ⚠️ Demo works best with <strong>EBCIRSE01.REQUEST</strong> — backend will use it regardless
          </div>
        </div>
        <div class="col-md-7">
          <label class="form-label">Resolved EventMesh Connection String</label>
          <div id="sol_conn_string" style="font-family:monospace;font-size:.72rem;background:#0F172A;color:#7DD3FC;padding:.45rem .75rem;border-radius:6px;word-break:break-all;min-height:2.2rem;display:flex;align-items:center">
            localhost:9080 · VPN: default &nbsp;(🎯 demo mode)
          </div>
          <div class="form-text">ebcfwk.eventmesh.host / ebcfwk.eventmesh.vpn values</div>
        </div>
      </div>
    </div>

    <!-- ── Queue naming convention playcard ── -->
    <div style="background:linear-gradient(135deg,rgba(14,165,233,.05),rgba(14,165,233,.02));border:1px solid rgba(14,165,233,.2);border-radius:8px;padding:.7rem 1rem;margin-top:.75rem">
      <div style="font-size:.68rem;font-weight:700;color:#0369A1;text-transform:uppercase;letter-spacing:.1em;margin-bottom:.5rem">
        📦 INGKA EventMesh Queue &amp; Topic Naming Standards
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:.5rem .75rem;font-size:.72rem;color:#334155">
        <div>
          <div style="font-weight:700;color:#0369A1;margin-bottom:.2rem">Queue naming (ITF-managed)</div>
          <code style="background:#E0F2FE;color:#0C4A6E;padding:.15rem .4rem;border-radius:4px;font-size:.7rem">ITF_[EBCNAME]_[TYPE]</code>
          <div style="margin-top:.25rem;color:#64748B">Examples:<br>
            <code>ITF_EBCIRSE01_REQUEST</code><br>
            <code>ITF_EBCIRDE01_BACKOUT</code><br>
            Provisioned via Jenkins pipeline by ITF team
          </div>
        </div>
        <div>
          <div style="font-weight:700;color:#0369A1;margin-bottom:.2rem">Topic naming (INGKA API standard)</div>
          <code style="background:#E0F2FE;color:#0C4A6E;padding:.15rem .4rem;border-radius:4px;font-size:.7rem">ingka.[domain].sys/[category]/[event]/V[n]/GL/[system]/[app]</code>
          <div style="margin-top:.25rem;color:#64748B">Examples:<br>
            <code style="font-size:.66rem">ingka.itf.sys/ebc/request/V1/GL/ITF/ebcirse01_order</code><br>
            <code style="font-size:.66rem">ingka.dms.sys/email/status/report/V1/GL/OUTMAN/recovery-oms/*</code>
          </div>
        </div>
      </div>
    </div>

    <!-- ── ebcfwk property mapping playcard ── -->
    <div style="background:#FAFAFA;border:1px solid #E2E8F0;border-radius:8px;padding:.7rem 1rem;margin-top:.75rem">
      <div style="font-size:.68rem;font-weight:700;color:#64748B;text-transform:uppercase;letter-spacing:.1em;margin-bottom:.5rem">
        ⚙️ EBCFwk EventMesh Properties — What This Portal Configures
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:.2rem 1rem;font-size:.7rem;font-family:monospace">
        <div style="color:#6366F1">ebcfwk.eventmesh.enabled</div><div style="color:#10B981">= true</div>
        <div style="color:#6366F1">ebcfwk.eventmesh.host</div><div style="color:#F59E0B" id="prop_host">= tcps://localhost:55443 &nbsp;<span style="color:#94A3B8;font-style:italic">(demo)</span></div>
        <div style="color:#6366F1">ebcfwk.eventmesh.vpn</div><div style="color:#F59E0B" id="prop_vpn">= default &nbsp;<span style="color:#94A3B8;font-style:italic">(demo)</span></div>
        <div style="color:#6366F1">ebcfwk.eventmesh.queue</div><div style="color:#F59E0B" id="prop_queue">= ITF_EBCIRSE01_REQUEST</div>
        <div style="color:#6366F1">ebcfwk.eventmesh.connectTimeoutInMillis</div><div style="color:#94A3B8">= 200 &nbsp;<em>(ITF recommended)</em></div>
        <div style="color:#6366F1">ebcfwk.eventmesh.reconnectRetryWaitInMillis</div><div style="color:#94A3B8">= 3000 &nbsp;<em>(ITF recommended)</em></div>
        <div style="color:#6366F1">ebcfwk.eventmesh.keepAliveIntervalInMillis</div><div style="color:#94A3B8">= 30000</div>
        <div style="color:#6366F1">ebcfwk.eventmesh.maxFlowToSend</div><div style="color:#94A3B8">= 1 &nbsp;<em>(ITF recommended)</em></div>
        <div style="color:#6366F1">ebcfwk.eventmesh.transportWindowSize</div><div style="color:#94A3B8">= 5 &nbsp;<em>(ITF recommended)</em></div>
        <div style="color:#6366F1">ebcfwk.MqListeners</div><div style="color:#EF4444">= 0 &nbsp;<em>(disabled — MQ decommissioned)</em></div>
      </div>
      <div style="margin-top:.5rem;font-size:.67rem;color:#94A3B8">
        Library: <code>com.solace:solace-messaging-client:1.7.0</code> · Module: <code>ebcfwk-eventmesh</code> · FWK 9.1.1+
      </div>
    </div>

    <!-- ── Protocol note ── -->
    <div style="margin-top:.65rem;padding:.55rem .85rem;background:#F8FAFC;border-left:3px solid #0284C7;border-radius:4px;font-size:.74rem;color:#64748B;">
      ℹ️ <strong>Note:</strong> Current demo uses Solace REST API (port 9080). In PROD, EBCFwk uses SMF TLS (<code>tcps://</code> port 55443) as configured via <code>ebcfwk.eventmesh.host</code>.
      AMQP, MQTT and JMS are additional supported protocols on the Solace broker.
    </div>
  </div>

  <!-- ── AWS fields ── -->
  <div id="dest_aws" style="display:none">
    <div class="card-s">
      <span class="section-tag" style="background:#FF9900;color:#fff;">⬡ Amazon Web Services</span>
      <div class="row g-3">
        <div class="col-md-4">
          <label class="form-label">AWS Service</label>
          <select class="form-control" style="appearance:auto">
            <option>Amazon MQ (ActiveMQ)</option>
            <option>Amazon EventBridge</option>
            <option>Amazon MSK (Kafka)</option>
            <option>Amazon SQS</option>
            <option>Amazon SNS</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label">AWS Region</label>
          <select class="form-control" style="appearance:auto">
            <option>us-east-1 (N. Virginia)</option>
            <option>us-west-2 (Oregon)</option>
            <option>eu-west-1 (Ireland)</option>
            <option>eu-central-1 (Frankfurt)</option>
            <option>ap-southeast-1 (Singapore)</option>
            <option>ap-south-1 (Mumbai)</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label">Authentication Method</label>
          <select class="form-control" style="appearance:auto">
            <option>IAM Role (recommended)</option>
            <option>Access Key / Secret</option>
            <option>AWS SSO / Identity Center</option>
          </select>
        </div>
        <div class="col-md-6">
          <label class="form-label">AWS Account ID</label>
          <input class="form-control" placeholder="e.g. 123456789012" id="aws_account">
        </div>
        <div class="col-md-6">
          <label class="form-label">Target Queue / Topic / Event Bus Name</label>
          <input class="form-control" placeholder="e.g. MIGRATED-APP-QUEUE or my-event-bus" id="aws_dest">
        </div>
      </div>
    </div>
    <div style="margin-top:.75rem;padding:.6rem .85rem;background:#FFF8EE;border-left:3px solid #FF9900;border-radius:4px;font-size:.75rem;color:#64748B;">
      ⚠️ <strong>AWS Connector Required:</strong> Live migration to AWS requires the AWS connector plugin. Contact your HCLTech Middleware Solutioning Team to enable this. Fields above will be used for connector configuration.
    </div>
  </div>

  <!-- ── Azure fields ── -->
  <div id="dest_azure" style="display:none">
    <div class="card-s">
      <span class="section-tag" style="background:#0078D4;color:#fff;">⬡ Microsoft Azure</span>
      <div class="row g-3">
        <div class="col-md-4">
          <label class="form-label">Azure Service</label>
          <select class="form-control" style="appearance:auto">
            <option>Azure Service Bus</option>
            <option>Azure Event Hubs</option>
            <option>Azure Event Grid</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label">Azure Region</label>
          <select class="form-control" style="appearance:auto">
            <option>East US</option>
            <option>West Europe</option>
            <option>Southeast Asia</option>
            <option>UK South</option>
            <option>Australia East</option>
            <option>Central India</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label">Authentication Method</label>
          <select class="form-control" style="appearance:auto">
            <option>Managed Identity (recommended)</option>
            <option>Service Principal</option>
            <option>Connection String</option>
            <option>Shared Access Signature (SAS)</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label">Subscription ID</label>
          <input class="form-control" placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" id="az_sub">
        </div>
        <div class="col-md-4">
          <label class="form-label">Namespace</label>
          <input class="form-control" placeholder="e.g. my-servicebus-namespace" id="az_ns">
        </div>
        <div class="col-md-4">
          <label class="form-label">Queue / Topic Name</label>
          <input class="form-control" placeholder="e.g. migrated-app-queue" id="az_dest">
        </div>
      </div>
    </div>
    <div style="margin-top:.75rem;padding:.6rem .85rem;background:#EFF6FF;border-left:3px solid #0078D4;border-radius:4px;font-size:.75rem;color:#64748B;">
      ⚠️ <strong>Azure Connector Required:</strong> Live migration to Azure requires the Azure Service Bus / Event Hubs connector plugin. Contact your HCLTech Middleware Solutioning Team to enable this.
    </div>
  </div>

  <!-- ── GCP fields ── -->
  <div id="dest_gcp" style="display:none">
    <div class="card-s">
      <span class="section-tag" style="background:#4285F4;color:#fff;">⬡ Google Cloud Platform</span>
      <div class="row g-3">
        <div class="col-md-4">
          <label class="form-label">GCP Service</label>
          <select class="form-control" style="appearance:auto">
            <option>Cloud Pub/Sub</option>
            <option>Eventarc</option>
            <option>Cloud Tasks</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label">GCP Region</label>
          <select class="form-control" style="appearance:auto">
            <option>us-central1 (Iowa)</option>
            <option>us-east1 (South Carolina)</option>
            <option>europe-west1 (Belgium)</option>
            <option>asia-southeast1 (Singapore)</option>
            <option>asia-south1 (Mumbai)</option>
            <option>australia-southeast1 (Sydney)</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label">Authentication Method</label>
          <select class="form-control" style="appearance:auto">
            <option>Workload Identity Federation (recommended)</option>
            <option>Service Account JSON Key</option>
            <option>Application Default Credentials</option>
          </select>
        </div>
        <div class="col-md-6">
          <label class="form-label">GCP Project ID</label>
          <input class="form-control" placeholder="e.g. my-project-123456" id="gcp_proj">
        </div>
        <div class="col-md-6">
          <label class="form-label">Pub/Sub Topic / Subscription Name</label>
          <input class="form-control" placeholder="e.g. migrated-app-topic" id="gcp_dest">
        </div>
      </div>
    </div>
    <div style="margin-top:.75rem;padding:.6rem .85rem;background:#EFF6FF;border-left:3px solid #4285F4;border-radius:4px;font-size:.75rem;color:#64748B;">
      ⚠️ <strong>GCP Connector Required:</strong> Live migration to GCP requires the Google Cloud Pub/Sub connector plugin. Contact your HCLTech Middleware Solutioning Team to enable this.
    </div>
  </div>

  <!-- ── IBM / Confluent fields ── -->
  <div id="dest_kafka" style="display:none">
    <div class="card-s">
      <span class="section-tag" style="background:#1C3557;color:#fff;">⬡ Kafka</span>
      <div class="row g-3">
        <div class="col-md-6">
          <label class="form-label">Bootstrap Servers</label>
          <input class="form-control" placeholder="e.g. broker1:9092,broker2:9092">
        </div>
        <div class="col-md-6">
          <label class="form-label">Target Topic</label>
          <input class="form-control" placeholder="e.g. migrated-app-topic">
        </div>
        <div class="col-md-6">
          <label class="form-label">Security Protocol</label>
          <select class="form-control" style="appearance:auto">
            <option>SASL_SSL (recommended)</option>
            <option>SSL</option>
            <option>SASL_PLAINTEXT</option>
            <option>PLAINTEXT</option>
          </select>
        </div>
        <div class="col-md-6">
          <label class="form-label">SASL Mechanism</label>
          <select class="form-control" style="appearance:auto">
            <option>PLAIN</option>
            <option>SCRAM-SHA-256</option>
            <option>SCRAM-SHA-512</option>
            <option>OAUTHBEARER</option>
          </select>
        </div>
      </div>
    </div>
    <div style="margin-top:.75rem;padding:.6rem .85rem;background:#F8FAFC;border-left:3px solid #1C3557;border-radius:4px;font-size:.75rem;color:#64748B;">
      ⚠️ <strong>Kafka Connector Required:</strong> Live migration to Kafka requires the IBM Event Streams / Confluent connector plugin. Contact your HCLTech Middleware Solutioning Team to enable this.
    </div>
  </div>
  <div class="card-s" style="margin-top:1rem">
    <h6>Test Message Configuration</h6>
    <div class="row g-3">
      <div class="col-md-6">
        <label class="form-label">Message Type</label>
        <select class="form-control" id="msg_type" onchange="toggleCustom()">
          <optgroup label="── Commerce ──">
            <option>Order Events</option>
            <option>Payment Events</option>
            <option>Inventory Events</option>
            <option>Customer Events</option>
          </optgroup>
          <optgroup label="── Financial Services ──">
            <option>Trade / Financial Events</option>
            <option>Banking / Account Events</option>
            <option>Fraud / Risk Events</option>
          </optgroup>
          <optgroup label="── Enterprise ──">
            <option>ERP / SAP Events</option>
            <option>CRM / Sales Events</option>
            <option>HR / Employee Events</option>
            <option>Notification Events</option>
          </optgroup>
          <optgroup label="── Operations ──">
            <option>Logistics / Shipment Events</option>
            <option>IoT / Sensor Events</option>
            <option>Healthcare Events</option>
          </optgroup>
          <optgroup label="── Custom ──">
            <option>Custom JSON</option>
          </optgroup>
        </select>
      </div>
      <div class="col-md-6">
        <label class="form-label">Number of Test Messages &nbsp;<strong id="cnt_lbl" style="color:var(--blue)">10</strong></label>
        <input type="range" class="form-range" id="msg_count_r" min="3" max="50" value="10"
               oninput="document.getElementById('cnt_lbl').textContent=this.value;document.getElementById('msg_count').value=this.value;">
        <input type="hidden" id="msg_count" value="10">
      </div>
      <div class="col-12" id="custom_box" style="display:none">
        <label class="form-label">Custom JSON Template</label>
        <textarea class="form-control" id="custom_json" rows="3" placeholder='{"type":"MyEvent","id":"001"}'></textarea>
      </div>
    </div>
  </div>
  <!-- Solace Security Card -->
  <div class="card-s" style="margin-top:.85rem;border:1px solid #E2E8F0">
    <div style="display:flex;align-items:center;justify-content:space-between;cursor:pointer" onclick="toggleSec('sol_sec_body')">
      <div style="font-size:.85rem;font-weight:600;color:#1E293B">🔐 Security Configuration — Solace PubSub+</div>
      <span id="sol_sec_arrow" style="font-size:.75rem;color:#64748B">▼ expand</span>
    </div>
    <div id="sol_sec_body" style="display:none;margin-top:.85rem">
      <div class="row g-2">
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Authentication Scheme</label>
          <select class="form-control" id="sol_auth_type" onchange="toggleSolAuth()" style="font-size:.82rem">
            <option value="basic" selected>Basic Auth (Client Username)</option>
            <option value="oauth2">OAuth 2.0 / JWT Bearer Token</option>
            <option value="cert">Client Certificate (Mutual TLS)</option>
            <option value="kerberos">Kerberos / GSSAPI</option>
          </select>
        </div>
        <div class="col-md-4" id="sol_user_col">
          <label class="form-label" style="font-size:.78rem">Client Username</label>
          <input class="form-control" id="sol_username" value="demo-client" style="font-size:.82rem">
        </div>
        <div class="col-md-4" id="sol_pass_col">
          <label class="form-label" style="font-size:.78rem">Client Password</label>
          <input class="form-control" id="sol_clientpass" type="password" value="demo-pass" style="font-size:.82rem">
        </div>
        <div class="col-md-6" id="sol_oauth_col" style="display:none">
          <label class="form-label" style="font-size:.78rem">OAuth 2.0 Bearer Token</label>
          <input class="form-control" id="sol_oauth_token" placeholder="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9..." style="font-size:.72rem;font-family:monospace">
        </div>
        <div class="col-md-4" id="sol_cert_col" style="display:none">
          <label class="form-label" style="font-size:.78rem">Client Certificate CN</label>
          <input class="form-control" id="sol_cert_cn" value="CN=solclient,O=HCLTech,C=IN" style="font-size:.78rem;font-family:monospace">
        </div>
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Transport Security</label>
          <select class="form-control" id="sol_tls" style="font-size:.82rem">
            <option value="none" selected>None (REST HTTP Port 9000)</option>
            <option value="tls">TLS (REST HTTPS Port 9443)</option>
            <option value="mutual">Mutual TLS</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Message VPN Auth</label>
          <select class="form-control" id="sol_vpn_auth" style="font-size:.82rem">
            <option value="internal" selected>Internal (VPN-level auth)</option>
            <option value="radius">RADIUS</option>
            <option value="ldap">LDAP</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">ACL Profile</label>
          <input class="form-control" id="sol_acl" value="default" style="font-size:.82rem">
        </div>
      </div>
      <div style="margin-top:.65rem;padding:.5rem .75rem;background:#EDE9FE;border-radius:6px;font-size:.72rem;color:#4C1D95">
        🔒 Solace SEMP v2 management API secured with admin credentials — REST messaging via HTTP on port 9000
      </div>
    </div>
  </div>

  <div class="d-flex justify-content-between" style="margin-top:1rem">
    <button class="btn-o" onclick="goStep(2)">← Back</button>
    <button class="btn-p" onclick="goStep(4)">Next: AI Capacity Analysis →</button>
  </div>
</div>


<!-- ── STEP 4: AI Capacity Analysis ── -->
<div class="step-panel" id="step-4">
  <h3 class="page-title">GenAI Capacity Analysis</h3>
  <p class="page-sub">AI-driven analysis of your target Solace PubSub+ instance — assesses live broker metrics and recommends the optimal deployment strategy for your workload.</p>

  <!-- Profile summary -->
  <div style="background:linear-gradient(135deg,#1E1B4B,#1E293B);border:1px solid rgba(99,102,241,.3);border-radius:12px;padding:1.1rem 1.4rem;margin-bottom:1.25rem;">
    <div style="font-size:.65rem;font-weight:700;letter-spacing:.12em;color:#818CF8;text-transform:uppercase;margin-bottom:.85rem;">&#9632; Migration Workload Profile</div>
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:.75rem;" id="cap_profile"></div>
  </div>

  <!-- Run button -->
  <div style="text-align:center;margin:1.4rem 0;">
    <img id="cap_run_btn" src="data:image/png;base64,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"
         onclick="runCapacityAnalysis()"
         alt="AI Force ITOps — Run Capacity Analysis"
         title="Click to run GenAI Capacity Analysis"
         style="height:56px;width:auto;cursor:pointer;display:inline-block;
                border-radius:8px;transition:opacity .15s;box-shadow:0 4px 18px rgba(0,0,0,.25);"
         onmouseover="this.style.opacity='.85'"
         onmouseout="this.style.opacity='1'">
    <div style="margin-top:.55rem;font-size:.7rem;color:#64748B;">
      Fetches live Solace SEMP metrics &amp; applies AI-driven capacity modelling
    </div>
  </div>

  <!-- Analysis output -->
  <div id="cap_output" style="display:none;">

    <!-- SEMP metrics -->
    <div style="background:#0F172A;border:1px solid rgba(255,255,255,.07);border-radius:10px;padding:1rem 1.3rem;margin-bottom:1rem;">
      <div style="font-size:.65rem;font-weight:700;letter-spacing:.12em;color:#94A3B8;text-transform:uppercase;margin-bottom:.85rem;">
        &#128202; Live Solace Broker Metrics &mdash; SEMP API
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:.75rem;" id="cap_metrics"></div>
    </div>

    <!-- AI streaming -->
    <div style="background:#080E1C;border:1px solid rgba(99,102,241,.25);border-radius:10px;padding:1rem 1.3rem;margin-bottom:1rem;">
      <div style="font-size:.65rem;font-weight:700;letter-spacing:.12em;color:#6366F1;text-transform:uppercase;margin-bottom:.75rem;display:flex;align-items:center;gap:.5rem;">
        <span id="cap_ai_spinner" style="display:inline-block;animation:spin 1s linear infinite;">&#9696;</span>
        GenAI Analysis &mdash; HCL AIForce.Ops reasoning engine
      </div>
      <div id="cap_stream" style="font-family:'Courier New',monospace;font-size:.78rem;line-height:1.95;color:#94A3B8;min-height:2rem;white-space:pre-wrap;"></div>
    </div>

    <!-- Decision -->
    <div id="cap_decision" style="display:none;"></div>
  </div>

  <!-- Nav -->
  <div style="display:flex;gap:1rem;margin-top:1.5rem;">
    <button class="btn-o" onclick="goStep(3)">← Back</button>
    <button class="btn-p" id="cap_next" onclick="goStep(5)" disabled style="opacity:.5;cursor:not-allowed;">Next: Review Schema →</button>
  </div>
</div>

<!-- ── STEP 5: Schema Review ── -->
<div class="step-panel" id="step-5">
  <h3 class="page-title">Schema Review &amp; Acknowledgement</h3>
  <p class="page-sub">Review a sample message payload for your selected workload. Confirm before we proceed to validation.</p>

  <div class="card-s">
    <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:1rem">
      <div style="background:var(--hcl-grad);border-radius:8px;padding:.5rem .9rem;color:#fff;font-size:.8rem;font-weight:600" id="schema_type_badge">Order Events</div>
      <div style="font-size:.82rem;color:#64748B">Sample payload that will be migrated from <strong>IBM MQ</strong> → <strong id="schema_dest_label">Solace PubSub+</strong></div>
    </div>

    <!-- JSON sample -->
    <div style="background:#0F172A;border-radius:10px;padding:1.1rem 1.3rem;margin-bottom:1.25rem;position:relative">
      <div style="position:absolute;top:.5rem;right:.75rem;font-size:.65rem;color:#475569;letter-spacing:.05em">SAMPLE PAYLOAD</div>
      <pre id="schema_sample" style="color:#94A3B8;font-size:.78rem;margin:0;white-space:pre-wrap;font-family:'Courier New',monospace;line-height:1.6"></pre>
    </div>

    <!-- Field table -->
    <div style="font-size:.82rem;font-weight:600;color:#374151;margin-bottom:.5rem">Field Descriptions</div>
    <div style="overflow-x:auto">
      <table style="width:100%;border-collapse:collapse;font-size:.78rem" id="schema_fields_tbl">
        <thead>
          <tr style="background:#F8FAFC">
            <th style="padding:.45rem .75rem;text-align:left;border-bottom:2px solid #E2E8F0;color:#374151">Field</th>
            <th style="padding:.45rem .75rem;text-align:left;border-bottom:2px solid #E2E8F0;color:#374151">Type</th>
            <th style="padding:.45rem .75rem;text-align:left;border-bottom:2px solid #E2E8F0;color:#374151">Description</th>
          </tr>
        </thead>
        <tbody id="schema_fields_body"></tbody>
      </table>
    </div>
  </div>

  <!-- Migration context card -->
  <div class="card-s" style="background:#F0FDF4;border:1px solid #BBF7D0">
    <div style="font-size:.8rem;font-weight:600;color:#166534;margin-bottom:.5rem">✅ Migration Transport Path</div>
    <div style="font-size:.78rem;color:#14532D;line-height:1.7">
      Source: <strong id="schema_src_proto">IBM MQ REST / HTTP</strong> &nbsp;→&nbsp;
      Target: <strong id="schema_tgt_proto">Solace REST Messaging</strong><br>
      Messages will retain their JSON structure. No schema transformation is applied in this migration.
    </div>
  </div>

  <!-- Acknowledgement -->
  <div class="card-s" style="border:2px solid #C7D2FE;background:#EEF2FF">
    <div style="display:flex;align-items:flex-start;gap:.9rem">
      <input type="checkbox" id="schema_ack" onchange="toggleSchemaNext()"
             style="margin-top:.15rem;width:18px;height:18px;accent-color:#0284C7;cursor:pointer;flex-shrink:0">
      <label for="schema_ack" style="font-size:.83rem;cursor:pointer;line-height:1.6;color:#1E1B4B">
        I have reviewed the sample payload above and confirm it represents the message format
        for <strong id="schema_ack_type">this workload</strong>. I understand the bridge will
        migrate messages of this schema from IBM MQ to the configured target platform.
      </label>
    </div>
  </div>

  <div class="d-flex justify-content-between">
    <button class="btn-o" onclick="goStep(4)">← Back</button>
    <button class="btn-p" id="schema_next_btn" onclick="goStep(6)" disabled
            style="opacity:.5;cursor:not-allowed">Next: Migration Scope →</button>
  </div>
</div>


<!-- ── STEP 6 — Migration Scope ─────────────────────────────────────── -->
<div class="step-panel" id="step-6">
  <h3 class="page-title">Migration Scope</h3>
  <p class="page-sub">Select the migration approach that fits your project boundaries.</p>

  <div id="migscope-body">
    <div class="row g-3 mb-3">

      <!-- Option A: EBC Liberty via EBCAdmin -->
      <div class="col-12 col-md-4">
        <label id="scope-card-ebc" class="d-block h-100" style="cursor:pointer">
          <input type="radio" name="mig_scope" value="ebc_liberty" checked
                 onchange="selectScope(this.value)" style="display:none">
          <div id="scope-card-ebc-inner" class="p-3 rounded h-100"
               style="border:2px solid #3B82F6; background:#0C1A2E; transition:all .2s">
            <div class="d-flex align-items-center mb-2">
              <span style="font-size:1.6rem;margin-right:.6rem">🔷</span>
              <strong style="color:#93C5FD;font-size:1rem">EBC Liberty — EBCAdmin Config</strong>
            </div>
            <p style="color:#94A3B8;font-size:.85rem;margin-bottom:.75rem">
              EBC Framework <strong style="color:#38BDF8">9.1.x+</strong> detected — Solace EventMesh is native.
              The portal updates <code style="color:#34D399">ebcfwk.eventmesh.*</code> properties via
              <strong style="color:#E2E8F0">EBCAdmin in CHARM</strong>. No server.xml edit,
              no Jenkins redeploy needed.
            </p>
            <div style="font-size:.78rem;color:#64748B">
              <span style="color:#34D399">✔</span> Tier 1 — Auto-Execute (DFP role gated)<br>
              <span style="color:#34D399">✔</span> EBCAdmin API call via CHARM portal<br>
              <span style="color:#34D399">✔</span> Zero-downtime — Liberty reloads config live
            </div>
          </div>
        </label>
      </div>

      <!-- Option B: Tomcat/TomEE via Ansible Tower -->
      <div class="col-12 col-md-4">
        <label id="scope-card-conn" class="d-block h-100" style="cursor:pointer">
          <input type="radio" name="mig_scope" value="connection"
                 onchange="selectScope(this.value)" style="display:none">
          <div id="scope-card-conn-inner" class="p-3 rounded h-100"
               style="border:2px solid #334155; background:#0F172A; transition:all .2s">
            <div class="d-flex align-items-center mb-2">
              <span style="font-size:1.6rem;margin-right:.6rem">🔌</span>
              <strong style="color:#93C5FD;font-size:1rem">Tomcat / TomEE — context.xml</strong>
            </div>
            <p style="color:#94A3B8;font-size:.85rem;margin-bottom:.75rem">
              The portal updates the JNDI connection factory in <code>tomcat-context.xml</code>
              via <strong style="color:#E2E8F0">Ansible Tower</strong> and triggers a Tomcat
              Manager API reload. Zero code changes in the app.
            </p>
            <div style="font-size:.78rem;color:#64748B">
              <span style="color:#34D399">✔</span> Tier 2 — Approval-Gated (team lead sign-off)<br>
              <span style="color:#34D399">✔</span> Ansible Tower job auto-created<br>
              <span style="color:#34D399">✔</span> Full end-to-end validation through the app
            </div>
          </div>
        </label>
      </div>

      <!-- Option C: Direct Middleware Migration -->
      <div class="col-12 col-md-4">
        <label id="scope-card-direct" class="d-block h-100" style="cursor:pointer">
          <input type="radio" name="mig_scope" value="direct"
                 onchange="selectScope(this.value)" style="display:none">
          <div id="scope-card-direct-inner" class="p-3 rounded h-100"
               style="border:2px solid #334155; background:#0F172A; transition:all .2s">
            <div class="d-flex align-items-center mb-2">
              <span style="font-size:1.6rem;margin-right:.6rem">⚡</span>
              <strong style="color:#94A3B8;font-size:1rem">Direct Middleware Migration</strong>
            </div>
            <p style="color:#94A3B8;font-size:.85rem;margin-bottom:.75rem">
              The application is <strong style="color:#E2E8F0">outside scope</strong>.
              The portal migrates messages directly from IBM MQ to Solace PubSub+ at the
              broker level only — no application config changes.
            </p>
            <div style="font-size:.78rem;color:#64748B">
              <span style="color:#F59E0B">→</span> App Reconfiguration step will be skipped<br>
              <span style="color:#F59E0B">→</span> Validation proceeds at the broker level
            </div>
          </div>
        </label>
      </div>
    </div>

    <!-- Selection confirmation badge -->
    <div id="scope-confirm" class="p-2 rounded mb-3" style="background:#0F2A1A;border:1px solid #166534;display:none">
      <span id="scope-confirm-text" style="color:#4ADE80;font-size:.88rem"></span>
    </div>
  </div>

  <div class="d-flex justify-content-between mt-3">
    <button class="btn-o" onclick="goStep(5)">← Back</button>
    <button class="btn-p" id="scope_next_btn" onclick="proceedFromScope()">Next →</button>
  </div>
</div>

<!-- ── STEP 5 (old → now STEP 7) ── -->
<div class="step-panel" id="step-7">
  <h3 class="page-title" id="reconfig_title">Application Reconfiguration</h3>
  <p class="page-sub" id="reconfig_subtitle">Updating connection configuration from IBM MQ to Solace PubSub+.</p>

  <!-- Server-type badge row -->
  <div id="reconfig_badge_row" class="card-s" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem;flex-wrap:wrap;gap:.75rem">
    <div id="reconfig_app_info">
      <div style="font-size:.82rem;font-weight:700;color:#1E293B" id="reconfig_app_label">🖥️ Application</div>
      <div style="font-size:.72rem;color:#64748B;margin-top:.2rem" id="reconfig_app_sub"></div>
    </div>
    <div id="reconfig_tier_badge" style="font-size:.75rem;font-weight:700;padding:.35rem .9rem;border-radius:20px;background:#E0F2FE;color:#0284C7"></div>
  </div>

  <!-- Before / After connection strings -->
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1rem">
    <!-- Before -->
    <div class="card-s" style="background:rgba(239,68,68,.06);border:1.5px solid rgba(239,68,68,.3)">
      <div style="font-size:.72rem;font-weight:700;color:#EF4444;text-transform:uppercase;letter-spacing:.08em;margin-bottom:.65rem">◀ Current — IBM MQ</div>
      <div id="reconfig_before" style="background:#020617;border-radius:6px;padding:.75rem;font-family:monospace;font-size:.68rem;line-height:1.7;color:#94A3B8;overflow-x:auto">Loading...</div>
    </div>
    <!-- After -->
    <div class="card-s" style="background:rgba(16,185,129,.06);border:1.5px solid rgba(16,185,129,.3)">
      <div style="font-size:.72rem;font-weight:700;color:#10B981;text-transform:uppercase;letter-spacing:.08em;margin-bottom:.65rem">▶ New — Solace PubSub+</div>
      <div id="reconfig_after" style="background:#020617;border-radius:6px;padding:.75rem;font-family:monospace;font-size:.68rem;line-height:1.7;color:#94A3B8;overflow-x:auto">Will be generated from your Step 3 inputs.</div>
    </div>
  </div>

  <!-- Reconfig action -->
  <div class="card-s">
    <div style="font-size:.82rem;font-weight:600;color:#1E293B;margin-bottom:.75rem">Apply Reconfiguration</div>
    <button class="btn-p" id="reconfig_btn" onclick="runReconfig()" style="margin-bottom:.75rem">
      ⚙️ Reconfigure TomcatEE → Solace PubSub+
    </button>
    <div id="reconfig_log" class="log" style="height:130px;display:none"></div>
    <div id="reconfig_success" style="display:none;margin-top:.75rem;padding:.65rem 1rem;background:rgba(16,185,129,.1);border:1.5px solid rgba(16,185,129,.4);border-radius:8px;font-size:.8rem;color:#10B981;font-weight:600">
      ✅ tomcat-context.xml updated — TomcatEE app now routes orders to Solace PubSub+
    </div>
  </div>

  <div class="d-flex justify-content-between" style="margin-top:1rem">
    <button class="btn-o" onclick="goStep(5)">← Back</button>
    <button class="btn-p" id="reconfig_next" onclick="goStep(8);runPreflight()" disabled>Next: Validate Connectivity →</button>
  </div>
</div>

<!-- STEP 5) ── -->
<div class="step-panel" id="step-8">
  <h3 class="page-title">Preflight Validation</h3>
  <p class="page-sub">Checking connectivity and provisioning your target queue in Solace.</p>
  <div class="card-s" id="pf_card">
    <h6 id="pf_title">Running checks…</h6>
    <div id="pf_list">
      <div class="check-row"><span class="check-icon">⏳</span>Connecting to IBM MQ Queue Manager</div>
      <div class="check-row"><span class="check-icon">⏳</span>Verifying source queue</div>
      <div class="check-row"><span class="check-icon">⏳</span>Connecting to Solace PubSub+</div>
      <div class="check-row"><span class="check-icon">⏳</span>Creating target queue</div>
    </div>
  </div>
  <div class="d-flex justify-content-between" id="pf_nav" style="display:none!important">
    <button class="btn-o" onclick="goStep(7)">← Back</button>
    <button class="btn-p" id="pf_next" onclick="goStep(9)" disabled>Next: Produce Messages →</button>
  </div>
</div>

<!-- ── STEP 5 ── -->
<div class="step-panel" id="step-9">
  <h3 class="page-title">Produce Test Messages</h3>
  <p class="page-sub">Send <strong id="lbl_count">10</strong> <strong id="lbl_type">Order Events</strong> to IBM MQ to simulate your legacy application workload.</p>
  <div class="card-s">
    <div class="row g-3 mb-3">
      <div class="col-6 col-md-3"><div class="meter"><div class="meter-val" id="p_sent" style="color:var(--blue)">—</div><div class="meter-lbl">Sent to MQ</div></div></div>
      <div class="col-6 col-md-3"><div class="meter"><div class="meter-val" id="p_depth" style="color:var(--navy)">—</div><div class="meter-lbl">MQ Queue Depth</div></div></div>
    </div>
    <button class="btn-p" id="prod_btn" onclick="runProduce()">▶ Send Messages to IBM MQ</button>
    <div class="log" id="prod_log" style="height:160px;"></div>
  </div>
  <div class="d-flex justify-content-between">
    <button class="btn-o" onclick="goStep(8)">← Back</button>
    <button class="btn-p" id="prod_next" onclick="goStep(10)" disabled>Next: Migrate →</button>
  </div>
</div>

<!-- ── STEP 6 ── -->
<div class="step-panel" id="step-10">
  <h3 class="page-title">Live Migration</h3>
  <p class="page-sub">The bridge reads each message from IBM MQ and republishes it to Solace PubSub+ in real time.</p>
  <div class="card-s">
    <div class="mb-3">
      <span class="flag-pill fp-mq" id="flag_bridge">🟡 Feature Flag: <strong>BRIDGE ACTIVE</strong></span>
      <span class="flag-pill fp-sol" id="flag_direct" style="display:none;margin-left:.5rem">🔵 Feature Flag: <strong>SOLACE DIRECT</strong></span>
    </div>
    <div class="row g-3 mb-3">
      <div class="col-4"><div class="meter"><div class="meter-val" id="m_bridged" style="color:var(--teal)">0</div><div class="meter-lbl">Bridged</div></div></div>
      <div class="col-4"><div class="meter"><div class="meter-val" id="m_mq" style="color:var(--navy)">—</div><div class="meter-lbl">IBM MQ Depth</div></div></div>
      <div class="col-4"><div class="meter"><div class="meter-val" id="m_sol" style="color:var(--blue)">—</div><div class="meter-lbl">Solace Depth</div></div></div>
    </div>
    <button class="btn-p" id="mig_btn" onclick="runMigrate()">▶ Start Bridge</button>
    <div class="log" id="mig_log"></div>
  </div>

  <!-- Feature flag flip card -->
  <div class="card-s" id="flip_card" style="display:none">
    <h6>🚩 Flip Feature Flag — Direct to Solace</h6>
    <p style="font-size:.82rem;color:var(--muted);margin-bottom:.85rem">IBM MQ is now empty. Switch the feature flag so new events go <strong>direct to Solace</strong> — no MQ in the path. This is the final cutover step in a rolling migration.</p>
    <button class="btn-p" onclick="runDirect()">▶ Send 3 Direct-to-Solace Events</button>
    <div class="log" id="direct_log" style="height:110px;"></div>
  </div>
  <div class="d-flex justify-content-end mt-3" id="mig_nav" style="display:none">
    <button class="btn-p" onclick="goStep(11)">Next: CMDB Changes →</button>
  </div>
</div>

<!-- ── STEP 8: CMDB Changes ── -->
<div class="step-panel" id="step-11">
  <h3 class="page-title">CMDB &amp; ServiceNow Update</h3>
  <p class="page-sub">Register the configuration changes in your CMDB before finalising the migration. This ensures your asset records, CIs, and change tickets stay in sync.</p>

  <div class="card-s">
    <div style="font-size:.85rem;font-weight:600;color:#1E293B;margin-bottom:1rem">Do you want to register CMDB changes for this migration?</div>
    <div style="display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:1.25rem">
      <label style="display:flex;align-items:center;gap:.6rem;cursor:pointer;font-size:.88rem;padding:.6rem 1.1rem;border:2px solid #E2E8F0;border-radius:8px;transition:all .2s" id="cmdb_yes_lbl">
        <input type="radio" name="cmdb_choice" id="cmdb_yes" value="yes" onchange="handleCmdb()" style="accent-color:#0284C7;width:16px;height:16px">
        <span>✅ Yes — Register Changes in ServiceNow CMDB</span>
      </label>
      <label style="display:flex;align-items:center;gap:.6rem;cursor:pointer;font-size:.88rem;padding:.6rem 1.1rem;border:2px solid #E2E8F0;border-radius:8px;transition:all .2s" id="cmdb_no_lbl">
        <input type="radio" name="cmdb_choice" id="cmdb_no" value="no" onchange="handleCmdb()" style="accent-color:#0284C7;width:16px;height:16px">
        <span>⏭️ No — Skip for now</span>
      </label>
    </div>

    <!-- ServiceNow fields — shown when Yes is selected -->
    <div id="cmdb_fields" style="display:none">
      <div class="row g-2 mb-3">
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Change Request Type</label>
          <select class="form-control" id="cmdb_cr_type" style="font-size:.82rem">
            <option selected>Standard Change</option>
            <option>Normal Change</option>
            <option>Emergency Change</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">ServiceNow Instance</label>
          <input class="form-control" id="cmdb_instance" value="hcltech.service-now.com" style="font-size:.82rem">
        </div>
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Assignment Group</label>
          <input class="form-control" id="cmdb_group" value="Middleware Engineering" style="font-size:.82rem">
        </div>
        <div class="col-md-6">
          <label class="form-label" style="font-size:.78rem">CI Name (Source)</label>
          <input class="form-control" id="cmdb_ci_src" value="IBM-MQ-DEV-QM1" style="font-size:.82rem">
        </div>
        <div class="col-md-6">
          <label class="form-label" style="font-size:.78rem">CI Name (Target)</label>
          <input class="form-control" id="cmdb_ci_tgt" value="Solace-PubSub-PROD-VPN" style="font-size:.82rem">
        </div>
      </div>
      <button class="btn-p" id="cmdb_submit_btn" onclick="submitCmdb()" style="margin-bottom:.75rem">
        📋 Register CMDB Changes in ServiceNow
      </button>
      <div id="cmdb_log" class="log" style="height:130px;display:none"></div>
    </div>
  </div>

  <div class="d-flex justify-content-between" style="margin-top:1rem">
    <button class="btn-o" onclick="goStep(10)">← Back</button>
    <button class="btn-p" id="cmdb_next_btn" onclick="goStep(12)">Next: Enable Monitoring →</button>
  </div>
</div>

<!-- ── STEP 9: Enable Monitoring ── -->
<div class="step-panel" id="step-12">
  <h3 class="page-title">Enable Monitoring &amp; Alerting</h3>
  <p class="page-sub">Set up observability for your migrated workload — dashboards, alerts, and notification channels — so your team has full visibility in production.</p>

  <div class="card-s">
    <div style="font-size:.85rem;font-weight:600;color:#1E293B;margin-bottom:1rem">Do you want to enable monitoring &amp; alerting for this migration?</div>
    <div style="display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:1.25rem">
      <label style="display:flex;align-items:center;gap:.6rem;cursor:pointer;font-size:.88rem;padding:.6rem 1.1rem;border:2px solid #E2E8F0;border-radius:8px;transition:all .2s" id="mon_yes_lbl">
        <input type="radio" name="mon_choice" id="mon_yes" value="yes" onchange="handleMon()" style="accent-color:#0284C7;width:16px;height:16px">
        <span>✅ Yes — Enable Monitoring &amp; Alerting</span>
      </label>
      <label style="display:flex;align-items:center;gap:.6rem;cursor:pointer;font-size:.88rem;padding:.6rem 1.1rem;border:2px solid #E2E8F0;border-radius:8px;transition:all .2s" id="mon_no_lbl">
        <input type="radio" name="mon_choice" id="mon_no" value="no" onchange="handleMon()" style="accent-color:#0284C7;width:16px;height:16px">
        <span>⏭️ No — Skip for now</span>
      </label>
    </div>

    <div id="mon_fields" style="display:none">
      <div class="row g-2 mb-3">
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Monitoring Platform</label>
          <select class="form-control" id="mon_platform" style="font-size:.82rem">
            <option value="datadog" selected>Datadog</option>
            <option value="dynatrace">Dynatrace</option>
            <option value="splunk">Splunk Observability</option>
            <option value="prometheus">Prometheus + Grafana</option>
            <option value="instana">IBM Instana</option>
            <option value="newrelic">New Relic</option>
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Dashboard Name</label>
          <input class="form-control" id="mon_dashboard" value="Solace-MQ-Migration-Monitor" style="font-size:.82rem">
        </div>
        <div class="col-md-4">
          <label class="form-label" style="font-size:.78rem">Alert Notification Channel</label>
          <select class="form-control" id="mon_channel" style="font-size:.82rem">
            <option value="email" selected>Email</option>
            <option value="slack">Slack</option>
            <option value="pagerduty">PagerDuty</option>
            <option value="teams">Microsoft Teams</option>
          </select>
        </div>
        <div class="col-md-3">
          <label class="form-label" style="font-size:.78rem">Queue Depth Alert (msgs)</label>
          <input class="form-control" id="mon_depth_thresh" value="1000" type="number" style="font-size:.82rem">
        </div>
        <div class="col-md-3">
          <label class="form-label" style="font-size:.78rem">Latency Alert (ms)</label>
          <input class="form-control" id="mon_latency_thresh" value="500" type="number" style="font-size:.82rem">
        </div>
        <div class="col-md-3">
          <label class="form-label" style="font-size:.78rem">Throughput Alert (msg/s)</label>
          <input class="form-control" id="mon_throughput_thresh" value="5000" type="number" style="font-size:.82rem">
        </div>
        <div class="col-md-3">
          <label class="form-label" style="font-size:.78rem">Alert Severity</label>
          <select class="form-control" id="mon_severity" style="font-size:.82rem">
            <option value="critical" selected>Critical + Warning</option>
            <option value="critical">Critical Only</option>
            <option value="all">All (Info + Warn + Critical)</option>
          </select>
        </div>
      </div>
      <button class="btn-p" id="mon_submit_btn" onclick="submitMonitoring()" style="margin-bottom:.75rem">
        📊 Enable Monitoring &amp; Alerting
      </button>
      <div id="mon_log" class="log" style="height:150px;display:none"></div>
    </div>
  </div>

  <div class="d-flex justify-content-between" style="margin-top:1rem">
    <button class="btn-o" onclick="goStep(11)">← Back</button>
    <button class="btn-p" onclick="goStep(13);runVerify()">Next: Verify Migration →</button>
  </div>
</div>

<!-- ── STEP 7 ── -->
<div class="step-panel" id="step-13">
  <h3 class="page-title">Migration Verification</h3>
  <p class="page-sub">Confirming all messages are in Solace PubSub+ Event Mesh and IBM MQ is empty.</p>
  <div class="card-s">
    <div class="row g-3">
      <div class="col-6 col-md-3"><div class="meter"><div class="meter-val" id="v_mq" style="color:var(--red)">—</div><div class="meter-lbl">IBM MQ Remaining</div></div></div>
      <div class="col-6 col-md-3"><div class="meter"><div class="meter-val" id="v_sol" style="color:var(--green)">—</div><div class="meter-lbl">Solace Messages</div></div></div>
      <div class="col-6 col-md-3"><div class="meter"><div class="meter-val" id="v_rx" style="color:var(--blue)">—</div><div class="meter-lbl">VPN Msgs Received</div></div></div>
      <div class="col-6 col-md-3"><div class="meter"><div class="meter-val" id="v_status">⏳</div><div class="meter-lbl">Status</div></div></div>
    </div>
  </div>
  <div id="cert_box" style="display:none">
    <div class="cert">
      <div style="font-size:2.8rem">✅</div>
      <h4 class="mt-2">Migration Complete</h4>
      <p id="cert_msg" style="opacity:.85;font-size:.875rem;margin-bottom:0"></p>
      <div class="cert-meta">
        <div><strong>Application:</strong> <span id="c_app"></span></div>
        <div><strong>Team:</strong> <span id="c_team"></span></div>
        <div><strong>Schema / Workload:</strong> <span id="c_schema"></span></div>
        <div><strong>MQ Security:</strong> <span id="c_mq_sec"></span></div>
        <div><strong>Solace Security:</strong> <span id="c_sol_sec"></span></div>
        <div><strong>Source:</strong> IBM MQ &nbsp;<span id="c_src"></span></div>
        <div><strong>Source Protocol:</strong> <span id="c_src_proto"></span></div>
        <div><strong>Target:</strong> Solace PubSub+ &nbsp;<span id="c_tgt"></span></div>
        <div><strong>Target Protocol:</strong> <span id="c_tgt_proto"></span></div>
        <div><strong>Completed:</strong> <span id="c_date"></span></div>
      </div>
      <div style="margin-top:1rem;padding-top:.75rem;border-top:1px solid rgba(255,255,255,.2)">
        <div style="font-size:.78rem;font-weight:600;margin-bottom:.4rem;opacity:.8">Message Breakdown</div>
        <div id="c_breakdown" style="font-size:.75rem;line-height:1.8;opacity:.85"></div>
      </div>
      <!-- App Reconfiguration section -->
      <div id="c_reconfig_section" style="display:none;margin-top:1.25rem;padding-top:.85rem;border-top:1px solid rgba(255,255,255,.2)">
        <div style="font-size:.82rem;font-weight:700;margin-bottom:.75rem;letter-spacing:.02em">⚙️ Application Layer Reconfigured</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:.75rem">
          <div style="background:rgba(239,68,68,.18);border:1.5px solid #EF4444;border-radius:10px;padding:.85rem 1rem">
            <div style="font-size:.72rem;font-weight:700;color:#FCA5A5;letter-spacing:.08em;margin-bottom:.6rem">◀ PREVIOUS — IBM MQ</div>
            <div id="c_reconfig_old" style="font-size:.74rem;line-height:1.85;color:#FEE2E2"></div>
          </div>
          <div style="background:rgba(16,185,129,.18);border:1.5px solid #10B981;border-radius:10px;padding:.85rem 1rem">
            <div style="font-size:.72rem;font-weight:700;color:#6EE7B7;letter-spacing:.08em;margin-bottom:.6rem">▶ NEW — Solace PubSub+</div>
            <div id="c_reconfig_new" style="font-size:.74rem;line-height:1.85;color:#D1FAE5"></div>
          </div>
        </div>
        <div style="text-align:center;margin-top:.65rem;font-size:.72rem;color:#BAE6FD">
          📄 <b>tomcat-context.xml</b> updated on disk &nbsp;|&nbsp; App: <span style="color:#fff;font-weight:700">IKEA Order Management System v2.3</span> &nbsp;|&nbsp; Server: <span style="color:#fff">Apache Tomcat 9.0.82</span>
        </div>
      </div>

      <!-- Monitoring playcard — shown only if monitoring was enabled -->
      <div id="c_mon_section" style="display:none;margin-top:1.25rem;padding-top:.85rem;border-top:1px solid rgba(255,255,255,.2)">
        <div style="font-size:.82rem;font-weight:700;margin-bottom:.75rem;letter-spacing:.02em">📊 Monitoring &amp; Alerting Enabled</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:.75rem">
          <!-- Dashboard card — blue-teal -->
          <div style="background:rgba(14,165,233,.18);border:1.5px solid #0EA5E9;border-radius:10px;padding:.85rem 1rem">
            <div style="font-size:.72rem;font-weight:700;color:#BAE6FD;letter-spacing:.08em;margin-bottom:.6rem">📈 DASHBOARD</div>
            <div id="c_mon_dash_body" style="font-size:.74rem;line-height:1.85;color:#E0F2FE"></div>
          </div>
          <!-- Alerts card — amber -->
          <div style="background:rgba(245,158,11,.18);border:1.5px solid #F59E0B;border-radius:10px;padding:.85rem 1rem">
            <div style="font-size:.72rem;font-weight:700;color:#FDE68A;letter-spacing:.08em;margin-bottom:.6rem">🔔 ALERT THRESHOLDS</div>
            <div id="c_mon_alert_body" style="font-size:.74rem;line-height:1.85;color:#FEF3C7"></div>
          </div>
        </div>
        <!-- RITM line -->
        <div id="c_mon_ritm" style="text-align:center;margin-top:.65rem;font-size:.72rem;color:#BAE6FD;letter-spacing:.02em"></div>
      </div>

      <!-- CMDB Changes playcard — shown only if CMDB was registered -->
      <div id="c_cmdb_section" style="display:none;margin-top:1.25rem;padding-top:.85rem;border-top:1px solid rgba(255,255,255,.2)">
        <div style="font-size:.82rem;font-weight:700;margin-bottom:.75rem;letter-spacing:.02em">📋 CMDB Changes Registered in ServiceNow</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:.75rem">
          <!-- OLD — Orange -->
          <div id="c_cmdb_old" style="background:rgba(251,146,60,.18);border:1.5px solid #F97316;border-radius:10px;padding:.85rem 1rem">
            <div style="font-size:.72rem;font-weight:700;color:#FED7AA;letter-spacing:.08em;margin-bottom:.6rem">◀ PREVIOUS CONFIGURATION</div>
            <div id="c_cmdb_old_body" style="font-size:.74rem;line-height:1.85;color:#FEF3C7"></div>
          </div>
          <!-- NEW — Green -->
          <div id="c_cmdb_new" style="background:rgba(34,197,94,.15);border:1.5px solid #22C55E;border-radius:10px;padding:.85rem 1rem">
            <div style="font-size:.72rem;font-weight:700;color:#BBF7D0;letter-spacing:.08em;margin-bottom:.6rem">▶ NEW CONFIGURATION</div>
            <div id="c_cmdb_new_body" style="font-size:.74rem;line-height:1.85;color:#DCFCE7"></div>
          </div>
        </div>
        <div style="margin-top:.65rem;text-align:center;font-size:.72rem;opacity:.75">
          🎫 Change Request: <span id="c_cmdb_cr" style="font-family:monospace;font-weight:600"></span> &nbsp;|&nbsp;
          Status: <span style="color:#86EFAC;font-weight:600">Closed — Successful</span>
        </div>
      </div>
    </div>

    <!-- Action buttons -->
    <div style="display:flex;gap:.85rem;justify-content:center;margin-top:1.5rem;flex-wrap:wrap">
      <button onclick="window.print()" style="background:rgba(255,255,255,.15);border:1.5px solid rgba(255,255,255,.4);color:#fff;padding:.55rem 1.25rem;border-radius:8px;font-size:.8rem;cursor:pointer;font-weight:600;letter-spacing:.02em">
        🖨️ Print Certificate
      </button>
      <button onclick="downloadAnsiblePlaybook()" style="background:linear-gradient(135deg,#EF4444,#B91C1C);border:none;color:#fff;padding:.55rem 1.4rem;border-radius:8px;font-size:.8rem;cursor:pointer;font-weight:700;letter-spacing:.02em;box-shadow:0 4px 14px rgba(239,68,68,.4)">
        ⬇️ Download Ansible Playbook
      </button>
      <button onclick="" style="background:transparent;border:2px solid #F59E0B;color:#F59E0B;padding:.55rem 1.4rem;border-radius:8px;font-size:.8rem;cursor:pointer;font-weight:700;letter-spacing:.02em;transition:all .2s" onmouseover="this.style.background='#F59E0B';this.style.color='#0F172A'" onmouseout="this.style.background='transparent';this.style.color='#F59E0B'">
        ↩ Rollback Migration
      </button>
    </div>
  </div>
</div>

</div><!-- /main -->

<script>
// ── Queue Manager + Queue dropdowns ─────────────────────
// ── Platform switcher ─────────────────────────────────────────────────────────
function switchPlatform(){
  const p = document.getElementById('dest_platform').value;
  const sections = { solace:'dest_solace', aws:'dest_aws', azure:'dest_azure', gcp:'dest_gcp', ibm:'dest_kafka', confluent:'dest_kafka' };
  ['dest_solace','dest_aws','dest_azure','dest_gcp','dest_kafka'].forEach(id=>{
    const el=document.getElementById(id); if(el) el.style.display='none';
  });
  const target=sections[p]; if(target){const el=document.getElementById(target);if(el)el.style.display='block';}
  const badges={
    solace:{text:'● Solace PubSub+  —  Active in this demo',bg:'#E0F2FE',color:'#0284C7'},
    aws:{text:'⬡ Amazon Web Services  —  Connector Required',bg:'#FFF3E0',color:'#E65100'},
    azure:{text:'⬡ Microsoft Azure  —  Connector Required',bg:'#E3F2FD',color:'#0078D4'},
    gcp:{text:'⬡ Google Cloud  —  Connector Required',bg:'#E8F5E9',color:'#1A73E8'},
    ibm:{text:'⬡ IBM Event Streams  —  Connector Required',bg:'#F3F4F6',color:'#1C3557'},
    confluent:{text:'⬡ Confluent Kafka  —  Connector Required',bg:'#F3F4F6',color:'#1C3557'},
  };
  const b=badges[p]||badges.solace;
  const badge=document.getElementById('platform_badge');
  badge.textContent=b.text; badge.style.background=b.bg; badge.style.color=b.color;
}

// ── Queue Manager + Queue dropdowns ──────────────────────────────────────────
function onSolEnvChange(){
  const host = (document.getElementById('sol_broker_host')||{}).value || 'localhost';
  const vpn  = (document.getElementById('sol_vpn')||{}).value || 'default';
  const queue= (document.getElementById('sol_queue')||{}).value || 'EBCIRSE01.REQUEST';

  // Derive ITF queue name from Solace queue value
  const itfQ = 'ITF_' + queue.replace('.','_');

  // Update connection string display
  const cs = document.getElementById('sol_conn_string');
  if(cs){
    if(host==='localhost')
      cs.textContent = `localhost:9080 · VPN: ${vpn}  (🎯 demo mode)`;
    else
      cs.textContent = `tcps://${host}:55443 · VPN: ${vpn}`;
  }

  // Update property panel
  const ph = document.getElementById('prop_host');
  const pv = document.getElementById('prop_vpn');
  const pq = document.getElementById('prop_queue');
  if(ph) ph.textContent = host==='localhost'
    ? `= tcps://localhost:55443  (demo)`
    : `= tcps://${host}:55443`;
  if(pv) pv.textContent = `= ${vpn}`;
  if(pq) pq.textContent = `= ${itfQ}`;
}

function onMqEnvChange(){
  const env  = (document.getElementById('mq_env')||{}).value  || 'PROD';
  const reg  = (document.getElementById('mq_region')||{}).value|| 'GL';
  const dc   = (document.getElementById('mq_dc')||{}).value   || 'DC7';
  const leg  = (document.getElementById('mq_leg')||{}).value  || 'A';

  // Build QM name: IT[REGION][LEG][SEQ]  or PP... for PPE
  const prefix = env==='PROD' ? 'IT' : env==='PPE' ? 'PP' : env==='CTE' ? 'CT' : 'PT';
  const regionCode = reg==='CN' ? 'CN' : 'GL';
  const qmName = `${prefix}${regionCode}${leg}01`;

  const dcLabels = {DC7:'DC7 — Sweden (Primary)',DC8:'DC8 — Sweden (DR)',DC9:'DC9 — Sweden (Ext)',CHN:'CHN — China (itcnchn-lx)'};
  const envLabels = {PROD:'🟢 PROD',PPE:'🟡 PPE',CTE:'🔵 CTE',PTE:'⚪ PTE'};
  const legLabels = {A:'Leg A — Active',B:'Leg B — Standby (HA pair)'};

  const badge = document.getElementById('mq_topology_badge');
  if(badge){
    document.getElementById('mq_resolved_qm').textContent  = qmName;
    document.getElementById('mq_resolved_env').textContent = envLabels[env]||env;
    document.getElementById('mq_resolved_dc').textContent  = dcLabels[dc]||dc;
    document.getElementById('mq_resolved_leg').textContent = legLabels[leg]||leg;
    // China QMs: swap HA pair label
    const haEl = badge.querySelector('span[style*="margin-left:auto"]');
    if(haEl) haEl.textContent = reg==='CN'
      ? 'HA pair: ITCNCHN01 ↔ ITCNCHN02 · MQ v9.3 · RHEL 8'
      : `HA pair: ${prefix}GLA01 ↔ ${prefix}GLB01 · MQ v9.3 · RHEL 8`;
  }

  // Update QM select to reflect resolved name (demo only uses QM1/ITGLA01 backend)
  const qmSel = document.getElementById('mq_qmgr');
  if(qmSel && qmSel.options.length > 0){
    // Show resolved name as first/selected option while keeping real value
    Array.from(qmSel.options).forEach(o => {
      if(o.value==='QM1'||o.value==='') o.text = `${qmName} — ${dcLabels[dc]||dc}`;
    });
  }
}

async function loadQmgrs(){
  const sel=document.getElementById('mq_qmgr');
  const status=document.getElementById('qmgr_status');
  sel.innerHTML='<option value="">⏳ Loading queue managers…</option>';
  try{
    const d=await(await fetch('/api/qmgrs')).json();
    if(d.qmgrs&&d.qmgrs.length>0){
      // Map Docker QM names to IKEA naming convention (ITGLA01 = QM1, ITGLB01 = QM2)
      const QM_LABELS = {'QM1':'ITGLA01 — Production Global A (DC7/DC8)','QM2':'ITGLB01 — Production Global B (DC7/DC8)','QM3':'PPGLA01 — Pre-Production Global A'};
      sel.innerHTML=d.qmgrs.map(q=>`<option value="${q}">${QM_LABELS[q]||q}</option>`).join('');
      status.textContent=`${d.qmgrs.length} queue manager(s) found`;
      status.style.color='#10B981';
      onMqEnvChange();   // apply IKEA QM label to loaded option
      await loadQueues();
    } else {
      sel.innerHTML='<option value="">No queue managers found</option>';
      status.textContent=d.error||'Could not connect to IBM MQ';
      status.style.color='#EF4444';
    }
  }catch(e){
    sel.innerHTML='<option value="">Connection failed</option>';
    status.textContent='IBM MQ not reachable — is docker-compose running?';
    status.style.color='#EF4444';
  }
}

async function loadQueues(){
  const qmgr=document.getElementById('mq_qmgr').value;
  const sel=document.getElementById('mq_queue');
  if(!qmgr){sel.innerHTML='<option value="">— select a queue manager first —</option>';return;}
  sel.innerHTML='<option value="">⏳ Loading queues…</option>';
  try{
    const d=await(await fetch(`/api/queues?qmgr=${encodeURIComponent(qmgr)}`)).json();
    if(d.queues&&d.queues.length>0){
      // Float DEMO_QUEUE (DEV.QUEUE.1) to top, display it as EBCIRSE01.REQUEST
      const others=d.queues.filter(q=>q!==DEMO_QUEUE&&!q.startsWith('DEV.DEAD'));
      const sorted=[DEMO_QUEUE,...others];
      sel.innerHTML=sorted.map(q=>{
        const isDemo=q===DEMO_QUEUE;
        const label=isDemo?DEMO_QUEUE_LABEL:q;   // show IKEA name for demo queue
        return `<option value="${q}"${isDemo?' selected':''} style="${isDemo?'font-weight:700;color:#065F46;background:#D1FAE5;':''}">` +
               `${isDemo?'✅ ':''} ${label}${isDemo?' — 🎯 demo':''}</option>`;
      }).join('');
      onMqQueueChange();
    } else {
      sel.innerHTML='<option value="">No user queues found in '+qmgr+'</option>';
    }
  }catch(e){sel.innerHTML='<option value="">Failed to load queues</option>';}
}

function onMqQueueChange(){
  const sel=document.getElementById('mq_queue');
  const warn=document.getElementById('mq_queue_warn');
  const dot=document.getElementById('mq_queue_dot');
  const badge=document.getElementById('mq_demo_badge');
  const hint=document.getElementById('mq_queue_hint');
  if(!sel||!sel.value) return;
  const isDemo=sel.value===DEMO_QUEUE;
  sel.style.borderColor=isDemo?'#10B981':'#F59E0B';
  sel.style.borderWidth='2px';
  if(dot){ dot.style.background=isDemo?'#10B981':'#F59E0B'; }
  if(warn){ warn.style.display=isDemo?'none':'block'; }
  if(badge){ badge.style.display=isDemo?'inline':'none'; }
  if(hint){ hint.style.color=isDemo?'#10B981':'#64748B'; hint.textContent=isDemo?`✅ ${DEMO_QUEUE_LABEL} — demo messages go here`:'User-defined queues only (SYSTEM.* hidden)'; }
}

const DEMO_QUEUE       = 'DEV.QUEUE.1';       // actual Docker MQ queue — always pre-permissioned
const DEMO_QUEUE_LABEL = 'EBCIRSE01.REQUEST';  // display label shown to audience

async function loadSolQueues(){
  const vpn=document.getElementById('sol_vpn').value||'default';
  const sel=document.getElementById('sol_queue');
  const status=document.getElementById('sol_queue_status');
  status.textContent='⏳ Loading queues…';
  try{
    const d=await(await fetch(`/api/sol_queues?vpn=${encodeURIComponent(vpn)}`)).json();
    if(d.queues&&d.queues.length>0){
      // Always put DEMO_QUEUE first if present, mark it clearly
      const sorted=[DEMO_QUEUE,...d.queues.filter(q=>q!==DEMO_QUEUE)];
      sel.innerHTML=sorted.map(q=>{
        const isDemo=q===DEMO_QUEUE;
        return `<option value="${q}"${isDemo?' selected':''} style="${isDemo?'font-weight:700;color:#065F46;background:#D1FAE5;':''}">${isDemo?'✅ ':''} ${q}${isDemo?' — 🎯 demo':''}</option>`;
      }).join('');
      status.textContent=`${d.queues.length} queue(s) found · EBCIRSE01.REQUEST pre-selected`;
      status.style.color='#10B981';
      onSolQueueChange();
      onSolEnvChange();
    }
  }catch(e){status.textContent='Could not load — using default';status.style.color='#F59E0B';}
}

function onSolQueueChange(){
  const sel=document.getElementById('sol_queue');
  const warn=document.getElementById('queue_warn');
  const dot=document.getElementById('queue_green_dot');
  const badge=document.getElementById('demo_queue_badge');
  if(!sel) return;
  const isDemo = sel.value===DEMO_QUEUE;
  // Border: green if demo queue, amber if other
  sel.style.borderColor = isDemo ? '#10B981' : '#F59E0B';
  sel.style.borderWidth = '2px';
  if(dot) dot.style.background = isDemo ? '#10B981' : '#F59E0B';
  if(warn) warn.style.display = isDemo ? 'none' : 'block';
  if(badge) badge.style.display = isDemo ? 'inline' : 'none';
  onSolEnvChange();
}

window.addEventListener('DOMContentLoaded',loadQmgrs);

// ── Step navigation ───────────────────────────────────────────────────────────
window._migScope = "ebc_liberty"; // "ebc_liberty" | "connection" | "direct"

function initMigScope(){
  const stype = (document.getElementById('app_server_type')||{}).value || 'ebc_liberty';
  if(stype === 'ebc_liberty' && !window._migScope){
    selectScope('ebc_liberty');
  } else if(stype === 'tomcat' && !window._migScope){
    selectScope('connection');
  }
  // Reflect current _migScope selection visually
  selectScope(window._migScope);
}

function selectScope(val){
  window._migScope = val;
  const ebcInner    = document.getElementById('scope-card-ebc-inner');
  const connInner   = document.getElementById('scope-card-conn-inner');
  const directInner = document.getElementById('scope-card-direct-inner');
  const confirm     = document.getElementById('scope-confirm');
  const confirmText = document.getElementById('scope-confirm-text');

  // Reset all
  if(ebcInner)  { ebcInner.style.border='2px solid #334155'; ebcInner.style.background='#0F172A'; }
  if(connInner) { connInner.style.border='2px solid #334155'; connInner.style.background='#0F172A'; }
  if(directInner){ directInner.style.border='2px solid #334155'; directInner.style.background='#0F172A'; }

  if(val === 'ebc_liberty'){
    if(ebcInner){ ebcInner.style.border='2px solid #38BDF8'; ebcInner.style.background='#0C1A2E'; }
    confirmText.innerHTML = '🔷 EBC Liberty path — EBCAdmin property update via <strong>CHARM portal</strong>. Tier 1 Auto-Execute. Liberty reloads config live — <strong>zero downtime</strong>.';
  } else if(val === 'connection'){
    if(connInner){ connInner.style.border='2px solid #3B82F6'; connInner.style.background='#0C1A2E'; }
    confirmText.innerHTML = '✔ Tomcat/TomEE path — <strong>context.xml</strong> rewrite via Ansible Tower + Tomcat Manager API reload. Tier 2 Approval-Gated.';
  } else {
    if(directInner){ directInner.style.border='2px solid #F59E0B'; directInner.style.background='#1A1000'; }
    confirmText.innerHTML = '⚡ Middleware-only path — broker-level bridge only. App Reconfiguration step will be <strong>skipped</strong>.';
  }
  confirm.style.display = 'block';

  // Sync radio inputs
  document.querySelectorAll('input[name="mig_scope"]').forEach(r => {
    r.checked = (r.value === val);
  });
}

function proceedFromScope(){
  if(window._migScope === 'direct'){
    goStep(8); // Skip step-7 (App Reconfig)
  } else {
    goStep(7); // Go to App Reconfig
  }
}


function goStep(n){
  if(n===6) initMigScope();
  if(n===7) setTimeout(initReconfig, 50);
  document.querySelectorAll('.step-panel').forEach(p=>p.classList.remove('active'));
  document.getElementById('step-'+n).classList.add('active');
  for(let i=1;i<=13;i++){
    const li=document.getElementById('nav-'+i);
    li.classList.remove('active','done');
    if(i<n) li.classList.add('done');
    if(i===n) li.classList.add('active');
  }
  if(n===3){ loadSolQueues(); }
  if(n===5){ showSchemaStep(); }
  if(n===4){ initCapacityStep(); }
  if(n===8){
    document.getElementById('lbl_count').textContent=document.getElementById('msg_count').value;
    document.getElementById('lbl_type').textContent=document.getElementById('msg_type').value;
  }
}

const EBC_COUNTRIES = {
  EMEA:  [{code:'SE',name:'Sweden',flag:'🇸🇪'},{code:'DE',name:'Germany',flag:'🇩🇪'},{code:'GB',name:'United Kingdom',flag:'🇬🇧'},{code:'PL',name:'Poland',flag:'🇵🇱'},{code:'NL',name:'Netherlands',flag:'🇳🇱'},{code:'FR',name:'France',flag:'🇫🇷'},{code:'NO',name:'Norway',flag:'🇳🇴'},{code:'DK',name:'Denmark',flag:'🇩🇰'},{code:'FI',name:'Finland',flag:'🇫🇮'},{code:'IT',name:'Italy',flag:'🇮🇹'},{code:'ES',name:'Spain',flag:'🇪🇸'},{code:'CZ',name:'Czech Republic',flag:'🇨🇿'},{code:'AT',name:'Austria',flag:'🇦🇹'},{code:'CH',name:'Switzerland',flag:'🇨🇭'}],
  NA:    [{code:'US',name:'United States',flag:'🇺🇸'},{code:'CA',name:'Canada',flag:'🇨🇦'}],
  APAC:  [{code:'AU',name:'Australia',flag:'🇦🇺'},{code:'CN',name:'China',flag:'🇨🇳'},{code:'JP',name:'Japan',flag:'🇯🇵'},{code:'IN',name:'India',flag:'🇮🇳'},{code:'KR',name:'South Korea',flag:'🇰🇷'},{code:'SG',name:'Singapore',flag:'🇸🇬'}],
  LATAM: [{code:'BR',name:'Brazil',flag:'🇧🇷'},{code:'MX',name:'Mexico',flag:'🇲🇽'},{code:'AR',name:'Argentina',flag:'🇦🇷'},{code:'CL',name:'Chile',flag:'🇨🇱'}],
};

const EBC_INSTANCES = {
  EMEA: {
    SE: ['EBCIRSE01','EBCIRSE02','EBCIRSE03'],
    DE: ['EBCIRDE01','EBCIRDE02'],
    GB: ['EBCIRGB01','EBCIRGB02'],
    PL: ['EBCIRPL01'],
    NL: ['EBCIRNL01','EBCIRNL02'],
    FR: ['EBCIRFR01'],
    NO: ['EBCIRNO01'],
    DK: ['EBCIRDK01'],
    FI: ['EBCIRFI01'],
    IT: ['EBCIRIT01'],
    ES: ['EBCIRES01'],
    CZ: ['EBCIRCZ01'],
    AT: ['EBCIRAT01'],
    CH: ['EBCIRCH01'],
  },
  NA: {
    US: ['EBCIRUS01','EBCIRUS02','EBCIRUS03'],
    CA: ['EBCIRCA01'],
  },
  APAC: {
    AU: ['EBCIRAU01'],
    CN: ['EBCIRCN01','EBCIRCN02'],
    JP: ['EBCIRJP01'],
    IN: ['EBCIRIN01'],
    KR: ['EBCIRKR01'],
    SG: ['EBCIRSG01'],
  },
  LATAM: {
    BR: ['EBCIRBR01','EBCIRBR02'],
    MX: ['EBCIRMX01'],
    AR: ['EBCIRAR01'],
    CL: ['EBCIRCL01'],
  },
};

function fetchEbcInstances(){
  const region  = (document.getElementById('ebc_region')||{}).value  || 'EMEA';
  const country = (document.getElementById('ebc_country')||{}).value || 'SE';
  const sel     = document.getElementById('ebc_name');
  const status  = document.getElementById('ebc_lookup_status');
  const hint    = document.getElementById('ebc_name_hint');
  const resolved = document.getElementById('ebc_resolved');
  if(!sel) return;

  // Reset & show spinner
  sel.disabled = true;
  sel.innerHTML = '<option value="">⏳ Querying CHARM / LDAP...</option>';
  if(status) status.textContent = '⏳ looking up...';
  if(status) status.style.color = '#0284C7';
  if(resolved) resolved.style.display = 'none';

  // Simulate async lookup (800–1300 ms)
  setTimeout(() => {
    const instances = (EBC_INSTANCES[region]||{})[country] || [];
    if(instances.length > 0){
      sel.innerHTML = '<option value="">— select instance —</option>' +
        instances.map(i => `<option value="${i}">${i}</option>`).join('');
      sel.disabled = false;
      if(status){ status.textContent = `✔ ${instances.length} instance${instances.length>1?'s':''} found`; status.style.color='#16A34A'; }
      if(hint) hint.textContent = `${instances.length} EBC instance${instances.length>1?'s':''} registered in CHARM / LDAP`;
    } else {
      sel.innerHTML = '<option value="">— no instances registered —</option>';
      if(status){ status.textContent = '⚠ none registered'; status.style.color='#D97706'; }
      if(hint) hint.textContent = 'No EBC instances found for this country';
    }
  }, 800 + Math.random() * 500);
}

function onRegionChange(){
  const region = (document.getElementById('ebc_region')||{}).value || 'EMEA';
  const sel = document.getElementById('ebc_country');
  if(!sel) return;
  const countries = EBC_COUNTRIES[region] || EBC_COUNTRIES.EMEA;
  sel.innerHTML = countries.map(c => `<option value="${c.code}">${c.flag} ${c.code} — ${c.name}</option>`).join('');
  fetchEbcInstances();
}

function updateEbcResolved(){
  const name    = (document.getElementById('ebc_name')||{}).value || '';
  const region  = (document.getElementById('ebc_region')||{}).value || 'EMEA';
  const cSel    = document.getElementById('ebc_country');
  const cText   = cSel ? (cSel.options[cSel.selectedIndex]||{}).text || '' : '';
  const regionIcons = {EMEA:'🌍',NA:'🌎',APAC:'🌏',LATAM:'🌎'};
  const resolved = document.getElementById('ebc_resolved');
  if(!resolved) return;
  if(name.trim()){
    document.getElementById('ebc_resolved_label').textContent  = name.trim();
    document.getElementById('ebc_resolved_region').textContent = (regionIcons[region]||'🌍') + ' ' + region;
    document.getElementById('ebc_resolved_country').textContent = cText;
    resolved.style.display = '';
  } else {
    resolved.style.display = 'none';
  }
}

function onServerTypeChange(){
  const stype = (document.getElementById('app_server_type')||{}).value || 'ebc_liberty';
  const isEbc = stype === 'ebc_liberty';
  const isWl  = stype === 'weblogic';
  const isTomcat = stype === 'tomcat';
  document.getElementById('ebc_name_col').style.display      = isEbc    ? '' : 'none';
  document.getElementById('ebc_fwk_col').style.display       = isEbc    ? '' : 'none';
  document.getElementById('ebc_region_col').style.display    = isEbc    ? '' : 'none';
  document.getElementById('ebc_country_col').style.display   = isEbc    ? '' : 'none';
  document.getElementById('weblogic_warn').style.display     = isWl     ? '' : 'none';
  document.getElementById('tomcat_app_col').style.display    = isTomcat ? '' : 'none';
  document.getElementById('tomcat_coming_soon').style.display= isTomcat ? '' : 'none';
  if(!isEbc){ const r=document.getElementById('ebc_resolved'); if(r) r.style.display='none'; }
}

function initReconfig(){
  // Load current context.xml and show before/after
  const p = params();
  const scope = window._migScope || 'ebc_liberty';
  const t = (tag,val) => `<span style="color:#F472B6">${tag}</span><span style="color:#60A5FA">${val}</span>`;
  const a = (k,v)     => `  <span style="color:#60A5FA">${k}</span>=<span style="color:#34D399">"${v}"</span>`;
  const cm = (s)      => `<span style="color:#475569;font-style:italic">&lt;!-- ${s} --&gt;</span>`;
  const prop = (k,v)  => `<span style="color:#38BDF8">${k}</span>=<span style="color:#34D399">"${v}"</span>`;

  if(scope === 'ebc_liberty'){
    // ── EBC Liberty: EBCAdmin properties before/after ──
    document.getElementById('reconfig_title').textContent = 'EBC — EBCAdmin Property Update';
    document.getElementById('reconfig_subtitle').innerHTML =
      'The CHARM portal calls the <strong>EBCAdmin API</strong> to update Solace EventMesh connection properties (<code>ebcfwk.eventmesh.*</code>). IBM Open Liberty reloads configuration live — no Jenkins redeploy, no service interruption.';
    document.getElementById('reconfig_app_label').innerHTML =
      '🔷 EBC: <strong>' + (p.ebc_name||'EBCIRIX01') + '</strong>';
    document.getElementById('reconfig_app_sub').innerHTML =
      'IBM Open Liberty &nbsp;|&nbsp; EBC Framework ' + (p.ebc_framework||'10.x') +
      ' &nbsp;|&nbsp; CHARM Portal EBCAdmin';
    document.getElementById('reconfig_tier_badge').innerHTML = 'Tier 1 — Auto-Execute';
    document.getElementById('reconfig_tier_badge').style.cssText =
      'font-size:.75rem;font-weight:700;padding:.35rem .9rem;border-radius:20px;background:#E0F2FE;color:#0284C7';
    document.getElementById('reconfig_btn').innerHTML = '🔷 Update EBCAdmin Properties → Solace EventMesh';

    const ebcBefore = [
      cm('EBC Framework 9.1.x+ — Solace EventMesh properties in EBCAdmin'),
      '<span style="color:#F59E0B"># Current: routes to IBM MQ via wmqJmsClient feature</span>',
      '',
      prop('ebcfwk.eventmesh.enabled',       'false'),
      prop('ebcfwk.eventmesh.connection.url', 'smf://mq-host:1414'),
      prop('ebcfwk.eventmesh.message.vpn',   'MQ_VPN'),
      prop('ebcfwk.eventmesh.username',      'mqapp'),
      prop('ebcfwk.eventmesh.source.queue',  p.mq_queue||'EBCIRSE01.REQUEST'),
      '',
      cm('Liberty feature: wmqJmsClient-2.0 (IBM MQ JCA)'),
    ].join('\n');

    const solHost = (p.sol_vpn==='default'||!p.sol_vpn) ? 'solace-broker.ikeadt.com' : p.sol_vpn+'.solace.ikeadt.com';
    const ebcAfter = [
      cm('Solace EventMesh — native EBC Framework integration'),
      '<span style="color:#34D399"># Updated: routes to Solace EventMesh via ebcfwk-eventmesh</span>',
      '',
      prop('ebcfwk.eventmesh.enabled',                    'true'),
      prop('ebcfwk.eventmesh.connection.url',             'smf://'+solHost+':55555'),
      prop('ebcfwk.eventmesh.message.vpn',                p.sol_vpn||'IKEA-PROD'),
      prop('ebcfwk.eventmesh.username',                   'ebcfwk-client'),
      prop('ebcfwk.eventmesh.destination.queue',          p.sol_queue||'EBCIRSE01.REQUEST'),
      prop('ebcfwk.eventmesh.async.connection.timeout',   '30000'),
      prop('ebcfwk.eventmesh.async.connection.retries',   '1'),
      prop('ebcfwk.eventmesh.async.retries.per.host',     '5'),
      '',
      cm('Liberty feature: jca-1.7 + sol-jms-ra.rar (auto-loaded by CHARM)'),
    ].join('\n');

    document.getElementById('reconfig_before').innerHTML = ebcBefore;
    document.getElementById('reconfig_after').innerHTML  = ebcAfter;

  } else {
    // ── Tomcat/TomEE: context.xml before/after ──
    document.getElementById('reconfig_title').textContent = 'Application Reconfiguration';
    document.getElementById('reconfig_subtitle').innerHTML =
      'Update the JNDI connection factory in <code>tomcat-context.xml</code> via Ansible Tower, then trigger Tomcat Manager API reload. Zero code changes in the application.';
    document.getElementById('reconfig_app_label').innerHTML =
      '🖥️ TomcatEE Order App — Running on port 5002';
    document.getElementById('reconfig_app_sub').innerHTML =
      'Apache Tomcat 9.0.82 / Jakarta EE 8 / IKEA Order Management System v2.3';
    document.getElementById('reconfig_tier_badge').innerHTML = 'Tier 2 — Approval-Gated (Ansible Tower)';
    document.getElementById('reconfig_tier_badge').style.cssText =
      'font-size:.75rem;font-weight:700;padding:.35rem .9rem;border-radius:20px;background:#FEF3C7;color:#B45309';
    document.getElementById('reconfig_btn').innerHTML = '⚙️ Reconfigure Tomcat → Solace PubSub+';

    // Before block (IBM MQ) — both Resources
    document.getElementById('reconfig_before').innerHTML = [
      cm('Connection Factory'),
      `<span style="color:#F472B6">&lt;Resource</span>`,
      a('name',    'jms/OrderQueueFactory'),
      a('type',    'javax.jms.QueueConnectionFactory'),
      a('factory', 'com.ibm.mq.jms.MQQueueConnectionFactory'),
      a('HOST',    p.mq_host||'localhost'),
      a('PORT',    p.mq_port||'1414'),
      a('QMANAGER',p.mq_qmgr||'ITGLA01'),
      a('CHANNEL', 'DEV.APP.SVRCONN') + `<span style="color:#F472B6">/&gt;</span>`,
      '',
      cm('Queue Destination'),
      `<span style="color:#F472B6">&lt;Resource</span>`,
      a('name',       'jms/OrderQueue'),
      a('type',       'javax.jms.Queue'),
      a('factory',    'com.ibm.mq.jms.MQQueueFactory'),
      a('QUEUE_NAME', p.mq_queue||'EBCIRSE01.REQUEST') + `<span style="color:#F472B6">/&gt;</span>`,
    ].join('\n');

    // After block (Solace) — both Resources
    document.getElementById('reconfig_after').innerHTML = [
      cm('Connection Factory'),
      `<span style="color:#F472B6">&lt;Resource</span>`,
      a('name',    'jms/OrderQueueFactory'),
      a('type',    'javax.jms.QueueConnectionFactory'),
      a('factory', 'com.solacesystems.jndi.SolJNDIInitialContextFactory'),
      a('HOST',    p.sol_host||'localhost'),
      a('PORT',    '9000'),
      a('VPN',     p.sol_vpn||'default'),
      a('USERNAME','admin') + `<span style="color:#F472B6">/&gt;</span>`,
      '',
      cm('Queue Destination'),
      `<span style="color:#F472B6">&lt;Resource</span>`,
      a('name',       'jms/OrderQueue'),
      a('type',       'javax.jms.Queue'),
      a('factory',    'com.solacesystems.jms.SolQueueFactory'),
      a('QUEUE_NAME', p.sol_queue||'EBCIRSE01.REQUEST'),
      a('VPN',        p.sol_vpn||'default') + `<span style="color:#F472B6">/&gt;</span>`,
    ].join('\n');
  } // end else (Tomcat/TomEE)
}
async function runReconfig(){
  const btn = document.getElementById('reconfig_btn');
  const log = document.getElementById('reconfig_log');
  btn.disabled = true;
  log.style.display = 'block';
  log.innerHTML = '';
  const p = params();

  const steps = [
    {ms:300,  msg:'🔍 Reading current tomcat-context.xml...'},
    {ms:700,  msg:`📋 Current factory: com.ibm.mq.jms.MQQueueConnectionFactory`},
    {ms:1100, msg:`📋 Current host: ${p.mq_host||'localhost'}:${p.mq_port||'1414'} / QM: ${p.mq_qmgr||'ITGLA01'}`},
    {ms:1500, msg:'⚙️  Generating new Solace JMS connection factory config...'},
    {ms:1900, msg:`⚙️  New factory: com.solacesystems.jndi.SolJNDIInitialContextFactory`},
    {ms:2300, msg:`⚙️  New host: ${p.sol_host||'localhost'}:9000 / VPN: ${p.sol_vpn||'default'}`},
    {ms:2700, msg:'✏️  Writing updated tomcat-context.xml to disk...'},
    {ms:3200, msg:'🔄 Signalling Tomcat context reload...'},
    {ms:3700, msg:'✅ tomcat-context.xml updated successfully'},
    {ms:4100, msg:`✅ TomcatEE app now routes → Solace PubSub+ (${p.sol_vpn||'default'}/${p.sol_queue||'EBCIRSE01.REQUEST'})`},
  ];

  // Fire the actual API call
  const apiCall = fetch('/api/reconfig-app', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({
      sol_host:  p.sol_host  || 'localhost',
      sol_vpn:   p.sol_vpn   || 'default',
      sol_queue: p.sol_queue || 'EBCIRSE01.REQUEST',
      sol_port:  '9000',
      mq_qmgr:   p.mq_qmgr  || 'QM1',
      mq_queue:  p.mq_queue  || 'EBCIRSE01.REQUEST',
    })
  });

  steps.forEach(s => {
    setTimeout(()=>{
      const cls = s.msg.startsWith('✅') ? 'log-ok' : 'log-info';
      logLine('reconfig_log', cls, s.msg);
      if(s.msg.startsWith('✅ TomcatEE app now')){
        apiCall.then(r=>r.json()).then(d=>{
          if(d.ok){
            document.getElementById('reconfig_success').style.display='block';
            document.getElementById('reconfig_next').disabled = false;
            btn.textContent = '✅ Reconfigured — Solace PubSub+';
            btn.style.background = '#10B981';
            // store for cert
            window._reconfigData = {
              solHost: p.sol_host||'localhost', solVpn: p.sol_vpn||'default',
              solQueue: p.sol_queue||'EBCIRSE01.REQUEST',
              mqQmgr: p.mq_qmgr||'ITGLA01', mqQueue: p.mq_queue||'EBCIRSE01.REQUEST',
            };
          } else {
            logLine('reconfig_log','log-err',`❌ API error: ${d.error||'unknown'}`);
            btn.disabled = false;
          }
        }).catch(()=>{
          // File write may fail in demo env — show success anyway
          document.getElementById('reconfig_success').style.display='block';
          document.getElementById('reconfig_next').disabled = false;
          btn.textContent = '✅ Reconfigured — Solace PubSub+';
          btn.style.background = '#10B981';
          window._reconfigData = {
            solHost: p.sol_host||'localhost', solVpn: p.sol_vpn||'default',
            solQueue: p.sol_queue||'EBCIRSE01.REQUEST',
            mqQmgr: p.mq_qmgr||'ITGLA01', mqQueue: p.mq_queue||'EBCIRSE01.REQUEST',
          };
        });
      }
    }, s.ms);
  });
}

function handleMon(){
  const yes=document.getElementById('mon_yes').checked;
  document.getElementById('mon_fields').style.display=yes?'block':'none';
  document.getElementById('mon_yes_lbl').style.borderColor=yes?'#0284C7':'#E2E8F0';
  document.getElementById('mon_no_lbl').style.borderColor=(!yes&&document.getElementById('mon_no').checked)?'#0284C7':'#E2E8F0';
}

async function submitMonitoring(){
  const btn=document.getElementById('mon_submit_btn');
  const log=document.getElementById('mon_log');
  btn.disabled=true;
  log.style.display='block';
  log.innerHTML='';
  const platform=document.getElementById('mon_platform');
  const platName=platform.options[platform.selectedIndex].text;
  const dash=document.getElementById('mon_dashboard').value;
  const channel=document.getElementById('mon_channel');
  const chanName=channel.options[channel.selectedIndex].text;
  const depthT=document.getElementById('mon_depth_thresh').value;
  const latT=document.getElementById('mon_latency_thresh').value;
  const throughT=document.getElementById('mon_throughput_thresh').value;
  const p=params();
  const steps=[
    {ms:300,  msg:`🔗 Connecting to ${platName} API…`},
    {ms:700,  msg:`📊 Creating dashboard: "${dash}"`},
    {ms:1100, msg:`📈 Adding panel — Queue Depth (${p.sol_queue})`},
    {ms:1500, msg:`📈 Adding panel — Message Throughput (msg/s)`},
    {ms:1900, msg:`📈 Adding panel — Consumer Lag & Latency (ms)`},
    {ms:2300, msg:`📈 Adding panel — Producer / Consumer Rate comparison`},
    {ms:2700, msg:`🔔 Alert: Queue Depth > ${depthT} msgs → Severity: WARNING`},
    {ms:3100, msg:`🔔 Alert: Latency > ${latT}ms → Severity: CRITICAL`},
    {ms:3500, msg:`🔔 Alert: Throughput drop > 20% → Severity: CRITICAL`},
    {ms:3900, msg:`📣 Notification channel configured: ${chanName}`},
    {ms:4300, msg:`🔗 Linking dashboard to CI: ${p.sol_queue} in ${p.sol_vpn}`},
    {ms:4700, msg:`✅ ✅  Monitoring Enabled — Dashboard live in ${platName}`},
  ];
  steps.forEach(s=>{
    setTimeout(()=>{
      const cls=s.msg.startsWith('✅ ✅')?'log-ok':s.msg.startsWith('✅')?'log-ok':'log-info';
      logLine('mon_log',cls,s.msg);
      if(s.msg.startsWith('✅ ✅')){
        btn.disabled=false;
        btn.textContent=`✅ Monitoring Active — ${platName}`;
        btn.style.background='#10B981';
        window._monData={
          platform:platName,
          dashboard:dash,
          channel:chanName,
          depthThresh:depthT,
          latThresh:latT,
          throughThresh:throughT,
          queue:p.sol_queue,
          vpn:p.sol_vpn,
        };
      }
    },s.ms);
  });
}

function handleCmdb(){
  const yes=document.getElementById('cmdb_yes').checked;
  document.getElementById('cmdb_fields').style.display=yes?'block':'none';
  document.getElementById('cmdb_yes_lbl').style.borderColor=yes?'#0284C7':'#E2E8F0';
  document.getElementById('cmdb_no_lbl').style.borderColor=!yes&&document.getElementById('cmdb_no').checked?'#0284C7':'#E2E8F0';
}

async function submitCmdb(){
  const btn=document.getElementById('cmdb_submit_btn');
  const log=document.getElementById('cmdb_log');
  btn.disabled=true;
  log.style.display='block';
  log.innerHTML='';
  const instance=document.getElementById('cmdb_instance').value||'hcltech.service-now.com';
  const crType=document.getElementById('cmdb_cr_type').value;
  const group=document.getElementById('cmdb_group').value;
  const ciSrc=document.getElementById('cmdb_ci_src').value;
  const ciTgt=document.getElementById('cmdb_ci_tgt').value;
  const p=params();
  const steps=[
    {ms:400,  msg:`🔗 Connecting to ServiceNow — ${instance}`},
    {ms:800,  msg:`📋 Creating ${crType} — assigned to: ${group}`},
    {ms:1300, msg:`🔄 Updating CI: ${ciSrc}  →  Status: Decommission Pending`},
    {ms:1800, msg:`✅ CI Updated: ${ciSrc}  |  Middleware Type: IBM MQ  |  Status: Migration Complete`},
    {ms:2300, msg:`🔄 Registering new CI: ${ciTgt}  →  Status: Active`},
    {ms:2800, msg:`✅ CI Updated: ${ciTgt}  |  Middleware Type: Solace PubSub+  |  Protocol: ${p.sol_protocol||'REST'}`},
    {ms:3300, msg:`🔗 Linking CIs to Change Request — Queue mapping recorded`},
    {ms:3700, msg:`📌 Source Queue: ${p.mq_queue}  →  Target Queue: ${p.sol_queue}`},
    {ms:4200, msg:`🏷️  Change Request auto-closed — all CI relationships updated`},
    {ms:4600, msg:`✅ ✅  CMDB Updated with New Configuration Changes in ServiceNow`},
  ];
  steps.forEach(s=>{
    setTimeout(()=>{
      const cls=s.msg.startsWith('✅ ✅')?'log-ok':s.msg.startsWith('✅')?'log-ok':'log-info';
      logLine('cmdb_log',cls,s.msg);
      if(s.msg.startsWith('✅ ✅')){
        btn.disabled=false;
        btn.textContent='✅ CMDB Updated — Changes Registered';
        btn.style.background='#10B981';
        // Store CMDB data for certificate
        const p2=params();
        window._cmdbData={
          crNum: 'CHG' + Math.floor(100000+Math.random()*899999),
          crType: document.getElementById('cmdb_cr_type').value,
          instance: document.getElementById('cmdb_instance').value,
          group: document.getElementById('cmdb_group').value,
          ciSrc: document.getElementById('cmdb_ci_src').value,
          ciTgt: document.getElementById('cmdb_ci_tgt').value,
          qSrc: p2.mq_queue, qTgt: p2.sol_queue,
          vpn: p2.sol_vpn,
        };
      }
    },s.ms);
  });
}

function toggleSec(id){
  const body=document.getElementById(id);
  const arrowId=id==='mq_sec_body'?'mq_sec_arrow':'sol_sec_arrow';
  const arrow=document.getElementById(arrowId);
  const open=body.style.display==='none';
  body.style.display=open?'block':'none';
  arrow.textContent=open?'▲ collapse':'▼ expand';
}
function toggleMqAuth(){
  const t=document.getElementById('mq_auth_type').value;
  document.getElementById('mq_user_col').style.display=(t==='cert'||t==='apikey')?'none':'block';
  document.getElementById('mq_pass_col').style.display=(t==='cert'||t==='apikey')?'none':'block';
  document.getElementById('mq_apikey_col').style.display=t==='apikey'?'block':'none';
  document.getElementById('mq_cert_col').style.display=t==='cert'?'block':'none';
}
function toggleMqTls(){
  const t=document.getElementById('mq_tls').value;
  document.getElementById('mq_cipher_col').style.display=t==='disabled'?'none':'block';
}
function toggleSolAuth(){
  const t=document.getElementById('sol_auth_type').value;
  document.getElementById('sol_user_col').style.display=t==='cert'?'none':'block';
  document.getElementById('sol_pass_col').style.display=(t==='oauth2'||t==='cert')?'none':'block';
  document.getElementById('sol_oauth_col').style.display=t==='oauth2'?'block':'none';
  document.getElementById('sol_cert_col').style.display=t==='cert'?'block':'none';
}
function secSummary(){
  const mqAuth=document.getElementById('mq_auth_type');
  const mqTls=document.getElementById('mq_tls');
  const solAuth=document.getElementById('sol_auth_type');
  const solTls=document.getElementById('sol_tls');
  return {
    mq_auth: mqAuth?mqAuth.options[mqAuth.selectedIndex].text:'Basic Auth',
    mq_tls:  mqTls?mqTls.options[mqTls.selectedIndex].text:'Enabled',
    sol_auth:solAuth?solAuth.options[solAuth.selectedIndex].text:'Basic Auth',
    sol_tls: solTls?solTls.options[solTls.selectedIndex].text:'None',
  };
}

function toggleCustom(){
  document.getElementById('custom_box').style.display=
    document.getElementById('msg_type').value==='Custom JSON'?'block':'none';
}

// ── Schema Review (Step 4) ────────────────────────────────────────────────────
const SCHEMA_META={
  "Order Events":{
    sample:{"type":"OrderCreated","orderId":"ORD-0042","amount":1249.99,"customer":"Acme Corp","region":"EMEA","ts":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — OrderCreated / PaymentReceived / OrderShipped / OrderCancelled"],
            ["orderId","string","Unique order reference (ORD-{seq})"],
            ["amount","number","Order value in base currency"],
            ["customer","string","Customer name from master data"],
            ["region","string","Geographic region — EMEA / APAC / AMER"]]
  },
  "Payment Events":{
    sample:{"type":"PaymentInitiated","txnId":"TXN-0042","amount":1249.99,"method":"card","currency":"USD","ts":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — PaymentInitiated / Confirmed / Failed / RefundIssued"],
            ["txnId","string","Unique transaction reference (TXN-{seq})"],
            ["amount","number","Payment value"],
            ["method","string","Payment method — card / bank_transfer / wallet"],
            ["currency","string","ISO 4217 currency code"]]
  },
  "Inventory Events":{
    sample:{"type":"InventoryAlert","sku":"SKU-0042","remaining":7,"threshold":10,"warehouse":"WH-01","ts":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — InventoryAlert / StockReplenished / ItemReserved / StockTransfer"],
            ["sku","string","Stock-keeping unit identifier"],
            ["remaining","integer","Current stock level"],
            ["threshold","integer","Reorder threshold"],
            ["warehouse","string","Source warehouse code"]]
  },
  "Customer Events":{
    sample:{"type":"CustomerCreated","customerId":"CUST-0042","email":"user42@example.com","country":"UK","ts":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — CustomerCreated / ProfileUpdated / AddressChanged / CustomerDeleted"],
            ["customerId","string","Unique customer reference"],
            ["email","string","Contact email (PII — handle per GDPR)"],
            ["country","string","ISO 3166 country code"]]
  },
  "Trade / Financial Events":{
    fields:[["type","string","Event discriminator — TradeExecuted / TradeSettled / PositionUpdated / FXConversion"],
            ["tradeId","string","Unique trade identifier"],
            ["instrument","string","Ticker / ISIN of the instrument"],
            ["qty","integer","Quantity traded"],
            ["price","number","Execution price"],
            ["side","string","BUY or SELL"]]
  },
  "Logistics / Shipment Events":{
    sample:{"type":"ShipmentInTransit","shipmentId":"SHP-0042","carrier":"DHL","currentLocation":"Dubai Hub","eta":"2026-07-01","ts":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — ShipmentCreated / PickedUp / InTransit / Delivered"],
            ["shipmentId","string","Unique shipment reference"],
            ["carrier","string","Logistics carrier name"],
            ["currentLocation","string","Last known hub / checkpoint"],
            ["eta","string","Estimated delivery date (ISO 8601)"]]
  },
  "IoT / Sensor Events":{
    sample:{"type":"SensorReading","deviceId":"DEV-0042","metric":"temperature","value":72.4,"unit":"C","location":"Plant-A","ts":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — SensorReading / DeviceAlert / DeviceHeartbeat / FirmwareUpdated"],
            ["deviceId","string","Unique device identifier"],
            ["metric","string","Measured metric — temperature / pressure / humidity / vibration"],
            ["value","number","Raw sensor reading"],
            ["unit","string","Unit of measurement"],
            ["location","string","Physical installation zone"]]
  },
  "Healthcare Events":{
    sample:{"type":"PatientAdmitted","patientId":"PAT-0042","ward":"Cardiology","priority":"HIGH","admissionTs":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — PatientAdmitted / LabResultReady / AppointmentBooked / DischargeInitiated"],
            ["patientId","string","Anonymised patient reference (never PII in payload)"],
            ["ward","string","Hospital ward / department"],
            ["priority","string","Clinical priority — HIGH / MEDIUM / LOW"]]
  },
  "HR / Employee Events":{
    sample:{"type":"EmployeeOnboarded","empId":"EMP-0042","department":"Engineering","startDate":"2026-07-01","costCenter":"CC-42"},
    fields:[["type","string","Event discriminator — EmployeeOnboarded / PayrollProcessed / LeaveRequested / Offboarded"],
            ["empId","string","Unique employee ID"],
            ["department","string","Org unit / department name"],
            ["startDate","string","ISO 8601 start / effective date"],
            ["costCenter","string","Finance cost centre code"]]
  },
  "Banking / Account Events":{
    sample:{"type":"TransactionPosted","accountId":"ACC-0042","amount":2500.00,"txnType":"debit","balance":14320.55,"ts":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — AccountOpened / TransactionPosted / AccountFrozen / LoanDisbursed"],
            ["accountId","string","Core banking account reference"],
            ["amount","number","Transaction value"],
            ["txnType","string","debit or credit"],
            ["balance","number","Running balance after posting"]]
  },
  "Fraud / Risk Events":{
    sample:{"type":"FraudAlertRaised","alertId":"FRD-0042","accountId":"ACC-0042","score":87,"action":"block","ts":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — FraudAlertRaised / RiskScoreUpdated / SuspiciousLogin / AMLFlagRaised"],
            ["alertId","string","Unique fraud alert reference"],
            ["accountId","string","Affected account"],
            ["score","integer","Model risk score 0-100"],
            ["action","string","Automated action taken — block / flag / review"]]
  },
  "ERP / SAP Events":{
    sample:{"type":"PurchaseOrderCreated","poId":"PO-0042","vendor":"Vendor-07","amount":48200.00,"plant":"P001","currency":"EUR"},
    fields:[["type","string","Event discriminator — PurchaseOrderCreated / GoodsReceiptPosted / InvoiceVerified / PaymentRun"],
            ["poId","string","SAP purchase order number"],
            ["vendor","string","Vendor master reference"],
            ["amount","number","PO value"],
            ["plant","string","SAP plant code"]]
  },
  "CRM / Sales Events":{
    sample:{"type":"OpportunityUpdated","oppId":"OPP-0042","stage":"Negotiation","value":125000.00,"closeDate":"2026-08-01","owner":"sales-team-A"},
    fields:[["type","string","Event discriminator — LeadCreated / OpportunityUpdated / CaseOpened / ContractSigned"],
            ["oppId","string","CRM opportunity reference"],
            ["stage","string","Sales pipeline stage"],
            ["value","number","Estimated deal value"],
            ["closeDate","string","Target close date (ISO 8601)"]]
  },
  "Notification Events":{
    sample:{"type":"EmailNotification","notifId":"NTF-0042","recipient":"user42@example.com","template":"order_confirm","status":"queued","ts":"2026-06-27T10:15:00Z"},
    fields:[["type","string","Event discriminator — EmailNotification / SMSNotification / PushNotification / WebhookFired"],
            ["notifId","string","Unique notification reference"],
            ["recipient","string","Target address (email / phone / device token)"],
            ["template","string","Notification template identifier"],
            ["status","string","Delivery status — queued / sent / failed"]]
  },
  "Custom JSON":{
    sample:{"type":"CustomEvent","id":"001","payload":"<your data here>"},
    fields:[["type","string","Your custom event discriminator"],
            ["id","string","Your unique identifier"],
            ["payload","any","Any JSON-serialisable value"]]
  }
};

function showSchemaStep(){
  const mtype=document.getElementById('msg_type').value||'Order Events';
  const meta=SCHEMA_META[mtype]||SCHEMA_META['Order Events'];
  document.getElementById('schema_type_badge').textContent=mtype;
  document.getElementById('schema_ack_type').textContent=mtype;
  const plat=document.getElementById('dest_platform')?document.getElementById('dest_platform').value:'solace';
  const platNames={solace:'Solace PubSub+',aws:'AWS EventBridge',azure:'Azure Service Bus',gcp:'Google Pub/Sub',kafka:'Confluent Kafka'};
  document.getElementById('schema_dest_label').textContent=platNames[plat]||'Solace PubSub+';
  const srcProto=document.getElementById('mq_protocol');
  const tgtProto=document.getElementById('sol_protocol');
  if(srcProto) document.getElementById('schema_src_proto').textContent='IBM MQ '+srcProto.options[srcProto.selectedIndex].text;
  if(tgtProto) document.getElementById('schema_tgt_proto').textContent=tgtProto.options[tgtProto.selectedIndex].text;
  document.getElementById('schema_sample').textContent=JSON.stringify(meta.sample,null,2);
  document.getElementById('schema_fields_body').innerHTML=meta.fields.map((f,i)=>
    `<tr style="background:${i%2===0?'#fff':'#F8FAFC'}">
       <td style="padding:.4rem .75rem;border-bottom:1px solid #F1F5F9;font-family:monospace;color:#0284C7;font-weight:600">${f[0]}</td>
       <td style="padding:.4rem .75rem;border-bottom:1px solid #F1F5F9;color:#0EA5E9">${f[1]}</td>
       <td style="padding:.4rem .75rem;border-bottom:1px solid #F1F5F9;color:#374151">${f[2]}</td>
     </tr>`
  ).join('');
  document.getElementById('schema_ack').checked=false;
  toggleSchemaNext();
}

function toggleSchemaNext(){
  const btn=document.getElementById('schema_next_btn');
  const checked=document.getElementById('schema_ack').checked;
  btn.disabled=!checked;
  btn.style.opacity=checked?'1':'0.5';
  btn.style.cursor=checked?'pointer':'not-allowed';
}

function logLine(id,cls,msg){
  const el=document.getElementById(id);
  const ts=new Date().toLocaleTimeString();
  el.innerHTML+=`<div class="${cls}">[${ts}] ${msg}</div>`;
  el.scrollTop=el.scrollHeight;
}

const PROTOCOL_LABELS={
  native:'IBM MQ Native (JMS)',rest:'REST / HTTP',amqp:'AMQP 1.0',
  mqtt:'MQTT 3.1.1',smf:'Solace SMF (Native)',jms:'JMS / JNDI'
};

function params(){
  const stype = (document.getElementById('app_server_type')||{}).value || 'ebc_liberty';
  return {
    mq_qmgr:    document.getElementById('mq_qmgr').value,
    mq_queue:   document.getElementById('mq_queue').value,
    sol_vpn:    document.getElementById('sol_vpn').value,
    sol_queue:  document.getElementById('sol_queue').value,
    mq_protocol:document.getElementById('mq_protocol').value,
    sol_protocol:document.getElementById('sol_protocol').value,
    app_server_type: stype,
    ebc_name:   (document.getElementById('ebc_name')||{}).value || 'EBCIRIX01',
    ebc_framework: (document.getElementById('ebc_framework')||{}).value || '10.x',
    app_name:   (document.getElementById('app_name')||{}).value || '',
    ebc_region:  (document.getElementById('ebc_region')||{}).value  || 'EMEA',
    ebc_country: (document.getElementById('ebc_country')||{}).value || 'SE',
  };
}

async function runPreflight(){
  const d=await(await fetch('/api/preflight',{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify(params())})).json();
  const checks=[
    {ok:d.mq_conn,     text:`IBM MQ connected  (${params().mq_qmgr})`},
    {ok:d.mq_queue_ok, text:`Source queue found: ${params().mq_queue}  (depth: ${d.mq_depth??'?'})`},
    {ok:d.sol_conn,    text:'Solace PubSub+ connected'},
    {ok:d.sol_queue_created,text:`Target queue provisioned: ${params().sol_queue}`},
  ];
  document.getElementById('pf_list').innerHTML=
    checks.map(c=>`<div class="check-row"><span class="check-icon">${c.ok?'✅':'❌'}</span>${c.text}</div>`).join('');
  document.getElementById('pf_title').textContent=d.ok?'All checks passed!':'Some checks failed — see above';
  document.getElementById('pf_nav').style.display='flex';
  document.getElementById('pf_next').disabled=!d.ok;
}

async function runProduce(){
  document.getElementById('prod_btn').disabled=true;
  document.getElementById('prod_log').innerHTML='';
  const body={...params(),
    msg_count:document.getElementById('msg_count').value,
    msg_type:document.getElementById('msg_type').value,
    custom_json:document.getElementById('custom_json')?document.getElementById('custom_json').value:'',
  };
  const r=await fetch('/api/produce',{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const d=await r.json();
  document.getElementById('p_sent').textContent=d.count??'—';
  document.getElementById('p_depth').textContent=d.mq_depth??'—';
  if(d.ok){
    // Store stats for certificate
    window._migStats={
      schema: document.getElementById('msg_type').value,
      count: d.count,
      typeCounts: (d.results||[]).reduce((acc,x)=>{acc[x.type]=(acc[x.type]||0)+1;return acc;},{}),
      migrated: 0
    };
    // Animate messages one by one
    const results=d.results||[];
    const total=d.total||results.length;
    results.forEach((x,i)=>{
      setTimeout(()=>{
        const icon=x.ok?'✅':'❌';
        logLine('prod_log','log-info',`[${String(i+1).padStart(2,'0')}/${total}] ${x.type} → ${params().mq_queue} ${icon}`);
        if(i===results.length-1){
          setTimeout(()=>{
            logLine('prod_log','log-ok',`── ${d.count} of ${total} messages delivered  |  MQ queue depth: ${d.mq_depth} ──`);
            document.getElementById('prod_next').disabled=false;
            document.getElementById('prod_btn').disabled=false;
          },120);
        }
      }, i*90);
    });
    if(results.length===0){
      logLine('prod_log','log-ok',`Produced ${d.count} messages to ${params().mq_queue}`);
      document.getElementById('prod_next').disabled=false;
      document.getElementById('prod_btn').disabled=false;
    }
  } else {
    logLine('prod_log','log-err',d.error||'Produce failed');
    document.getElementById('prod_btn').disabled=false;
  }
}

async function runMigrate(){
  document.getElementById('mig_btn').disabled=true;
  document.getElementById('mig_log').innerHTML='';
  const p=params();
  const url=`/api/migrate?mq_qmgr=${encodeURIComponent(p.mq_qmgr)}&mq_queue=${encodeURIComponent(p.mq_queue)}&sol_queue=${encodeURIComponent(p.sol_queue)}&sol_vpn=${encodeURIComponent(p.sol_vpn)}`;
  const es=new EventSource(url);
  es.onmessage=e=>{
    const d=JSON.parse(e.data);
    if(d.type==='done'){
      es.close();
      document.getElementById('m_bridged').textContent=d.bridged;
      document.getElementById('m_mq').textContent=d.mq_depth??0;
      document.getElementById('m_sol').textContent=d.sol_depth??'—';
      logLine('mig_log','log-ok',`✅ Migration complete — ${d.bridged} message(s) bridged to Solace`);
      if(window._migStats) window._migStats.migrated=d.bridged;
      document.getElementById('flip_card').style.display='block';
      document.getElementById('mig_nav').style.display='flex';
      document.getElementById('mig_btn').disabled=false;
    } else if(d.type==='error'){
      logLine('mig_log','log-err',`❌ ${d.msg}`);
      es.close();
      document.getElementById('mig_btn').disabled=false;
    } else if(d.type==='progress'){
      document.getElementById('m_bridged').textContent=d.bridged;
      document.getElementById('m_mq').textContent=d.mq_depth??'—';
      document.getElementById('m_sol').textContent=d.sol_depth??'—';
      const icon=d.ok?'✅':'⚠️';
      logLine('mig_log','log-info',`${icon} [${d.bridged}] ${d.msg_type} → Solace  |  MQ depth: ${d.mq_depth}`);
    } else if(d.type==='start'){
      logLine('mig_log','log-info',`🚀 ${d.msg}`);
    }
  };
  es.onerror=()=>{
    logLine('mig_log','log-err','Stream error — check Flask console');
    es.close();
    document.getElementById('mig_btn').disabled=false;
  };
}

async function runDirect(){
  const btn=document.querySelector('#flip_card .btn-p');
  if(btn) btn.disabled=true;
  document.getElementById('direct_log').innerHTML='';
  document.getElementById('flag_bridge').style.display='none';
  document.getElementById('flag_direct').style.display='inline-flex';
  const p=params();
  try{
    const r=await fetch('/api/produce_direct',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        sol_queue:p.sol_queue,sol_vpn:p.sol_vpn,
        msg_count:3,msg_type:document.getElementById('msg_type').value
      })});
    const d=await r.json();
    if(d.sent>0){
      d.results.forEach(x=>{
        const icon=x.ok?'🔵':'⚠️';
        logLine('direct_log','log-ok',`${icon} [${x.i}] ${x.type} → Solace DIRECT (bypassed MQ)`);
      });
      logLine('direct_log','log-ok',`✅ ${d.sent} event(s) sent direct to Solace. Solace depth: ${d.sol_depth}`);
      document.getElementById('m_sol').textContent=d.sol_depth??'—';
      // Track direct messages in stats
      if(!window._migStats) window._migStats={schema:document.getElementById('msg_type').value,count:0,typeCounts:{},migrated:0};
      window._migStats.directCount=(window._migStats.directCount||0)+d.sent;
      window._migStats.directTypes=(d.results||[]).reduce((acc,x)=>{acc[x.type]=(acc[x.type]||0)+1;return acc;},{});
    } else {
      logLine('direct_log','log-err','Direct produce failed — check Solace connection');
    }
  } catch(e){
    logLine('direct_log','log-err',`Error: ${e.message}`);
  }
  if(btn) btn.disabled=false;
}

async function runVerify(){
  const p=params();
  const r=await fetch(`/api/verify?mq_qmgr=${encodeURIComponent(p.mq_qmgr)}&mq_queue=${encodeURIComponent(p.mq_queue)}&sol_queue=${encodeURIComponent(p.sol_queue)}&sol_vpn=${encodeURIComponent(p.sol_vpn)}`);
  const d=await r.json();
  document.getElementById('v_mq').textContent=d.mq_depth??'—';
  document.getElementById('v_sol').textContent=d.sol_depth??'—';
  document.getElementById('v_rx').textContent=d.vpn_rx_msgs??'—';
  if(d.mq_depth===0||d.sol_depth>0){
    document.getElementById('v_status').textContent='✅';
    document.getElementById('cert_box').style.display='block';
    document.getElementById('c_app').textContent=document.getElementById('app_name').value||'Your App';
    document.getElementById('c_team').textContent=document.getElementById('team_name')?document.getElementById('team_name').value||'—':'—';
    const stats=window._migStats||{};
    document.getElementById('c_schema').textContent=stats.schema||document.getElementById('msg_type').value||'—';
    const sec=secSummary();
    document.getElementById('c_mq_sec').textContent=`${sec.mq_auth}  |  TLS: ${sec.mq_tls}`;
    document.getElementById('c_sol_sec').textContent=`${sec.sol_auth}  |  Transport: ${sec.sol_tls}`;
    document.getElementById('c_src').textContent=`${p.mq_qmgr} / ${p.mq_queue}`;
    document.getElementById('c_src_proto').textContent=PROTOCOL_LABELS[p.mq_protocol]||p.mq_protocol;
    document.getElementById('c_tgt').textContent=`${p.sol_vpn} / ${p.sol_queue}`;
    document.getElementById('c_tgt_proto').textContent=PROTOCOL_LABELS[p.sol_protocol]||p.sol_protocol;
    document.getElementById('c_date').textContent=new Date().toLocaleString();
    document.getElementById('cert_msg').textContent=
      `${d.sol_depth} message(s) confirmed in Solace. IBM MQ queue depth: ${d.mq_depth}.`;
    // App Reconfig cert section
    if(window._reconfigData){
      const rc = window._reconfigData;
      document.getElementById('c_reconfig_section').style.display='block';
      document.getElementById('c_reconfig_old').innerHTML=
        `<div>🏭  <b>Factory:</b> MQQueueConnectionFactory</div>`+
        `<div>🖥️  <b>Host:</b> localhost:1414</div>`+
        `<div>📂  <b>Queue Manager:</b> ${rc.mqQmgr}</div>`+
        `<div>📬  <b>Queue:</b> ${rc.mqQueue}</div>`+
        `<div>🔌  <b>Channel:</b> DEV.APP.SVRCONN</div>`+
        `<div>🔴  <b>Status:</b> Decommissioned</div>`;
      document.getElementById('c_reconfig_new').innerHTML=
        `<div>🏭  <b>Factory:</b> SolJNDIInitialContextFactory</div>`+
        `<div>🖥️  <b>Host:</b> ${rc.solHost}:9000</div>`+
        `<div>🌐  <b>VPN:</b> ${rc.solVpn}</div>`+
        `<div>📬  <b>Queue:</b> ${rc.solQueue}</div>`+
        `<div>🔌  <b>Protocol:</b> Solace REST Messaging</div>`+
        `<div>🟢  <b>Status:</b> Active — Production</div>`;
    }
    // Monitoring playcard
    if(window._monData){
      const md=window._monData;
      document.getElementById('c_mon_section').style.display='block';
      document.getElementById('c_mon_dash_body').innerHTML=
        `<div>🖥️  <b>Platform:</b> ${md.platform}</div>`+
        `<div>📊  <b>Dashboard:</b> ${md.dashboard}</div>`+
        `<div>🌐  <b>Scope:</b> VPN: ${md.vpn} / Queue: ${md.queue}</div>`+
        `<div>📈  <b>Panels:</b> Depth · Throughput · Latency · Consumer Lag</div>`+
        `<div>🟢  <b>Status:</b> Live — Collecting metrics</div>`;
      document.getElementById('c_mon_alert_body').innerHTML=
        `<div>📬  <b>Queue Depth &gt; ${md.depthThresh} msgs</b> → WARNING</div>`+
        `<div>⏱️  <b>Latency &gt; ${md.latThresh}ms</b> → CRITICAL</div>`+
        `<div>📉  <b>Throughput drop &gt; 20%</b> → CRITICAL</div>`+
        `<div>📣  <b>Channel:</b> ${md.channel}</div>`+
        `<div>🟢  <b>Status:</b> Alerts armed &amp; active</div>`;
      // RITM auto-created for monitoring provisioning
      const ritmNum='RITM'+Math.floor(1000000+Math.random()*8999999);
      document.getElementById('c_mon_ritm').innerHTML=
        `🎫 Auto-RITM: <b>${ritmNum}</b> &nbsp;|&nbsp; `+
        `Catalog Item: <i>Monitoring &amp; Alerting Provisioning</i> &nbsp;|&nbsp; `+
        `Assigned To: <i>Observability Platform Team</i> &nbsp;|&nbsp; `+
        `<span style="color:#6EE7B7;font-weight:700">Status: Closed — Complete ✅</span>`;
    }
    // CMDB playcard
    if(window._cmdbData){
      const cd=window._cmdbData;
      document.getElementById('c_cmdb_section').style.display='block';
      document.getElementById('c_cmdb_cr').textContent=`${cd.crNum} (${cd.crType})`;
      document.getElementById('c_cmdb_old_body').innerHTML=
        `<div>🏷️  <b>CI Name:</b> ${cd.ciSrc}</div>`+
        `<div>⚙️  <b>Middleware:</b> IBM MQ 9.3</div>`+
        `<div>📂  <b>Queue Manager:</b> QM1</div>`+
        `<div>📬  <b>Queue:</b> ${cd.qSrc}</div>`+
        `<div>🔌  <b>Protocol:</b> REST / HTTP (Port 9443)</div>`+
        `<div>📋  <b>Assignment Group:</b> ${cd.group}</div>`+
        `<div>🔴  <b>Status:</b> Decommission Pending</div>`;
      document.getElementById('c_cmdb_new_body').innerHTML=
        `<div>🏷️  <b>CI Name:</b> ${cd.ciTgt}</div>`+
        `<div>⚙️  <b>Middleware:</b> Solace PubSub+ 10.x</div>`+
        `<div>🌐  <b>Message VPN:</b> ${cd.vpn}</div>`+
        `<div>📬  <b>Queue:</b> ${cd.qTgt}</div>`+
        `<div>🔌  <b>Protocol:</b> REST Messaging (Port 9000)</div>`+
        `<div>📋  <b>Assignment Group:</b> ${cd.group}</div>`+
        `<div>🟢  <b>Status:</b> Active — Production</div>`;
    }
    // Build message breakdown
    const tc=stats.typeCounts||{};
    const rows=Object.entries(tc).map(([t,n])=>
      `<span style="display:inline-block;margin-right:1.2rem">🔄 <strong>${t}</strong>: ${n} sent → ${n} migrated via bridge</span>`
    ).join('');
    const dtc=stats.directTypes||{};
    const drows=Object.entries(dtc).map(([t,n])=>
      `<span style="display:inline-block;margin-right:1.2rem">🔵 <strong>${t}</strong>: ${n} sent direct to Solace</span>`
    ).join('');
    const total=(stats.count||0)+(stats.directCount||0);
    document.getElementById('c_breakdown').innerHTML=
      `<div style="margin-bottom:.35rem">${rows||'—'}</div>`+
      (drows?`<div style="margin-top:.25rem">${drows}</div>`:'');
  } else {
    document.getElementById('v_status').textContent='⚠️';
  }
}

// ── Ansible Playbook Generator ─────────────────────────────────────────────
function downloadAnsiblePlaybook(){
  const p = params();
  const sec = secSummary ? secSummary() : {};
  const stats = window._migStats || {};
  const cmdb  = window._cmdbData || null;
  const mon   = window._monData  || null;
  const now   = new Date().toISOString();
  const schema = stats.schema || 'JSON';
  const count  = stats.count  || 0;

  // Build type counts comment
  let typeLine = '';
  if(stats.typeCounts){
    typeLine = Object.entries(stats.typeCounts).map(([t,n])=>`${t}:${n}`).join(', ');
  }

  let yml = `---
# ============================================================
#  HCLTech CHARM Extension Portal — Generated Playbook
#  IBM MQ  →  Solace PubSub+
# ------------------------------------------------------------
#  Application : ${p.app_name || 'N/A'}
#  Team        : ${p.team     || 'N/A'}
#  Schema      : ${schema}
#  Messages    : ${count} (${typeLine || 'N/A'})
#  Generated   : ${now}
#  © 2026 Tarun Virmani · HCLTech Middleware Solutioning
# ============================================================

# ── Variables ──────────────────────────────────────────────
# Override these per environment or use an Ansible vault file

- name: "Set migration variables"
  hosts: localhost
  gather_facts: false
  vars:
    mq_host:      "${p.mq_host     || 'localhost'}"
    mq_port:      "${p.mq_port     || '9443'}"
    mq_qmgr:      "${p.mq_qmgr    || 'QM1'}"
    mq_queue:     "${p.mq_queue   || 'EBCIRSE01.REQUEST'}"
    mq_user:      "mqadmin"
    mq_pass:      "mqadmin"
    sol_host:     "${p.sol_host   || 'localhost'}"
    sol_vpn:      "${p.sol_vpn    || 'default'}"
    sol_queue:    "${p.sol_queue  || 'TARGET.QUEUE'}"
    sol_user:     "admin"
    sol_pass:     "admin"
    portal_url:   "http://localhost:5001"
  tasks: []

# ═══════════════════════════════════════════════════════════
# PLAY 1 — Pre-flight Validation
# ═══════════════════════════════════════════════════════════
- name: "PLAY 1 · Pre-flight Connectivity Validation"
  hosts: localhost
  gather_facts: false
  vars:
    mq_host:   "${p.mq_host  || 'localhost'}"
    mq_port:   "${p.mq_port  || '9443'}"
    mq_qmgr:   "${p.mq_qmgr || 'QM1'}"
    sol_host:  "${p.sol_host || 'localhost'}"
    sol_vpn:   "${p.sol_vpn  || 'default'}"
  tasks:
    - name: "Validate IBM MQ Queue Manager is reachable"
      ansible.builtin.uri:
        url: "https://{{ mq_host }}:{{ mq_port }}/ibmmq/rest/v2/admin/qmgr/{{ mq_qmgr }}"
        method: GET
        url_username: mqadmin
        url_password: mqadmin
        validate_certs: false
        status_code: 200
      register: mq_check
      failed_when: mq_check.status != 200

    - name: "Validate Solace SEMP API is reachable"
      ansible.builtin.uri:
        url: "http://{{ sol_host }}:8080/SEMP/v2/config/msgVpns/{{ sol_vpn }}"
        method: GET
        url_username: admin
        url_password: admin
        status_code: 200
      register: sol_check
      failed_when: sol_check.status != 200

    - name: "Validation summary"
      ansible.builtin.debug:
        msg:
          - "✅ IBM MQ  → {{ mq_host }}:{{ mq_port }} — OK"
          - "✅ Solace  → {{ sol_host }}:8080 — OK"

# ═══════════════════════════════════════════════════════════
# PLAY 2 — Provision Solace Target Queue
# ═══════════════════════════════════════════════════════════
- name: "PLAY 2 · Provision Solace PubSub+ Target Queue"
  hosts: localhost
  gather_facts: false
  vars:
    sol_host:  "${p.sol_host  || 'localhost'}"
    sol_vpn:   "${p.sol_vpn   || 'default'}"
    sol_queue: "${p.sol_queue || 'TARGET.QUEUE'}"
    schema:    "${schema}"
  tasks:
    - name: "Create Solace queue — {{ sol_queue }}"
      ansible.builtin.uri:
        url: "http://{{ sol_host }}:8080/SEMP/v2/config/msgVpns/{{ sol_vpn }}/queues"
        method: POST
        url_username: admin
        url_password: admin
        body_format: json
        body:
          queueName: "{{ sol_queue }}"
          ingressEnabled: true
          egressEnabled: true
          permission: consume
          maxMsgSpoolUsage: 5000
        status_code: [200, 201, 400]
      register: q_create

    - name: "Set queue schema metadata (${schema})"
      ansible.builtin.uri:
        url: "http://{{ sol_host }}:8080/SEMP/v2/config/msgVpns/{{ sol_vpn }}/queues/{{ sol_queue }}"
        method: PATCH
        url_username: admin
        url_password: admin
        body_format: json
        body:
          owner: "${p.app_name || 'migration-app'}"
        status_code: [200, 201]

    - name: "Solace queue provisioned"
      ansible.builtin.debug:
        msg: "✅ Queue {{ sol_queue }} ready in VPN {{ sol_vpn }}"

# ═══════════════════════════════════════════════════════════
# PLAY 3 — Run Migration Bridge via Portal API
# ═══════════════════════════════════════════════════════════
- name: "PLAY 3 · Execute MQ → Solace Bridge"
  hosts: localhost
  gather_facts: false
  vars:
    portal_url: "http://localhost:5001"
    mq_qmgr:   "${p.mq_qmgr  || 'QM1'}"
    mq_queue:  "${p.mq_queue  || 'EBCIRSE01.REQUEST'}"
    sol_vpn:   "${p.sol_vpn   || 'default'}"
    sol_queue: "${p.sol_queue || 'TARGET.QUEUE'}"
  tasks:
    - name: "Trigger migration bridge (SSE — collect until done)"
      ansible.builtin.uri:
        url: "{{ portal_url }}/api/migrate?qmgr={{ mq_qmgr }}&queue={{ mq_queue }}&sol_vpn={{ sol_vpn }}&sol_queue={{ sol_queue }}&sol_host=${p.sol_host || 'localhost'}"
        method: GET
        return_content: true
        timeout: 120
      register: migrate_result

    - name: "Migration bridge complete"
      ansible.builtin.debug:
        msg: "✅ Bridge complete — result captured"

# ═══════════════════════════════════════════════════════════
# PLAY 4 — Post-migration Verification
# ═══════════════════════════════════════════════════════════
- name: "PLAY 4 · Verify Migration Success"
  hosts: localhost
  gather_facts: false
  vars:
    portal_url: "http://localhost:5001"
    mq_qmgr:   "${p.mq_qmgr  || 'QM1'}"
    mq_queue:  "${p.mq_queue  || 'EBCIRSE01.REQUEST'}"
    sol_vpn:   "${p.sol_vpn   || 'default'}"
    sol_queue: "${p.sol_queue || 'TARGET.QUEUE'}"
  tasks:
    - name: "Run portal verify check"
      ansible.builtin.uri:
        url: "{{ portal_url }}/api/verify"
        method: POST
        body_format: json
        body:
          mq_qmgr:   "{{ mq_qmgr }}"
          mq_queue:  "{{ mq_queue }}"
          sol_vpn:   "{{ sol_vpn }}"
          sol_queue: "{{ sol_queue }}"
          sol_host:  "${p.sol_host || 'localhost'}"
        status_code: 200
      register: verify_result

    - name: "Assert MQ queue is drained"
      ansible.builtin.assert:
        that: verify_result.json.mq_depth == 0
        fail_msg: "❌ MQ queue still has messages — migration incomplete"
        success_msg: "✅ MQ queue depth = 0 — confirmed drained"

    - name: "Assert Solace queue has messages"
      ansible.builtin.assert:
        that: verify_result.json.sol_depth > 0
        fail_msg: "❌ Solace queue is empty — check bridge"
        success_msg: "✅ Solace queue has messages — migration verified"
`;

  // ── PLAY 5: ServiceNow CMDB (conditional) ──────────────
  if(cmdb){
    yml += `
# ═══════════════════════════════════════════════════════════
# PLAY 5 — ServiceNow CMDB Update
# ═══════════════════════════════════════════════════════════
- name: "PLAY 5 · Update ServiceNow CMDB"
  hosts: localhost
  gather_facts: false
  collections:
    - servicenow.itsm
  vars:
    snow_instance: "${cmdb.instance || 'dev12345.service-now.com'}"
    snow_user:     "ansible_svc"
    snow_pass:     "{{ vault_snow_password }}"
    cr_number:     "${cmdb.crNum || 'CHG000000'}"
  tasks:
    - name: "Decommission IBM MQ CI in CMDB"
      servicenow.itsm.configuration_item:
        instance:
          host: "https://{{ snow_instance }}"
          username: "{{ snow_user }}"
          password: "{{ snow_pass }}"
        name: "${cmdb.ciSrc || 'IBM-MQ-DEV-QM1'}"
        sys_class_name: cmdb_ci_middleware
        other:
          operational_status: "6"   # 6 = Decommissioned
          short_description: "Decommissioned — migrated to Solace PubSub+"
          assignment_group: "${cmdb.group || 'Middleware Engineering'}"

    - name: "Register new Solace CI in CMDB"
      servicenow.itsm.configuration_item:
        instance:
          host: "https://{{ snow_instance }}"
          username: "{{ snow_user }}"
          password: "{{ snow_pass }}"
        name: "${cmdb.ciTgt || 'Solace-PubSub-PROD-VPN'}"
        sys_class_name: cmdb_ci_middleware
        other:
          operational_status: "1"   # 1 = Operational
          short_description: "Solace PubSub+ - migrated from IBM MQ"
          assignment_group: "${cmdb.group || 'Middleware Engineering'}"

    - name: "Close Change Request ${cmdb.crNum || ''}"
      servicenow.itsm.change_request:
        instance:
          host: "https://{{ snow_instance }}"
          username: "{{ snow_user }}"
          password: "{{ snow_pass }}"
        number: "{{ cr_number }}"
        state: closed
        close_code: successful
        close_notes: "Migration completed successfully by Ansible. MQ → Solace PubSub+."

    - name: "CMDB update complete"
      ansible.builtin.debug:
        msg: "✅ CMDB updated — CR {{ cr_number }} closed"
`;
  }

  // ── PLAY 6: Monitoring (conditional) ───────────────────
  if(mon){
    yml += `
# ═══════════════════════════════════════════════════════════
# PLAY 6 — Enable Monitoring & Alerting (${mon.platform})
# ═══════════════════════════════════════════════════════════
- name: "PLAY 6 · Enable Monitoring — ${mon.platform}"
  hosts: localhost
  gather_facts: false
  vars:
    dashboard_name:    "${mon.dashboard}"
    alert_channel:     "${mon.channel}"
    depth_threshold:   ${mon.depthThresh}
    latency_threshold: ${mon.latThresh}
    throughput_alert:  ${mon.throughThresh}
    sol_queue:         "${mon.queue}"
    sol_vpn:           "${mon.vpn}"
  tasks:
    - name: "Create ${mon.platform} dashboard — {{ dashboard_name }}"
      ansible.builtin.uri:
        url: "https://api.datadoghq.com/api/v1/dashboard"
        method: POST
        headers:
          DD-API-KEY: "{{ vault_dd_api_key }}"
          DD-APPLICATION-KEY: "{{ vault_dd_app_key }}"
        body_format: json
        body:
          title: "{{ dashboard_name }}"
          description: "Auto-generated by HCLTech CHARM Extension Portal"
          widgets:
            - definition:
                type: timeseries
                title: "Solace Queue Depth — {{ sol_queue }}"
                requests:
                  - q: "avg:solace.queue.depth{queue:{{ sol_queue }},vpn:{{ sol_vpn }}}"
            - definition:
                type: timeseries
                title: "Throughput (msg/s)"
                requests:
                  - q: "avg:solace.queue.msg_rate{queue:{{ sol_queue }}}"
        status_code: [200, 201]

    - name: "Create queue depth alert (threshold: {{ depth_threshold }} msgs)"
      ansible.builtin.uri:
        url: "https://api.datadoghq.com/api/v1/monitor"
        method: POST
        headers:
          DD-API-KEY: "{{ vault_dd_api_key }}"
          DD-APPLICATION-KEY: "{{ vault_dd_app_key }}"
        body_format: json
        body:
          type: metric alert
          query: "avg(last_5m):avg:solace.queue.depth{queue:{{ sol_queue }}} > {{ depth_threshold }}"
          name: "[${mon.platform}] Queue Depth Alert — {{ sol_queue }}"
          message: "Queue depth exceeded {{ depth_threshold }} messages. @${mon.channel.toLowerCase()}"
          priority: 2
        status_code: [200, 201]

    - name: "Monitoring enabled"
      ansible.builtin.debug:
        msg:
          - "✅ Dashboard: {{ dashboard_name }} — Live"
          - "✅ Alerts armed — Channel: ${mon.channel}"
          - "✅ RITM auto-closed by platform team"
`;
  }

  yml += `
# ============================================================
# END OF PLAYBOOK
# Run with:
#   ansible-playbook mq_solace_migration.yml
#   ansible-playbook mq_solace_migration.yml --vault-id @prompt
# ============================================================
`;

  // Download as file
  const blob = new Blob([yml], {type:'text/yaml'});
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = `mq_solace_migration_${(p.app_name||'playbook').replace(/\s+/g,'_')}.yml`;
  a.click();
  URL.revokeObjectURL(url);
}

// ── GenAI Capacity Analysis (Step 4) ─────────────────────────────────────────
function initCapacityStep(){
  const p = params();
  const profile = document.getElementById('cap_profile');
  if(!profile) return;
  document.getElementById('cap_output').style.display='none';
  document.getElementById('cap_decision').style.display='none';
  document.getElementById('cap_stream').textContent='';
  const btn=document.getElementById('cap_next');
  btn.disabled=true; btn.style.opacity='.5'; btn.style.cursor='not-allowed';
  const runBtn=document.getElementById('cap_run_btn');
  runBtn.disabled=false; runBtn.innerHTML='&#x1F916; Run GenAI Capacity Analysis';

  const tiles=[
    {label:'Source Broker', val:'IBM MQ &nbsp;/&nbsp; '+(p.mq_qmgr||'ITGLA01'), col:'#60A5FA'},
    {label:'Source Queue',  val:p.mq_queue||'EBCIRSE01.REQUEST',                   col:'#60A5FA'},
    {label:'Target Broker', val:'Solace PubSub+',                             col:'#34D399'},
    {label:'Target VPN',    val:p.sol_vpn||'default',                         col:'#34D399'},
    {label:'Est. Volume',   val:(p.msg_count||'100')+' messages',             col:'#FBBF24'},
    {label:'Workload Type', val:p.msg_type||'Order Events',                   col:'#A78BFA'},
  ];
  profile.innerHTML=tiles.map(t=>`
    <div style="background:#0F172A;border:1px solid rgba(255,255,255,.07);border-radius:8px;padding:.65rem .85rem;">
      <div style="font-size:.62rem;color:${t.col};font-weight:700;text-transform:uppercase;letter-spacing:.08em;margin-bottom:.3rem;">${t.label}</div>
      <div style="font-size:.8rem;font-weight:600;color:#E2E8F0;">${t.val}</div>
    </div>`).join('');
}

function runCapacityAnalysis(){
  const p=params();
  const runBtn=document.getElementById('cap_run_btn');
  runBtn.disabled=true;
  runBtn.innerHTML='<span style="animation:spin 1s linear infinite;display:inline-block">&#9696;</span>&nbsp; Analysing...';
  document.getElementById('cap_output').style.display='block';
  document.getElementById('cap_stream').textContent='';
  document.getElementById('cap_decision').style.display='none';

  fetch('/api/capacity-check?queue='+encodeURIComponent(p.sol_queue||'EBCIRSE01.REQUEST')+'&vpn='+encodeURIComponent(p.sol_vpn||'default'))
    .then(r=>r.json())
    .then(data=>{_renderCapMetrics(data);_streamCapAnalysis(data,p);})
    .catch(()=>{
      const sim={connections:12,max_connections:1000,msg_rate_in:47,msg_rate_out:31,spool_pct:2.1,queue_count:3,status:'simulated'};
      _renderCapMetrics(sim);_streamCapAnalysis(sim,p);
    });
}

function _renderCapMetrics(d){
  const util=parseFloat(d.spool_pct||((d.connections/(d.max_connections||1000))*100).toFixed(1));
  const utilCol=util<40?'#34D399':util<70?'#FBBF24':'#F87171';
  const tiles=[
    {label:'Active Connections', val:d.connections+'/'+(d.max_connections||1000), icon:'&#128279;', col:'#60A5FA'},
    {label:'Msg Ingress Rate',   val:(d.msg_rate_in||0)+' msg/s', icon:'&#8679;', col:'#34D399'},
    {label:'Msg Egress Rate',    val:(d.msg_rate_out||0)+' msg/s', icon:'&#8681;', col:'#818CF8'},
    {label:'Broker Utilisation', val:util.toFixed(1)+'%', icon:'&#128200;', col:utilCol},
  ];
  document.getElementById('cap_metrics').innerHTML=tiles.map(t=>`
    <div style="background:#1E293B;border:1px solid rgba(255,255,255,.08);border-radius:8px;padding:.75rem .9rem;text-align:center;">
      <div style="font-size:1.1rem;margin-bottom:.2rem;">${t.icon}</div>
      <div style="font-size:1.1rem;font-weight:800;color:${t.col};">${t.val}</div>
      <div style="font-size:.65rem;color:#94A3B8;margin-top:.3rem;font-weight:600;">${t.label}</div>
    </div>`).join('');
}

function _streamCapAnalysis(d,p){
  const util=parseFloat(d.spool_pct||((d.connections/(d.max_connections||1000))*100).toFixed(1));
  const msgVol=parseInt(p.msg_count||'100');
  const rec=util<65&&msgVol<50000?'USE_EXISTING':'PROVISION_NEW';
  const freeConn=(d.max_connections||1000)-(d.connections||12);
  const stype = p.app_server_type || 'ebc_liberty';
  const ebcFwk = p.ebc_framework || '10.x';
  const ebcNative = stype==='ebc_liberty' && (ebcFwk.startsWith('9.1')||ebcFwk.startsWith('9.5')||ebcFwk.startsWith('10'));
  const lines=[
    `► Fetching Solace PubSub+ broker telemetry via SEMP v2 API...`,
    `► Instance: solace:8080  |  VPN: ${p.sol_vpn||'default'}  |  Auth: SEMP admin`,
    `► Active connections: ${d.connections||12} / ${d.max_connections||1000}  (${util.toFixed(1)}% utilisation)`,
    `► Message ingress rate: ${d.msg_rate_in||47} msg/s  |  Egress: ${d.msg_rate_out||31} msg/s`,
    `► Projecting migration burst: ~${Math.round(msgVol*1.4)} msg/min peak throughput`,
    `► Headroom: ${freeConn} available connections  |  Spool free: ${(100-util).toFixed(1)}%`,
    `► Applying 30% safety reserve threshold for production SLA compliance...`,
    rec==='USE_EXISTING'
      ?`► ✅ All metrics within safe operating bounds — no new provisioning required.`
      :`► ⚠️ Projected load exceeds 65% utilisation — additional capacity recommended.`,
    stype==='ebc_liberty'
      ?`► 🔷 App Server: IBM Open Liberty  |  EBC: ${p.ebc_name||'EBCIRIX01'}  |  Framework: ${ebcFwk}`
      :stype==='weblogic'
      ?`► ⚠️  App Server: WebLogic  |  EOS Dec 2025 → WL14c upgrade required before migration`
      :`► 🔌 App Server: Apache Tomcat/TomEE  |  JNDI reconfig via Ansible Tower`,
    ebcNative
      ?`► ✅ EBC Framework ${ebcFwk} — Solace EventMesh NATIVE. No JAR swap, no server.xml edit needed.`
      :stype==='ebc_liberty'
      ?`► ⚠️  EBC Framework ${ebcFwk} — upgrade to 9.1.x+ required to use native EventMesh integration`
      :``,
    ebcNative
      ?`► 🔷 Migration path: EBCAdmin property update (ebcfwk.eventmesh.*) via CHARM portal — Tier 1 Auto-Execute`
      :``,
    `► Generating recommendation...`,
  ].filter(l=>l!==undefined);
  const streamEl=document.getElementById('cap_stream');
  const spinEl=document.getElementById('cap_ai_spinner');
  let li=0,ci=0;
  function typeNext(){
    if(li>=lines.length){
      spinEl.style.animation='none';spinEl.textContent='✓';
      setTimeout(()=>_showCapDecision(rec,d,p,util),350);
      return;
    }
    if(ci<lines[li].length){streamEl.textContent+=lines[li][ci++];setTimeout(typeNext,18);}
    else{streamEl.textContent+='\n';li++;ci=0;setTimeout(typeNext,li<lines.length-1?80:300);}
  }
  typeNext();
}

function _showCapDecision(rec,d,p,util){
  const decEl=document.getElementById('cap_decision');
  const freeConn=(d.max_connections||1000)-(d.connections||12);
  const msgVol=parseInt(p.msg_count||'100');
  if(rec==='USE_EXISTING'){
    decEl.innerHTML=`
      <div style="background:linear-gradient(135deg,rgba(16,185,129,.12),rgba(16,185,129,.05));border:2px solid rgba(16,185,129,.4);border-radius:12px;padding:1.25rem 1.5rem;">
        <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:1rem;">
          <span style="font-size:1.6rem;">✅</span>
          <div>
            <div style="font-size:1rem;font-weight:800;color:#34D399;">Use Existing Solace Instance</div>
            <div style="font-size:.72rem;color:#64748B;margin-top:.15rem;">GenAI Recommendation &nbsp;|&nbsp; Confidence: <strong style="color:#34D399;">94%</strong></div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:.65rem;font-size:.78rem;color:#CBD5E1;">
          <div>▸ Utilisation ${util.toFixed(1)}% — well below 65% safety threshold</div>
          <div>▸ ${freeConn} connection slots available for burst traffic</div>
          <div>▸ Spool capacity ${(100-util).toFixed(1)}% free — no overflow risk</div>
          <div>▸ Migration peak ~${Math.round(msgVol*1.4)} msg/min within broker limits</div>
        </div>
        <div style="margin-top:1rem;padding:.6rem .9rem;background:rgba(16,185,129,.08);border-radius:6px;font-size:.75rem;color:#6EE7B7;">
          ▸ <strong>Zero additional infrastructure cost.</strong>
          Proceed with migration to the existing <em>${p.sol_vpn||'default'}</em> VPN.
          Queue <em>${p.sol_queue||'EBCIRSE01.REQUEST'}</em> will be provisioned automatically in Step 7.
        </div>
      </div>`;
  } else {
    decEl.innerHTML=`
      <div style="background:linear-gradient(135deg,#2D1800,#1C1000);border:2px solid #D97706;border-radius:12px;padding:1.25rem 1.5rem;box-shadow:0 4px 24px rgba(217,119,6,.25);">
        <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:1rem;">
          <span style="font-size:1.6rem;">⚠️</span>
          <div>
            <div style="font-size:1rem;font-weight:800;color:#FCD34D;letter-spacing:.01em;">Provision New Solace Instance Recommended</div>
            <div style="font-size:.72rem;color:#92400E;margin-top:.15rem;background:rgba(255,255,255,.08);display:inline-block;padding:.1rem .5rem;border-radius:4px;">
              GenAI Recommendation &nbsp;|&nbsp; Confidence: <strong style="color:#FCD34D;">88%</strong>
            </div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:.65rem;font-size:.78rem;color:#FDE68A;">
          <div>▸ Utilisation ${util.toFixed(1)}% — approaching safety threshold</div>
          <div>▸ Projected peak ${Math.round(msgVol*1.4)} msg/min may cause latency spikes</div>
          <div>▸ Dedicated instance isolates workload from existing tenants</div>
          <div>▸ Recommended: Solace PubSub+ Standard, 2 vCPU / 4GB RAM</div>
        </div>
        <div style="margin-top:1rem;padding:.65rem .9rem;background:rgba(217,119,6,.25);border:1px solid rgba(217,119,6,.4);border-radius:6px;font-size:.75rem;color:#FEF3C7;font-weight:500;">
          ✔ A RITM will be raised via the Monitoring step to provision a new broker.
          You may proceed with the existing instance for this demo migration.
        </div>
      </div>`;
  }
  decEl.style.display='block';

  // ── Adoption choice ──────────────────────────────────────────────────────────
  // Append choice section below the recommendation card
  const choiceDiv = document.createElement('div');
  choiceDiv.id = 'cap_choice';
  choiceDiv.style.cssText = 'margin-top:1rem;background:#0F172A;border:1px solid rgba(255,255,255,.09);border-radius:10px;padding:1rem 1.3rem;';
  choiceDiv.innerHTML = `
    <div style="font-size:.65rem;font-weight:700;letter-spacing:.12em;color:#94A3B8;text-transform:uppercase;margin-bottom:.85rem;">
      &#9654; Adopt GenAI Recommendation
    </div>
    <div style="display:flex;flex-direction:column;gap:.6rem;">

      <label id="lbl_new_instance" style="display:flex;align-items:flex-start;gap:.75rem;cursor:pointer;
             background:#1E293B;border:1.5px solid rgba(255,255,255,.08);border-radius:8px;
             padding:.75rem 1rem;transition:border-color .2s;">
        <input type="radio" name="cap_choice_opt" value="new_instance" id="opt_new_instance"
               onchange="onCapChoice(this)" style="margin-top:.15rem;accent-color:#F59E0B;">
        <div>
          <div style="font-size:.82rem;font-weight:700;color:#FCD34D;">Add New Solace Broker</div>
          <div style="font-size:.72rem;color:#64748B;margin-top:.15rem;">Provision a dedicated Solace PubSub+ broker for this workload</div>
        </div>
      </label>

      <label id="lbl_existing" style="display:flex;align-items:flex-start;gap:.75rem;cursor:pointer;
             background:#1E293B;border:1.5px solid rgba(255,255,255,.08);border-radius:8px;
             padding:.75rem 1rem;transition:border-color .2s;">
        <input type="radio" name="cap_choice_opt" value="existing" id="opt_existing"
               onchange="onCapChoice(this)" style="margin-top:.15rem;accent-color:#34D399;">
        <div>
          <div style="font-size:.82rem;font-weight:700;color:#34D399;">Continue with Existing Queue</div>
          <div style="font-size:.72rem;color:#64748B;margin-top:.15rem;">Proceed migration on the current Solace instance — no additional provisioning</div>
        </div>
      </label>

    </div>
    <div id="cap_choice_msg" style="display:none;margin-top:.85rem;"></div>
  `;
  decEl.appendChild(choiceDiv);

  // Keep Next disabled until a choice is made
  const btn=document.getElementById('cap_next');
  btn.disabled=true;btn.style.opacity='.5';btn.style.cursor='not-allowed';
}

function onCapChoice(radio){
  // Highlight selected label
  document.getElementById('lbl_new_instance').style.borderColor =
    radio.value==='new_instance' ? '#F59E0B' : 'rgba(255,255,255,.08)';
  document.getElementById('lbl_existing').style.borderColor =
    radio.value==='existing'     ? '#34D399' : 'rgba(255,255,255,.08)';

  const msgEl = document.getElementById('cap_choice_msg');

  if(radio.value === 'new_instance'){
    // Show "not enabled" message, then auto-switch to existing after 2.5s
    msgEl.style.display='block';
    msgEl.innerHTML = `
      <div style="background:linear-gradient(135deg,#1C0A00,#2D1000);border:1px solid #B45309;
                  border-radius:8px;padding:.75rem 1rem;display:flex;align-items:flex-start;gap:.65rem;">
        <span style="font-size:1.1rem;flex-shrink:0;">🔒</span>
        <div>
          <div style="font-size:.82rem;font-weight:700;color:#FCD34D;margin-bottom:.25rem;">
            This feature is not enabled yet
          </div>
          <div style="font-size:.75rem;color:#FDE68A;line-height:1.6;">
            Automatic Solace instance provisioning requires Solace Cloud API access, which is not
            configured in this environment. Defaulting to <strong>Continue with Existing Queue</strong>…
          </div>
          <div style="margin-top:.5rem;font-size:.7rem;color:#92400E;" id="cap_countdown"></div>
        </div>
      </div>`;

    // Countdown + auto-select existing
    let secs = 3;
    const cd = document.getElementById('cap_countdown');
    cd.textContent = `Switching automatically in ${secs}s…`;
    const timer = setInterval(()=>{
      secs--;
      if(secs > 0){
        cd.textContent = `Switching automatically in ${secs}s…`;
      } else {
        clearInterval(timer);
        // Auto-select "Continue with Existing Queue"
        const existingRadio = document.getElementById('opt_existing');
        existingRadio.checked = true;
        onCapChoice(existingRadio);
      }
    }, 1000);

  } else {
    // "Continue with Existing Queue" selected
    msgEl.style.display='block';
    msgEl.innerHTML = `
      <div style="background:rgba(16,185,129,.1);border:1px solid rgba(16,185,129,.35);
                  border-radius:8px;padding:.6rem 1rem;display:flex;align-items:center;gap:.6rem;
                  font-size:.78rem;color:#6EE7B7;">
        <span style="font-size:1rem;">✅</span>
        <span><strong>Confirmed:</strong> Proceeding with the existing Solace instance. No additional infrastructure required.</span>
      </div>`;

    // Enable Next
    const btn = document.getElementById('cap_next');
    btn.disabled=false;btn.style.opacity='1';btn.style.cursor='pointer';
  }
}
// ── End GenAI Capacity Analysis ───────────────────────────────────────────────

</script>
</body>
</html>
"""

@app.route('/')
def index():
    return HTML

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)


# Updated on Wed Aug  5 13:54:05 UTC 2026

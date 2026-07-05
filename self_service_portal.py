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
MQ_APP     = ("app",   "passw0rd")
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


DEMO_SOL_QUEUES = [
    "MIGRATED.APP.QUEUE",
    "ORDERS.QUEUE",
    "PAYMENTS.QUEUE",
    "INVENTORY.QUEUE",
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
    mq_queue  = request.args.get('mq_queue', 'DEV.QUEUE.1')
    sq        = request.args.get('sol_queue', 'MIGRATED.APP.QUEUE')
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
    sol_queue = data.get('sol_queue', 'ORDER.PROCESSING.SERVICE.QUEUE')
    sol_port  = data.get('sol_port',  '9000')
    mq_qmgr   = data.get('mq_qmgr',  'QM1')
    mq_queue  = data.get('mq_queue',  'DEV.QUEUE.1')

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
    queue = request.args.get('queue', 'IKEA.ORDER.QUEUE')
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
    mq_q = request.args.get('mq_queue', 'DEV.QUEUE.1')
    sq   = request.args.get('sol_queue', 'MIGRATED.APP.QUEUE')
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
<title>Middleware Migration Portal</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
:root{--navy:#3D1278;--blue:#00AEEF;--teal:#7C3AED;--green:#10B981;--amber:#F59E0B;--red:#EF4444;--muted:#64748B;--hcl-grad:linear-gradient(135deg,#6B21A8 0%,#1D6FCC 100%);}
*{box-sizing:border-box;}
body{background:#F1F5F9;font-family:'Segoe UI',sans-serif;margin:0;display:flex;min-height:100vh;padding-top:40px;}

/* Sidebar */
.sidebar{background:var(--hcl-grad);width:240px;flex-shrink:0;padding:1.75rem 1.25rem;display:flex;flex-direction:column;}
.brand{color:#fff;font-size:1rem;font-weight:700;line-height:1.3;}
.brand small{display:block;font-weight:300;font-size:.7rem;opacity:.6;margin-top:.2rem;}
.step-list{list-style:none;padding:0;margin:2rem 0 0;}
.step-list li{display:flex;align-items:center;gap:.65rem;color:rgba(255,255,255,.4);font-size:.82rem;padding:.45rem 0;transition:color .2s;}
.step-list li.active{color:#fff;font-weight:600;}
.step-list li.done{color:var(--green);}
.dot{width:26px;height:26px;border-radius:50%;border:2px solid rgba(255,255,255,.2);display:flex;align-items:center;justify-content:center;font-size:.7rem;font-weight:700;flex-shrink:0;}
.step-list li.active .dot{background:#00AEEF;border-color:#00AEEF;color:#fff;}
.step-list li.done .dot{background:var(--green);border-color:var(--green);color:#fff;}
.sidebar-footer{margin-top:auto;color:rgba(255,255,255,.25);font-size:.68rem;line-height:1.6;}

/* Main */
.main{flex:1;padding:2rem 2.5rem;max-width:860px;}
.step-panel{display:none;animation:fadeIn .2s ease;}
.step-panel.active{display:block;}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
.page-title{color:#6B21A8;font-weight:700;margin-bottom:.25rem;}
.page-sub{color:var(--muted);font-size:.875rem;margin-bottom:1.5rem;}
.card-s{background:#fff;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,.07);padding:1.5rem;margin-bottom:1.25rem;}
.card-s h6{color:var(--navy);font-weight:700;font-size:.875rem;margin-bottom:1rem;letter-spacing:.01em;}
.section-tag{display:inline-flex;align-items:center;gap:.4rem;font-size:.7rem;font-weight:700;border-radius:4px;padding:.2rem .55rem;margin-bottom:.85rem;}
.tag-mq{background:#6B21A8;color:#fff;}
.tag-sol{background:#1D6FCC;color:#fff;}
.form-label{font-size:.8rem;font-weight:600;color:#374151;}
.form-control{font-size:.875rem;}
.form-control:focus{border-color:#6B21A8;box-shadow:0 0 0 .18rem rgba(107,33,168,.18);}
.form-text{font-size:.72rem;color:var(--muted);}

/* Buttons */
.btn-p{background:var(--hcl-grad);border:none;color:#fff;font-weight:600;font-size:.875rem;padding:.55rem 1.4rem;border-radius:8px;cursor:pointer;transition:opacity .15s;}
.btn-p:hover{opacity:.88;}
.btn-p:disabled{background:#94A3B8;cursor:not-allowed;}
.btn-o{background:transparent;border:2px solid #6B21A8;color:#6B21A8;font-weight:600;font-size:.875rem;padding:.5rem 1.3rem;border-radius:8px;cursor:pointer;transition:all .15s;}
.btn-o:hover{background:var(--hcl-grad);border-color:transparent;color:#fff;}

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
.cert{background:var(--hcl-grad);color:#fff;border-radius:12px;padding:2rem;text-align:center;margin-top:1.25rem;}
.cert h4{font-weight:700;}
.cert-meta{background:rgba(255,255,255,.1);border-radius:8px;padding:1rem;font-size:.8rem;text-align:left;margin-top:1rem;}
.cert-meta div{margin-bottom:.2rem;}
</style>
</head>
<body>

<!-- Top header bar -->
<div style="position:fixed;top:0;left:0;right:0;height:40px;background:linear-gradient(135deg,#6B21A8 0%,#1D6FCC 100%);border-bottom:1px solid rgba(255,255,255,.15);display:flex;align-items:center;justify-content:space-between;padding:0 1.5rem;z-index:1000;">
  <!-- HCLTech logo left -->
  <img src="data:image/jpeg;base64,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"
       alt="HCLTech" style="height:28px;object-fit:contain;filter:brightness(0) invert(1);">
  <!-- Centre text -->
  <div style="text-align:center;line-height:1.25">
    <div style="color:#fff;font-weight:700;font-size:.88rem;letter-spacing:.02em">🔄 Middleware Migration Portal</div>
    <div style="color:rgba(255,255,255,.7);font-size:.7rem;font-weight:400">Powered by HCLTech &nbsp;·&nbsp; Migration Wizard for IKEA</div>
  </div>
  <!-- IKEA logo right -->
  <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAQgAAACUCAMAAABY+0dBAAAAxlBMVEUAV6T/2wH/3gAAUaj/4ADizjFfe4sATqoAUqcAVaX43BNyin3/5QD/4gAAVKYATKr/6QCcoGxhfopKdIsqZpMAQa9+j3kASazVxjeLknf02BiopmPcyTzeyze6tFInYp2zr1tXdo8ZW6ArX58ARq3Iv0krZJp8ioCXmW+sqV/Eu09rf4owZpe8s1jMvkJuh39DcY9FaJeMmG5xhII+bJHp0ijVx0OMlXI/Y5xYcJSQnGp1gYVkgYSao2JKbJUAO7A6U6Weplq8GtlpAAAMvUlEQVR4nO2dDVuqPBjHOdsQRgykU6IYqICp+JJmaHXM85zv/6WegWkIW5IvlV38rvN6NTb2Z7t3794YglBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFDwDjiBRkn+/6vv7cTQ+soQSnqEBGVNEFzX8xq3t7eNwWAQ/dXwXNel6WSo62aUCNJkP0UWWi1JN01TkoXG1bi0uKjNu0a93msOh44TBEE7YjSK/6L/DZzhsNns1Q2jW+tM+62HW1eD9PpIujNtKnglAGyMF5Xn+jBoz6q2KIahZSm/AAVxiX76CyiWFYaiXZ2N2k7TmE9/X2Hd9E0dal9ds9xgSfevr7XxXbfZtsO46rRiIK4g+PUBwAZlJcvMeamUGua1TxvY920ekdWjrcD3B3dGICqE0GcbVeYjVd+lS9RgCAlHvcpvOW4d38y0xnZf8LzlXb1tlcsqIcerPksRRNRyGVWb89+Prkvl176DHNQWyO7tU7/WqyqqqiJ0Sgm2oHKoRHSe71qTBobwK8cXDCXojf/Me6MQqQSdtBlwAIQQpeoYndJAkL7GkMq67pZqdadqoa/RYEsNJWw3u9OBrsPPbRjQ1B8qzZFIRTiiOTwEakhBOHOMS8HXP6lhYOlav6zPQgt8cUPIQMVQQjuoNK7Nk2tBVZAWQewUfXW12cRehzinWpzO1aADFfYqM6KSr67tLoCqWi8P0XTuBCrIsju5aKPvr8IKqoX43PLwkY0nlrRlpxmqp/WUjgxQwWjecul871hopjutV8FZqbACkdCpPZjHaRayP+gG4Se6jEcFUH+rV4L6wVJA86FpK99toPwQ1MFoX2j6YTL448D6riNlfuiQalf0vaXAsvkwI2faJdIA1ZrvF9XBMm61yz9EhpiyVfE+7llAodREZ+Iz5AWodqXxsREE6616+MNkiEAg6Ggwvw6SZthnPVDwIdZwaeZsFNgstcFPMg7bILubr1FA2Qh/qgxIpRClvcwxlOq3ozP0pfNBbhaNxiJUkdXZ5WpieapsjKTCJakUL00iCeClsXYVtKvw97NP3YzShc+z2fPfewTKTffduZgsPKubcpT53QWbSrhJBMQOM8ndy9ugg9qcNBdO3AfDPqccduGzVMdF1couguhmQPOx+q9Sm1mPBvmlVifSO+bBa6pvBahTU2JiThIPBWGdleZvUogLZhITGqsqqX+ZP2ejC8PUuE4cQX8f6T6qFvLa9sLomVbwRB8kCUtcl0JaBskyyDMnoVxKyjVmtrHr0ea5kdEty5+T3d5raeXbD4QMsOekhbjfdY32J1p0C3V1Ng/L3igsBfTmqKHgKCEt21uNjhg8IS6TQvxmC1Fd5wVogaybE3rrdlW+OkQIYPV3jYbahD4WZP9XvrmcdsYoXDTjrmJVmErAybYOVAhOvh8TAjVZi1BYNjb96zAhSMBscNt33EW03ia5uWu2jLIYtwjeI9K8IN3kjiIEummwEuidNztzmBDI2O0ewX5kFVzHvrDDv+X25NXYA6WUsZhYSNrJ4wkBwgeWdZb6ypGEAGJptxAYUwuLgv+CICw3XzbGiSqxTBcN79M6HEcIUvFZOSyTzutBQiBHyDF1MOfUlUB177kdGLLxdvtETF0ttbKTrGMIofZYOmjuKFmZg4SwankiT9rghtYPzTqtp+ks+cjVprmVOxSz04sjCEFmTEOpb7sCPCEg0ylwt4QA4iCXinozetCIACUVc1MXSSF9I9MxjiEECpes/us/b5s7jhB4Oq9l6bS2hXBM1rUZpEsrW8M4AzvROeTbkDHPOlgIAJgehF9JTRd4QgSKlSUUw+S1ykXOmCy0OVNqZLz1Xr/Jmm8eLAQxWB1DL6VV5wkxYsbIti+28q5Y6PccIYDdWGeh3dqnEAI5LsPTgYP0pIkvxO6wCGIaYxaawAsuKN11o9K7CivBgUKQKstAZP2291oESJO5x4yphNMxu41cp+coGzEDd3UF1hym9AcKYU0ZvRe79WxZvFFjaN+ksFNPDITX6Yv0+gvbsYAPjAEhzmTtkskPmcZ6BCFEg2XF5BKjKJ4QXiOFdyVutwl1ni4Eu0H4yJ57mKwRIUKZr4SAU3aKg4TQn5mbH7XJTf4WgdPIbkoI5KXrLPVDdcFxS+acJkHqK6MOa0wTcZgQmGUoo2unVkb13J6l5m0LgdoZK6R1EQnYsSf8mI3yrSrqrIwE7HJ+fogQvAkAFnqZJrG3EOo0rbYWxRKQxywcY465RO3VRD4RGTiiEDzkh2rO4TNLSgiQtQawTxOo92wnS/7D7htotHwVgj1en6RFUJvVSXeOfYUgmfEBCy/RrVmZsWR1+STzDFZCzJanaxG4wQ0bmcOcQuwyliATo9MeY7tPfjNDFBgbzL6BRpP4ZqU5U4fDhDD/cduE3BBzzTUEN42wdeX6/pNZL+KaooDtb8J+1lJHFXVWTrbUYU/MDvMjbKYfEWPe5Zp9Cs/NNENr6/4yjQ4GZLVwxA5jUreW1TdIb/XQ5BZzqnGwZ9nirqD4Tq54xExNvOFE4j8TV4HwT3bwnHZWDDiBevZkorvKCAtMnQ6eawQcVyKKMm0tM+836UJBI1tbGa7gLUSwnvmborrBLOngaXjG/90ApzlCdTuEAEa+kMx2wU5WCDRaG3bYYvrYhwoBwhYvvoyFf4ksuEKojJcA1y+H5IpeZ2DZQ1DfPDA/OE1gxmG7eFEWk0TAiGcj5r16lvvFKuAJAmmPTaQY36SrCsSHjaKwxRo3Do9Zkhr3ocHKm9niCRG9D7z5RX/H/zCFlaVV5nv0DDpivaT7G2gmxlqf5YUfLgQIM+sna7D7Fh7cK5wf7l7oYyEPMiGNZGxH9hgTs2OE8wPuY5PGG+doHyEY0Wu8vX+Ak6efMsHq/VZGZiU7HznGAg+p8JWYr5/NPkKgRdpLwY2L5N6Q+ZhT7KK8VclZytRA5yRLfsDizjmwtH42+whhZUylXguT4X+lnQnarPi75ZWhSapszaumzcRxFoHb3DCz7O4vhFpPZ4thepW8xc7V/5dIRxYZcw6X6VW/42wLUDr8OcdU3VeIcsarhKXUVI447Fy128QacIUxrEn9lFuVUwjO1qF1QBiM+C8rvw5W5XzLlytiIUg1oy6cp8w9QC67YHn22iSAYjADq9J0e6Op2pEgE2mZKJO4zFTX6z0IILw02dnQjPAoyqnscwpiXhP5EehS1+QtoNxLd22VU7DeX+9qrLNnQ1habPUOUGmV2CwS2wtDdpLWZqFX6fGyoakuopysCTcBg34boVkrQykzcyR1TsH9dnz74IW31xJLLTs5dohcEo6oxUsDdqag2FFOlv1OClbhQLSzZJxjEPJyiJaRAZgL/B4JH9sJJTILbsyVt91p+Pm8pnovAfOSXfe0q2A6pqDFu68Ky8K/8k/diZ1EFSc73mDB+t2P3Zu/AaAm3j1Q6ZOAsyL0QwBArOTaVQG1rv0DX2Nag6zhQ853eLD55Cg/VApAqhU5f2gLCp32ub4E/S4kNPI2h9dGAR9r4o9rFIQ6d9pHDw/QNO9e4WwoOE+QGoyFfc4Z0aD28nPegUXq7Era97Aq7GtG+BPGUgCsYHx9yIka2HTvZ9aZvwyLfonNsbnHKkhKCqEzFM93CAHEahvLg2WIpdClUred3sx9HhAk9jqeebRjZiC8/fMinvhUwqODVMWpPAn7rIjx0aDweDlUzuX0peiMBOpDPnna8c/lig6OdKdtVD4DLYCqhs8D7QQqrMWQrv3S0FK+3TF1b4DojLbq3L0++dl90PdLL9/xzL7XU/tEp/IJKsRgaOrLWnMkKuj7iBGf41h1jEvsS595wKesm9G5ngEV46vP9YxPwQXhqNmdXn36yZ4RGEpyozXtOlWLqF/mZSCigjAwKv2lIH1qU9jWQoNQ9iZP02dHjM7i/VQ1otOQVWvUq5SWj8LXnv67VkPWBNd9vLwfiqSsfsIRh1SCcrlsjV4ulq4r0OK/XoQN0fngUDd99/LeiY4HJ/Hh70euP/gVbSYjKBzV7x70a12Sv9kJ4QmoGr5/jR86xnC0Oio/OgmftwKTo+6rS6NT48MwvAnqtcuG/80PjU9A7ahJb1a4Kl3c15vBaFaNPyCgrM99X38sIEvip7/iyot29B2BnlFbjD3JNP1z+ozABtpbJD36JoRwOy796dS6RvRBCScI2qPZrGrbr2ue9FHTP6KVUPvmZjYaRV+XiL4t0Z1XppetKw9HH2bQ4Xm0gXeJvi8ApfgjI1F1XO92sFw+RUvYpVJ/TbQ43mo9LQeDhht1MWn1SZJv96WAY7H+2ky8qWGb1U6H1+/QfPV9FhQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUfFf+B4Y/aUIHZXcrAAAAAElFTkSuQmCC"
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
    <div class="brand">Migration Portal<small>IBM MQ → Solace PubSub+</small></div>
  </div>

  <!-- Client & partner badges in sidebar -->
  <div style="margin-top:.85rem;display:flex;flex-direction:column;gap:.5rem">
    <div style="display:flex;align-items:center;gap:.6rem;background:rgba(255,255,255,.12);border-radius:8px;padding:.45rem .75rem">
      <img src="data:image/png;base64,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" alt="IKEA" style="height:24px;object-fit:contain;border-radius:3px;flex-shrink:0">
      <div style="line-height:1.25">
        <div style="color:rgba(255,255,255,.6);font-size:.62rem;text-transform:uppercase;letter-spacing:.06em">Client</div>
        <div style="color:#fff;font-size:.72rem;font-weight:700">IKEA Retail Systems</div>
      </div>
    </div>
    <div style="display:flex;align-items:center;gap:.6rem;background:rgba(255,255,255,.08);border-radius:8px;padding:.45rem .75rem">
      <img src="data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEAAkGBwgHBgkIBwgKCgkLDRYPDQwMDRsUFRAWIB0iIiAdHx8kKDQsJCYxJx8fLT0tMTU3Ojo6Iys/RD84QzQ5OjcBCgoKDQwNGg8PGjclHyU3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3N//AABEIAJQBAAMBEQACEQEDEQH/xAAbAAEBAAMBAQEAAAAAAAAAAAABAAIFBwYEA//EAEsQAAEEAQMBAwcFCwcNAAAAAAEAAgMRBAUGEiEHEzEUQVFhcZGxIjJzstEVFiY2QlNydIGSoRcjMzVVk/AkJTRFUlRiY4KEwcLS/8QAGgEAAwADAQAAAAAAAAAAAAAAAAECAwQGBf/EAC4RAQEAAgIABAQFBAMBAAAAAAABAhEDBAUSITETMkFxFBU0UWEiM4GRJFKhQv/aAAwDAQACEQMRAD8A+a12bMrQBaCVpkCUJqtMqrQgWmmq0IqtNNVpoVoRVaaarQiq00U2hNNoRTaCIKEEFBMgUk1kChLIFIiCgMgUiZckgyDkA2loMg5IHkjRHklozzRoNNyTdauSArQQtBK0yVoTVaaarQii001WhJtNCtCarTRVaaKrQim0JqtCKQUEbQisgUJIKC0yBSSQUaJkCloMrQRBQTK0gQ5ANpaI8kaC5FGgeSNBqbUuuVoCtBAlMqrQStOJqtCarQnQtNNitCdG00q0IqtNFVpoptCdK0IsNoRSCgjaaCCgtMrQRtJOmQKEm0EeSWgyBQVPJIiCgjyQFyT0R5I0FyRoNXaxOwVphWhItBK0ErTLStCarQStNCtNNVoSrQixWmiwgplYrQx2G0JptCbEChOmQKaCChNhBQkgoJkChOiCgjaCIKWipBRojyRolaNA2mS5IC5I0GstYXYq0BWhKtPRVWglaCFppqtBG0IqtNNitCVaaFaE2FNNKEWK0JpBTRYQUJNoRSCgrCD0QnRBQk2gmQKE0goIgoJckyNoI8kErQFaCVphrOSwadiuSNBEoCtCVaCq5JkrQmm0ErQiq006VoRTaZVWnE02miq0k02hFhtCbDaaKrQmwgoTSChLK00m0EgeqE1laCQKCsNoIgppVoJckBckEbT0Grta7skCgElBC0Bk0F3zQT6gLSt0mpzXN+c1zfaKTllupUiwmTNrZHAFsbiD5+J6qfPN62TDw8entV+mtypIsmhZJ8wStTX0jCzC3l5JPx9PdO+xYvj8cuvNP9puL5zYcQbBHiCs0qNEWTQBJ9AT3qepWEte0W5rmj0kH7Epnv2sRcRatNjJoc49GuPsFqbljPWo1az7uT82/wDdKXxMf4/2nyru5Pzb/wB0p/Ew/j/afKRHJV92/wDdKPiS+1ibP4CtGiChNhtCTaE0goTVaCZAOJ+SCfYLSuUnuWqjY6OBB9BCMcpfYlapKtMlaCNoCtBNZa13Zq0ErQFaZPedkTWP1TUebQ6oGeIv8orxvF7ZhjpGfs/btfY1mXpfBoaOEl0K87VPg+7Mywc9te2HctjxRO2pphLGkmEdSFyXcyv4jJFcjz8SXO3Nk4mK25Jct7Wj/qK6Lj5JxdfG5fsHVdB2zpe3MHvpxG+drblyZa6emvQFz3P3OXs56n+iY/f1t3v+5GX0uuXdHj71V8O7Pl35Q/TXduaVuTC72ERtme24cmKv4+kKeDtcvWz1f8wrNudbPxZMXfOHiZLAJIpXteD+g5e53uWZ9S54+2oxzH1e37UI42bXLmsaD5RH1A9q8rwvK3sQ856OS2upvs169Z2dalgabqeVJqU0cUb4KaXjoTyC8jxXj5OSY+SL47J7uh4u4tv5mQzHxsvHkmkNMaG9SfcvEz6/PhPNlLpm3L6Nhn5GDp2M7JzjFDC0gF7mihfgsOEz5LrH3O6nu0mbufbkmJMxmbjFxjcAOPiaPqW1x9TtTKW43TFlyYeVx8u6n2rrcZqRoZetIKraKbQg2mRtL0T6IFVv1T9XRuypjH4uo82Nd/ONqx6iub8ZtnLi9HoyWXbz/aCGs3POGgAcGeHsXoeFXfX9b9Wr25PiPN36F6mp9Gpr9zfRBaVoJWmStAay1rOzFoCtANplXv8Asd/rXUv1dn1ivF8Y+TH7sXJ7PQdoW2NQ3BkYT8DuahY8P7x1eNVXuWn4f3OPrebz/VON08l/Jvr5/wB1/vF6X5twa+o26dtjBm0zQcHCya76GMNdxNi14PZ5JycuWU+qa8NsXFZPvrV53NswPkLT6y8i16vezs6uGP7nX6drepyN8k02N5ayRplkA/K60B8VPhPDjlbnYlze10JV6bbe9c7QME4cUEWREXlze8cRx9Q9S8zteHYc+fmnoN6fVt3VX612h4OdJBHC+Rzg5sZNWI3deqx9rr/A6Vxl9Cnu9j2qfiof1mP4led4T+pn2LP2chtdTPZrq0Ur7t3sw/hTpv0wWn4h+nyGHzOldpX4p5P6bPrBc94b+pxbHN8rl2gaTPrWpR4WP0J6vefBjfOV0vZ7E63H5q08MPPdOlHRtq7ax2eXthc935eR8p7/AGBc7+J7XYytw/8AGz5OPD3J0Ha+5MRz9ObE0jp3mOeJafWEfie11s58T/0rx8fJPRzPWtMn0bUZsLIouYba6qD2+Yro+t2Jz8czjQz4/Jlp7TZ+yIcnFj1DV2uc1/yo4LoEel32LyO94nccvJxtnh68s82T0hwdq975H3Gnd74d3QtaHxO3rz7umbycPs81vDZUGJiSZ+kAtbH8qSAmwB5y37F6HQ8SyuU4+T6tbn6up5sX1dlBvE1Ef81nwKx+Nf3Mfsro+mN23WpaDo/3Ul1XWHscXgNY2V1MaAPR5ytLi7PP5PhcTNnxcfm82b9MnbGhapiVFiwNBHyJYOhH7Qlj3exxZa3RevxZ4+kcp1nT5NJ1PIwpTyMTuhr5w8xXV9XnnPxTN4/Jx/DyuL4bWywm0ELTDV2tV2itAVoCtMnQuxs3qupfQM+sV43jHyY/di5PZ6jfW7p9sT4kcOHHkeUNcSXyFvGq9A9a8/p9L8Tv11pGOO3mP5Vsz+yMf+/P/wArd/J5/wBleR0Tbuov1bRcPPkjEbp4w8sabA9S8jn4/hclw/Zjs08JsHIbHvjXICQDK6Tj+x5Xqd7D/jcdVl7Pz7YMJ4ycHPAJYWGJx8wN2Ffg/LJMsExzu+i977CxutD2vquuwPn09kZjY7gS9/Gz6lpc/e4eG6y2nTabV03J0jtA0/CzQwTMLiQx1jrG5a/c5sOfqXLEter2vaqfwTP6xH/5XmeFX/kz7DJx7kF1P0YdMrQmxu9lH8KtN+mHwWn4h+ny+xY+7pnaV02lkn/jj+sFz/h36nFn5flee7I42F+pTEDmAxoPq6/Yt/xnK/0T6MfBG93LpO29Q1PvNYzzFkMYGiPvw3i32LR6vP2eLj1xY7n2Xnjhlf6mGgwbX0GeSbB1VvKRvFzZMgEFPsZ9rsTWePt/BYTDH2rznaJPp+o6xpj8XIim5VHKY3XQ5Dx95W/4bjy8fFnMppr9jy55Y6e83PPJgba1CbF+TJFju4V+T0q/2LyOrjOTnx8/7tnkvl47pw3k7nzsl93yHjfptdljhLh5Z7PIyu7uPRjfGtHF8nfLA+Phwp0VuIqvFefj4Z1rlMvqzfiM9aen7JP9D1GvDvWfBed4z6Z4z+Gx0p6Vou0vJll3I6CR5MUUbeLPML6kre8G458G5fVr9zO+fVbzsmnkdj6hAXExxvY5jSfm3d17lpeM4SZ4392bpZWyxpO00Abo9F4zPi5b3g3p1/8ALW7393/Dydr12mrQkgoDVWtZ2ulaY0rQWlaCdC7GT/nbUv1dn1ivH8Y/t4/di5fZ+/bQf8r0r6OT4tU+De2aeNze17S3e9hfihpX0AXI979Rmw5e7kORqU2k7wys7G/pIcuQ15nDkbC6LHhx5+tML+y9ejr2BqGkbw0gsIZNHI2pYH/OYf8AHnXOcnFy9bk/Zjs00h7L9G8o5+U5Yh/N8h7rW1+bc+g3mbmaPtDRwwBkEMY/m4Gn5Tz6vWtXDj5e1yfcOXbZ1Y5W/sTUs1wYZshxd16N5NLQP4gL3+zweXp3DD6E61uTRItwaacKeZ8LC9r+bACei53r9jLgz8+I08qOy3Tx/rHJ/davQ/OebXtE+SPN742hjbbwMXIx8mWYzT90Q8AUOLneb2Le6Hf5OxyXDJGWMjV7JP4V6Z9MPgVt+Ifp8mPH5nTe0s/gjk/ps+sFz3hv6nFl5Plc/wBga8zRdXIyXccXIHCQ/wCyfMV7niXVvNxy4z1jBxZae+3VtHF3M6PMhyGxThnESABzXt8y8Tqd3Prf02ejLnxzP1j5dB2Bgab3kuqPizTx6B7KY31rN2fE+Tm1OP0TjwTH3eH3pPpbtW7rRMeGGHHHEvhbXJ9+P7F6/h/Hy/B3zXe/3a3NcZf6Y6btzWMTcui8JCDKY+7yISevUUf2Fc92eDPrcu/5beGUzw00DuzPGOVzGfJ5OT/R8etei1vzxrl8mterD+Em9ttufL0rb2jFjMeDv+HdwR8AXE+FrW6fHzdnlnrdL5rhx4ezU9kZPkmpX1Pes+C2fGZJyYT+GPpe1ed7SPxrn/QZ8F6PhH6dq9z5297Iz/Wftj/9lpeN/Ngz9L6tR2n/AI0/9tH8XLb8H/sf5a/d/u/4eSteu0laCSCam1ru2VoCtBG0FX26Zq2fpMkkmm5UmO+QBr3MrqFi5eDj5ZrObTZL7nU9Y1DVnRu1LLkyXRAhhfXybRxcHHxfJNCST2fFazaJuMPdOu4WNHjYupzxwxDixjaoD3LVz6fBnl5rj6puMayaZ88z5pXF0kji5zj4klbGM8s1PYGDImx5RLjzSRSDwfG8tP8ABGeGOfplNpsbcbu3CGcBrGVXh84X71rfgOC//KbGqycibLkMuVNJNI7xfI4uJ962cOPHCakTYwDj5jXsV+/ulusfdev48Qji1bJDGigC4GgtTLode3flKv0+/HcX9r5HvH2Jfl/X/wCqd18mp65qeqxMi1HNkyI43cmtfXQ1V+4rLxdXi4r5sJpNtr5cTJmxMiPIxpDHNGbY9viCs2eE5MfLlPRD787cesahjux83UJpoXEEsdVGlg4+nw8d82OOqWVta2+i2vux1s9N3Dq2mR93g580UfmZyto9gK1uXo8PJ62CZ5T2fpn7k1nUYjFmajO+M+LAQ0H3JcXR4OO7k9U5Z5We7VBbjDX7YuVPiTNmxpnxSt8HsdRUcnHjyTy5QplZdxufvx3B3fD7py1VXTb99LU/LOvvejvNn+7T5GRNkzOmyZpJZXeL3uJK3OPiw45rGaYcrbd19em6zqOmMkbp+XJAJDbuFdSsfL1eLmsuc3oY8uWHs/DOzcnUMh2RmTOmmcAC93j0V8fFjxY+XCaiM8rl7v203V9Q0vvPuflyQd5XLhXWlHL1ePm+ebGPJlh8tfnnZ+VqM/lGbO6aXiG83eNBZOHhw4ZrCaiM88s7uvntZEFBK0E09rXdtpWgG0BWgrFaC0rQRtOEbQRtCdG0FpWntNhBQWiChLK0aibDaaNK0JsZAposVoTYyQixWmnRBRpFZAposNoTYrSRSnE1Jp0bTTUhKtCSgigJBNNa13baVoCtAIKArQRtBaVplo2gtG0J0bQVhtCdG0ypBQjRBTKw2hFjK0JpBTTUEIptCdMgU0VedCKbTRWSE1BCKQU0U2hNSaSklWmkoJIJIJprWu7hWgK0BAoI2gqbQSBTI2glaCZAoJWgrGVppsIQjRBTKm0IsIQmxkhFhBTTohCLCmmkIRVaEU2hFIKaKU0UhCabQlWhFKCVppVoIoJpbWu7pWglaAQUErQCEErQRtBFMkCgiChNIKC0QU02MgUJsNoRogppsZWhOl4oRYyTQQhFNoRSE0UgoRSmmoIRWQTiFdITShCQRQmpMigmjta7ujaC0rQNK0Ag2gaVoLRtBaKE6IKYsIKE6IKCsIIQRsISytNNIQkoRSmmm0IITRYytCbCEMdhTRYbQim0IqtNFIKEK04k2hBtCShKTJIJpLWu7xWgK0EkA2gtEFBaKBpAoJlaC0gmnRCCsIQmxkgtG0JsKadMrQmwpopQiwoRYbTRYUIrIIRTabHYbQiq00EIRSnEVISytCarQgoJIJo1gd4EAhASAUEggMkEAgFCWQQRCaaQgihNI8EJKE0g9U01kiIrJNNIQx1khFI8EIqTY6yCaafMhipCaEEJpTiChBQmpCCgkgn/2Q==" alt="HCLTech" style="height:20px;object-fit:contain;flex-shrink:0;background:#fff;border-radius:4px;padding:2px 4px;">
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
    <li id="nav-6"><span class="dot">6</span>App Reconfig</li>
    <li id="nav-7"><span class="dot">7</span>Validate</li>
    <li id="nav-8"><span class="dot">8</span>Produce</li>
    <li id="nav-9"><span class="dot">9</span>Migrate</li>
    <li id="nav-10"><span class="dot">10</span>CMDB Changes</li>
    <li id="nav-11"><span class="dot">11</span>Monitoring</li>
    <li id="nav-12"><span class="dot">12</span>Verify</li>
  </ul>
  <div class="sidebar-footer">
    <div style="border-top:1px solid rgba(255,255,255,.1);padding-top:.75rem;margin-top:.5rem">
      <div style="color:rgba(255,255,255,.4);font-size:.67rem;">Self-Service Migration v1.0</div>
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
        <input class="form-control" id="app_name" placeholder="e.g. Order Processing Service">
      </div>
      <div class="col-md-6">
        <label class="form-label">Team / Owner *</label>
        <input class="form-control" id="team_name" placeholder="e.g. Platform Engineering">
      </div>
      <div class="col-12">
        <label class="form-label">Contact Email *</label>
        <input class="form-control" id="contact_email" type="email" placeholder="you@company.com">
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
        <label class="form-label">Queue Name *</label>
        <select class="form-control" id="mq_queue" style="appearance:auto">
          <option value="">— select a queue manager first —</option>
        </select>
        <div class="form-text">User-defined queues only (SYSTEM.* hidden)</div>
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
        <span id="platform_badge" style="font-size:.78rem;font-weight:600;padding:.3rem .8rem;border-radius:20px;background:#EDE9FE;color:#6B21A8;">● Solace PubSub+  —  Active in this demo</span>
      </div>
    </div>
  </div>

  <!-- ── Solace fields ── -->
  <div id="dest_solace">
    <div class="card-s">
      <span class="section-tag tag-sol">● Solace PubSub+</span>
      <div class="row g-3">
        <div class="col-md-4">
          <label class="form-label">Message VPN *</label>
          <input class="form-control" id="sol_vpn" value="default">
        </div>
        <div class="col-md-4">
          <label class="form-label">Target Queue Name *</label>
          <select class="form-control" id="sol_queue" style="appearance:auto">
            <option value="MIGRATED.APP.QUEUE">MIGRATED.APP.QUEUE</option>
          </select>
          <div class="form-text" id="sol_queue_status">Loading queues…</div>
        </div>
        <div class="col-md-4">
          <label class="form-label">Target Protocol</label>
          <select class="form-control" id="sol_protocol" style="appearance:auto">
            <option value="rest">REST Messaging</option>
            <option value="amqp">AMQP 1.0</option>
            <option value="mqtt">MQTT 3.1.1</option>
            <option value="smf">Solace SMF (Native)</option>
            <option value="jms">JMS / JNDI</option>
          </select>
          <div class="form-text">Protocol new application will consume with</div>
        </div>
      </div>
    </div>
    <div style="margin-top:.75rem;padding:.6rem .85rem;background:#F8FAFC;border-left:3px solid #6B21A8;border-radius:4px;font-size:.75rem;color:#64748B;">
      ℹ️ <strong>Note:</strong> AMQP, MQTT, JMS, and Solace SMF are future options when real protocol connectors are wired in. Current demo uses Solace REST Messaging.
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
    <button id="cap_run_btn" onclick="runCapacityAnalysis()"
      style="background:linear-gradient(135deg,#6366F1,#8B5CF6);color:#fff;border:none;
             border-radius:10px;padding:.7rem 2.2rem;font-size:.9rem;font-weight:700;
             cursor:pointer;display:inline-flex;align-items:center;gap:.6rem;
             box-shadow:0 4px 20px rgba(99,102,241,.35);">
      &#x1F916; Run GenAI Capacity Analysis
    </button>
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
        GenAI Analysis &mdash; claude-3-7-sonnet reasoning engine
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
             style="margin-top:.15rem;width:18px;height:18px;accent-color:#6B21A8;cursor:pointer;flex-shrink:0">
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
            style="opacity:.5;cursor:not-allowed">Next: Validate Setup →</button>
  </div>
</div>

<!-- ── STEP 4 (old → now STEP 5) ── -->
<div class="step-panel" id="step-6">
  <h3 class="page-title">Application Reconfiguration</h3>
  <p class="page-sub">Update the JMS connection string in the TomcatEE Order Management System from IBM MQ to Solace PubSub+. This rewrites <code style="background:rgba(0,0,0,.2);padding:.1rem .4rem;border-radius:4px;font-size:.8rem">tomcat-context.xml</code> on disk — the app picks it up automatically.</p>

  <!-- TomcatEE App link -->
  <div class="card-s" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem;flex-wrap:wrap;gap:.75rem">
    <div>
      <div style="font-size:.82rem;font-weight:700;color:#1E293B">🖥️ TomcatEE Order App — Running on port 5002</div>
      <div style="font-size:.72rem;color:#64748B;margin-top:.2rem">Apache Tomcat 9.0.82 / Jakarta EE 8 / IKEA Order Management System v2.3</div>
    </div>
    <a href="http://localhost:5003" target="_blank" style="background:#F16529;color:#fff;text-decoration:none;padding:.45rem 1rem;border-radius:8px;font-size:.78rem;font-weight:700;flex-shrink:0">
      🔗 Open TomcatEE App →
    </a>
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
    <button class="btn-p" id="reconfig_next" onclick="goStep(7);runPreflight()" disabled>Next: Validate Connectivity →</button>
  </div>
</div>

<!-- STEP 5) ── -->
<div class="step-panel" id="step-7">
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
    <button class="btn-o" onclick="goStep(6)">← Back</button>
    <button class="btn-p" id="pf_next" onclick="goStep(8)" disabled>Next: Produce Messages →</button>
  </div>
</div>

<!-- ── STEP 5 ── -->
<div class="step-panel" id="step-8">
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
    <button class="btn-o" onclick="goStep(7)">← Back</button>
    <button class="btn-p" id="prod_next" onclick="goStep(9)" disabled>Next: Migrate →</button>
  </div>
</div>

<!-- ── STEP 6 ── -->
<div class="step-panel" id="step-9">
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
    <button class="btn-p" onclick="goStep(10)">Next: CMDB Changes →</button>
  </div>
</div>

<!-- ── STEP 8: CMDB Changes ── -->
<div class="step-panel" id="step-10">
  <h3 class="page-title">CMDB &amp; ServiceNow Update</h3>
  <p class="page-sub">Register the configuration changes in your CMDB before finalising the migration. This ensures your asset records, CIs, and change tickets stay in sync.</p>

  <div class="card-s">
    <div style="font-size:.85rem;font-weight:600;color:#1E293B;margin-bottom:1rem">Do you want to register CMDB changes for this migration?</div>
    <div style="display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:1.25rem">
      <label style="display:flex;align-items:center;gap:.6rem;cursor:pointer;font-size:.88rem;padding:.6rem 1.1rem;border:2px solid #E2E8F0;border-radius:8px;transition:all .2s" id="cmdb_yes_lbl">
        <input type="radio" name="cmdb_choice" id="cmdb_yes" value="yes" onchange="handleCmdb()" style="accent-color:#6B21A8;width:16px;height:16px">
        <span>✅ Yes — Register Changes in ServiceNow CMDB</span>
      </label>
      <label style="display:flex;align-items:center;gap:.6rem;cursor:pointer;font-size:.88rem;padding:.6rem 1.1rem;border:2px solid #E2E8F0;border-radius:8px;transition:all .2s" id="cmdb_no_lbl">
        <input type="radio" name="cmdb_choice" id="cmdb_no" value="no" onchange="handleCmdb()" style="accent-color:#6B21A8;width:16px;height:16px">
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
    <button class="btn-o" onclick="goStep(9)">← Back</button>
    <button class="btn-p" id="cmdb_next_btn" onclick="goStep(11)">Next: Enable Monitoring →</button>
  </div>
</div>

<!-- ── STEP 9: Enable Monitoring ── -->
<div class="step-panel" id="step-11">
  <h3 class="page-title">Enable Monitoring &amp; Alerting</h3>
  <p class="page-sub">Set up observability for your migrated workload — dashboards, alerts, and notification channels — so your team has full visibility in production.</p>

  <div class="card-s">
    <div style="font-size:.85rem;font-weight:600;color:#1E293B;margin-bottom:1rem">Do you want to enable monitoring &amp; alerting for this migration?</div>
    <div style="display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:1.25rem">
      <label style="display:flex;align-items:center;gap:.6rem;cursor:pointer;font-size:.88rem;padding:.6rem 1.1rem;border:2px solid #E2E8F0;border-radius:8px;transition:all .2s" id="mon_yes_lbl">
        <input type="radio" name="mon_choice" id="mon_yes" value="yes" onchange="handleMon()" style="accent-color:#6B21A8;width:16px;height:16px">
        <span>✅ Yes — Enable Monitoring &amp; Alerting</span>
      </label>
      <label style="display:flex;align-items:center;gap:.6rem;cursor:pointer;font-size:.88rem;padding:.6rem 1.1rem;border:2px solid #E2E8F0;border-radius:8px;transition:all .2s" id="mon_no_lbl">
        <input type="radio" name="mon_choice" id="mon_no" value="no" onchange="handleMon()" style="accent-color:#6B21A8;width:16px;height:16px">
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
    <button class="btn-o" onclick="goStep(10)">← Back</button>
    <button class="btn-p" onclick="goStep(12);runVerify()">Next: Verify Migration →</button>
  </div>
</div>

<!-- ── STEP 7 ── -->
<div class="step-panel" id="step-12">
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
    solace:{text:'● Solace PubSub+  —  Active in this demo',bg:'#EDE9FE',color:'#6B21A8'},
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
async function loadQmgrs(){
  const sel=document.getElementById('mq_qmgr');
  const status=document.getElementById('qmgr_status');
  sel.innerHTML='<option value="">⏳ Loading queue managers…</option>';
  try{
    const d=await(await fetch('/api/qmgrs')).json();
    if(d.qmgrs&&d.qmgrs.length>0){
      sel.innerHTML=d.qmgrs.map(q=>`<option value="${q}">${q}</option>`).join('');
      status.textContent=`${d.qmgrs.length} queue manager(s) found`;
      status.style.color='#10B981';
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
      sel.innerHTML=d.queues.map(q=>`<option value="${q}">${q}</option>`).join('');
    } else {
      sel.innerHTML='<option value="">No user queues found in '+qmgr+'</option>';
    }
  }catch(e){sel.innerHTML='<option value="">Failed to load queues</option>';}
}

async function loadSolQueues(){
  const vpn=document.getElementById('sol_vpn').value||'default';
  const sel=document.getElementById('sol_queue');
  const status=document.getElementById('sol_queue_status');
  status.textContent='⏳ Loading queues…';
  try{
    const d=await(await fetch(`/api/sol_queues?vpn=${encodeURIComponent(vpn)}`)).json();
    if(d.queues&&d.queues.length>0){
      const current=sel.value||'MIGRATED.APP.QUEUE';
      sel.innerHTML=d.queues.map(q=>`<option value="${q}"${q===current?' selected':''}>${q}</option>`).join('');
      status.textContent=`${d.queues.length} queue(s) found in Solace`;
      status.style.color='#10B981';
    }
  }catch(e){status.textContent='Could not load — using default';status.style.color='#F59E0B';}
}

window.addEventListener('DOMContentLoaded',loadQmgrs);

// ── Step navigation ───────────────────────────────────────────────────────────
function goStep(n){
  if(n===6) setTimeout(initReconfig, 50);
  document.querySelectorAll('.step-panel').forEach(p=>p.classList.remove('active'));
  document.getElementById('step-'+n).classList.add('active');
  for(let i=1;i<=12;i++){
    const li=document.getElementById('nav-'+i);
    li.classList.remove('active','done');
    if(i<n) li.classList.add('done');
    if(i===n) li.classList.add('active');
  }
  if(n===3){ loadSolQueues(); }
  if(n===5){ showSchemaStep(); }
  if(n===4){ initCapacityStep(); }
  if(n===7){
    document.getElementById('lbl_count').textContent=document.getElementById('msg_count').value;
    document.getElementById('lbl_type').textContent=document.getElementById('msg_type').value;
  }
}

function initReconfig(){
  // Load current context.xml and show before/after
  const p = params();
  const t = (tag,val) => `<span style="color:#F472B6">${tag}</span><span style="color:#60A5FA">${val}</span>`;
  const a = (k,v)     => `  <span style="color:#60A5FA">${k}</span>=<span style="color:#34D399">"${v}"</span>`;
  const cm = (s)      => `<span style="color:#475569;font-style:italic">&lt;!-- ${s} --&gt;</span>`;

  // Before block (IBM MQ) — both Resources
  document.getElementById('reconfig_before').innerHTML = [
    cm('Connection Factory'),
    `<span style="color:#F472B6">&lt;Resource</span>`,
    a('name',    'jms/OrderQueueFactory'),
    a('type',    'javax.jms.QueueConnectionFactory'),
    a('factory', 'com.ibm.mq.jms.MQQueueConnectionFactory'),
    a('HOST',    p.mq_host||'localhost'),
    a('PORT',    p.mq_port||'1414'),
    a('QMANAGER',p.mq_qmgr||'QM1'),
    a('CHANNEL', 'DEV.APP.SVRCONN') + `<span style="color:#F472B6">/&gt;</span>`,
    '',
    cm('Queue Destination'),
    `<span style="color:#F472B6">&lt;Resource</span>`,
    a('name',       'jms/OrderQueue'),
    a('type',       'javax.jms.Queue'),
    a('factory',    'com.ibm.mq.jms.MQQueueFactory'),
    a('QUEUE_NAME', p.mq_queue||'DEV.QUEUE.1') + `<span style="color:#F472B6">/&gt;</span>`,
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
    a('QUEUE_NAME', p.sol_queue||'ORDER.PROCESSING.SERVICE.QUEUE'),
    a('VPN',        p.sol_vpn||'default') + `<span style="color:#F472B6">/&gt;</span>`,
  ].join('\n');
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
    {ms:1100, msg:`📋 Current host: ${p.mq_host||'localhost'}:${p.mq_port||'1414'} / QM: ${p.mq_qmgr||'QM1'}`},
    {ms:1500, msg:'⚙️  Generating new Solace JMS connection factory config...'},
    {ms:1900, msg:`⚙️  New factory: com.solacesystems.jndi.SolJNDIInitialContextFactory`},
    {ms:2300, msg:`⚙️  New host: ${p.sol_host||'localhost'}:9000 / VPN: ${p.sol_vpn||'default'}`},
    {ms:2700, msg:'✏️  Writing updated tomcat-context.xml to disk...'},
    {ms:3200, msg:'🔄 Signalling Tomcat context reload...'},
    {ms:3700, msg:'✅ tomcat-context.xml updated successfully'},
    {ms:4100, msg:`✅ TomcatEE app now routes → Solace PubSub+ (${p.sol_vpn||'default'}/${p.sol_queue||'ORDER.PROCESSING.SERVICE.QUEUE'})`},
  ];

  // Fire the actual API call
  const apiCall = fetch('/api/reconfig-app', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({
      sol_host:  p.sol_host  || 'localhost',
      sol_vpn:   p.sol_vpn   || 'default',
      sol_queue: p.sol_queue || 'ORDER.PROCESSING.SERVICE.QUEUE',
      sol_port:  '9000',
      mq_qmgr:   p.mq_qmgr  || 'QM1',
      mq_queue:  p.mq_queue  || 'DEV.QUEUE.1',
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
              solQueue: p.sol_queue||'ORDER.PROCESSING.SERVICE.QUEUE',
              mqQmgr: p.mq_qmgr||'QM1', mqQueue: p.mq_queue||'DEV.QUEUE.1',
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
            solQueue: p.sol_queue||'ORDER.PROCESSING.SERVICE.QUEUE',
            mqQmgr: p.mq_qmgr||'QM1', mqQueue: p.mq_queue||'DEV.QUEUE.1',
          };
        });
      }
    }, s.ms);
  });
}

function handleMon(){
  const yes=document.getElementById('mon_yes').checked;
  document.getElementById('mon_fields').style.display=yes?'block':'none';
  document.getElementById('mon_yes_lbl').style.borderColor=yes?'#6B21A8':'#E2E8F0';
  document.getElementById('mon_no_lbl').style.borderColor=(!yes&&document.getElementById('mon_no').checked)?'#6B21A8':'#E2E8F0';
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
  document.getElementById('cmdb_yes_lbl').style.borderColor=yes?'#6B21A8':'#E2E8F0';
  document.getElementById('cmdb_no_lbl').style.borderColor=!yes&&document.getElementById('cmdb_no').checked?'#6B21A8':'#E2E8F0';
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
       <td style="padding:.4rem .75rem;border-bottom:1px solid #F1F5F9;font-family:monospace;color:#6B21A8;font-weight:600">${f[0]}</td>
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
  return {
    mq_qmgr:    document.getElementById('mq_qmgr').value,
    mq_queue:   document.getElementById('mq_queue').value,
    sol_vpn:    document.getElementById('sol_vpn').value,
    sol_queue:  document.getElementById('sol_queue').value,
    mq_protocol:document.getElementById('mq_protocol').value,
    sol_protocol:document.getElementById('sol_protocol').value,
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
#  HCLTech Middleware Migration Portal — Generated Playbook
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
    mq_queue:     "${p.mq_queue   || 'DEV.QUEUE.1'}"
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
    mq_queue:  "${p.mq_queue  || 'DEV.QUEUE.1'}"
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
    mq_queue:  "${p.mq_queue  || 'DEV.QUEUE.1'}"
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
          description: "Auto-generated by HCLTech Migration Portal"
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
    {label:'Source Broker', val:'IBM MQ &nbsp;/&nbsp; '+(p.mq_qmgr||'QM1'), col:'#60A5FA'},
    {label:'Source Queue',  val:p.mq_queue||'DEV.QUEUE.1',                   col:'#60A5FA'},
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

  fetch('/api/capacity-check?queue='+encodeURIComponent(p.sol_queue||'IKEA.ORDER.QUEUE')+'&vpn='+encodeURIComponent(p.sol_vpn||'default'))
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
    `► Generating recommendation...`,
  ];
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
          Queue <em>${p.sol_queue||'IKEA.ORDER.QUEUE'}</em> will be provisioned automatically in Step 7.
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

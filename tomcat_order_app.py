"""
tomcat_order_app.py
IKEA Order Management System — TomcatEE Mock Application
Simulates a Java EE order app that sends messages to IBM MQ or Solace
depending on the current tomcat-context.xml configuration.

Run with: python tomcat_order_app.py
Port: 5002
"""

import json, os, random, time
import xml.etree.ElementTree as ET
import requests
import urllib3
from flask import Flask, jsonify, request, Response, stream_with_context

urllib3.disable_warnings()

app = Flask(__name__)

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CONFIG_XML = os.path.join(BASE_DIR, 'tomcat-context.xml')

MQ_BASE    = 'https://localhost:9443/ibmmq/rest/v2'
MQ_APP     = ('app', 'passw0rd')
SOL_REST   = 'http://localhost:9000'
SOL_CLIENT = ('admin', 'admin')

CUSTOMERS = ['IKEA Sweden','IKEA Germany','IKEA UK','IKEA US',
             'IKEA Japan','IKEA France','IKEA Australia','IKEA Canada']
REGIONS   = ['EMEA','APAC','AMER','NORDICS','LATAM','ANZ']
PRODUCTS  = ['BILLY Bookcase','KALLAX Shelf','POÄNG Chair','MALM Bed Frame',
             'LACK Table','HEMNES Dresser','PAX Wardrobe','EKET Cabinet']


# ── Config reader ──────────────────────────────────────────────────────────────
def read_config():
    """Parse tomcat-context.xml and return connection details dict."""
    try:
        tree = ET.parse(CONFIG_XML)
        root = tree.getroot()
        for res in root.findall('Resource'):
            name = res.get('name', '')
            if 'Factory' not in name:
                continue
            factory = res.get('factory', '')
            if 'ibm' in factory.lower() or 'mq' in factory.lower():
                return {
                    'broker':   'ibmmq',
                    'label':    'IBM MQ',
                    'host':     res.get('HOST', 'localhost'),
                    'port':     res.get('PORT', '1414'),
                    'qmgr':     res.get('QMANAGER', 'QM1'),
                    'channel':  res.get('CHANNEL', 'DEV.APP.SVRCONN'),
                    'queue':    'DEV.QUEUE.1',
                    'factory':  factory,
                }
            elif 'solace' in factory.lower():
                return {
                    'broker':  'solace',
                    'label':   'Solace PubSub+',
                    'host':    res.get('HOST', 'localhost'),
                    'port':    res.get('PORT', '9000'),
                    'vpn':     res.get('VPN', 'default'),
                    'queue':   res.get('QUEUE_NAME', 'ORDER.PROCESSING.SERVICE.QUEUE'),
                    'factory': factory,
                }
    except Exception as e:
        pass
    # Default fallback
    return {'broker': 'ibmmq', 'label': 'IBM MQ', 'host': 'localhost',
            'port': '1414', 'qmgr': 'QM1', 'channel': 'DEV.APP.SVRCONN',
            'queue': 'DEV.QUEUE.1', 'factory': 'com.ibm.mq.jms.MQQueueConnectionFactory'}


def make_order(i):
    """Generate a realistic IKEA order payload."""
    product = random.choice(PRODUCTS)
    qty     = random.randint(1, 20)
    price   = round(random.uniform(19.99, 999.99), 2)
    return json.dumps({
        "order_id":    1000 + i,
        "type":        "OrderEvent",
        "product":     product,
        "quantity":    qty,
        "amount":      round(price * qty, 2),
        "customer":    random.choice(CUSTOMERS),
        "region":      random.choice(REGIONS),
        "items":       qty,
        "timestamp":   "2026-06-28T10:00:00Z",
        "source":      "TomcatEE-OrderMgmt-v2.3",
        "app_server":  "Apache Tomcat 9.0.82 / Jakarta EE 8"
    })


# ── API routes ─────────────────────────────────────────────────────────────────
@app.route('/api/config')
def get_config():
    cfg = read_config()
    cfg['config_xml'] = open(CONFIG_XML, encoding='utf-8').read() if os.path.exists(CONFIG_XML) else ''
    return jsonify(cfg)


@app.route('/api/send-orders')
def send_orders():
    count = min(int(request.args.get('count', 5)), 50)
    cfg   = read_config()
    results = []

    for i in range(count):
        payload = make_order(i)
        try:
            if cfg['broker'] == 'ibmmq':
                r = requests.post(
                    f"{MQ_BASE}/messaging/qmgr/{cfg['qmgr']}/queue/{cfg['queue']}/message",
                    auth=MQ_APP,
                    headers={"Content-Type": "application/json",
                             "ibm-mq-rest-csrf-token": "tomcat-app"},
                    data=payload, verify=False, timeout=8
                )
                ok     = r.status_code in (200, 201)
                dest   = f"IBM MQ › {cfg['qmgr']} / {cfg['queue']}"
                status = r.status_code
            else:
                sol_queue = cfg.get('queue', 'ORDER.PROCESSING.SERVICE.QUEUE')
                r = requests.post(
                    f"{SOL_REST}/QUEUE/{sol_queue}",
                    auth=SOL_CLIENT,
                    headers={"Content-Type": "application/json",
                             "Solace-delivery-mode": "persistent"},
                    data=payload, timeout=8
                )
                ok     = r.status_code == 200
                dest   = f"Solace PubSub+ › {cfg.get('vpn','default')} / {sol_queue}"
                status = r.status_code
        except Exception as e:
            ok, dest, status = False, 'error', 0

        results.append({
            "i":      i + 1,
            "ok":     ok,
            "status": status,
            "dest":   dest,
            "order":  json.loads(payload)
        })
        time.sleep(0.05)

    sent = sum(1 for r in results if r['ok'])
    return jsonify({
        "ok":     sent > 0,
        "sent":   sent,
        "total":  count,
        "broker": cfg['broker'],
        "label":  cfg['label'],
        "results": results
    })


@app.route('/')
def index():
    return APP_HTML


# ── HTML ───────────────────────────────────────────────────────────────────────
APP_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>IKEA Order Management System — TomcatEE</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
:root{--tomcat-orange:#F16529;--tomcat-dark:#1A1A2E;--solace-green:#10B981;--mq-blue:#1D6FCC;}
*{box-sizing:border-box;}
body{background:#0F172A;font-family:'Segoe UI',sans-serif;margin:0;color:#E2E8F0;min-height:100vh;}

/* Top bar */
.topbar{background:linear-gradient(90deg,#1A1A2E 0%,#16213E 100%);border-bottom:3px solid var(--tomcat-orange);padding:.6rem 1.5rem;display:flex;align-items:center;justify-content:space-between;}
.topbar-brand{display:flex;align-items:center;gap:.75rem;}
.tomcat-badge{background:var(--tomcat-orange);color:#fff;font-size:.65rem;font-weight:800;padding:.2rem .5rem;border-radius:4px;letter-spacing:.06em;}
.topbar-title{color:#fff;font-size:.95rem;font-weight:700;letter-spacing:.02em;}
.topbar-sub{color:rgba(255,255,255,.5);font-size:.7rem;}
.status-badge{display:flex;align-items:center;gap:.4rem;padding:.35rem .8rem;border-radius:20px;font-size:.75rem;font-weight:700;letter-spacing:.03em;}
.status-mq{background:rgba(29,111,204,.2);border:1.5px solid #1D6FCC;color:#60A5FA;}
.status-solace{background:rgba(16,185,129,.2);border:1.5px solid #10B981;color:#34D399;}

/* Main layout */
.main{max-width:1100px;margin:0 auto;padding:1.5rem;}

/* Cards */
.panel{background:#1E293B;border-radius:12px;padding:1.25rem 1.5rem;margin-bottom:1.25rem;border:1px solid rgba(255,255,255,.07);}
.panel-title{font-size:.78rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;margin-bottom:.85rem;}

/* Config display */
.config-grid{display:grid;grid-template-columns:1fr 1fr;gap:1rem;}
.config-card{background:#0F172A;border-radius:8px;padding:1rem;border:1.5px solid rgba(255,255,255,.08);}
.config-card.mq{border-color:#1D6FCC;}
.config-card.sol{border-color:#10B981;}
.config-label{font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;margin-bottom:.5rem;}
.config-label.mq{color:#60A5FA;}
.config-label.sol{color:#34D399;}
.config-row{display:flex;justify-content:space-between;font-size:.75rem;padding:.2rem 0;border-bottom:1px solid rgba(255,255,255,.04);}
.config-row:last-child{border:none;}
.config-key{color:#64748B;}
.config-val{color:#E2E8F0;font-family:monospace;font-weight:600;}

/* Code block */
.xml-block{background:#020617;border-radius:8px;padding:1rem;font-family:'Courier New',monospace;font-size:.72rem;line-height:1.7;color:#94A3B8;overflow-x:auto;border:1px solid rgba(255,255,255,.06);}
.xml-tag{color:#F472B6;}
.xml-attr{color:#60A5FA;}
.xml-val{color:#34D399;}
.xml-comment{color:#475569;font-style:italic;}

/* Order controls */
.controls{display:flex;align-items:center;gap:1rem;flex-wrap:wrap;}
.btn-send{background:var(--tomcat-orange);color:#fff;border:none;padding:.6rem 1.4rem;border-radius:8px;font-weight:700;font-size:.85rem;cursor:pointer;transition:all .2s;}
.btn-send:hover{background:#D4541A;}
.btn-send:disabled{opacity:.5;cursor:not-allowed;}
.count-sel{background:#0F172A;border:1px solid rgba(255,255,255,.12);color:#E2E8F0;padding:.5rem .75rem;border-radius:8px;font-size:.85rem;}

/* Log */
.log{background:#020617;border-radius:8px;padding:.85rem;height:220px;overflow-y:auto;font-family:'Courier New',monospace;font-size:.72rem;line-height:1.6;border:1px solid rgba(255,255,255,.06);}
.log-ok{color:#34D399;}
.log-err{color:#F87171;}
.log-info{color:#94A3B8;}
.log-head{color:#F59E0B;font-weight:700;}

/* Meters */
.meter-row{display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:1rem;}
.meter{background:#0F172A;border-radius:8px;padding:.75rem 1rem;text-align:center;min-width:100px;border:1px solid rgba(255,255,255,.07);}
.meter-val{font-size:1.6rem;font-weight:800;color:#F59E0B;line-height:1;}
.meter-lbl{font-size:.65rem;color:#64748B;margin-top:.3rem;text-transform:uppercase;letter-spacing:.06em;}

/* Footer */
.footer{text-align:center;color:#334155;font-size:.7rem;padding:1.5rem 0 .5rem;}
</style>
</head>
<body>

<!-- Top bar -->
<div class="topbar">
  <div class="topbar-brand">
    <span class="tomcat-badge">TomcatEE</span>
    <div>
      <div class="topbar-title">IKEA Order Management System</div>
      <div class="topbar-sub">Apache Tomcat 9.0.82 &nbsp;/&nbsp; Jakarta EE 8 &nbsp;/&nbsp; JMS 2.0</div>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:1rem">
    <div id="status_badge" class="status-badge status-mq">🔴 IBM MQ</div>
    <div style="color:#475569;font-size:.72rem">v2.3.1-SNAPSHOT</div>
  </div>
</div>

<!-- Main -->
<div class="main">

  <!-- Connection Config -->
  <div class="panel">
    <div class="panel-title">🔌 JMS Connection Configuration — tomcat-context.xml</div>
    <div class="config-grid">
      <div class="config-card mq">
        <div class="config-label mq">Current — IBM MQ</div>
        <div id="mq_config_rows"></div>
      </div>
      <div class="config-card sol" id="sol_card" style="opacity:.35">
        <div class="config-label sol">Target — Solace PubSub+</div>
        <div id="sol_config_rows">
          <div class="config-row"><span class="config-key">Factory</span><span class="config-val">SolJNDIInitialContextFactory</span></div>
          <div class="config-row"><span class="config-key">Host</span><span class="config-val">localhost</span></div>
          <div class="config-row"><span class="config-key">Port</span><span class="config-val">9000</span></div>
          <div class="config-row"><span class="config-key">VPN</span><span class="config-val">default</span></div>
          <div class="config-row"><span class="config-key">Status</span><span class="config-val" style="color:#F59E0B">Pending migration</span></div>
        </div>
      </div>
    </div>
    <div style="margin-top:1rem">
      <div style="font-size:.7rem;color:#64748B;margin-bottom:.4rem">📄 Live context.xml</div>
      <div class="xml-block" id="xml_display">Loading...</div>
    </div>
  </div>

  <!-- Order Generator -->
  <div class="panel">
    <div class="panel-title">📦 Order Generator</div>
    <div class="meter-row">
      <div class="meter"><div class="meter-val" id="m_sent">0</div><div class="meter-lbl">Sent</div></div>
      <div class="meter"><div class="meter-val" id="m_ok">0</div><div class="meter-lbl">Success</div></div>
      <div class="meter"><div class="meter-val" id="m_fail">0</div><div class="meter-lbl">Failed</div></div>
      <div class="meter"><div class="meter-val" id="m_broker" style="font-size:1rem;color:#60A5FA">IBM MQ</div><div class="meter-lbl">Broker</div></div>
    </div>
    <div class="controls" style="margin-bottom:.85rem">
      <select class="count-sel" id="order_count">
        <option value="5">5 orders</option>
        <option value="10" selected>10 orders</option>
        <option value="20">20 orders</option>
        <option value="50">50 orders</option>
      </select>
      <button class="btn-send" id="send_btn" onclick="sendOrders()">▶ Generate &amp; Send Orders</button>
      <button class="btn-send" style="background:#334155" onclick="clearLog()">🗑 Clear Log</button>
    </div>
    <div class="log" id="order_log"><span class="log-info">// Ready — click Generate to send orders to the configured broker</span></div>
  </div>

  <!-- Links -->
  <div class="panel" style="padding:.85rem 1.25rem">
    <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.75rem">
      <span style="font-size:.78rem;color:#64748B">Manage this migration in the HCLTech Self-Service Portal</span>
      <a href="http://localhost:5001" target="_blank" style="background:linear-gradient(135deg,#6B21A8,#1D6FCC);color:#fff;text-decoration:none;padding:.45rem 1rem;border-radius:8px;font-size:.78rem;font-weight:700">
        🔄 Open Migration Portal →
      </a>
    </div>
  </div>

</div>

<div class="footer">© 2026 Tarun Virmani &nbsp;·&nbsp; HCLTech Middleware Solutioning &nbsp;·&nbsp; IKEA Retail Systems</div>

<script>
function escapeHtml(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

function logLine(cls, msg){
  const log = document.getElementById('order_log');
  const d   = document.createElement('div');
  d.className = cls;
  d.textContent = msg;
  log.appendChild(d);
  log.scrollTop = log.scrollHeight;
}

function clearLog(){
  document.getElementById('order_log').innerHTML = '<span class="log-info">// Log cleared</span>';
}

function renderConfig(cfg){
  // MQ card
  if(cfg.broker === 'ibmmq'){
    document.getElementById('mq_config_rows').innerHTML =
      row('Factory','MQQueueConnectionFactory') +
      row('Host', cfg.host) + row('Port', cfg.port) +
      row('Queue Manager', cfg.qmgr) + row('Channel', cfg.channel) +
      row('Queue', cfg.queue) +
      `<div class="config-row"><span class="config-key">Status</span><span class="config-val" style="color:#34D399">● Active</span></div>`;
    document.getElementById('sol_card').style.opacity = '.35';
    document.getElementById('status_badge').className = 'status-badge status-mq';
    document.getElementById('status_badge').textContent = '🔴 IBM MQ';
    document.getElementById('m_broker').textContent = 'IBM MQ';
    document.getElementById('m_broker').style.color = '#60A5FA';
  } else {
    document.getElementById('mq_config_rows').innerHTML =
      row('Factory','MQQueueConnectionFactory') +
      row('Host','localhost') + row('Port','1414') +
      row('Queue Manager','QM1') + row('Channel','DEV.APP.SVRCONN') +
      `<div class="config-row"><span class="config-key">Status</span><span class="config-val" style="color:#F87171">● Decommissioned</span></div>`;
    document.getElementById('sol_card').style.opacity = '1';
    document.getElementById('sol_config_rows').innerHTML =
      row('Factory','SolJNDIInitialContextFactory') +
      row('Host', cfg.host) + row('Port', cfg.port||'9000') +
      row('VPN', cfg.vpn||'default') + row('Queue', cfg.queue) +
      `<div class="config-row"><span class="config-key">Status</span><span class="config-val" style="color:#34D399">● Active — Production</span></div>`;
    document.getElementById('status_badge').className = 'status-badge status-solace';
    document.getElementById('status_badge').textContent = '🟢 Solace PubSub+';
    document.getElementById('m_broker').textContent = 'Solace';
    document.getElementById('m_broker').style.color = '#34D399';
  }

  // XML display
  const xml = cfg.config_xml || '';
  document.getElementById('xml_display').innerHTML = syntaxHighlight(xml);
}

function row(k,v){
  return `<div class="config-row"><span class="config-key">${escapeHtml(k)}</span><span class="config-val">${escapeHtml(v)}</span></div>`;
}

function syntaxHighlight(xml){
  return escapeHtml(xml)
    .replace(/(&lt;\?[^?]*\?&gt;)/g,'<span class="xml-comment">$1</span>')
    .replace(/(&lt;!--[\s\S]*?--&gt;)/g,'<span class="xml-comment">$1</span>')
    .replace(/(&lt;\/?[\w:]+)/g,'<span class="xml-tag">$1</span>')
    .replace(/(\w+)=&quot;([^&]*)&quot;/g,'<span class="xml-attr">$1</span>=<span class="xml-val">"$2"</span>');
}

async function loadConfig(){
  try{
    const r = await fetch('/api/config');
    renderConfig(await r.json());
  } catch(e){ console.error(e); }
}

async function sendOrders(){
  const btn   = document.getElementById('send_btn');
  const count = document.getElementById('order_count').value;
  btn.disabled = true;
  document.getElementById('order_log').innerHTML = '';

  logLine('log-head', `[${new Date().toLocaleTimeString()}] Generating ${count} orders...`);

  try{
    const r  = await fetch(`/api/send-orders?count=${count}`);
    const d  = await r.json();
    let ok=0, fail=0;

    d.results.forEach((x,i)=>{
      setTimeout(()=>{
        const icon = x.ok ? '✅' : '❌';
        const o    = x.order;
        logLine(x.ok?'log-ok':'log-err',
          `${icon} [${String(x.i).padStart(2,'0')}/${d.total}] `+
          `Order #${o.order_id} | ${o.product} x${o.quantity} | $${o.amount} | `+
          `${o.customer} | → ${x.dest} [HTTP ${x.status}]`);
        if(x.ok) ok++; else fail++;
        document.getElementById('m_sent').textContent  = x.i;
        document.getElementById('m_ok').textContent    = ok;
        document.getElementById('m_fail').textContent  = fail;
        if(x.i === d.total){
          setTimeout(()=>{
            logLine('log-head', `─── Summary: ${ok} sent ✅  ${fail} failed ❌  → ${d.label} ───`);
            btn.disabled = false;
          }, 150);
        }
      }, i * 120);
    });

  } catch(e){
    logLine('log-err', `Connection error: ${e.message}`);
    btn.disabled = false;
  }
}

// Poll config every 3s so UI updates automatically after portal reconfigures it
loadConfig();
setInterval(loadConfig, 3000);
</script>
</body>
</html>"""  # noqa




# ── Startup reset ──────────────────────────────────────────────────────────────
def reset_to_ibmmq():
    """Write IBM MQ config to tomcat-context.xml on startup so each demo run starts fresh."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<!--
  Tomcat Context Configuration - IKEA Order Management System v2.3
  Application: ikea-order-mgmt
  Managed by:  HCLTech Middleware Solutioning
  Status:      IBM MQ (baseline - awaiting Solace migration)

  factory="org.apache.naming.factory.BeanFactory" is Tomcat's built-in
  factory that creates Java beans by calling setters on the type class.
  transportType=1 means CLIENT mode (TCP connection to MQ broker).
-->
<Context>

  <!-- JMS Connection Factory - IBM MQ
       BeanFactory calls: new MQQueueConnectionFactory()
         .setHostName("ibmmq") .setPort(1414)
         .setQueueManager("QM1") .setChannel("DEV.APP.SVRCONN")
         .setTransportType(1)
  -->
  <Resource
    name="jms/OrderQueueFactory"
    auth="Container"
    type="com.ibm.mq.jms.MQQueueConnectionFactory"
    factory="org.apache.naming.factory.BeanFactory"
    hostName="ibmmq"
    port="1414"
    queueManager="QM1"
    channel="DEV.APP.SVRCONN"
    transportType="1"
    description="IBM MQ JMS Connection Factory for Order Processing"/>

  <!-- JMS Queue Destination - IBM MQ
       BeanFactory calls: new MQQueue().setBaseQueueName("DEV.QUEUE.1")
  -->
  <Resource
    name="jms/OrderQueue"
    auth="Container"
    type="com.ibm.mq.jms.MQQueue"
    factory="org.apache.naming.factory.BeanFactory"
    baseQueueName="DEV.QUEUE.1"
    description="Order Processing Queue - IBM MQ"/>

</Context>
"""
    with open(CONFIG_XML, 'w', encoding='utf-8') as f:
        f.write(xml)
    print("  [startup] tomcat-context.xml reset to IBM MQ baseline")

    # Tell real Tomcat to reload so it re-reads context.xml and rebinds JNDI
    # Tomcat Manager text API: GET /manager/text/reload?path=/order-mgmt
    try:
        import requests as _req
        r = _req.get(
            "http://localhost:8888/manager/text/reload",
            params={"path": "/order-mgmt"},
            auth=("admin", "admin123"),
            timeout=5
        )
        print(f"  [startup] Tomcat reload: {r.text.strip()}")
    except Exception as te:
        print(f"  [startup] Tomcat reload skipped (not running yet?): {te}")

if __name__ == '__main__':
    print("═" * 55)
    print("  IKEA Order Management System — TomcatEE Mock App")
    print("  http://localhost:5003")
    print("  Config: tomcat-context.xml")
    print("═" * 55)
    reset_to_ibmmq()
    app.run(debug=True, port=5003)

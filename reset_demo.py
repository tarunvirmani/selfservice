"""
reset_demo.py — IBM MQ -> Solace Migration Demo Reset
======================================================
Run this before each demo to restore everything to baseline:

  1. Writes IBM MQ config to tomcat-context.xml
  2. Calls Tomcat Manager API to reload the webapp (JNDI re-bound to IBM MQ)
  3. Opens key demo URLs in the browser

Usage:
    python reset_demo.py

Requires: Docker containers running (docker-compose up -d)
"""

import os, sys, time, webbrowser
import requests
urllib3_available = False
try:
    import urllib3
    urllib3.disable_warnings()
    urllib3_available = True
except ImportError:
    pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_XML = os.path.join(BASE_DIR, 'tomcat-context.xml')

IBM_MQ_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
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

  <!-- JMS Queue Destination - IBM MQ -->
  <Resource
    name="jms/OrderQueue"
    auth="Container"
    type="com.ibm.mq.jms.MQQueue"
    factory="org.apache.naming.factory.BeanFactory"
    baseQueueName="DEV.QUEUE.1"
    description="Order Processing Queue - IBM MQ"/>

</Context>
"""

SEP = "=" * 58

def banner(msg):
    print(f"\n  {msg}")

def ok(msg):
    print(f"  [OK] {msg}")

def warn(msg):
    print(f"  [!!] {msg}")

def step(n, msg):
    print(f"\n[STEP {n}] {msg}")


print()
print(SEP)
print("  IBM MQ -> Solace Demo — Reset to Baseline")
print("  HCLTech Middleware Solutioning")
print(SEP)

# ── Step 1: Write IBM MQ config ───────────────────────────────────────
step(1, "Resetting tomcat-context.xml to IBM MQ ...")
try:
    with open(CONFIG_XML, 'w', encoding='utf-8') as f:
        f.write(IBM_MQ_XML)
    ok(f"tomcat-context.xml written → IBM MQ / QM1 / DEV.QUEUE.1")
except Exception as e:
    warn(f"Could not write tomcat-context.xml: {e}")
    sys.exit(1)

# ── Step 2: Reload real Tomcat ────────────────────────────────────────
step(2, "Reloading Tomcat webapp (JNDI rebind to IBM MQ) ...")
print("      Calling: GET http://localhost:8888/manager/text/reload?path=/order-mgmt")

# Tomcat may need a moment after docker-compose up — retry a few times
tomcat_ok = False
for attempt in range(1, 5):
    try:
        r = requests.get(
            "http://localhost:8888/manager/text/reload",
            params={"path": "/order-mgmt"},
            auth=("admin", "admin123"),
            timeout=6
        )
        result = r.text.strip()
        if result.startswith("OK"):
            ok(f"Tomcat: {result}")
            tomcat_ok = True
            break
        else:
            warn(f"Attempt {attempt}: {result}")
    except requests.exceptions.ConnectionError:
        if attempt < 4:
            print(f"      Tomcat not ready yet, retrying in 5s ... (attempt {attempt}/4)")
            time.sleep(5)
        else:
            warn("Tomcat not reachable — is docker-compose up -d running?")
    except Exception as e:
        warn(f"Tomcat reload error: {e}")
        break

if not tomcat_ok:
    warn("Tomcat reload skipped. Run manually after containers are up:")
    warn("  http://localhost:8888/manager/text/reload?path=/order-mgmt")

# ── Step 3: Open demo URLs ────────────────────────────────────────────
step(3, "Opening demo URLs in browser ...")

urls = [
    ("Migration Portal",           "http://localhost:5001"),
    ("TomcatEE Order App",         "http://localhost:8888/order-mgmt/"),
    ("Solace Admin Console",       "http://localhost:8080"),
]

for label, url in urls:
    try:
        webbrowser.open(url)
        ok(f"{label:30s} → {url}")
        time.sleep(0.8)
    except Exception:
        ok(f"{label:30s} → {url}  (open manually)")

# ── Done ──────────────────────────────────────────────────────────────
print()
print(SEP)
print("  DEMO READY")
print()
print("  Broker state    : IBM MQ (baseline)")
print("  TomcatEE app    : http://localhost:8888/order-mgmt/")
print("  Migration portal: http://localhost:5001")
print("  Tomcat Manager  : http://localhost:8888/manager/html")
print("                    login: admin / admin123")
print()
print("  When demo is done, run again to reset for next session.")
print(SEP)
print()

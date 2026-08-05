"""
setup.py  —  Wait for containers to be healthy, then provision IBM MQ + Solace resources.
Run this once after 'docker-compose up -d'

MQ Queue naming (IKEA demo):   [EBCNAME].[TYPE]
  e.g.  EBCIRSE01.REQUEST   EBCIRSE01.BACKOUT   EBCIRDE01.REQUEST
Queue Manager naming:  IT[REGION][LEG][SEQ]  e.g. ITGLA01 (Production Global A)

Solace queue naming:
  Demo (local Docker):  EBCIRSE01.REQUEST        — dot notation, used by bridge_to_solace.py
  PROD (ITF standard):  ITF_EBCIRSE01_REQUEST    — provisioned by ITF Jenkins pipeline
  VPN:  'default' (local)  →  'pro-gke-euwe4-cgeu-1' (PROD)
"""
import sys, time, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MQ_BASE   = "https://localhost:9443/ibmmq/rest/v2"
MQ_ADMIN  = ("admin", "passw0rd")
MQ_QMGR   = "QM1"          # Docker QM — displayed as ITGLA01 in the portal
SEMP      = "http://localhost:8080/SEMP/v2"
SEMP_AUTH = ("admin", "admin")
VPN       = "default"

BANNER = "=" * 62

# IKEA-style queue names derived from EBC instance naming convention
# Pattern: [EBCNAME].[TYPE]   QM: ITGLA01 (Docker: QM1)
IKEA_MQ_QUEUES = [
    ("EBCIRSE01.REQUEST",  "IKEA EBC Liberty SE-01 — Request Queue"),
    ("EBCIRSE01.BACKOUT",  "IKEA EBC Liberty SE-01 — Backout Queue"),
    ("EBCIRDE01.REQUEST",  "IKEA EBC Liberty DE-01 — Request Queue"),
    ("EBCIRUS01.REQUEST",  "IKEA EBC Liberty US-01 — Request Queue"),
]

IKEA_SOL_QUEUES = [
    # Demo-convention names (dot notation) — functional in local Docker Solace
    ("EBCIRSE01.REQUEST",  "IKEA EventMesh SE-01 — migrated from IBM MQ"),
    ("EBCIRSE01.BACKOUT",  "IKEA EventMesh SE-01 — backout / DLQ"),
    ("EBCIRDE01.REQUEST",  "IKEA EventMesh DE-01 — migrated from IBM MQ"),
    ("EBCIRUS01.REQUEST",  "IKEA EventMesh US-01 — migrated from IBM MQ"),
    # PROD-convention names (ITF standard: ITF_[EBCNAME]_[TYPE]) — provisioned by ITF Jenkins in PROD
    # Created here so demo Solace has both conventions available
    ("ITF_EBCIRSE01_REQUEST", "ITF EventMesh PROD convention — SE-01 Request"),
    ("ITF_EBCIRSE01_BACKOUT", "ITF EventMesh PROD convention — SE-01 Backout/DLQ"),
    ("ITF_EBCIRDE01_REQUEST", "ITF EventMesh PROD convention — DE-01 Request"),
    ("ITF_EBCIRUS01_REQUEST", "ITF EventMesh PROD convention — US-01 Request"),
]

def wait_for_service(name, url, auth, verify=True, timeout=180):
    print(f"\n  Waiting for {name} ...", flush=True)
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url, auth=auth, verify=verify, timeout=6)
            if r.status_code in (200, 400):
                elapsed = int(time.time() - start)
                print(f"  [OK]  {name} ready  ({elapsed}s)", flush=True)
                return True
        except Exception:
            pass
        elapsed = int(time.time() - start)
        print(f"        still waiting ... {elapsed}s / {timeout}s", flush=True)
        time.sleep(8)
    print(f"  [FAIL] {name} did not become ready in {timeout}s", flush=True)
    return False

def mq_create_queue(qmgr, queue_name, descr="IKEA EBC Migration Queue"):
    """Create a local MQ queue via MQSC REST API."""
    try:
        r = requests.post(
            f"{MQ_BASE}/admin/action/qmgr/{qmgr}/mqsc",
            auth=MQ_ADMIN,
            headers={"Content-Type": "application/json",
                     "ibm-mq-rest-csrf-token": "portal"},
            json={
                "type": "runCommandJSON",
                "command": "define",
                "qualifier": "qlocal",
                "name": queue_name,
                "parameters": {"replace": "yes"}
            },
            verify=False,
            timeout=8
        )
        resp = r.json()
        cmds = resp.get("commandResponse", [])
        if cmds and cmds[0].get("completionCode") == 0:
            print(f"  [OK]  MQ queue created: {queue_name}", flush=True)
        elif cmds and "already exists" in str(cmds).lower():
            print(f"  [OK]  MQ queue exists:  {queue_name}", flush=True)
        else:
            # Fallback: try via REST PUT
            r2 = requests.put(
                f"{MQ_BASE}/admin/qmgr/{qmgr}/queue/{queue_name}",
                auth=MQ_ADMIN,
                headers={"Content-Type": "application/json",
                         "ibm-mq-rest-csrf-token": "portal"},
                json={"type": "local"},
                verify=False, timeout=8
            )
            if r2.status_code in (200, 201):
                print(f"  [OK]  MQ queue created: {queue_name}", flush=True)
            elif r2.status_code == 409:
                print(f"  [OK]  MQ queue exists:  {queue_name}", flush=True)
            else:
                print(f"  [WARN] MQ queue {queue_name}: {r.status_code} / {r2.status_code}", flush=True)
    except Exception as e:
        print(f"  [WARN] MQ queue {queue_name}: {e}", flush=True)

def semp_post(path, body, name):
    r = requests.post(f"{SEMP}/config{path}", auth=SEMP_AUTH, json=body)
    if r.status_code in (200, 201):
        print(f"  [OK]  {name}", flush=True)
    elif r.status_code == 400 and "already exists" in r.text.lower():
        print(f"  [OK]  {name} (already exists)", flush=True)
    else:
        print(f"  [WARN] {name}: HTTP {r.status_code}  {r.text[:120]}", flush=True)

if __name__ == "__main__":
    print(f"\n{BANNER}")
    print("  IBM MQ  ->  Solace PubSub+  |  CHARM Extension Portal — DEMO SETUP")
    print("  Queue naming: IKEA/SAMLA convention  ([EBCNAME].[TYPE])")
    print(BANNER)

    # 1 — wait for both services
    ok_mq = wait_for_service(
        "IBM MQ (REST API)  [ITGLA01 / QM1]",
        f"{MQ_BASE}/admin/qmgr",
        MQ_ADMIN,
        verify=False
    )
    ok_sol = wait_for_service(
        "Solace PubSub+ (SEMP)",
        f"{SEMP}/config/",
        SEMP_AUTH,
        timeout=300
    )

    if not (ok_mq and ok_sol):
        print("\n  [FAIL] One or more services not ready. Is docker-compose running?")
        sys.exit(1)

    # 2 — provision IBM MQ queues (IKEA naming convention)
    print(f"\n  Provisioning IBM MQ queues on {MQ_QMGR} (ITGLA01) ...")
    for q_name, q_desc in IKEA_MQ_QUEUES:
        mq_create_queue(MQ_QMGR, q_name, q_desc)

    # 2b — grant 'app' user PUT/GET rights on all IKEA queues (DEV.* already pre-granted)
    print(f"\n  Granting 'app' user access to IKEA queues ...")
    for q_name, _ in IKEA_MQ_QUEUES:
        try:
            r = requests.post(
                f"{MQ_BASE}/admin/action/qmgr/{MQ_QMGR}/mqsc",
                auth=MQ_ADMIN,
                headers={"Content-Type": "application/json", "ibm-mq-rest-csrf-token": "portal"},
                json={
                    "type": "runCommandJSON",
                    "command": "set",
                    "qualifier": "authrec",
                    "name": q_name,
                    "parameters": {
                        "objtype": "queue",
                        "principal": "app",
                        "authadd": ["put", "get", "browse", "inq"]
                    }
                },
                verify=False, timeout=8
            )
            print(f"  [OK]  app user granted PUT/GET on {q_name}", flush=True)
        except Exception as e:
            print(f"  [WARN] Auth grant {q_name}: {e}", flush=True)

    # 3 — provision Solace queues (IKEA naming convention)
    print(f"\n  Provisioning Solace PubSub+ queues in VPN '{VPN}' ...")
    semp_post(
        f"/msgVpns/{VPN}/clientUsernames",
        {"clientUsername": "demo-client", "password": "demo-pass", "enabled": True},
        "Solace client user 'demo-client'"
    )
    for q_name, q_desc in IKEA_SOL_QUEUES:
        semp_post(
            f"/msgVpns/{VPN}/queues",
            {
                "queueName": q_name,
                "accessType": "non-exclusive",
                "permission": "consume",
                "ingressEnabled": True,
                "egressEnabled": True,
            },
            f"Solace queue '{q_name}'"
        )

    print(f"\n{BANNER}")
    print("  SETUP COMPLETE")
    print(f"  IBM MQ Console  :  https://localhost:9443/ibmmq/console")
    print(f"    QM: ITGLA01 (Docker: QM1)   user: admin   pass: passw0rd")
    print(f"  Solace Console  :  http://localhost:8080")
    print(f"    user: admin   pass: admin")
    print(f"\n  MQ Queues created  : {', '.join(q for q,_ in IKEA_MQ_QUEUES)}")
    print(f"  Solace Queues      : {', '.join(q for q,_ in IKEA_SOL_QUEUES)}")
    print(f"\n  Next: run  python produce_to_mq.py")
    print(BANNER)

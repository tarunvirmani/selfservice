"""
setup.py  —  Wait for containers to be healthy, then provision Solace resources.
Run this once after 'docker-compose up -d'
"""
import sys, time, requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MQ_BASE   = "https://localhost:9443/ibmmq/rest/v2"
MQ_ADMIN  = ("admin", "passw0rd")
SEMP      = "http://localhost:8080/SEMP/v2"
SEMP_AUTH = ("admin", "admin")
VPN       = "default"

BANNER = "=" * 62

def wait_for_service(name, url, auth, verify=True, timeout=180):
    print(f"\n  Waiting for {name} ...", flush=True)
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url, auth=auth, verify=verify, timeout=6)
            if r.status_code in (200, 400):   # 400 = alive but auth issue, still up
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

def semp_post(path, body, name):
    r = requests.post(f"{SEMP}/config{path}", auth=SEMP_AUTH, json=body)
    if r.status_code in (200, 201):
        print(f"  [OK]  {name} created", flush=True)
    elif r.status_code == 400 and "already exists" in r.text.lower():
        print(f"  [OK]  {name} already exists", flush=True)
    else:
        print(f"  [WARN] {name}: HTTP {r.status_code}  {r.text[:120]}", flush=True)

if __name__ == "__main__":
    print(f"\n{BANNER}")
    print("  IBM MQ  ->  Solace PubSub+  |  DEMO SETUP")
    print(BANNER)

    # 1 — wait for both services
    ok_mq = wait_for_service(
        "IBM MQ (REST API)",
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

    # 2 — provision Solace resources
    print("\n  Provisioning Solace resources ...")

    semp_post(
        f"/msgVpns/{VPN}/clientUsernames",
        {"clientUsername": "demo-client", "password": "demo-pass", "enabled": True},
        "Solace client user 'demo-client'"
    )

    semp_post(
        f"/msgVpns/{VPN}/queues",
        {
            "queueName": "LEGACY.APP.QUEUE",
            "accessType": "non-exclusive",
            "permission": "consume",
            "ingressEnabled": True,
            "egressEnabled": True,
        },
        "Solace queue 'LEGACY.APP.QUEUE'"
    )

    semp_post(
        f"/msgVpns/{VPN}/queues",
        {
            "queueName": "MIGRATED.APP.QUEUE",
            "accessType": "non-exclusive",
            "permission": "consume",
            "ingressEnabled": True,
            "egressEnabled": True,
        },
        "Solace queue 'MIGRATED.APP.QUEUE'"
    )

    print(f"\n{BANNER}")
    print("  SETUP COMPLETE")
    print(f"  IBM MQ Console  :  https://localhost:9443/ibmmq/console")
    print(f"    user: admin   pass: passw0rd")
    print(f"  Solace Console  :  http://localhost:8080")
    print(f"    user: admin   pass: admin")
    print(f"\n  Next: run  python produce_to_mq.py")
    print(BANNER)

==============================================================
  IBM MQ → Solace PubSub+ Migration Demo
  Self-Service Rolling Migration  |  Docker Desktop Edition
==============================================================

WHAT THIS DEMO DOES
───────────────────
Spins up a real IBM MQ Queue Manager and a real Solace PubSub+
Event Mesh broker side-by-side in Docker. Then walks through
the three migration phases described in the RFP:

  Phase 1  →  Legacy app produces to IBM MQ
  Phase 2  →  Bridge active: MQ drains into Solace (coexistence)
  Phase 3  →  Feature flag flipped: new events go direct-to-Solace
  Phase 4  →  Verify: IBM MQ empty, all messages in Solace

PREREQUISITES
─────────────
  ✓ Docker Desktop — running and signed in
  ✓ Python 3.8+   — on PATH  (check: python --version)
  ✓ pip           — bundled with Python

That's it. No IBM MQ client libraries needed (uses REST API).
No Solace SDK needed (uses REST + SEMP API).

QUICK START
───────────
  1. Open PowerShell or CMD in this folder
  2. Run:  run_demo.bat

     The batch file does everything:
       - pip install requirements
       - docker-compose up -d
       - Waits for containers to be healthy  (~2–3 min first run)
       - Opens both admin consoles in your browser
       - Steps through all 4 phases (press ENTER between each)

MANUAL STEPS (if you prefer)
──────────────────────────────
  pip install -r requirements.txt

  docker-compose up -d                 # start both brokers
  python setup.py                      # wait + provision Solace

  python produce_to_mq.py              # Phase 1: fill IBM MQ
  python bridge_to_solace.py           # Phase 2+3: bridge + flag flip
  python verify_solace.py              # Phase 4: verify Solace

ADMIN CONSOLES
──────────────
  IBM MQ  │ https://localhost:9443/ibmmq/console
           │ user: admin   password: passw0rd
           │ (accept self-signed cert warning)

  Solace  │ http://localhost:8080
           │ user: admin   password: admin

KEY QUEUES
──────────
  IBM MQ  : QM1 / DEV.QUEUE.1          (legacy — fills then empties)
  Solace  : default VPN / MIGRATED.APP.QUEUE   (target — fills up)

WHAT TO WATCH IN THE CONSOLES
──────────────────────────────
  1. IBM MQ console → Queues → DEV.QUEUE.1
     Queue depth goes from 0 → 10 (after produce) → 0 (after bridge)

  2. Solace console → Queues → MIGRATED.APP.QUEUE
     Queue depth goes from 0 → 10 (bridged) → 13 (+ 3 direct)

  3. In both consoles you can browse message bodies as JSON.

CLEANUP
───────
  docker-compose down -v    # stops containers AND removes volumes
                            # next 'up' starts fresh

FILES IN THIS FOLDER
────────────────────
  docker-compose.yml      Docker topology (MQ + Solace)
  requirements.txt        Python packages (just 'requests')
  setup.py                Health-wait + Solace queue provisioning
  produce_to_mq.py        Phase 1: 10 business events → IBM MQ
  bridge_to_solace.py     Phase 2+3: bridge + feature-flag demo
  verify_solace.py        Phase 4: SEMP queue stats + message browse
  run_demo.bat            One-click guided walkthrough
  README.txt              This file

TROUBLESHOOTING
───────────────
  "docker-compose: command not found"
  → Modern Docker Desktop uses 'docker compose' (space, not hyphen)
  → Edit run_demo.bat: replace 'docker-compose' with 'docker compose'

  "Connection refused on port 9443"
  → IBM MQ needs ~60–90s to initialise on first start. Just wait.
  → setup.py will wait up to 3 minutes automatically.

  "Solace queue not found" in bridge script
  → Rerun setup.py — it creates the queues.

  "HTTP 403 from IBM MQ REST"
  → Missing or wrong 'ibm-mq-rest-csrf-token' header. Already fixed
    in scripts — check you're running the version from this folder.

  "docker-compose down -v" failed
  → Close any IBM MQ console browser tabs first, then retry.

==============================================================

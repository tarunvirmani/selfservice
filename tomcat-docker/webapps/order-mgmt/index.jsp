<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%@ page import="javax.naming.*, javax.jms.*, java.util.*, javax.xml.parsers.*, org.w3c.dom.*, java.io.*" %>
<%--
  index.jsp — IKEA Order Management System
  Real Jakarta EE page running inside Apache Tomcat 9.

  Two data sources used here:
  1. XML parse of /usr/local/tomcat/conf/context.xml  → always works, shows live config
  2. JNDI lookup via InitialContext                    → works for IBM MQ (concrete class)
                                                         not for Solace (SolConnectionFactory
                                                         is a Java interface, not instantiable
                                                         by BeanFactory)
--%>
<%
  // ── 1. Parse context.xml directly ────────────────────────────────────────
  // This always gives us the current config regardless of JNDI state.
  String xmlBroker     = "ibmmq";   // detected from factory class in XML
  String xmlFactory    = "";
  String xmlHost       = "";
  String xmlPort       = "";
  String xmlQmgr       = "";
  String xmlChannel    = "";
  String xmlQueue      = "";
  String xmlVpn        = "";
  String xmlRawContent = "";

  try {
    File ctxFile = new File("/usr/local/tomcat/conf/context.xml");
    // Read raw content for display
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(ctxFile), "UTF-8"));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = br.readLine()) != null) sb.append(line).append("\n");
    br.close();
    xmlRawContent = sb.toString()
        .replace("&","&amp;").replace("<","&lt;").replace(">","&gt;");

    // Parse XML
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    Document doc = dbf.newDocumentBuilder().parse(ctxFile);
    NodeList resources = doc.getElementsByTagName("Resource");

    for (int i = 0; i < resources.getLength(); i++) {
      Element res = (Element) resources.item(i);
      String name = res.getAttribute("name");

      if (name.contains("Factory")) {
        // This is the ConnectionFactory resource
        xmlFactory  = res.getAttribute("type");
        // hostName (IBM MQ) or host (Solace)
        xmlHost     = res.getAttribute("hostName");
        if (xmlHost.isEmpty()) xmlHost = res.getAttribute("host");
        xmlPort     = res.getAttribute("port");
        xmlQmgr     = res.getAttribute("queueManager");
        xmlChannel  = res.getAttribute("channel");
        xmlVpn      = res.getAttribute("vpn");

        String fLower = xmlFactory.toLowerCase();
        if (fLower.contains("solace") || fLower.contains("sol")) {
          xmlBroker = "solace";
        }
      } else {
        // This is the Queue resource
        xmlQueue = res.getAttribute("baseQueueName");
        if (xmlQueue.isEmpty()) xmlQueue = res.getAttribute("QUEUE_NAME");
      }
    }
  } catch (Exception xe) {
    xmlRawContent = "Error reading context.xml: " + xe.getMessage();
  }

  // ── 2. JNDI Lookup ───────────────────────────────────────────────────────
  // IBM MQ: MQQueueConnectionFactory is a concrete JavaBean → BeanFactory works
  // Solace: SolConnectionFactory is a Java INTERFACE → BeanFactory cannot
  //         call new SolConnectionFactory() — it will throw NamingException.
  //         In a real app you'd use SolJmsHelper.createConnectionFactory() in code.
  //         For this demo we skip the lookup for Solace and read context.xml instead.
  String  lookupStatus  = "";
  String  lookupError   = "";
  String  factoryClass  = "";
  boolean lookupOk      = false;
  boolean lookupSkipped = false;

  if ("solace".equals(xmlBroker)) {
    lookupSkipped = true;
    lookupStatus  = "SKIPPED";
    lookupError   = "SolConnectionFactory is a Java interface — BeanFactory cannot instantiate it. "
                  + "Real apps use: SolJmsHelper.createConnectionFactory() in Java code.";
  } else {
    try {
      Context ctx = new InitialContext();
      Object factoryObj = ctx.lookup("java:comp/env/jms/OrderQueueFactory");
      factoryClass  = factoryObj.getClass().getName();
      lookupOk      = true;
      lookupStatus  = "SUCCESS";
      ctx.close();
    } catch (NameNotFoundException nnfe) {
      lookupStatus = "FAILED";
      lookupError  = "JNDI name not found: " + nnfe.getMessage();
    } catch (NamingException ne) {
      lookupStatus = "FAILED";
      lookupError  = ne.getMessage();
    } catch (Exception e) {
      lookupStatus = "FAILED";
      lookupError  = e.getClass().getSimpleName() + ": " + e.getMessage();
    }
  }

  String timestamp  = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
  String appVersion = application.getServerInfo();

  // Short class name for display
  String xmlFactoryShort = xmlFactory.contains(".")
      ? xmlFactory.substring(xmlFactory.lastIndexOf('.') + 1)
      : xmlFactory;
%>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>IKEA Order Management System — TomcatEE</title>
<style>
:root {
  --tomcat-orange: #F16529;
  --mq-blue:       #1D6FCC;
  --solace-green:  #10B981;
  --bg:            #0F172A;
  --card:          #1E293B;
  --border:        rgba(255,255,255,0.07);
  --text:          #E2E8F0;
  --muted:         #94A3B8;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); font-family: 'Segoe UI', system-ui, sans-serif;
       color: var(--text); min-height: 100vh; }

.navbar {
  background: #0A0F1E; border-bottom: 1px solid rgba(255,255,255,0.08);
  padding: .45rem 1.5rem; display: flex; align-items: center; gap: 1rem;
  font-size: .75rem;
}
.navbar a, .navbar button {
  text-decoration: none; cursor: pointer; font-size: .75rem; font-weight: 700;
}
.btn-portal {
  background: linear-gradient(90deg,#6366F1,#8B5CF6); color: #fff !important;
  padding: .3rem .9rem; border-radius: 6px; letter-spacing: .03em;
}
.btn-outline {
  color: #94A3B8; padding: .3rem .6rem; border-radius: 6px;
  border: 1px solid #334155; background: transparent;
}
.btn-green {
  color: #34D399; border: 1px solid #10B981; padding: .3rem .8rem;
  border-radius: 6px; background: transparent; letter-spacing: .02em;
}

.topbar {
  background: linear-gradient(90deg,#1A1A2E,#16213E);
  border-bottom: 3px solid var(--tomcat-orange);
  padding: .6rem 1.5rem;
  display: flex; align-items: center; justify-content: space-between;
}
.tb-left { display: flex; align-items: center; gap: .75rem; }
.tomcat-badge {
  background: var(--tomcat-orange); color: #fff;
  font-size: .65rem; font-weight: 800; padding: .2rem .5rem;
  border-radius: 4px; letter-spacing: .06em;
}
.tb-title { font-size: .95rem; font-weight: 700; }
.tb-sub   { color: var(--muted); font-size: .7rem; }

.broker-badge {
  display: flex; align-items: center; gap: .4rem;
  padding: .35rem .9rem; border-radius: 20px;
  font-size: .78rem; font-weight: 700;
}
.broker-mq     { background: rgba(29,111,204,.2); border: 1.5px solid var(--mq-blue);     color: #60A5FA; }
.broker-solace { background: rgba(16,185,129,.2); border: 1.5px solid var(--solace-green); color: #34D399; }
.dot { width: 8px; height: 8px; border-radius: 50%; }
.dot-mq     { background: var(--mq-blue); }
.dot-solace { background: var(--solace-green); box-shadow: 0 0 6px var(--solace-green); }

.main { max-width: 960px; margin: 0 auto; padding: 1rem 1.5rem; }

.card {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 12px; padding: .9rem 1.1rem; margin-bottom: .9rem;
}
.card-title {
  font-size: .68rem; font-weight: 700; letter-spacing: .1em;
  text-transform: uppercase; color: var(--muted); margin-bottom: .75rem;
  display: flex; align-items: center; gap: .5rem;
}

/* ── Order Dashboard ── */
.ikea-logo {
  background: #FBD914; border: 2.5px solid #0058A3; border-radius: 4px;
  padding: .2rem .55rem; display: inline-flex; align-items: center;
}
.ikea-logo span { color: #0058A3; font-size: .9rem; font-weight: 900;
                  letter-spacing: .06em; font-family: Arial,sans-serif; }
.ord-table { width: 100%; border-collapse: collapse; font-size: .74rem; margin-top: .5rem; }
.ord-table th { text-align: left; padding: .35rem .5rem; color: #475569;
                font-size: .62rem; font-weight: 700; text-transform: uppercase;
                letter-spacing: .07em; border-bottom: 1px solid rgba(255,255,255,.07); }
.ord-table td { padding: .38rem .5rem; border-bottom: 1px solid rgba(255,255,255,.04);
                color: #CBD5E1; }
.ord-table tr:last-child td { border-bottom: none; }
.ord-table tr:hover td { background: rgba(255,255,255,.02); }
.ord-id   { font-family: 'Consolas',monospace; font-size: .7rem; color: #60A5FA; font-weight: 600; }
.ord-prod { font-weight: 600; color: #E2E8F0; }
.badge-mq  { background: rgba(29,111,204,.2); border: 1px solid rgba(29,111,204,.4);
             color: #60A5FA; border-radius: 10px; padding: .1rem .5rem;
             font-size: .65rem; font-weight: 700; white-space: nowrap; }
.badge-sol { background: rgba(16,185,129,.15); border: 1px solid rgba(16,185,129,.4);
             color: #34D399; border-radius: 10px; padding: .1rem .5rem;
             font-size: .65rem; font-weight: 700; white-space: nowrap; }
.badge-proc { background: rgba(251,191,36,.1); border: 1px solid rgba(251,191,36,.3);
              color: #FCD34D; border-radius: 10px; padding: .1rem .5rem;
              font-size: .65rem; font-weight: 700; white-space: nowrap; }
.card-title span { color: var(--tomcat-orange); font-size: .85rem; }

.banner {
  border-radius: 8px; padding: .75rem 1rem;
  display: flex; align-items: flex-start; gap: .75rem;
  margin-bottom: 1.25rem; font-size: .85rem;
}
.banner-ok   { background: rgba(16,185,129,.1);  border: 1px solid rgba(16,185,129,.3);  color: #34D399; }
.banner-err  { background: rgba(239,68,68,.1);   border: 1px solid rgba(239,68,68,.3);   color: #FCA5A5; }
.banner-warn { background: rgba(251,191,36,.08); border: 1px solid rgba(251,191,36,.3);  color: #FCD34D; }

.cfg-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
.cfg-panel { border-radius: 8px; padding: 1rem 1.25rem; }
.cfg-panel-mq     { background: rgba(29,111,204,.08); border: 1.5px solid rgba(29,111,204,.3); }
.cfg-panel-solace { background: rgba(16,185,129,.08); border: 1.5px solid rgba(16,185,129,.3); }
.cfg-label { font-size: .65rem; font-weight: 700; text-transform: uppercase;
             letter-spacing: .1em; margin-bottom: .75rem; display:flex; align-items:center; gap:.5rem; }
.cfg-label-mq     { color: #60A5FA; }
.cfg-label-solace { color: #34D399; }
.cfg-row { display: flex; justify-content: space-between; align-items: baseline;
           padding: .3rem 0; border-bottom: 1px solid var(--border); font-size: .82rem; }
.cfg-row:last-child { border-bottom: none; }
.cfg-key { color: var(--muted); }
.cfg-val { font-weight: 600; font-family: 'Consolas', monospace; font-size: .78rem;
           text-align: right; max-width: 60%; word-break: break-all; }
.active-tag { display:inline-block; padding:.1rem .5rem; border-radius:10px;
              font-size:.65rem; font-weight:700; }
.active-tag-mq     { background:rgba(29,111,204,.2);  color:#60A5FA;  border:1px solid #1D6FCC; }
.active-tag-solace { background:rgba(16,185,129,.15); color:#34D399;  border:1px solid #10B981; }

.code-box {
  background: #0F172A; border: 1px solid var(--border); border-radius: 8px;
  padding: .75rem 1rem; font-family: 'Consolas', monospace; font-size: .73rem;
  line-height: 1.75; overflow-x: auto;
}
.c  { color: #475569; font-style: italic; }
.k  { color: #60A5FA; }
.v  { color: #34D399; }
.vw { color: #FBBF24; }
.ve { color: #F87171; }

.footer { text-align: center; color: var(--muted); font-size: .7rem; margin-top: 1.5rem; }

.tb-brand {
  text-align: right; line-height: 1.3;
  padding-left: 1.25rem; border-left: 1px solid rgba(255,255,255,0.1);
  margin-left: 1.25rem;
}
.tb-brand-name {
  font-size: .82rem; font-weight: 800; letter-spacing: .02em;
  color: #F5C542;
  text-shadow: 0 0 10px rgba(245,197,66,.35);
}
.tb-brand-sub {
  font-size: .65rem; color: #CBD5E1; letter-spacing: .03em;
}
.tb-right { display: flex; align-items: center; }
</style>
</head>
<body>

<!-- ── Nav bar ── -->
<div class="navbar">
  <span style="color:#64748B;font-weight:600;letter-spacing:.05em;">DEMO NAVIGATION</span>
  <a href="http://localhost:5001" target="_blank" class="btn-portal">&#8594; Migration Portal</a>
  <span style="color:#334155">|</span>
  <a href="http://localhost:8888/manager/html" target="_blank" class="btn-outline">Tomcat Manager</a>
  <span style="color:#334155">|</span>
  <button class="btn-green" onclick="reloadApp()">&#8635; Reload Config</button>
  <span id="reload-msg" style="color:#64748B;font-size:.7rem;"></span>
  <span style="margin-left:auto;color:#334155;font-size:.65rem;">Auto-refresh every 5s</span>
</div>

<!-- ── Top bar ── -->
<div class="topbar">
  <div class="tb-left">
    <span class="tomcat-badge">TomcatEE</span>
    <div>
      <div class="tb-title">IKEA Order Management System</div>
      <div class="tb-sub"><%= appVersion %> &nbsp;/&nbsp; Jakarta EE 8 &nbsp;/&nbsp; JMS 2.0</div>
    </div>
  </div>
  <div class="tb-right">
    <% if ("ibmmq".equals(xmlBroker)) { %>
      <div class="broker-badge broker-mq"><div class="dot dot-mq"></div> IBM MQ</div>
    <% } else { %>
      <div class="broker-badge broker-solace"><div class="dot dot-solace"></div> Solace PubSub+</div>
    <% } %>
    <div class="tb-brand">
      <div class="tb-brand-name">&#169; 2026 Tarun Virmani</div>
      <div class="tb-brand-sub">HCLTech Middleware Solutioning</div>
    </div>
  </div>
</div>

<div class="main">

  <!-- ── Order Dashboard ── -->
  <div class="card">
    <!-- Header -->
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:.85rem;">
      <div style="display:flex;align-items:center;gap:.75rem;">
        <div class="ikea-logo"><span>IKEA</span></div>
        <div>
          <div style="font-size:.88rem;font-weight:700;color:#E2E8F0;">Order Fulfilment Dashboard</div>
          <div style="font-size:.65rem;color:#475569;margin-top:.1rem;">
            Pending orders dispatched via
            <% if ("ibmmq".equals(xmlBroker)) { %>
              <strong style="color:#60A5FA;">IBM MQ</strong>
            <% } else { %>
              <strong style="color:#34D399;">Solace PubSub+</strong>
            <% } %>
            &rarr; Fulfilment Engine
          </div>
        </div>
      </div>
      <div style="display:flex;align-items:center;gap:.5rem;">
        <span style="background:rgba(239,68,68,.15);border:1px solid rgba(239,68,68,.3);
                     border-radius:20px;padding:.15rem .65rem;font-size:.65rem;
                     font-weight:700;color:#F87171;">&#9679; LIVE</span>
        <span style="background:rgba(251,191,36,.1);border:1px solid rgba(251,191,36,.3);
                     border-radius:20px;padding:.15rem .65rem;font-size:.65rem;
                     font-weight:700;color:#FCD34D;">6 Pending Dispatch</span>
      </div>
    </div>

    <!-- Orders table -->
    <table class="ord-table">
      <thead>
        <tr>
          <th>Order ID</th>
          <th>Product</th>
          <th>Destination</th>
          <th style="text-align:center;">Qty</th>
          <th>Value</th>
          <th>Queue Status</th>
        </tr>
      </thead>
      <tbody>
        <% String[][] orders = {
          {"ORD-2026-78341","BILLY Bookcase 2x1 (White)","Stockholm, SE","2","€189","proc"},
          {"ORD-2026-78342","KALLAX Shelf Unit (4x4)","London, UK","1","€249","queue"},
          {"ORD-2026-78343","POÄNG Armchair + Footstool","Dubai, UAE","3","€594","queue"},
          {"ORD-2026-78344","LACK Side Table (White)","Mumbai, IN","4","€116","queue"},
          {"ORD-2026-78345","HEMNES Wardrobe (3-door)","Paris, FR","1","€429","proc"},
          {"ORD-2026-78346","SÖDERHAMN 3-seat Sofa","New York, US","2","€998","queue"},
        };
        for (String[] o : orders) {
          String statusLabel = "ibmmq".equals(xmlBroker) ? "In MQ Queue" : "Via Solace";
          String badgeClass  = o[5].equals("proc") ? "badge-proc" :
                               ("ibmmq".equals(xmlBroker) ? "badge-mq" : "badge-sol");
          String statusText  = o[5].equals("proc") ? "Processing" : statusLabel;
        %>
        <tr>
          <td class="ord-id"><%= o[0] %></td>
          <td class="ord-prod"><%= o[1] %></td>
          <td><%= o[2] %></td>
          <td style="text-align:center;font-weight:600;"><%= o[3] %></td>
          <td style="font-family:'Consolas',monospace;color:#94A3B8;"><%= o[4] %></td>
          <td><span class="<%= badgeClass %>"><%= statusText %></span></td>
        </tr>
        <% } %>
      </tbody>
    </table>

    <!-- Footer note -->
    <div style="margin-top:.75rem;padding:.5rem .75rem;background:rgba(255,255,255,.03);
                border-radius:6px;font-size:.67rem;color:#475569;display:flex;
                align-items:center;gap:.5rem;">
      <span>&#9432;</span>
      <span>
        Orders are placed into
        <% if ("ibmmq".equals(xmlBroker)) { %>
          <strong style="color:#60A5FA;">DEV.QUEUE.1</strong> on IBM MQ (QM1)
          and consumed by the downstream fulfilment microservice.
        <% } else { %>
          <strong style="color:#34D399;"><%= xmlQueue.isEmpty() ? "IKEA.ORDER.QUEUE" : xmlQueue %></strong>
          on Solace PubSub+ (VPN: <%= xmlVpn %>)
          following successful broker migration.
        <% } %>
        &nbsp;|&nbsp; Last synced: <%= timestamp %>
      </span>
    </div>
  </div>

  <!-- ── Status banner ── -->
  <% if (lookupOk) { %>
  <div class="banner banner-ok">
    <span style="font-size:1.1rem">&#10003;</span>
    <div>
      <strong>IBM MQ Active &mdash; JNDI Lookup Successful</strong> &nbsp;|&nbsp;
      <code>java:comp/env/jms/OrderQueueFactory</code> &rarr;
      <code><%= factoryClass.substring(factoryClass.lastIndexOf('.')+1) %></code>
      &nbsp;|&nbsp; Host: <strong><%= xmlHost %></strong> &nbsp; Queue: <strong><%= xmlQueue %></strong>
    </div>
  </div>
  <% } else if (lookupSkipped) { %>
  <div class="banner banner-ok">
    <span style="font-size:1.1rem">&#10003;</span>
    <div>
      <strong>Solace PubSub+ Active &mdash; Configuration Applied</strong> &nbsp;|&nbsp;
      <code>context.xml</code> updated &rarr; <code>SolConnectionFactory</code>
      &nbsp;|&nbsp; Host: <strong><%= xmlHost %></strong>
      &nbsp; VPN: <strong><%= xmlVpn %></strong>
      &nbsp; Queue: <strong><%= xmlQueue %></strong>
    </div>
  </div>
  <% } else { %>
  <div class="banner banner-err">
    <span style="font-size:1.1rem">&#x26A0;</span>
    <div><strong>JNDI Lookup Failed</strong> &mdash; <%= lookupError %></div>
  </div>
  <% } %>

  <!-- ── Config panels (live from context.xml) ── -->
  <div class="card">
    <div class="card-title">
      <span>&#8644;</span> JMS CONNECTION CONFIGURATION &mdash; LIVE FROM TOMCAT-CONTEXT.XML
    </div>
    <div class="cfg-grid">

      <!-- IBM MQ panel -->
      <div class="cfg-panel cfg-panel-mq">
        <div class="cfg-label cfg-label-mq">
          &#9632; IBM MQ
          <% if ("ibmmq".equals(xmlBroker)) { %>
            <span class="active-tag active-tag-mq">ACTIVE</span>
          <% } %>
        </div>
        <div class="cfg-row"><span class="cfg-key">Factory</span>
          <span class="cfg-val" style="color:<%= "ibmmq".equals(xmlBroker) ? "#34D399" : "#475569" %>">
            MQQueueConnectionFactory</span></div>
        <div class="cfg-row"><span class="cfg-key">Host</span>
          <span class="cfg-val"><%= "ibmmq".equals(xmlBroker) ? xmlHost : "ibmmq" %></span></div>
        <div class="cfg-row"><span class="cfg-key">Port</span>
          <span class="cfg-val"><%= "ibmmq".equals(xmlBroker) ? xmlPort : "1414" %></span></div>
        <div class="cfg-row"><span class="cfg-key">Queue Manager</span>
          <span class="cfg-val">QM1</span></div>
        <div class="cfg-row"><span class="cfg-key">Channel</span>
          <span class="cfg-val">DEV.APP.SVRCONN</span></div>
        <div class="cfg-row"><span class="cfg-key">Queue</span>
          <span class="cfg-val"><%= "ibmmq".equals(xmlBroker) ? xmlQueue : "DEV.QUEUE.1" %></span></div>
        <div class="cfg-row"><span class="cfg-key">Status</span>
          <span class="cfg-val" style="color:<%= "ibmmq".equals(xmlBroker) ? "#34D399" : "#EF4444" %>">
            <%= "ibmmq".equals(xmlBroker) ? "Active" : "Decommissioned" %></span></div>
      </div>

      <!-- Solace panel -->
      <div class="cfg-panel cfg-panel-solace">
        <div class="cfg-label cfg-label-solace">
          &#9632; Solace PubSub+
          <% if ("solace".equals(xmlBroker)) { %>
            <span class="active-tag active-tag-solace">ACTIVE</span>
          <% } %>
        </div>
        <div class="cfg-row"><span class="cfg-key">Factory</span>
          <span class="cfg-val" style="color:<%= "solace".equals(xmlBroker) ? "#34D399" : "#475569" %>">
            SolConnectionFactory</span></div>
        <div class="cfg-row"><span class="cfg-key">Host</span>
          <span class="cfg-val"><%= "solace".equals(xmlBroker) ? xmlHost : "solace:55555" %></span></div>
        <div class="cfg-row"><span class="cfg-key">Protocol</span>
          <span class="cfg-val">SMF / TCP</span></div>
        <div class="cfg-row"><span class="cfg-key">VPN</span>
          <span class="cfg-val"><%= "solace".equals(xmlBroker) ? xmlVpn : "default" %></span></div>
        <div class="cfg-row"><span class="cfg-key">Queue</span>
          <span class="cfg-val"><%= "solace".equals(xmlBroker) ? xmlQueue : "MIGRATED.APP.QUEUE" %></span></div>
        <div class="cfg-row"><span class="cfg-key">Status</span>
          <span class="cfg-val" style="color:<%= "solace".equals(xmlBroker) ? "#34D399" : "#475569" %>">
            <%= "solace".equals(xmlBroker) ? "Active &mdash; Production" : "Standby" %></span></div>
      </div>

    </div>
  </div>

  <!-- ── Live JNDI detail ── -->
  <div class="card">
    <div class="card-title"><span>&#9654;</span> LIVE JNDI LOOKUP RESULT &mdash; JAVA:COMP/ENV</div>
    <div class="code-box">
      <span class="c">// <%= timestamp %> — auto-refresh every 5s</span><br>
      <span class="c">// Context ctx = new InitialContext();</span><br>
      <span class="c">// Object obj = ctx.lookup("java:comp/env/jms/OrderQueueFactory");</span><br>
      &nbsp;<br>
      <span class="k">Broker detected from context.xml </span>: <span class="v"><%= xmlBroker.equals("solace") ? "Solace PubSub+" : "IBM MQ" %></span><br>
      <span class="k">Factory type in XML             </span>: <span class="v"><%= xmlFactoryShort.isEmpty() ? "(parse error)" : xmlFactoryShort %></span><br>
      <span class="k">JNDI lookup status              </span>:
        <span class="<%= lookupOk ? "v" : (lookupSkipped ? "vw" : "ve") %>"><%= lookupStatus %></span><br>
      <% if (lookupOk) { %>
      <span class="k">Resolved class                  </span>: <span class="v"><%= factoryClass %></span><br>
      <% } else if (lookupSkipped) { %>
      &nbsp;<br>
      <span class="c">// Why Solace skips JNDI BeanFactory lookup:</span><br>
      <span class="c">// SolConnectionFactory is declared as: public interface SolConnectionFactory</span><br>
      <span class="c">// BeanFactory calls: new SolConnectionFactory() → FAILS (can't instantiate interface)</span><br>
      <span class="c">// Real Java code uses: SolJmsHelper.createConnectionFactory()</span><br>
      <span class="c">//   factory.setHost("smf://solace:55555");</span><br>
      <span class="c">//   factory.setVPN("default");</span><br>
      <% } else { %>
      <span class="k">Error                           </span>: <span class="ve"><%= lookupError %></span><br>
      <% } %>
      &nbsp;<br>
      <span class="c">// context.xml location: /usr/local/tomcat/conf/context.xml</span><br>
      <span class="c">// JMS jars in:          /usr/local/tomcat/lib/</span>
    </div>
  </div>

  <!-- ── Raw context.xml ── -->
  <div class="card">
    <div class="card-title"><span>&#128196;</span> LIVE CONTEXT.XML — CURRENT FILE CONTENT</div>
    <div class="code-box" style="font-size:.72rem;line-height:1.7;">
      <pre style="margin:0;white-space:pre-wrap;"><%= xmlRawContent %></pre>
    </div>
  </div>

  <div class="footer">
    Auto-refreshes every 5s &nbsp;|&nbsp;
    Tomcat Manager: <a href="/manager/html" style="color:#60A5FA;">http://localhost:8888/manager/html</a>
    &nbsp;(admin / admin123) &nbsp;|&nbsp;
    HCLTech Middleware Solutioning
  </div>
</div>

<script>
function reloadApp() {
  var btn = document.querySelector('.btn-green');
  var msg = document.getElementById('reload-msg');
  btn.disabled = true; btn.textContent = 'Reloading...'; msg.textContent = '';
  fetch('http://localhost:8888/manager/text/reload?path=/order-mgmt', {
    headers: { 'Authorization': 'Basic ' + btoa('admin:admin123') }
  })
  .then(r => r.text())
  .then(t => {
    msg.style.color = t.startsWith('OK') ? '#34D399' : '#F87171';
    msg.textContent = t.trim();
    btn.disabled = false; btn.textContent = '↻ Reload Config';
    if (t.startsWith('OK')) setTimeout(() => location.reload(), 1500);
  })
  .catch(e => {
    msg.style.color = '#F87171'; msg.textContent = 'Failed: ' + e.message;
    btn.disabled = false; btn.textContent = '↻ Reload Config';
  });
}
</script>
</body>
</html>

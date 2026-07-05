# ═══════════════════════════════════════════════════════════════════
#  Tomcat EE — IKEA Order Management System
#  Base: Apache Tomcat 9.0 / JDK 11 (Jakarta EE 8 compatible)
#
#  IMPORTANT NOTE about the official Tomcat Docker image:
#  Since Tomcat 9.0.31+, the official image ships with an EMPTY
#  webapps/ folder. The default apps (manager, host-manager, ROOT,
#  examples, docs) live in webapps.dist/ and must be explicitly
#  copied into webapps/ to activate them.
#
#  What this Dockerfile does:
#  1. Starts from the official Tomcat 9 + JDK 11 image
#  2. Copies manager + host-manager from webapps.dist/ → webapps/
#  3. Downloads real IBM MQ and Solace JMS client jars into lib/
#  4. Configures Manager for remote access + admin user
#  5. Deploys our order-mgmt webapp
# ═══════════════════════════════════════════════════════════════════

FROM tomcat:9.0-jdk11-temurin

LABEL maintainer="HCLTech Middleware Solutioning"
LABEL description="IKEA Order Management System — TomcatEE demo"

# ── 1. Install curl ────────────────────────────────────────────────
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# ── 2. Activate Manager from webapps.dist/ ─────────────────────────
#
#  The official Tomcat image ships apps in webapps.dist/ but leaves
#  webapps/ empty for security (so a bare Tomcat has no attack surface).
#  We copy only manager and host-manager — no ROOT/examples/docs.
#
RUN cp -r /usr/local/tomcat/webapps.dist/manager \
          /usr/local/tomcat/webapps/manager && \
    cp -r /usr/local/tomcat/webapps.dist/host-manager \
          /usr/local/tomcat/webapps/host-manager

# ── 3. Configure Manager for remote access ─────────────────────────
#
#  By default the Manager's context.xml has a RemoteAddrValve that
#  blocks all access except from 127.0.0.1 / ::1.
#  We replace it with an open context (demo only — restrict in prod).
#
COPY conf/manager-context.xml \
     /usr/local/tomcat/webapps/manager/META-INF/context.xml

COPY conf/manager-context.xml \
     /usr/local/tomcat/webapps/host-manager/META-INF/context.xml

# ── 4. Add admin user with manager roles ───────────────────────────
COPY conf/tomcat-users.xml \
     /usr/local/tomcat/conf/tomcat-users.xml

# ── 5. Download JMS client jars into Tomcat's shared lib/ ──────────
#
#  WHY lib/ and not webapps/app/WEB-INF/lib/ ?
#  JNDI Resources in context.xml are instantiated by the SERVER
#  classloader, not the webapp classloader. The factory class
#  (MQQueueConnectionFactory or SolJNDIInitialContextFactory) must
#  be visible at the server level → goes in $CATALINA_HOME/lib/
#
#  IBM MQ All-Client jar (JMS + transport + MQSC bindings)
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/com/ibm/mq/com.ibm.mq.allclient/9.3.5.0/com.ibm.mq.allclient-9.3.5.0.jar" \
    -o /usr/local/tomcat/lib/mq-allclient.jar

#  Solace JMS client jar
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/com/solacesystems/sol-jms/10.22.0/sol-jms-10.22.0.jar" \
    -o /usr/local/tomcat/lib/sol-jms.jar

#  Jakarta JMS 2.0 API (interfaces — required by Solace jar)
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/jakarta/jms/jakarta.jms-api/2.0.3/jakarta.jms-api-2.0.3.jar" \
    -o /usr/local/tomcat/lib/jakarta-jms-api.jar

#  org.json — transitive dependency of IBM MQ allclient.jar
#  MQQueueConnectionFactory's constructor references JSONObject internally.
#  Must be in lib/ (server classloader) alongside mq-allclient.jar.
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/json/json/20240303/json-20240303.jar" \
    -o /usr/local/tomcat/lib/org-json.jar

# ── 6. Deploy IKEA Order Management webapp ─────────────────────────
COPY webapps/order-mgmt /usr/local/tomcat/webapps/order-mgmt

# ── 7. Expose Tomcat HTTP port ─────────────────────────────────────
EXPOSE 8080

# Chapter 19 — Kafka Networking, Listeners, Protocols & Connectivity Deep Dive

## Kafka Developer (CCDAK) & Kafka Administrator (CCAAK) Certification Book

> **Certification focus:** TCP/IP, DNS, listeners, advertised listeners, 
> Kafka protocols, TLS, SASL, authentication, authorization boundaries, 
> internal/external connectivity, NAT/load balancers, containers, 
> cloud networking, packet-flow troubleshooting, and certification scenarios.

---

## 18.1 Learning Objectives

By the end of this chapter, you should be able to:

- Explain how a Kafka client actually establishes and maintains broker connections.
- Distinguish `bootstrap.servers`, `listeners`, and `advertised.listeners`.
- Design internal and external listener topologies.
- Explain Kafka's relationship with TCP/IP, DNS, routing, NAT, firewalls, and load balancers.
- Understand PLAINTEXT, SSL, SASL_PLAINTEXT, and SASL_SSL.
- Distinguish encryption, authentication, and authorization.
- Explain TLS certificate and hostname validation failures.
- Explain SASL mechanisms and their role in authentication.
- Troubleshoot the classic "bootstrap works, metadata connection fails" problem.
- Reason about Kafka networking in Docker, Kubernetes, and cloud environments.
- Understand controller/broker networking in KRaft.
- Build a systematic packet-flow troubleshooting methodology.
- Answer certification questions involving listeners, security protocols, and connectivity.

---

# 18.2 The Kafka Networking Mental Model

The most important networking concept in Kafka is:

> **A Kafka client does not simply connect to one Kafka server and stay there.**

The simplified flow is:

```text
Client
  |
  | 1. Connect to bootstrap address
  v
Bootstrap broker
  |
  | 2. Metadata request
  v
Cluster metadata
  |
  | 3. Broker endpoints returned
  v
Client
  |
  | 4. Connect to advertised broker endpoints
  v
Partition leader / required broker
```

Therefore:

```text
bootstrap connectivity
≠
Kafka connectivity
```

A client can successfully contact the bootstrap broker and still fail when it attempts to connect to the addresses returned in metadata.

This distinction explains a large percentage of real-world Kafka networking incidents.

---

# 18.3 The Four Layers You Must Separate

When troubleshooting Kafka networking, separate:

```text
1. Network reachability
2. Kafka protocol
3. TLS / SASL authentication
4. Kafka authorization
```

For example:

```text
TCP connection fails
```

means you have not yet reached Kafka authentication.

Whereas:

```text
TCP succeeds
TLS handshake succeeds
SASL authentication fails
```

is an entirely different problem.

A useful layered model:

```text
Application
    |
Kafka protocol
    |
SASL authentication
    |
TLS encryption
    |
TCP
    |
IP routing
    |
Ethernet / cloud network
```

Not every deployment uses every layer.

---

# 18.4 TCP/IP Fundamentals for Kafka

Kafka is fundamentally a TCP-based distributed system.

A client needs:

```text
source IP
destination IP
destination port
route
firewall permission
Kafka listener
```

For example:

```text
Application
10.0.10.20

       |
       | TCP
       | destination 10.0.20.15:9092
       v

Kafka broker
10.0.20.15
```

If TCP cannot establish:

```text
Kafka protocol does not matter yet.
```

---

# 18.5 DNS

Kafka clients commonly connect using DNS names:

```text
broker-1.kafka.example.com:9092
```

DNS must resolve from the client's network context.

Important distinction:

```text
DNS resolution
≠
network reachability
```

A hostname can resolve successfully to an IP that is:

- unreachable
- private
- incorrect
- behind the wrong load balancer
- blocked by firewall rules

---

# 18.6 `bootstrap.servers`

Client configuration:

```properties
bootstrap.servers=broker-1:9092,broker-2:9092
```

The bootstrap addresses are initial contact points.

They are not necessarily:

```text
all brokers
```

and they are not necessarily:

```text
the addresses clients will ultimately use
```

After bootstrap, Kafka metadata tells the client where brokers and partition leaders are located.

---

# 18.7 `listeners`

Broker configuration:

```properties
listeners=INTERNAL://0.0.0.0:9092
```

This tells Kafka where to bind and listen.

Conceptually:

```text
Kafka process
     |
     +---- bind 0.0.0.0:9092
```

The hostname `0.0.0.0` means bind on all local interfaces.

It is useful for binding.

It is not a valid address to advertise to remote clients.

---

# 18.8 `advertised.listeners`

Example:

```properties
advertised.listeners=INTERNAL://broker-1.kafka.local:9092
```

This tells Kafka what endpoint to publish for clients.

The distinction is:

```text
listeners
    =
where Kafka listens

advertised.listeners
    =
what Kafka tells clients to use
```

This distinction is one of the most important Kafka certification concepts.

---

# 18.9 The Classic Advertised Listener Failure

Consider:

```properties
listeners=PLAINTEXT://0.0.0.0:9092

advertised.listeners=PLAINTEXT://localhost:9092
```

Kafka may be reachable from another machine through:

```text
server-ip:9092
```

The client bootstraps successfully.

But metadata says:

```text
localhost:9092
```

The client then attempts:

```text
client → localhost:9092
```

which means:

```text
client → itself
```

Result:

```text
bootstrap works
metadata works
broker connection fails
```

This is a canonical troubleshooting scenario.

---

# 18.10 `advertised.listeners` Must Be Reachable From the Client

A broker can advertise:

```text
broker-1.internal:9092
```

That is correct for an internal client.

But if an external client receives:

```text
broker-1.internal:9092
```

and cannot resolve or route to it, the configuration is wrong for that client network.

Therefore the question is not:

> "Is the advertised address correct?"

The better question is:

> "Is the advertised address correct **from the perspective of this client network**?"

---

# 18.11 Internal and External Listeners

A common architecture is:

```text
                    Kafka Cluster
                         |
              +----------+----------+
              |                     |
        INTERNAL listener     EXTERNAL listener
              |                     |
        private network         public / routed network
              |                     |
        internal apps          external clients
```

Example:

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094

advertised.listeners=INTERNAL://broker-1.kafka.local:9092,EXTERNAL://kafka.example.com:9094

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:SSL
```

The listener names are logical identifiers.

---

# 18.12 Listener Names Versus Security Protocols

Kafka supports named listeners.

Example:

```properties
listeners=CLIENT://0.0.0.0:9092,REPLICATION://0.0.0.0:9093
```

If the listener name itself is not a protocol such as `PLAINTEXT` or `SSL`, Kafka needs a mapping:

```properties
listener.security.protocol.map=CLIENT:SSL,REPLICATION:SSL
```

Current Kafka broker documentation explicitly supports mapping listener names to security protocols and allows listener-specific security configuration.

---

# 18.13 Listener-Specific Configuration

Listener-specific properties use the listener name.

For example:

```properties
listener.name.internal.ssl.keystore.location=/etc/kafka/internal.keystore
```

This allows different listeners to have different security configuration.

Conceptually:

```text
Generic SSL config
        |
        +---- INTERNAL listener override
        |
        +---- EXTERNAL listener override
```

This is especially useful when internal and external traffic have different trust and certificate requirements.

---

# 18.14 Kafka Security Protocols

The major security protocol combinations are:

```text
PLAINTEXT
SSL
SASL_PLAINTEXT
SASL_SSL
```

Think of them as:

| Protocol | Encryption | Authentication |
|---|---:|---:|
| PLAINTEXT | No | No |
| SSL | Yes | TLS client/server authentication depending on configuration |
| SASL_PLAINTEXT | No | SASL |
| SASL_SSL | Yes | SASL + TLS |

Kafka's security model also includes authorization after authentication.

---

# 18.15 Encryption vs Authentication vs Authorization

These concepts must not be confused.

### Encryption

Protects data in transit:

```text
Client
  |
  | encrypted
  v
Broker
```

### Authentication

Answers:

```text
Who are you?
```

### Authorization

Answers:

```text
What are you allowed to do?
```

Example:

```text
Authentication
    ↓
User = orders-service

Authorization
    ↓
orders-service may WRITE orders
orders-service may READ orders
orders-service may NOT ALTER cluster
```

---

# 18.16 TLS Fundamentals

TLS provides:

- encryption
- server authentication
- optionally client authentication through mutual TLS

Typical flow:

```text
Client
  |
  | ClientHello
  v
Broker
  |
  | ServerHello + certificate
  v
Client
  |
  | certificate validation
  v
Encrypted channel
```

With mutual TLS:

```text
Client certificate
+
Broker certificate
```

both sides authenticate through certificates.

---

# 18.17 TLS Certificate Hostname Validation

Suppose:

```text
advertised.listeners=SSL://broker-1.kafka.example.com:9093
```

The broker certificate must be valid for the hostname the client uses.

If the certificate contains:

```text
DNS:broker-1.kafka.example.com
```

that is appropriate.

If the client connects to:

```text
10.10.20.15
```

but the certificate only contains:

```text
DNS:broker-1.kafka.example.com
```

hostname verification can fail.

Mental model:

```text
Address used by client
        ↓
must match certificate identity
```

---

# 18.18 Common TLS Failures

Typical symptoms include:

```text
SSLHandshakeException
PKIX path building failed
No subject alternative names
certificate expired
unknown CA
```

Possible causes:

- wrong truststore
- missing CA certificate
- expired certificate
- incorrect SAN
- hostname mismatch
- wrong certificate chain
- incorrect keystore
- TLS protocol/cipher incompatibility

Do not treat every TLS error as a Kafka broker problem.

---

# 18.19 SASL

SASL provides authentication mechanisms.

Kafka supports mechanisms including:

```text
GSSAPI
PLAIN
SCRAM-SHA-256
SCRAM-SHA-512
OAUTHBEARER
```

The exact mechanism used depends on the deployment.

SASL can operate with:

```text
SASL_PLAINTEXT
```

or:

```text
SASL_SSL
```

With `SASL_SSL`:

```text
TLS encryption
+
SASL authentication
```

---

# 18.20 SASL Authentication Flow

Conceptually:

```text
TCP connection
      ↓
TLS handshake if SSL
      ↓
SASL negotiation
      ↓
authentication
      ↓
Kafka protocol requests
```

Failure at each stage has a different diagnostic meaning.

---

# 18.21 SASL/PLAIN

Conceptually:

```text
username
password
```

Because credentials are sensitive, PLAIN is normally paired with TLS:

```text
SASL_SSL
```

Using:

```text
SASL_PLAINTEXT
```

does not encrypt the SASL credentials or traffic.

---

# 18.22 SCRAM

SCRAM provides challenge-response authentication and avoids sending the password directly as a plain credential exchange.

Common mechanisms:

```text
SCRAM-SHA-256
SCRAM-SHA-512
```

For production deployments, combine authentication with encrypted transport when credentials or data must be protected in transit.

---

# 18.23 Kerberos / GSSAPI

GSSAPI is commonly associated with Kerberos environments.

Mental model:

```text
Client
   |
   | Kerberos identity
   v
KDC
   |
   | credentials / ticket
   v
Kafka
```

Operational complexity includes:

- principals
- keytabs
- clock synchronization
- DNS
- realm configuration
- ticket lifecycle

---

# 18.24 OAUTHBEARER

OAUTHBEARER integrates Kafka authentication with OAuth-style bearer tokens.

Conceptually:

```text
Client
  |
  | token
  v
Kafka
  |
  | validate token
  v
Authenticated principal
```

Production deployments require careful token lifecycle and identity-provider integration.

---

# 18.25 Authentication Does Not Grant Permissions

A successful SASL or TLS authentication does not mean the client can read every topic.

The flow is:

```text
Connection
   ↓
Authentication
   ↓
Principal
   ↓
Authorization
   ↓
Operation allowed / denied
```

Typical authorization failures:

```text
TopicAuthorizationException
ClusterAuthorizationException
GroupAuthorizationException
```

---

# 18.26 KRaft Controller Networking

KRaft introduces controller communication in addition to ordinary client/broker traffic.

Conceptually:

```text
              Controller quorum
             /       |       \
            /        |        \
       Broker 1   Broker 2   Broker 3
           |
       Client traffic
           |
        Producers
        Consumers
```

Do not confuse:

```text
client listener
```

with:

```text
controller listener
```

Controller endpoints are part of the cluster's internal control-plane architecture.

---

# 18.27 Client Plane vs Control Plane

A useful operational distinction:

### Data/client plane

```text
Producer
Consumer
AdminClient
    |
    v
Broker listeners
```

### Control plane

```text
KRaft controllers
    |
    v
Cluster metadata / controller communication
```

A cluster can have healthy client connectivity while controller networking has a problem, or vice versa.

---

# 18.28 Inter-Broker Communication

Brokers communicate for:

- replication
- metadata operations
- cluster coordination
- internal protocol traffic

Depending on the Kafka version and deployment architecture, the security protocol used for inter-broker traffic must be configured consistently with the selected listener/security model.

For example:

```text
INTERNAL:SSL
```

can provide encrypted broker-to-broker communication.

---

# 18.29 NAT

NAT creates an important Kafka challenge.

Example:

```text
Kafka broker
10.0.2.15:9092

NAT gateway / load balancer

Public address
203.0.113.10:9094
```

If Kafka advertises:

```text
10.0.2.15:9092
```

to an external client, the client may have no route to that address.

The advertised endpoint must reflect the reachable client-facing path.

---

# 18.30 Load Balancers

Kafka is not equivalent to a typical stateless HTTP service.

A single bootstrap load balancer can be useful:

```text
client
  |
  v
load balancer
  |
  v
bootstrap broker
```

But metadata subsequently identifies individual brokers.

Therefore:

```text
one generic load balancer
≠
automatic Kafka connectivity solution
```

The client must ultimately reach the advertised broker endpoints.

Kafka-aware networking design must account for this.

---

# 18.31 Docker Networking

A classic Docker mistake:

```text
advertised.listeners=localhost:9092
```

inside a broker container.

From another container:

```text
localhost
```

means:

```text
the other container itself
```

not the Kafka container.

A better design uses Docker DNS/service names for internal traffic:

```text
broker-1:9092
```

and a separately designed external listener for host/external clients.

---

# 18.32 Docker Internal/External Pattern

Conceptual example:

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094

advertised.listeners=INTERNAL://broker-1:9092,EXTERNAL://localhost:9094

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
```

This can work for a local development topology where:

```text
containers → broker-1:9092
host → localhost:9094
```

Production should use appropriate DNS, TLS, and network isolation rather than copying this local configuration blindly.

---

# 18.33 Kubernetes Networking

Kubernetes introduces:

```text
Pod IP
Service IP
DNS
Ingress / LoadBalancer
NodePort
```

Kafka brokers need stable, individually reachable identities.

A generic HTTP ingress pattern is usually insufficient because Kafka clients need broker-specific endpoints after metadata discovery.

A common architecture therefore provides:

```text
Broker 1 → stable endpoint
Broker 2 → stable endpoint
Broker 3 → stable endpoint
```

and ensures those endpoints are correctly advertised.

---

# 18.34 Cloud Networking

Cloud Kafka deployments require consideration of:

- VPC/VNet
- subnets
- route tables
- security groups
- network ACLs
- private DNS
- public endpoints
- NAT
- load balancers
- TLS
- cross-zone traffic
- cross-region traffic

A successful TCP test from one subnet does not prove that every application subnet can connect.

---

# 18.35 Security Groups and Firewalls

A Kafka port must be permitted at every relevant layer.

Example:

```text
Application
    ↓
host firewall
    ↓
subnet routing
    ↓
security group
    ↓
network ACL
    ↓
load balancer / NAT
    ↓
Kafka listener
```

Troubleshooting should identify which layer blocks the connection.

---

# 18.36 Packet-Flow Troubleshooting

Use a layered approach.

### Step 1 — DNS

```bash
getent hosts broker-1.kafka.local
```

or:

```bash
nslookup broker-1.kafka.local
```

### Step 2 — TCP

```bash
nc -vz broker-1.kafka.local 9092
```

or:

```bash
timeout 3 bash -c '</dev/tcp/broker-1.kafka.local/9092'
```

### Step 3 — TLS

```bash
openssl s_client \
  -connect broker-1.kafka.local:9093 \
  -servername broker-1.kafka.local
```

### Step 4 — Kafka protocol

Use:

```bash
kafka-broker-api-versions.sh
```

or a suitable Kafka CLI/client.

### Step 5 — Authentication

Verify:

```text
security.protocol
sasl.mechanism
credentials
truststore
keystore
```

### Step 6 — Authorization

Verify ACLs and the authenticated principal.

---

# 18.37 A Systematic Troubleshooting Matrix

| Symptom | First Layer to Investigate |
|---|---|
| DNS name does not resolve | DNS |
| Connection refused | listener/process/port |
| Connection timeout | routing/firewall/security group |
| TLS handshake fails | certificate/trust/TLS |
| SASL authentication fails | mechanism/credentials/config |
| Metadata returns unreachable host | advertised listeners |
| Authorization denied | ACLs/authorizer |
| Bootstrap succeeds but consumer fails | advertised broker endpoints |
| Only external clients fail | external listener/NAT/firewall |
| Only containers fail | container DNS/network |
| Only one broker fails | broker-specific listener/DNS/firewall |
| Cross-AZ connections fail | routes/security/network policy |

---

# 18.38 `connection refused` vs `timeout`

These are not the same.

### Connection refused

Often means:

```text
IP reachable
+
nothing listening on that port
```

or an active rejection occurred.

Investigate:

- Kafka process
- listener binding
- port
- local firewall
- service mapping

### Timeout

Often means:

```text
packets cannot complete the path
```

Investigate:

- route
- security group
- firewall
- ACL
- NAT
- wrong IP

These are clues, not absolute guarantees.

---

# 18.39 TLS Handshake vs TCP Handshake

TCP:

```text
SYN
SYN-ACK
ACK
```

TLS comes after TCP:

```text
TCP established
     ↓
TLS handshake
     ↓
encrypted application protocol
```

Therefore:

```text
TLS failure
```

means the TCP connection was already established far enough to begin TLS.

---

# 18.40 Protocol Version Negotiation

Kafka clients and brokers negotiate protocol capabilities.

A modern client may support features unavailable in an older broker.

This is why compatibility should consider:

```text
client version
broker version
protocol version
feature availability
```

Do not assume:

```text
same Java client version
=
same Kafka broker version
```

---

# 18.41 Kafka Request Flow

A simplified request:

```text
Client
   |
   | TCP
   v
Broker listener
   |
   | Kafka request
   v
Request handler
   |
   v
Broker operation
   |
   v
Response
```

Examples:

```text
MetadataRequest
ProduceRequest
FetchRequest
OffsetFetchRequest
JoinGroup
SyncGroup
Heartbeat
```

Understanding request flow helps interpret metrics and logs.

---

# 18.42 Metadata Is Central to Kafka Networking

Metadata contains information such as:

- brokers
- topics
- partitions
- leaders
- replicas
- ISR

A client uses this information to determine where requests should go.

Therefore:

```text
Bad metadata endpoint
→ bad client connectivity
```

This is why advertised listener errors can look like random application failures.

---

# 18.43 Why Bootstrap Does Not Need Every Broker

A client needs enough bootstrap addresses to initially discover the cluster.

Example:

```properties
bootstrap.servers=broker-1:9092,broker-2:9092
```

Even if broker-3 is not listed, metadata can tell the client about broker-3.

However, operationally it is useful to provide multiple bootstrap addresses to avoid dependence on one initial endpoint.

---

# 18.44 Multiple Bootstrap Servers

Prefer:

```properties
bootstrap.servers=broker-1:9092,broker-2:9092,broker-3:9092
```

over:

```properties
bootstrap.servers=broker-1:9092
```

Why?

Because bootstrap addresses are initial discovery points.

Multiple endpoints improve resilience against:

- one broker outage
- DNS issues
- maintenance
- transient network failures

---

# 18.45 DNS Strategy

For production, DNS names should be:

- stable
- correctly scoped
- resolvable from all intended clients
- aligned with TLS certificates
- independent of ephemeral container IPs

Avoid advertising ephemeral IPs when stable identities are available.

---

# 18.46 Port Strategy

Typical examples:

```text
9092 → PLAINTEXT / internal development
9093 → SSL
9094 → custom external listener
```

The exact port is not important.

The important concepts are:

```text
listener name
security protocol
bind address
advertised address
reachability
```

Never memorize a port as if it were a protocol requirement.

---

# 18.47 Network Security Design

A production design might look like:

```text
                  Internet
                     |
              Firewall / LB
                     |
               EXTERNAL SSL
                     |
              Kafka brokers
                     |
              INTERNAL SSL
                     |
            Private applications
```

Security controls can then be applied independently:

```text
External access
→ strict authentication + TLS

Internal access
→ private routing + TLS/SASL
```

---

# 18.48 Encryption Does Not Replace Network Isolation

Using TLS does not mean:

```text
open Kafka to the internet
```

A strong architecture uses multiple layers:

```text
Network isolation
+
firewalls
+
TLS
+
authentication
+
authorization
+
monitoring
```

Defense in depth matters.

---

# 18.49 Authorization and Network Reachability

These are separate.

A client can be:

```text
network reachable
authenticated
```

but still receive:

```text
authorization denied
```

Conversely, a client with perfect ACLs cannot access a broker it cannot reach.

Therefore:

```text
Reachability
→ Authentication
→ Authorization
```

is a useful troubleshooting sequence.

---

# 18.50 Scenario Drill — Localhost Trap

Configuration:

```properties
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://localhost:9092
```

Application runs on another host.

Question:

> Why does bootstrap work from the host in some situations but remote clients fail?

Answer:

Because `localhost` is relative to the client machine. The broker is advertising an endpoint that is not the remote broker address.

Fix the advertised endpoint according to the client network.

---

# 18.51 Scenario Drill — Docker Works, Host Fails

Containers can connect using:

```text
broker-1:9092
```

Host client fails.

Likely cause:

```text
internal listener advertised to host
```

The host needs an externally reachable listener, for example:

```text
localhost:9094
```

or a proper host/DNS endpoint.

---

# 18.52 Scenario Drill — Host Works, Container Fails

Host connects using:

```text
localhost:9094
```

Container fails.

Likely issue:

```text
container cannot use host's localhost as broker address
```

Use:

```text
broker-1:9092
```

or another address reachable from the container network.

---

# 18.53 Scenario Drill — TLS Fails Only Externally

Internal clients work.

External clients receive hostname verification errors.

Investigate:

```text
advertised external hostname
certificate SAN
external DNS
load balancer hostname
```

The internal and external listener identities may require different certificates.

---

# 18.54 Scenario Drill — SASL Authentication Fails

Symptoms:

```text
TCP connection succeeds
TLS succeeds
SASL authentication fails
```

Investigate:

```text
security.protocol
sasl.mechanism
username
password/token
JAAS configuration
listener-specific SASL configuration
```

Do not troubleshoot routing first if TCP and TLS have already succeeded.

---

# 18.55 Scenario Drill — Authorization Fails

Symptoms:

```text
client authenticated successfully
produce/read request denied
```

Investigate:

```text
principal
topic ACL
group ACL
cluster ACL
operation
resource pattern
```

Authentication is not authorization.

---

# 18.56 Scenario Drill — Only One Broker Is Unreachable

If:

```text
broker-1 works
broker-2 works
broker-3 fails
```

investigate broker-specific:

- advertised hostname
- DNS
- listener binding
- port
- firewall
- certificate
- security group
- route

This is more likely than a cluster-wide client configuration issue.

---

# 18.57 Scenario Drill — All Brokers Unreachable

If every broker fails:

```text
DNS
routing
firewall
security group
listener
network policy
load balancer
```

should be investigated before individual broker metadata.

---

# 18.58 Scenario Drill — Metadata Contains Private IP

External client receives:

```text
10.20.30.40:9092
```

The client is on the public internet.

The problem is not the Kafka topic.

The problem is:

```text
advertised endpoint not reachable from client network
```

---

# 18.59 Scenario Drill — Correct Port, Wrong Protocol

Broker listener:

```text
SSL://broker:9093
```

Client:

```properties
security.protocol=PLAINTEXT
```

TCP may succeed.

Kafka/TLS communication will fail.

The port being correct does not mean the protocol is correct.

---

# 18.60 Scenario Drill — SASL_SSL vs SSL

Broker:

```text
SASL_SSL
```

Client:

```properties
security.protocol=SSL
```

TLS may establish, but SASL authentication expectations do not match.

The security protocol must be compatible on both sides.

---

# 18.61 Scenario Drill — `SASL_PLAINTEXT`

Configuration:

```properties
security.protocol=SASL_PLAINTEXT
```

Question:

> Is the traffic encrypted?

Answer:

```text
No.
```

SASL provides authentication; `PLAINTEXT` means transport encryption is not provided.

For encrypted SASL transport:

```text
SASL_SSL
```

---

# 18.62 Scenario Drill — Wrong Certificate Authority

Symptoms:

```text
PKIX path building failed
```

Likely issue:

```text
client does not trust the CA
```

Investigate the truststore and certificate chain.

---

# 18.63 Scenario Drill — Certificate Expired

Symptoms:

```text
certificate expired
```

The network may be completely healthy.

The TLS identity is invalid.

Fix:

```text
renew certificate
deploy chain
reload/restart according to deployment procedure
validate all broker identities
```

---

# 18.64 Scenario Drill — DNS Works but Kafka Fails

Suppose:

```bash
nslookup broker-1.example.com
```

works.

But:

```bash
nc -vz broker-1.example.com 9093
```

times out.

DNS is not the problem.

Continue with:

```text
routing
firewall
security groups
ACLs
listener binding
```

---

# 18.65 Scenario Drill — TCP Works but Kafka Client Fails

Suppose:

```bash
nc -vz broker-1.example.com 9093
```

succeeds.

Kafka client still fails.

Next layers:

```text
TLS
SASL
Kafka protocol
advertised metadata
authorization
```

Do not keep changing firewall rules.

---

# 18.66 Scenario Drill — Producer Can Send, Consumer Cannot

Possible causes include:

- consumer cannot reach advertised broker
- consumer authentication differs
- consumer lacks topic READ ACL
- consumer group ACL is missing
- consumer-specific DNS/routing issue

Do not assume a cluster network outage simply because one client works.

---

# 18.67 Scenario Drill — Consumer Connects but Rebalances Repeatedly

This can involve networking if:

- heartbeat requests time out
- broker connections flap
- DNS is unstable
- security negotiation repeatedly fails

But also investigate:

- consumer processing time
- `max.poll.interval.ms`
- session timeout
- group coordinator connectivity
- application pauses

Not every rebalance is a network failure.

---

# 18.68 Scenario Drill — High Connection Churn

Symptoms:

```text
connections repeatedly created and closed
```

Investigate:

- client lifecycle
- connection timeout settings
- DNS
- TLS handshake overhead
- authentication
- load balancer behavior
- broker overload
- network instability

Kafka clients are intended to maintain long-lived connections.

---

# 18.69 Scenario Drill — Cross-AZ Latency

Application and Kafka brokers span availability zones.

Potential consequences:

```text
higher latency
higher network cost
replication traffic across zones
```

Architecture should deliberately decide:

```text
where clients run
where brokers run
which traffic crosses zones
```

---

# 18.70 Scenario Drill — Cross-Region Kafka Traffic

Cross-region traffic introduces:

- latency
- bandwidth cost
- failure domains
- replication architecture
- disaster recovery concerns

Do not treat a cross-region Kafka deployment as simply:

```text
same cluster + longer network cable
```

---

# 18.71 Certification Traps

1. **`listeners` is the address clients use.**  
   False. It describes broker bind/listen endpoints.

2. **`advertised.listeners` can use `0.0.0.0`.**  
   False. It must advertise a client-reachable endpoint.

3. **Bootstrap servers are all brokers.**  
   False. They are initial discovery endpoints.

4. **If bootstrap succeeds, networking is correct.**  
   False.

5. **SASL means encryption.**  
   False. SASL is primarily authentication.

6. **SSL always means user authentication.**  
   Not necessarily. TLS server authentication is different from Kafka principal authentication/authorization design.

7. **SASL_PLAINTEXT encrypts credentials.**  
   False.

8. **SASL_SSL provides only authentication.**  
   False. It combines SASL authentication with TLS.

9. **Authentication grants topic permissions.**  
   False.

10. **A load balancer alone solves Kafka external connectivity.**  
    False.

11. **A DNS record proving resolution proves connectivity.**  
    False.

12. **A successful TCP test proves Kafka protocol configuration.**  
    False.

13. **Correct port means correct Kafka security protocol.**  
    False.

14. **Docker `localhost` means the host machine.**  
    False inside a normal container network namespace.

15. **More consumers fix a network problem.**  
    False.

---

# 18.72 Production Connectivity Checklist

## DNS

- [ ] Broker names resolve from every intended client network
- [ ] Reverse DNS considered where SASL/Kerberos requires it
- [ ] DNS names align with TLS certificates
- [ ] No dependency on ephemeral container IPs

## TCP

- [ ] Broker ports reachable
- [ ] Firewall rules verified
- [ ] Security groups verified
- [ ] Routes verified
- [ ] Network ACLs verified

## Kafka listeners

- [ ] `listeners` verified
- [ ] `advertised.listeners` verified
- [ ] Internal listener tested
- [ ] External listener tested
- [ ] Every advertised broker endpoint reachable

## Security

- [ ] Correct `security.protocol`
- [ ] TLS certificates valid
- [ ] Trust chain valid
- [ ] Hostname verification valid
- [ ] SASL mechanism correct
- [ ] Credentials/tokens valid
- [ ] ACLs configured

## KRaft

- [ ] Controller endpoints reachable
- [ ] Controller quorum healthy
- [ ] Broker/controller roles documented
- [ ] Control-plane network isolated appropriately

## Containers/cloud

- [ ] Docker DNS verified
- [ ] Kubernetes service/endpoints verified
- [ ] Load balancer behavior verified
- [ ] NAT behavior verified
- [ ] Cross-zone routing verified

---

# 18.73 Connectivity Troubleshooting Runbook

Use this sequence:

```text
1. What hostname is the client using?
             ↓
2. Does DNS resolve?
             ↓
3. Does TCP connect?
             ↓
4. Does TLS succeed?
             ↓
5. Does SASL authenticate?
             ↓
6. Does Kafka metadata return?
             ↓
7. Are advertised brokers reachable?
             ↓
8. Does authorization allow the operation?
             ↓
9. Is the application healthy?
```

This prevents random configuration changes.

---

# 18.74 The "Network Five" Mental Model

For certification questions, remember:

```text
DNS
 ↓
TCP
 ↓
TLS/SASL
 ↓
Kafka metadata
 ↓
Authorization
```

Each layer answers a different question.

---

# 18.75 Advanced Listener Design Example

Example three-listener design:

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094

advertised.listeners=INTERNAL://broker-1.internal.example:9092,EXTERNAL://broker-1.example.com:9093

listener.security.protocol.map=INTERNAL:SSL,EXTERNAL:SASL_SSL,CONTROLLER:SSL
```

Conceptually:

```text
INTERNAL
→ private application traffic
→ TLS

EXTERNAL
→ external clients
→ SASL + TLS

CONTROLLER
→ KRaft control plane
→ TLS
```

The exact topology must be adapted to the Kafka version and deployment.

---

# 18.76 What a Senior Kafka Engineer Should Ask

When someone says:

> "Kafka is unreachable."

Do not immediately ask:

> "Which port?"

Ask:

1. From which client?
2. To which broker?
3. What hostname?
4. What IP does DNS return?
5. What advertised endpoint did Kafka return?
6. Does TCP connect?
7. What security protocol is expected?
8. Does TLS succeed?
9. Does SASL succeed?
10. Which principal is authenticated?
11. What operation fails?
12. Does authorization allow it?
13. Is the problem broker-specific or cluster-wide?
14. Is the problem internal or external?
15. Did the network topology recently change?

This is the difference between trial-and-error troubleshooting and systematic diagnosis.

---

# 18.77 Certification Master Matrix

| Concept | Remember | Typical Trap |
|---|---|---|
| `bootstrap.servers` | Initial discovery endpoints | Treating them as permanent broker endpoints |
| `listeners` | Broker bind endpoints | Confusing with advertised endpoints |
| `advertised.listeners` | Client-visible endpoints | Using unreachable/private addresses |
| Listener map | Listener → security protocol | Forgetting named listener mapping |
| PLAINTEXT | No transport encryption | Assuming authentication exists |
| SSL | TLS transport | Confusing TLS with Kafka authorization |
| SASL | Authentication mechanisms | Assuming it encrypts traffic |
| SASL_PLAINTEXT | SASL without TLS | Assuming traffic is encrypted |
| SASL_SSL | SASL + TLS | Forgetting TLS configuration |
| TLS | Encryption + certificate identity | Ignoring SAN/trust chain |
| Authentication | Who are you? | Treating it as permission |
| Authorization | What can you do? | Troubleshooting it as networking |
| DNS | Name → IP | Assuming resolution proves reachability |
| TCP | Network transport | Assuming TCP success proves Kafka correctness |
| NAT | Address translation | Advertising private endpoints |
| Load balancer | Network entry point | Assuming it replaces broker-specific endpoints |
| Docker | Separate network namespace | Using `localhost` incorrectly |
| KRaft controller | Control plane | Confusing with client listeners |
| Metadata | Broker/partition information | Ignoring advertised endpoint failures |

---

# 18.78 Final Cheat Sheet

## Listener concepts

```text
listeners
    ↓
where broker listens

advertised.listeners
    ↓
what clients are told to use
```

## Discovery

```text
bootstrap.servers
    ↓
initial broker
    ↓
metadata
    ↓
advertised broker endpoints
```

## Security

```text
PLAINTEXT
→ no TLS

SSL
→ TLS

SASL_PLAINTEXT
→ SASL authentication, no TLS

SASL_SSL
→ SASL authentication + TLS
```

## Security layers

```text
Encryption
Authentication
Authorization
```

are different concepts.

## Troubleshooting

```text
DNS
→ TCP
→ TLS
→ SASL
→ Metadata
→ Advertised endpoints
→ ACLs
```

## Docker

```text
localhost
=
current network namespace
```

not automatically the host.

## Production

```text
Private DNS
+
stable broker identities
+
correct advertised endpoints
+
TLS
+
authentication
+
authorization
+
network isolation
```

---

# 18.79 Chapter Summary

Remember these principles:

1. Kafka networking is metadata-driven.
2. `bootstrap.servers` is for initial discovery.
3. `listeners` defines where Kafka binds.
4. `advertised.listeners` defines what clients are told to connect to.
5. Bootstrap success does not prove complete connectivity.
6. Every advertised broker endpoint must be reachable from the relevant client network.
7. DNS resolution is different from TCP reachability.
8. TCP connectivity is different from TLS success.
9. TLS is different from SASL authentication.
10. Authentication is different from authorization.
11. `SASL_PLAINTEXT` does not provide transport encryption.
12. `SASL_SSL` combines SASL authentication with TLS.
13. TLS certificates must match the identity used by clients.
14. Internal and external listeners should be designed explicitly.
15. NAT and load balancers can change the reachable endpoint.
16. Docker and Kubernetes require careful handling of network identity.
17. KRaft introduces controller/control-plane networking that must be distinguished from client traffic.
18. A systematic troubleshooting sequence is more reliable than changing random Kafka properties.
19. Version-specific Kafka networking and security behavior should always be verified against the documentation for the deployed version.
20. The most important Kafka networking question is: **"What endpoint did Kafka advertise, and can this specific client reach it?"**

---

# Official Reference Material

Current Apache Kafka documentation should be used for version-specific configuration and security behavior:

- Apache Kafka 4.3 Broker Configuration
- Apache Kafka 4.3 Security
- Apache Kafka 4.3 Security Overview
- Apache Kafka 4.3 SASL Authentication
- Apache Kafka Listener Configuration

Official documentation confirms that Kafka supports authentication using SSL or SASL, TLS encryption, and authorization; it also documents named listeners and `listener.security.protocol.map`.

---

# Next Chapter

## Chapter 19 — Kafka Security Deep Dive: TLS, SASL, ACLs, Authentication & Authorization

Topics:

- TLS certificates and PKI
- truststores and keystores
- mutual TLS
- SASL mechanisms
- SCRAM
- Kerberos
- OAuth/OAUTHBEARER
- Kafka principals
- ACLs
- StandardAuthorizer
- super users
- listener-specific security
- inter-broker security
- KRaft security
- production hardening
- security troubleshooting
- CCDAK/CCAAK security scenario drills

# Chapter 20 — Kafka Security Deep Dive: TLS, SASL, ACLs, Authentication & Authorization

> Kafka Developer & Administrator Certification Preparation
> Based on the security concepts covered in *Kafka: The Definitive Guide*, with certification-oriented explanations,
> operational examples, troubleshooting scenarios, and exam traps.

---

## 19.1 Learning Objectives

By the end of this chapter you should be able to:

-   Separate encryption, authentication and authorization.
-   Explain TLS, certificates, CAs, keystores, truststores and mutual
    TLS.
-   Configure and troubleshoot `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` and
    `SASL_SSL`.
-   Explain SASL/PLAIN, SCRAM, GSSAPI/Kerberos and OAUTHBEARER.
-   Understand listener-specific security configuration.
-   Explain Kafka principals and principal mapping.
-   Configure and reason about Kafka ACLs.
-   Understand `StandardAuthorizer` in KRaft.
-   Identify permissions required by producers, consumers, transactions
    and administration.
-   Secure client, inter-broker and controller communication.
-   Perform staged security migrations.
-   Troubleshoot failures from DNS/TCP through TLS, SASL and ACL
    evaluation.

------------------------------------------------------------------------

## 19.2 The Three Questions of Kafka Security

Kafka security is easiest to understand as three separate concerns:

``` text
Encryption
    Can someone read or modify traffic?

Authentication
    Who is connecting?

Authorization
    What may that identity do?
```

A secure Kafka deployment commonly combines:

``` text
TLS
    -> transport encryption and integrity

SASL / mTLS
    -> authentication

ACLs
    -> authorization
```

Authentication does not imply authorization.

A client can be:

``` text
Authenticated = YES
Authorized    = NO
```

------------------------------------------------------------------------

## 19.3 Kafka Security Protocols

Kafka commonly uses four security protocol combinations:

  Protocol             TLS encryption   SASL authentication
  ------------------ ---------------- ---------------------
  `PLAINTEXT`                      No                    No
  `SSL`                           Yes                    No
  `SASL_PLAINTEXT`                 No                   Yes
  `SASL_SSL`                      Yes                   Yes

Certification shortcut:

``` text
SSL  = TLS
SASL = authentication
ACL  = authorization
```

Therefore:

``` text
SASL_SSL
    = SASL authentication + TLS

SASL_PLAINTEXT
    = SASL authentication without TLS transport encryption
```

------------------------------------------------------------------------

## 19.4 TLS Fundamentals

TLS provides:

-   confidentiality
-   integrity
-   server authentication
-   optionally client authentication

Simplified flow:

``` text
Client                         Broker
  |                              |
  | ClientHello                  |
  |----------------------------->|
  |                              |
  | ServerHello + Certificate    |
  |<-----------------------------|
  |                              |
  | Validate certificate         |
  |                              |
  | Establish session keys       |
  |<============================>|
  |                              |
  | Encrypted Kafka traffic      |
  |<============================>|
```

The certificate is meaningful only when the client can validate its
trust chain and endpoint identity.

------------------------------------------------------------------------

## 19.5 CA, Certificate, Keystore and Truststore

A typical PKI hierarchy:

``` text
Root CA
   |
Intermediate CA
   |
   +---- Broker certificate
   +---- Broker certificate
   +---- Client certificate
```

### Keystore

Contains the application's own identity:

``` text
Private key
Certificate
Certificate chain
```

Mental model:

``` text
Keystore = "Who am I?"
```

### Truststore

Contains trusted CA/certificate material.

Mental model:

``` text
Truststore = "Who do I trust?"
```

Therefore:

``` text
Client keystore
    -> client private key + certificate, for mTLS

Client truststore
    -> CA certificates used to trust brokers

Broker keystore
    -> broker private key + certificate

Broker truststore
    -> CA certificates used to trust clients
```

------------------------------------------------------------------------

## 19.6 TLS Hostname Verification

Suppose the client connects to:

``` text
broker-1.kafka.example.com
```

The certificate should contain that identity in its SAN:

``` text
Subject Alternative Name:
    DNS:broker-1.kafka.example.com
```

The important relationship is:

``` text
advertised.listeners
        |
        v
hostname used by client
        |
        v
certificate SAN
```

All three must agree.

A hostname mismatch should be fixed by correcting the certificate or
endpoint identity, not by blindly disabling hostname verification.

------------------------------------------------------------------------

## 19.7 Mutual TLS

Normal TLS can authenticate the broker:

``` text
Client -> verifies Broker
```

Mutual TLS adds:

``` text
Client -> verifies Broker
Broker -> verifies Client
```

For mandatory client certificates:

``` properties
ssl.client.auth=required
```

Typical values:

``` text
none
requested
required
```

`requested` does not enforce mTLS.

------------------------------------------------------------------------

## 19.8 TLS Failure Patterns

  -----------------------------------------------------------------------
  Symptom                             Likely cause
  ----------------------------------- -----------------------------------
  Unknown CA                          Truststore does not trust CA

  PKIX path failure                   Invalid/incomplete certificate
                                      chain

  Hostname mismatch                   SAN does not match endpoint

  Certificate expired                 Certificate lifecycle problem

  Handshake failure                   TLS protocol/cipher/certificate
                                      mismatch

  Client certificate required         Missing client keystore

  Works only when hostname            Endpoint/certificate identity
  verification is disabled            mismatch
  -----------------------------------------------------------------------

Useful diagnostics:

``` bash
openssl s_client   -connect broker-1.example.com:9093   -showcerts
```

``` bash
openssl x509   -in broker.crt   -text   -noout
```

Inspect:

-   issuer
-   subject
-   SAN
-   validity
-   key usage
-   extended key usage
-   certificate chain

------------------------------------------------------------------------

## 19.9 Listener-Specific Security

Kafka listeners can expose different security protocols.

Example:

``` properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093

listener.security.protocol.map=INTERNAL:SSL,EXTERNAL:SASL_SSL
```

Conceptually:

``` text
Internal applications
        |
       SSL
        |
     Brokers

External applications
        |
    SASL_SSL
        |
     Brokers
```

This is a common production pattern.

------------------------------------------------------------------------

## 19.10 `listeners` vs `advertised.listeners`

Remember:

``` text
listeners
    = where Kafka binds/listens

advertised.listeners
    = endpoints Kafka returns to clients
```

Example:

``` properties
listeners=CLIENT://0.0.0.0:9093

advertised.listeners=CLIENT://broker-1.example.com:9093
```

A broker can listen correctly while advertising an endpoint that a
client cannot reach.

------------------------------------------------------------------------

## 19.11 SASL

Kafka supports SASL mechanisms including:

``` text
GSSAPI
PLAIN
SCRAM-SHA-256
SCRAM-SHA-512
OAUTHBEARER
```

SASL can run over:

``` text
SASL_PLAINTEXT
```

or:

``` text
SASL_SSL
```

SASL provides authentication. TLS determines whether the transport is
encrypted.

------------------------------------------------------------------------

## 19.12 SASL/PLAIN

Example:

``` properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN

sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="orders-service" password="secret";
```

Important:

``` text
PLAIN = authentication
PLAIN != encryption
```

For production transport security:

``` text
SASL_SSL + PLAIN
```

is preferable to:

``` text
SASL_PLAINTEXT + PLAIN
```

------------------------------------------------------------------------

## 19.13 SCRAM

Kafka supports:

``` text
SCRAM-SHA-256
SCRAM-SHA-512
```

Example:

``` properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512

sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="orders-service" password="secret";
```

SCRAM uses salted challenge-response authentication.

It does not replace TLS.

A common production design is:

``` text
SCRAM
+
TLS
=
SASL_SSL
```

Example credential creation:

``` bash
kafka-configs.sh   --bootstrap-server broker-1:9092   --alter   --add-config 'SCRAM-SHA-512=[password=secret]'   --entity-type users   --entity-name orders-service
```

------------------------------------------------------------------------

## 19.14 Kerberos / GSSAPI

GSSAPI is commonly used with Kerberos.

Conceptually:

``` text
Kerberos KDC
      |
    ticket
      |
      v
Kafka client
      |
 SASL/GSSAPI
      |
      v
Kafka broker
```

A principal can resemble:

``` text
orders-service@EXAMPLE.COM
```

Kerberos is particularly relevant in enterprise environments with an
existing centralized identity infrastructure.

------------------------------------------------------------------------

## 19.15 OAUTHBEARER

OAuth bearer authentication uses an access token:

``` text
Client
  |
  v
Identity Provider
  |
 token
  |
  v
Kafka client
  |
SASL/OAUTHBEARER
  |
  v
Kafka broker
```

The broker validates the token and establishes the Kafka principal.

Production deployments should use an appropriate trusted OAuth/OIDC
identity infrastructure.

------------------------------------------------------------------------

## 19.16 JAAS and Listener-Specific SASL Configuration

Client example:

``` properties
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="orders-service" password="secret";
```

For brokers, listener/mechanism-specific configuration follows the
pattern:

``` properties
listener.name.<listener>.<mechanism>.sasl.jaas.config=...
```

Do not assume a global SASL setting applies identically to every
listener.

Secrets should not be committed to Git.

Prefer:

``` text
Secret manager
      |
      v
Deployment system
      |
      v
Kafka configuration
```

------------------------------------------------------------------------

## 19.17 Kafka Principals

After authentication Kafka has an identity:

``` text
Principal
```

Examples:

``` text
User:alice
User:orders-service
User:kafka
```

Different authentication mechanisms produce identities differently.

Examples:

``` text
SCRAM username
       |
       v
User:orders-service
```

or:

``` text
TLS client certificate
       |
       v
Kafka principal
```

Authorization operates on the resulting principal.

------------------------------------------------------------------------

## 19.18 Authorization and ACLs

An ACL can be viewed as:

``` text
Principal
    +
ALLOW/DENY
    +
Operation
    +
Resource
    +
Host
```

Example:

``` text
User:orders-service
ALLOW
WRITE
Topic:orders
Host:*
```

Authorization answers:

``` text
Can this principal perform this Kafka operation?
```

It does not authenticate the principal.

------------------------------------------------------------------------

## 19.19 Important Kafka ACL Resources

Important resource types include:

``` text
Topic
Group
Cluster
TransactionalId
DelegationToken
```

A consumer commonly needs authorization involving:

``` text
Topic
+
Consumer Group
```

A transactional producer may require permissions involving:

``` text
Topic
+
TransactionalId
+
other protocol operations
```

Always reason from the Kafka feature being used.

------------------------------------------------------------------------

## 19.20 Important Kafka ACL Operations

Common operations include:

``` text
READ
WRITE
CREATE
DELETE
ALTER
DESCRIBE
DESCRIBE_CONFIGS
ALTER_CONFIGS
CLUSTER_ACTION
IDEMPOTENT_WRITE
```

Do not memorize only strings.

Ask:

``` text
What Kafka operation is the client performing?
What resource does it operate on?
What principal is making the request?
```

------------------------------------------------------------------------

## 19.21 `kafka-acls.sh`

List ACLs:

``` bash
kafka-acls.sh   --bootstrap-server broker-1:9092   --list
```

Grant topic write:

``` bash
kafka-acls.sh   --bootstrap-server broker-1:9092   --add   --allow-principal User:orders-service   --operation Write   --topic orders
```

Grant topic read:

``` bash
kafka-acls.sh   --bootstrap-server broker-1:9092   --add   --allow-principal User:analytics   --operation Read   --topic orders
```

Grant group read:

``` bash
kafka-acls.sh   --bootstrap-server broker-1:9092   --add   --allow-principal User:analytics   --operation Read   --group analytics-group
```

In a secured cluster the CLI itself must authenticate and be authorized.

------------------------------------------------------------------------

## 19.22 ACL Pattern Types

Kafka supports resource patterns including:

``` text
LITERAL
PREFIXED
```

and matching/query patterns such as:

``` text
ANY
MATCH
```

Literal:

``` text
orders
```

matches the exact resource.

Prefixed:

``` text
orders
```

can match resources beginning with that prefix:

``` text
orders
orders-eu
orders-us
orders-v2
```

### Certification trap

A prefix ACL can unintentionally authorize future resources.

Use prefixes deliberately.

------------------------------------------------------------------------

## 19.23 Allow and Deny

ACLs can contain:

``` text
ALLOW
DENY
```

Example:

``` text
ALLOW User:app READ orders
DENY  User:app WRITE orders
```

Authorization decisions require considering matching:

``` text
Principal
Operation
Resource
Pattern
Host
ALLOW/DENY
```

Do not infer the result from one ACL entry without considering other
matching rules.

------------------------------------------------------------------------

## 19.24 StandardAuthorizer in KRaft

Modern KRaft deployments use Kafka's built-in authorizer:

``` properties
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
```

This is an important distinction from older ZooKeeper-era Kafka
material.

Certification rule:

> First identify whether the question describes a KRaft cluster or an
> older ZooKeeper-based cluster.

Then choose the appropriate authorization configuration.

------------------------------------------------------------------------

## 19.25 Super Users

Kafka can define super users:

``` properties
super.users=User:admin;User:kafka
```

Super users bypass ordinary ACL restrictions.

Useful for:

-   infrastructure administration
-   broker identities
-   recovery/bootstrap operations

But application identities should normally use least privilege.

Bad:

``` text
every application -> super user
```

Good:

``` text
application -> minimal ACLs
```

------------------------------------------------------------------------

## 19.26 Least Privilege

Suppose:

``` text
orders-service
```

needs:

``` text
WRITE orders
READ order-events
READ orders-group
```

Do not automatically grant:

``` text
DELETE *
ALTER *
CLUSTER_ACTION
```

Authorization should map:

``` text
Business responsibility
        |
        v
Required Kafka operations
        |
        v
Minimal ACLs
```

------------------------------------------------------------------------

## 19.27 Producer Authorization

A producer commonly needs:

``` text
WRITE
```

on its target topic.

Features such as idempotence and transactions can introduce additional
authorization requirements.

For certification questions, consider:

``` text
Producer
  |
  +-- topic write
  +-- idempotence
  +-- transactional.id
  +-- transaction lifecycle
```

Do not assume ordinary topic WRITE is the complete permission model for
every producer feature.

------------------------------------------------------------------------

## 19.28 Consumer Authorization

A consumer commonly needs:

``` text
READ
```

on the topic.

It also operates within a:

``` text
Consumer Group
```

Therefore authorization should account for:

``` text
Topic permissions
+
Group permissions
```

Authentication can succeed while group or topic authorization fails.

------------------------------------------------------------------------

## 19.29 Transactional Producer Authorization

A transactional producer uses:

``` properties
transactional.id=orders-producer
```

Authorization can therefore involve:

``` text
Topic
TransactionalId
Cluster/protocol operations
```

Think about the complete lifecycle:

``` text
authenticate
    ↓
begin transaction
    ↓
write records
    ↓
send offsets
    ↓
commit transaction
```

------------------------------------------------------------------------

## 19.30 Inter-Broker Security

Kafka brokers communicate with one another.

Client security:

``` text
Client -> Broker
```

is not enough.

Also secure:

``` text
Broker -> Broker
```

Example:

``` properties
security.inter.broker.protocol=SASL_SSL
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
```

This gives:

``` text
TLS encryption
+
SCRAM authentication
```

A secure external listener does not automatically secure inter-broker
traffic.

------------------------------------------------------------------------

## 19.31 KRaft Controller Security

KRaft introduces controller quorum communication.

A production deployment must account for:

``` text
Client listeners
Inter-broker communication
Controller listeners
Administrative access
```

Think of these as distinct communication paths.

``` text
Client plane
     |
     v
Kafka brokers
     |
     +---- inter-broker plane
     |
     +---- controller/control plane
```

Securing one plane does not automatically secure the others.

------------------------------------------------------------------------

## 19.32 Security Is Not Network Isolation

TLS does not replace:

``` text
firewalls
security groups
private subnets
routing
VPN/private connectivity
network segmentation
```

Use defense in depth:

``` text
Network isolation
      +
TLS
      +
Authentication
      +
Authorization
      +
Observability
```

------------------------------------------------------------------------

## 19.33 Security Troubleshooting Sequence

Use this order:

``` text
1. DNS
2. TCP
3. TLS handshake
4. Certificate validation
5. Hostname verification
6. SASL authentication
7. Kafka principal
8. Metadata
9. Advertised broker endpoints
10. ACL authorization
11. Application behavior
```

This prevents mixing failure domains.

------------------------------------------------------------------------

## 19.34 TCP vs TLS vs SASL vs ACL

``` text
TCP fails
    -> listener/network/routing/firewall

TCP works, TLS fails
    -> certificate/TLS/endpoint

TLS works, SASL fails
    -> credentials/mechanism/JAAS

SASL works, authorization fails
    -> principal/ACL

ACL works, application still fails
    -> Kafka protocol/application
```

This is one of the most useful operational decision trees in the
chapter.

------------------------------------------------------------------------

## 19.35 Scenario --- Connection Refused

Symptom:

``` text
Connection refused
```

Start with:

``` text
listener
port
broker process
container port
network path
```

Typical causes:

-   nothing listening
-   wrong port
-   wrong listener
-   broker unavailable
-   container port not published

Do not begin with ACLs.

------------------------------------------------------------------------

## 19.36 Scenario --- Timeout

Symptom:

``` text
Connection timed out
```

Investigate:

-   routing
-   firewall
-   security group
-   unreachable network
-   wrong IP
-   bad advertised endpoint

Timeout generally indicates a reachability problem before it indicates
authorization.

------------------------------------------------------------------------

## 19.37 Scenario --- TLS Fails

TCP works.

TLS fails.

Check:

``` text
truststore
broker certificate
certificate chain
SAN
hostname
TLS protocol
cipher compatibility
client certificate requirement
```

If:

``` properties
ssl.client.auth=required
```

the client needs an appropriate certificate/private key.

------------------------------------------------------------------------

## 19.38 Scenario --- SASL Fails

TLS succeeds.

SASL fails.

Check:

``` text
security.protocol
sasl.mechanism
username
password
JAAS
listener-specific configuration
broker-enabled mechanism
SCRAM credentials
Kerberos configuration
OAuth token
```

Example mismatch:

``` text
Client:
SCRAM-SHA-512

Broker:
SCRAM-SHA-256 only
```

------------------------------------------------------------------------

## 19.39 Scenario --- Authorization Fails

Logs show:

``` text
Authenticated principal = User:orders-service
```

Then:

``` text
TopicAuthorizationException
```

The chain is:

``` text
TLS       OK
SASL      OK
Principal OK
ACL       FAIL
```

Inspect:

``` bash
kafka-acls.sh   --bootstrap-server broker-1:9092   --list
```

Check:

-   exact principal
-   operation
-   resource
-   pattern type
-   host
-   group permissions

------------------------------------------------------------------------

## 19.40 Scenario --- Bootstrap Works, Broker Connection Fails

Client bootstraps successfully.

Metadata advertises:

``` text
broker-2.private.example.com:9093
```

The client is outside the private network.

Diagnosis:

``` text
Bootstrap      OK
Metadata       OK
Advertised     WRONG FOR CLIENT NETWORK
```

Fix:

``` text
advertised.listeners
DNS
routing
firewall
listener architecture
```

------------------------------------------------------------------------

## 19.41 Scenario --- TLS Works Internally but Fails Externally

Internal:

``` text
broker-1.internal.example.com
```

External:

``` text
broker-1.public.example.com
```

Certificate:

``` text
SAN=broker-1.internal.example.com
```

External hostname verification fails.

Correct the certificate identity and/or advertised endpoint.

Do not simply disable endpoint identification.

------------------------------------------------------------------------

## 19.42 Scenario --- `SASL_SSL` vs `SSL`

Question:

> What does SASL add to SSL?

Answer:

``` text
SSL
    TLS transport security

SASL_SSL
    TLS transport security
    +
    SASL authentication
```

Authorization is a separate step.

------------------------------------------------------------------------

## 19.43 Scenario --- mTLS

Requirement:

> Every Kafka application must present a client certificate.

Use:

``` properties
ssl.client.auth=required
```

Configure:

``` text
client keystore
client truststore
broker keystore
broker truststore
```

Then ensure the resulting principal is represented correctly in ACLs.

------------------------------------------------------------------------

## 19.44 Scenario --- Wrong Principal

Authenticated identity:

``` text
User:orders-service
```

ACL:

``` text
User:orders
```

Result:

``` text
authentication succeeds
authorization fails
```

Always inspect the exact principal before modifying ACLs.

------------------------------------------------------------------------

## 19.45 Scenario --- Prefix ACL Accident

ACL:

``` text
PREFIXED orders
```

can affect:

``` text
orders
orders-eu
orders-us
orders-v2
```

If the requirement is only:

``` text
orders
```

use a literal pattern unless broader authorization is intentional.

------------------------------------------------------------------------

## 19.46 Scenario --- Secured CLI

The Kafka CLI is another Kafka client.

Example:

``` bash
kafka-topics.sh   --bootstrap-server broker-1.example.com:9093   --command-config admin.properties   --list
```

The command configuration can contain:

``` properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=...
ssl.truststore.location=...
ssl.truststore.password=...
```

If the application works but the CLI fails, compare their security
configuration.

------------------------------------------------------------------------

## 19.47 Scenario --- Migrating a Running Cluster

Do not switch every broker and client simultaneously.

Safer sequence:

``` text
Phase 1
    Add secure listener

Phase 2
    Move clients

Phase 3
    Secure inter-broker communication

Phase 4
    Verify

Phase 5
    Remove plaintext
```

The objective is to minimize the blast radius of a configuration error.

------------------------------------------------------------------------

## 19.48 Example Security Migration

Initial:

``` properties
listeners=PLAINTEXT://0.0.0.0:9092
```

Transition:

``` properties
listeners=PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9093
```

Move clients:

``` properties
bootstrap.servers=broker-1:9093
security.protocol=SSL
```

Then secure broker communication.

Finally remove:

``` text
PLAINTEXT
```

only after all required communication paths are migrated and validated.

------------------------------------------------------------------------

## 19.49 Certificate Rotation

Avoid:

``` text
delete old certificate
        |
deploy new certificate
```

Prefer:

``` text
1. Issue new certificate
2. Trust new CA/certificate
3. Deploy new certificate
4. Validate
5. Rotate
6. Remove old trust after migration
```

Overlap prevents avoidable outages.

------------------------------------------------------------------------

## 19.50 Credential Rotation

For username/password authentication:

``` text
Old credential
      |
Create new credential
      |
Deploy new credential
      |
Verify
      |
Revoke old credential
```

Never revoke the only known-good credential before the replacement has
been deployed and validated.

------------------------------------------------------------------------

## 19.51 Security Observability

Monitor:

### TLS

-   handshake failures
-   certificate expiration
-   connection failures
-   TLS-related CPU

### SASL

-   authentication failures
-   invalid credentials
-   mechanism mismatches
-   token failures

### Authorization

-   authorization failures
-   denied operations
-   ACL changes

### Infrastructure

-   connection count
-   CPU
-   network
-   request latency

Security should be observable and operationally testable.

------------------------------------------------------------------------

## 19.52 Security and Connection Churn

TLS and SASL add connection-establishment work.

A high rate of short-lived connections can cause:

``` text
TLS handshakes
+
SASL authentication
+
CPU overhead
+
latency
```

Kafka clients should normally maintain long-lived connections.

Bad:

``` text
connect
send one record
disconnect
repeat
```

Better:

``` text
persistent client
        |
many requests
```

------------------------------------------------------------------------

## 19.53 Production Security Architecture

A production-oriented architecture can look like:

``` text
External applications
        |
     SASL_SSL
        |
   Client listener
        |
   Kafka brokers
        |
  secured internal
   communication
        |
   KRaft quorum
```

Security layers:

``` text
Network segmentation
       +
TLS
       +
SASL/mTLS
       +
StandardAuthorizer
       +
ACLs
       +
Monitoring
```

------------------------------------------------------------------------

## 19.54 Production Hardening Checklist

### Network

-   [ ] Kafka is not unnecessarily public
-   [ ] Firewall/security-group rules are minimal
-   [ ] DNS names are stable
-   [ ] Internal and external paths are explicit
-   [ ] Controller connectivity is protected

### TLS

-   [ ] Trusted CA
-   [ ] Correct certificate chain
-   [ ] SAN matches advertised hostname
-   [ ] Hostname verification enabled
-   [ ] Expiration monitored
-   [ ] Private keys protected
-   [ ] mTLS enabled where required

### SASL

-   [ ] Authentication mechanism selected deliberately
-   [ ] Credentials stored securely
-   [ ] No production secrets in Git
-   [ ] Broker/client mechanisms match
-   [ ] Listener-specific settings verified
-   [ ] Credential rotation tested

### Authorization

-   [ ] KRaft authorizer configured
-   [ ] Least privilege
-   [ ] Topic ACLs
-   [ ] Group ACLs
-   [ ] Transactional ID permissions where required
-   [ ] Prefix ACLs reviewed
-   [ ] Super users minimized

### Operations

-   [ ] Security migration tested
-   [ ] Certificate rotation tested
-   [ ] Credential rotation tested
-   [ ] Authentication failures monitored
-   [ ] Authorization failures monitored
-   [ ] ACL changes audited
-   [ ] Recovery runbook documented

------------------------------------------------------------------------

## 19.55 Certification Master Matrix

  Concept               Question
  --------------------- ------------------------------------------------------
  TLS                   Is traffic encrypted/protected?
  CA                    Who signed the identity?
  Certificate           What identity does the endpoint present?
  Keystore              What is my identity?
  Truststore            Who do I trust?
  mTLS                  Does the broker authenticate the client certificate?
  SASL                  How is the client authenticated?
  PLAIN                 Username/password mechanism
  SCRAM                 Salted challenge-response
  GSSAPI                Kerberos
  OAUTHBEARER           Token authentication
  Principal             Which identity did Kafka establish?
  ACL                   What may that principal do?
  StandardAuthorizer    KRaft authorization
  Listener              Which network/security endpoint?
  Advertised listener   What endpoint does Kafka tell clients to use?
  Super user            Which identity bypasses ordinary ACL restrictions?
  Security migration    Can security be introduced incrementally?

------------------------------------------------------------------------

## 19.56 Final Cheat Sheet

``` text
PLAINTEXT
    no TLS
    no SASL

SSL
    TLS
    optional client certificate authentication

SASL_PLAINTEXT
    SASL authentication
    no TLS transport encryption

SASL_SSL
    SASL authentication
    TLS encryption
```

``` text
Keystore
    my private key + certificate

Truststore
    CA/certificates I trust
```

``` text
Authentication
    Who are you?

Authorization
    What may you do?
```

``` text
TLS
    transport security

SASL
    authentication

ACL
    authorization
```

``` text
KRaft
    StandardAuthorizer
```

``` text
DNS
  ↓
TCP
  ↓
TLS
  ↓
SASL
  ↓
Principal
  ↓
Metadata
  ↓
ACL
  ↓
Kafka operation
```

------------------------------------------------------------------------

## 19.57 Senior-Level Mental Model

When Kafka security breaks, ask these questions in order:

``` text
1. Where is the client connecting?

2. Can DNS resolve the endpoint?

3. Can TCP reach it?

4. Does TLS handshake?

5. Does the certificate chain validate?

6. Does hostname verification succeed?

7. Does SASL authenticate?

8. What principal did Kafka establish?

9. What broker endpoints did metadata advertise?

10. Can the client reach every required broker?

11. Does the principal have the required ACL?

12. Is the requested Kafka operation authorized?
```

This turns Kafka security troubleshooting into layered diagnosis rather
than trial-and-error configuration changes.

------------------------------------------------------------------------

## 19.58 Key Certification Traps

1.  **SASL encrypts traffic** --- false. SASL authenticates; TLS
    provides transport encryption.
2.  **Authentication grants topic access** --- false. Authorization is
    separate.
3.  **SSL and SASL_SSL are identical** --- false.
4.  **SASL_PLAINTEXT provides TLS encryption** --- false.
5.  **Truststore contains your private key** --- false.
6.  **Disable hostname verification to fix TLS** --- generally the wrong
    production solution.
7.  **`ssl.client.auth=requested` enforces mTLS** --- false.
8.  **KRaft uses the old ZooKeeper authorization configuration** --- not
    for modern KRaft deployments.
9.  **Bootstrap success proves complete Kafka connectivity** --- false.
10. **Prefix ACLs are equivalent to literal ACLs** --- false.
11. **A secure external listener automatically secures
    inter-broker/controller traffic** --- false.
12. **A successful SASL login means the application can perform every
    operation** --- false.

------------------------------------------------------------------------

## 19.59 Chapter Summary

The essential ideas are:

1.  Kafka security has distinct encryption, authentication and
    authorization concerns.
2.  TLS protects transport confidentiality and integrity and can
    authenticate peers.
3.  A keystore represents the local identity; a truststore represents
    trusted certificate authorities/certificates.
4.  Certificate SANs must align with the hostname clients actually use.
5.  Hostname verification should normally remain enabled.
6.  `SASL_SSL` combines SASL authentication with TLS transport security.
7.  `SASL_PLAINTEXT` authenticates without TLS transport encryption.
8.  PLAIN, SCRAM, GSSAPI and OAUTHBEARER are authentication mechanisms,
    not authorization systems.
9.  Authentication produces a Kafka principal.
10. ACLs authorize operations for that principal.
11. Modern KRaft deployments use `StandardAuthorizer` for Kafka's
    built-in authorization model.
12. Listener-specific security allows different security properties on
    different network paths.
13. Client, broker and controller communication are separate security
    paths.
14. Certificate and credential rotation should use overlap rather than
    abrupt replacement.
15. Least privilege is the correct production authorization model.
16. Prefix ACLs must be used carefully.
17. Security failures should be debugged in layers: DNS → TCP → TLS →
    SASL → principal → metadata → ACL.
18. Bootstrap success does not prove that every broker endpoint is
    reachable.
19. Security should be observable, testable and operationally
    maintainable.
20. Certification questions should be solved by identifying the
    protocol, identity, resource and operation involved.

------------------------------------------------------------------------

## Official Reference Areas

For exact property names and version-specific behavior, use the Apache
Kafka documentation matching the cluster version:

-   Kafka Security Overview
-   SSL Encryption and Authentication
-   SASL Authentication
-   Authorization and ACLs
-   KRaft StandardAuthorizer
-   Broker Configuration
-   Security Migration

------------------------------------------------------------------------

# Next Chapter

## Chapter 20 --- Kafka CLI, AdminClient & Certification Command Mastery

The next chapter will turn the previous architecture concepts into
command-level CCDAK/CCAAK skills:

-   `kafka-topics.sh`
-   `kafka-configs.sh`
-   `kafka-acls.sh`
-   `kafka-consumer-groups.sh`
-   `kafka-console-producer.sh`
-   `kafka-console-consumer.sh`
-   `kafka-storage.sh`
-   `kafka-reassign-partitions.sh`
-   `kafka-metadata-quorum.sh`
-   AdminClient APIs
-   security-aware CLI commands
-   cluster inspection
-   partition reassignment
-   configuration inspection
-   consumer lag analysis
-   ACL inspection
-   KRaft administration
-   troubleshooting command sequences
-   CCDAK/CCAAK command traps
-   hands-on certification scenarios

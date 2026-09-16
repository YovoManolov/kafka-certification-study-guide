# Chapter 21 — Kafka Certification Scenario Drills: Developer + Administrator

> Kafka Developer & Administrator Certification Preparation
> Based on the security concepts covered in *Kafka: The Definitive Guide*, with certification-oriented explanations,
> operational examples, troubleshooting scenarios, and exam traps.
---

## 21.1 Learning Objectives

After completing this chapter, you should be able to:

-   Diagnose Kafka incidents systematically.
-   Distinguish producer, consumer, broker, network, security,
    replication, and application failures.
-   Select the most useful first diagnostic action.
-   Avoid changing configuration before establishing evidence.
-   Reason from symptoms to likely root causes.
-   Recognize CCDAK-style developer scenarios.
-   Recognize CCAAK-style administrator scenarios.
-   Explain why an answer is correct rather than merely recognizing it.
-   Handle multi-symptom production incidents.
-   Prioritize remediation according to impact and risk.
-   Separate immediate mitigation from permanent remediation.
-   Identify certification traps.
-   Answer scenario questions efficiently under time pressure.

------------------------------------------------------------------------

## 21.2 The Certification Scenario Mindset

Kafka certification questions often look simple:

> A consumer is experiencing high lag. What should you check?

The weak approach is:

``` text
High lag → increase consumers
```

The senior approach is:

``` text
Symptom
   ↓
Define scope
   ↓
Collect evidence
   ↓
Classify failure
   ↓
Form hypotheses
   ↓
Test highest-probability hypothesis
   ↓
Mitigate
   ↓
Verify
```

Kafka problems are frequently symptoms of another bottleneck.

------------------------------------------------------------------------

## 21.3 The Universal Kafka Diagnostic Framework

Use this sequence whenever the scenario is unclear:

``` text
1. Scope
2. Connectivity
3. Metadata
4. Kafka state
5. Client state
6. Resource utilization
7. Application behavior
8. Configuration
9. Security
10. Recent changes
```

Do not mechanically execute all ten steps. Use the symptom to prioritize
them.

------------------------------------------------------------------------

## 21.4 First Question: What Changed?

A powerful production question is:

> What changed immediately before the incident?

Possible changes:

-   deployment
-   configuration
-   broker restart
-   network change
-   certificate rotation
-   ACL modification
-   topic partition increase
-   consumer scaling
-   producer change
-   schema change
-   traffic increase
-   infrastructure migration

Recent changes dramatically increase the probability of causal
relationships.

------------------------------------------------------------------------

## 21.5 Second Question: What Is the Blast Radius?

Determine whether the problem affects:

``` text
One record
One partition
One consumer
One topic
One consumer group
One broker
Multiple brokers
Entire cluster
Multiple applications
```

Blast radius is one of the fastest ways to narrow the hypothesis space.

------------------------------------------------------------------------

## Producer Scenarios

## 21.6 Scenario 1 --- Producer Cannot Connect

Symptoms:

``` text
Producer startup fails.
Connection timeout.
```

Check:

``` text
DNS
TCP
listener
advertised.listeners
security protocol
```

Do not immediately tune producer retries.

Likely causes include wrong hostname, wrong port, firewall rules,
incorrect advertised listeners, wrong security protocol, TLS failure, or
SASL failure.

------------------------------------------------------------------------

## 21.7 Scenario 2 --- Bootstrap Works, Metadata Fails

Symptoms:

``` text
Producer reaches bootstrap broker.
Then requests to other brokers fail.
```

Strong hypotheses:

``` text
advertised.listeners
routing
DNS
firewall
```

Bootstrap provides initial discovery. Kafka then returns broker
endpoints through metadata.

Key lesson:

``` text
Bootstrap connectivity ≠ cluster connectivity
```

------------------------------------------------------------------------

## 21.8 Scenario 3 --- TLS Handshake Failure

Symptoms:

``` text
SSL handshake failed
certificate validation error
```

Investigate:

``` text
CA / truststore
certificate validity
SAN / hostname
TLS configuration
listener configuration
```

Do not diagnose this as an ACL problem first. The connection has not
reached authorization.

------------------------------------------------------------------------

## 21.9 Scenario 4 --- SASL Authentication Failure

Symptoms:

``` text
Authentication failed
Invalid credentials
SASL handshake failure
```

Investigate:

``` text
security.protocol
sasl.mechanism
credentials
JAAS configuration
listener-specific SASL configuration
```

Distinguish TLS encryption from SASL authentication.

------------------------------------------------------------------------

## 21.10 Scenario 5 --- Authentication Works, Authorization Fails

If the client successfully authenticates but a produce operation is
denied, investigate:

``` text
ACL / authorization
```

Mental model:

``` text
Authentication:
"Who are you?"

Authorization:
"What are you allowed to do?"
```

------------------------------------------------------------------------

## 21.11 Scenario 6 --- Producer Cannot Write

Check in this order:

``` text
1. Can producer connect?
2. Can it authenticate?
3. Can it retrieve metadata?
4. Is topic available?
5. Is producer authorized?
6. Is partition leader available?
7. Is ISR/min.insync.replicas satisfied?
8. Is message size acceptable?
```

A replica-related write failure points toward replication/write
durability conditions rather than basic connectivity.

------------------------------------------------------------------------

## 21.12 Scenario 7 --- Producer Latency Suddenly Increases

Possible causes:

``` text
broker overload
network latency
disk pressure
batching changes
compression cost
replication pressure
leader imbalance
acks configuration
```

Do not automatically weaken durability settings to reduce latency.

------------------------------------------------------------------------

## 21.13 Scenario 8 --- Producer Throughput Is Low

Investigate:

``` text
record size
compression
batch size
linger
partition count
broker capacity
network
CPU
```

A producer may fail to batch efficiently because records arrive slowly,
traffic is spread across many partitions, or configuration prevents
efficient batching.

------------------------------------------------------------------------

## 21.14 Scenario 9 --- Producer Buffer Exhaustion

Buffer exhaustion can mean records accumulate faster than they can be
sent.

Possible causes:

``` text
broker unavailable
network bottleneck
slow acknowledgements
insufficient broker capacity
too much application concurrency
```

Increasing buffer memory may only delay the symptom.

------------------------------------------------------------------------

## 21.15 Scenario 10 --- In-Flight Requests

Changing `max.in.flight.requests.per.connection` can affect:

-   throughput
-   request concurrency
-   ordering behavior under retries

Reason about its interaction with:

``` text
retries
idempotence
ordering
in-flight requests
```

before changing it.

------------------------------------------------------------------------

## Consumer Scenarios

## 21.16 Scenario 11 --- Consumer Lag Suddenly Increases

First determine:

``` text
Is lag global or partition-specific?
```

Then inspect:

``` text
producer rate
consumer processing rate
consumer assignment
consumer errors
downstream dependency
rebalance activity
broker fetch performance
```

The fundamental relationship is:

``` text
Lag increases when:

incoming rate > processing rate
```

for a sustained period.

------------------------------------------------------------------------

## 21.17 Scenario 12 --- One Partition Has Huge Lag

Example:

``` text
P0 = 20
P1 = 30
P2 = 900000
P3 = 25
```

Likely areas:

``` text
hot key
partition skew
slow processing
poison record
consumer assignment
downstream dependency
```

Adding consumers does not automatically solve a single hot partition.

------------------------------------------------------------------------

## 21.18 Scenario 13 --- All Partitions Have Increasing Lag

This points more toward:

``` text
consumer processing capacity
downstream dependency
broker fetch performance
traffic increase
```

Compare:

``` text
producer throughput
consumer throughput
broker metrics
application latency
```

------------------------------------------------------------------------

## 21.19 Scenario 14 --- Consumer Group Rebalances Continuously

Possible causes:

-   consumer crashes
-   session timeout
-   heartbeat failure
-   network instability
-   long processing
-   consumer deployment churn
-   coordinator problems
-   resource starvation

First determine whether consumers are actually restarting. Then
investigate heartbeat/liveness and processing behavior.

------------------------------------------------------------------------

## 21.20 Scenario 15 --- Consumer Processing Is Slow

Suppose:

``` text
poll()
→ process 500 records
→ process takes 10 minutes
```

Potential issue:

``` text
consumer liveness / poll interval behavior
```

Do not blindly increase every timeout.

Consider:

``` text
bounded processing
smaller poll batches
parallel processing with careful offset management
```

while preserving correctness.

------------------------------------------------------------------------

## 21.21 Scenario 16 --- Consumer Crashes on One Record

Possible causes:

``` text
deserialization error
schema incompatibility
application bug
poison message
unexpected payload
```

Diagnostic sequence:

``` text
Identify partition
Identify offset
Inspect record
Check deserializer/schema
Inspect application logs
```

------------------------------------------------------------------------

## 21.22 Scenario 17 --- Consumer Group Has No Active Members

If lag exists but the group has no members, possible causes include:

-   application down
-   deployment issue
-   authentication failure
-   configuration failure
-   startup crash
-   network problem

Check the application before manipulating offsets.

------------------------------------------------------------------------

## 21.23 Scenario 18 --- Consumer Has Topic Access but Cannot Join Group

Possible security issue:

``` text
topic READ allowed
group authorization denied
```

This is a classic authorization distinction.

------------------------------------------------------------------------

## Replication and Broker Scenarios

## 21.24 Scenario 19 --- Broker Disk Is Filling

First determine why disk is growing.

Possible causes:

-   retention
-   compaction
-   traffic increase
-   consumer lag
-   replication
-   segment behavior
-   incorrect retention configuration

Do not manually delete Kafka log files.

------------------------------------------------------------------------

## 21.25 Scenario 20 --- Retention Appears Not to Work

Investigate:

``` text
retention.ms
retention.bytes
cleanup.policy
segment.ms
segment.bytes
topic-level overrides
broker defaults
```

Retention is evaluated around log segments, so expiration is not an
exact per-record timestamp deletion mechanism.

------------------------------------------------------------------------

## 21.26 Scenario 21 --- Compacted Topic Keeps Growing

Possible explanations:

``` text
keys are unique
tombstones have not been removed yet
compaction has not caught up
compaction configuration
segment state
```

Compaction does not mean instantaneous one-record-per-key storage.

------------------------------------------------------------------------

## 21.27 Scenario 22 --- ISR Shrinks

Example:

``` text
Replicas: 1,2,3
ISR:      1,2
```

Investigate broker 3:

``` text
CPU
disk
network
broker logs
replica fetcher health
resource saturation
```

If many partitions lose the same broker from ISR, suspect a broker-level
problem.

------------------------------------------------------------------------

## 21.28 Scenario 23 --- Multiple Brokers Leave ISR

If many partitions simultaneously show replication problems, consider:

``` text
network incident
cluster overload
disk subsystem
controller issues
large recovery event
```

The blast radius is now larger.

------------------------------------------------------------------------

## 21.29 Scenario 24 --- `min.insync.replicas` Blocks Writes

Suppose:

``` text
RF = 3
min.insync.replicas = 2
ISR = 1
```

With sufficiently strong producer acknowledgement requirements, writes
may fail because the durability condition cannot be satisfied.

This is intentional durability protection.

Do not automatically reduce `min.insync.replicas` during an incident.

------------------------------------------------------------------------

## 21.30 Scenario 25 --- Unclean Leader Election

If no in-sync replica is available, allowing an out-of-sync replica to
become leader can restore availability at the potential cost of data
loss.

Trade-off:

``` text
Availability
     ↕
Data safety
```

------------------------------------------------------------------------

## 21.31 Scenario 26 --- Broker Failure

Suppose broker 2 crashes.

Investigate:

``` text
Which partitions had broker 2 as leader?
Which had broker 2 as replica?
Did leaders move?
Did ISR recover?
Are clients reconnecting?
Is another broker overloaded?
```

A broker failure can trigger leader movement, recovery, replication
traffic, load redistribution, and client reconnection.

------------------------------------------------------------------------

## 21.32 Scenario 27 --- Broker Recovers but Cluster Is Slow

Recovery can generate significant:

``` text
replication traffic
disk IO
network IO
```

This can affect normal application traffic.

Recovery capacity must therefore be considered during capacity planning.

------------------------------------------------------------------------

## 21.33 Scenario 28 --- Hot Leader

If one broker hosts many high-volume partition leaders:

``` text
broker 2 CPU/network/request latency >> others
```

Investigate:

``` text
leader distribution
partition traffic
replica placement
```

------------------------------------------------------------------------

## 21.34 Scenario 29 --- Hot Partition

A partition receives dramatically more traffic than others.

Possible cause:

``` text
poor key distribution
```

If many records share one key:

``` text
same key → same partition
```

Kafka cannot parallelize that partition across multiple consumers in the
same group.

------------------------------------------------------------------------

## 21.35 Scenario 30 --- Adding Partitions Changes Key Distribution

Increasing partition count can change key-to-partition mapping depending
on the partitioning algorithm.

Potential effects:

-   ordering assumptions
-   stateful processing
-   locality
-   downstream partition alignment

Partition count is an architectural decision, not merely a capacity
knob.

------------------------------------------------------------------------

## Networking and Security Scenarios

## 21.36 Scenario 31 --- Network Timeout

Distinguish:

``` text
connection refused
```

from:

``` text
connection timeout
```

Connection refused usually means the host is reachable but the port is
not accepting connections.

A timeout often indicates routing, firewall, security group, or
unreachable endpoint problems.

------------------------------------------------------------------------

## 21.37 Scenario 32 --- DNS Resolves but Kafka Fails

DNS success proves only:

``` text
hostname → IP resolution
```

It does not prove:

``` text
TCP connectivity
TLS
SASL
Kafka protocol
authorization
```

Continue down the stack.

------------------------------------------------------------------------

## 21.38 Scenario 33 --- TLS Works Internally but Fails Externally

Likely areas:

``` text
external listener
advertised hostname
certificate SAN
external CA trust
load balancer
firewall/network path
```

The certificate must be valid for the hostname external clients actually
use.

------------------------------------------------------------------------

## 21.39 Scenario 34 --- SASL_SSL vs SSL

``` text
SSL
```

provides TLS encryption and can provide TLS client authentication.

``` text
SASL_SSL
```

combines:

``` text
TLS
+
SASL authentication
```

Changing one to the other changes the security handshake.

------------------------------------------------------------------------

## 21.40 Scenario 35 --- Wrong Listener Security Protocol

If a listener expects SASL but the client uses plain SSL configuration,
authentication can fail.

The client configuration must match the listener's security
expectations.

------------------------------------------------------------------------

## 21.41 Scenario 36 --- ACL Looks Correct but Access Still Fails

Investigate:

``` text
principal
resource
operation
pattern type
host restriction
super-user status
```

A common mistake is granting access to one principal while the client
actually authenticates as another.

------------------------------------------------------------------------

## 21.42 Scenario 37 --- Consumer Can Read Topic but Cannot Commit

Possible area:

``` text
consumer-group authorization
```

Topic authorization does not automatically grant every group operation.

------------------------------------------------------------------------

## 21.43 Scenario 38 --- Producer Gets Message Too Large

Relevant limits can exist at multiple levels:

``` text
producer
broker
topic
consumer
```

A consistent configuration must exist across the path.

------------------------------------------------------------------------

## Delivery Semantics and Application Scenarios

## 21.44 Scenario 39 --- Increasing JVM Heap Makes Kafka Slower

Kafka relies heavily on the OS page cache.

Excessive heap allocation can reduce memory available for:

``` text
page cache
```

and can increase:

``` text
GC
disk reads
latency
```

More heap is not automatically more Kafka performance.

------------------------------------------------------------------------

## 21.45 Scenario 40 --- CPU Is High

Determine which workload consumes it:

``` text
request processing
compression
decompression
TLS
GC
replication
```

Kafka performance tuning is workload-specific.

------------------------------------------------------------------------

## 21.46 Scenario 41 --- Disk Latency Is High

Investigate:

``` text
throughput
IOPS
queue depth
replication recovery
retention deletion
compaction
filesystem
volume characteristics
```

Do not confuse disk capacity with disk performance.

------------------------------------------------------------------------

## 21.47 Scenario 42 --- Consumer Lag Is Caused by Downstream Database

Example:

``` text
Kafka consumer
      ↓
Database write
      ↓
Database latency increases
      ↓
Consumer throughput decreases
      ↓
Kafka lag increases
```

The symptom is Kafka lag, but the root cause is the database.

------------------------------------------------------------------------

## 21.48 Scenario 43 --- Consumer Lag After Deployment

Compare:

``` text
before deployment
vs
after deployment
```

Check:

-   application latency
-   error rate
-   processing time
-   consumer count
-   assignment
-   configuration
-   serialization
-   downstream dependencies

------------------------------------------------------------------------

## 21.49 Scenario 44 --- Producer Works but Records Are Delayed

Possible causes:

``` text
linger.ms
batching
acks
broker latency
network
application flush behavior
```

Trace:

``` text
produce timestamp
broker append behavior
consumer processing
application logs
```

------------------------------------------------------------------------

## 21.50 Scenario 45 --- Ordering Requirement

Requirement:

> Events for the same account must be processed in order.

Strong design:

``` text
key = accountId
```

Same-key records are routed consistently to the same partition under
normal keyed partitioning.

Ordering is primarily partition-scoped.

------------------------------------------------------------------------

## 21.51 Scenario 46 --- Need Global Ordering

Requirement:

> Every event in the entire topic must be globally ordered.

Simplest architecture:

``` text
one partition
```

This sacrifices partition-level parallelism.

------------------------------------------------------------------------

## 21.52 Scenario 47 --- Duplicate Records

Possible causes:

``` text
producer retry
at-least-once delivery
consumer retry
application replay
offset commit timing
```

Trace where the duplicate was introduced before blaming Kafka.

------------------------------------------------------------------------

## 21.53 Scenario 48 --- Consumer Processes Record but Crashes Before Commit

Sequence:

``` text
poll
 ↓
process
 ↓
crash
 ↓
offset not committed
 ↓
record processed again
```

This is expected under at-least-once processing.

The application must tolerate duplicate processing or use an appropriate
stronger design.

------------------------------------------------------------------------

## 21.54 Scenario 49 --- Exactly-Once Requirement

For Kafka-to-Kafka read-process-write workloads, consider:

``` text
Kafka transactions
transactional producer
read-process-write
isolation.level=read_committed
```

Exactly-once Kafka processing does not automatically make arbitrary
external database side effects exactly once.

------------------------------------------------------------------------

## 21.55 Scenario 50 --- Transactional Producer Fails

Possible areas:

``` text
transactional.id
authorization
transaction coordinator
timeouts
broker availability
producer fencing
```

Incorrect reuse of transactional identities can trigger fencing.

------------------------------------------------------------------------

## 21.56 Scenario 51 --- Consumer Uses `read_uncommitted`

A consumer using:

``` text
isolation.level=read_uncommitted
```

can observe transactional records that are not committed transactional
results.

For committed-only visibility:

``` text
read_committed
```

is relevant.

------------------------------------------------------------------------

## 21.57 Scenario 52 --- Schema Compatibility Failure

Investigate:

``` text
schema evolution rule
producer schema
consumer expectations
subject strategy
serialization configuration
```

Do not bypass schema validation merely to restore deployment speed.

------------------------------------------------------------------------

## Connect, Streams, and KRaft Scenarios

## 21.58 Scenario 53 --- Kafka Connect Task Fails

Classify:

``` text
connector
task
worker
source
sink
external system
```

Check:

``` text
worker logs
connector configuration
task state
external endpoint
authentication
serialization
offset/state
```

Not every Connect problem is a broker problem.

------------------------------------------------------------------------

## 21.59 Scenario 54 --- Kafka Streams Application Falls Behind

Check:

``` text
input rate
processing latency
task assignment
state-store performance
disk
changelog traffic
rebalance
downstream sinks
```

Streams performance problems can originate outside Kafka brokers.

------------------------------------------------------------------------

## 21.60 Scenario 55 --- Streams Rebalance Loop

Potential causes:

``` text
instance instability
network issues
processing stalls
coordinator problems
deployment churn
state restoration problems
```

Check application logs and task state before modifying broker
configuration.

------------------------------------------------------------------------

## 21.61 Scenario 56 --- KRaft Metadata Quorum Problem

Symptoms:

``` text
metadata operations unstable
controller changes
cluster management operations fail
```

Investigate:

``` text
controller reachability
quorum voters
metadata leader
metadata log progression
network
disk
```

Separate metadata quorum health from partition data replication.

------------------------------------------------------------------------

## Operations and Architecture Scenarios

## 21.62 Scenario 57 --- Broker Restart During Incident

Before restarting another broker, ask:

``` text
How many replicas are already out of ISR?
What is the current RF?
What is min.insync.replicas?
What recovery is happening?
```

A second failure can turn a degraded system into an unavailable one.

------------------------------------------------------------------------

## 21.63 Scenario 58 --- Two Brokers Fail

Do not reason only from:

``` text
RF = 3
```

Examine:

``` text
which brokers host which replicas
```

Failure tolerance depends on replica placement.

------------------------------------------------------------------------

## 21.64 Scenario 59 --- Rack/AZ Awareness

If replicas are distributed across:

``` text
AZ-1
AZ-2
AZ-3
```

a single AZ failure is less likely to remove all replicas of a
partition.

Replication factor without failure-domain-aware placement is not
sufficient HA design.

------------------------------------------------------------------------

## 21.65 Scenario 60 --- Cross-Region Replication Is Slow

Possible causes:

``` text
WAN latency
bandwidth
network saturation
replication architecture
producer traffic
remote cluster capacity
```

Cross-region traffic does not have the same characteristics as intra-AZ
traffic.

------------------------------------------------------------------------

## 21.66 Scenario 61 --- Cluster Expansion

You add brokers but observe little improvement.

Possible reason:

``` text
existing partitions have not been redistributed
```

Adding brokers does not automatically guarantee balanced replica
placement.

You may need:

``` text
partition reassignment
leadership balancing
```

depending on the problem.

------------------------------------------------------------------------

## 21.67 Scenario 62 --- Too Many Partitions

Symptoms may include:

``` text
large metadata footprint
longer recovery
more open files
more replication overhead
controller pressure
```

Do not automatically increase partitions for more parallelism.

------------------------------------------------------------------------

## 21.68 Scenario 63 --- Disk Capacity Planning

Suppose:

``` text
ingress = 500 GB/day
RF = 3
retention = 7 days
```

Raw replicated storage before overhead:

``` text
500 × 7 × 3
= 10,500 GB
= 10.5 TB
```

Then add:

``` text
headroom
recovery space
segment overhead
operational safety margin
```

Never size disks to the exact calculated minimum.

------------------------------------------------------------------------

## 21.69 Scenario 64 --- Retention Calculation

If:

``` text
ingress = 100 GB/day
retention = 3 days
RF = 3
```

raw replicated data:

``` text
100 × 3 × 3 = 900 GB
```

Production sizing requires additional headroom.

------------------------------------------------------------------------

## 21.70 Scenario 65 --- Recovery Capacity

Broker failure creates:

``` text
recovery traffic
```

that competes with:

``` text
application traffic
```

Therefore capacity planning must consider:

``` text
normal traffic
+
failure recovery
```

------------------------------------------------------------------------

## 21.71 Scenario 66 --- Monitoring Shows High Request Queue

Possible areas:

``` text
CPU
request handler saturation
network processors
slow disk
broker overload
```

Use metrics to identify whether CPU, disk, or network is limiting the
broker.

------------------------------------------------------------------------

## 21.72 Scenario 67 --- Network Processor Idle Is Low

Investigate:

``` text
network throughput
request rate
connections
TLS overhead
packet size
broker capacity
```

Do not increase every broker thread count without evidence.

------------------------------------------------------------------------

## 21.73 Scenario 68 --- High GC Pause

Investigate:

``` text
heap size
allocation rate
message sizes
request volume
GC configuration
JVM version
```

Larger heap does not automatically mean better performance.

------------------------------------------------------------------------

## 21.74 Scenario 69 --- Connection Churn

Symptoms:

``` text
many connection opens/closes
CPU overhead
authentication overhead
TLS handshakes
```

Possible causes:

``` text
client lifecycle bug
short-lived clients
load balancer behavior
network instability
timeouts
```

Kafka clients should generally be long-lived.

------------------------------------------------------------------------

## 21.75 Scenario 70 --- Consumer Group Has Too Many Consumers

Suppose:

``` text
partitions = 12
consumers = 100
```

Most consumers cannot receive partitions.

Potential overhead:

``` text
heartbeats
connections
rebalances
resource usage
```

------------------------------------------------------------------------

## 21.76 Scenario 71 --- Rebalance After Every Deployment

Consumer restarts during deployment can cause expected rebalances.

If rebalances are prolonged or frequent outside deployments,
investigate:

``` text
stability
timeouts
processing
network
```

------------------------------------------------------------------------

## 21.77 Scenario 72 --- Producer Sends to Wrong Partition

Check:

``` text
record key
partitioner
partition count
custom partitioner
```

If a key is unexpectedly null, partitioning behavior can differ from
keyed routing.

------------------------------------------------------------------------

## 21.78 Scenario 73 --- Same-Key Records Are Not Ordered

Investigate:

``` text
Are all records using the same key?
Has partition count changed?
Is a custom partitioner involved?
Are multiple topics involved?
```

Kafka ordering is scoped to a partition.

------------------------------------------------------------------------

## 21.79 Scenario 74 --- Duplicate Business Effects

Trace:

``` text
record offset
processing
external side effect
offset commit
retry
```

A common sequence:

``` text
external side effect succeeds
offset commit fails
record is replayed
```

The external operation must be idempotent or coordinated appropriately.

------------------------------------------------------------------------

## 21.80 Scenario 75 --- Consumer Commits Before Processing

If the application commits the offset before business processing:

``` text
offset committed
 ↓
application crashes
 ↓
record not processed
```

This can cause application-level message loss.

------------------------------------------------------------------------

## 21.81 Scenario 76 --- At-Most-Once vs At-Least-Once

### At-most-once

``` text
commit
 ↓
process
```

Possible result:

``` text
message lost
```

### At-least-once

``` text
process
 ↓
commit
```

Possible result:

``` text
duplicate processing
```

------------------------------------------------------------------------

## 21.82 Scenario 77 --- Consumer Wants More Parallelism

Suppose:

``` text
topic = 4 partitions
consumer group = 2 consumers
```

Increasing to four consumers can increase active partition parallelism.

Increasing to twenty cannot create twenty-way partition parallelism for
four partitions.

------------------------------------------------------------------------

## 21.83 Scenario 78 --- Producer Ordering and Retries

When reliability settings change, reason about:

``` text
acks
retries
idempotence
in-flight requests
```

The objective is:

``` text
required correctness + required performance
```

------------------------------------------------------------------------

## 21.84 Scenario 79 --- Consumer Lag After Broker Failure

Broker failure can cause:

``` text
leader movement
reconnection
fetch interruption
partition recovery
```

Temporary lag may therefore be expected.

The key question:

> Does lag recover after the cluster stabilizes?

------------------------------------------------------------------------

## 21.85 Scenario 80 --- Cluster Is Healthy but Application Is Not

If Kafka metrics show healthy:

``` text
broker CPU
disk
ISR
request latency
```

but application processing is slow, investigate:

``` text
application CPU
GC
database
HTTP dependencies
thread pools
serialization
business logic
```

------------------------------------------------------------------------

## Advanced Scenario Reasoning

## 21.86 Scenario 81 --- The Best First Action

Question:

> Consumer lag is high. What should you do first?

Weak:

``` text
Increase partitions.
```

Better:

``` text
Determine whether lag is global or partition-specific and inspect consumer processing/assignment.
```

------------------------------------------------------------------------

## 21.87 Scenario 82 --- Broker Disk Exhaustion

Weak:

``` text
Delete Kafka log files.
```

Strong:

``` text
Protect the broker, determine why storage grew, inspect retention/traffic, restore safe capacity, and avoid corrupting Kafka's managed log state.
```

------------------------------------------------------------------------

## 21.88 Scenario 83 --- Producer Authorization Failure

Weak:

``` text
Restart broker.
```

Strong:

``` text
Identify authenticated principal, target topic, requested operation, and matching ACLs.
```

------------------------------------------------------------------------

## 21.89 Scenario 84 --- Bootstrap Works but Produce Fails

Strong:

``` text
Inspect metadata and advertised broker endpoints, then verify DNS/TCP/security connectivity to the returned broker.
```

------------------------------------------------------------------------

## 21.90 Scenario 85 --- ISR Shrinks

Weak:

``` text
Increase replication factor.
```

Strong:

``` text
Identify which broker/replica is leaving ISR and investigate disk, network, CPU, and broker health.
```

------------------------------------------------------------------------

## 21.91 Scenario 86 --- Kafka Throughput Is Low

Weak:

``` text
Increase broker threads.
```

Strong:

``` text
Measure producer, broker, network, disk, request, and consumer behavior to identify the actual bottleneck.
```

------------------------------------------------------------------------

## 21.92 Scenario 87 --- Broker Failure

Weak:

``` text
Immediately restart every broker.
```

Strong:

``` text
Assess replica/ISR state, affected partitions, leader movement, and remaining failure tolerance before taking additional disruptive actions.
```

------------------------------------------------------------------------

## 21.93 Scenario 88 --- Incident Prioritization

Suppose:

``` text
ISR degraded
consumer lag rising
disk 85%
```

Prioritize based on risk.

A reasonable approach is:

``` text
Protect cluster durability
↓
Prevent disk exhaustion
↓
Understand replication degradation
↓
Restore consumer processing
```

Exact priority depends on evidence and business impact.

------------------------------------------------------------------------

## 21.94 Scenario 89 --- Mitigation vs Root Cause

Example:

``` text
Consumer lag = 2 million
```

Temporary mitigation:

``` text
scale consumers
```

Root cause:

``` text
database became 10× slower
```

A mature incident response records both:

``` text
mitigation
+
root cause
```

------------------------------------------------------------------------

## 21.95 Scenario 90 --- Rollback

If an incident begins immediately after a configuration deployment and
evidence strongly indicates that configuration caused it:

``` text
rollback
```

may be safer than multiple experimental changes.

------------------------------------------------------------------------

## 21.96 Scenario 91 --- Change One Variable at a Time

Bad troubleshooting:

``` text
Increase heap
Increase threads
Increase partitions
Increase fetch size
Change acks
Restart brokers
```

Better:

``` text
Hypothesis
↓
One targeted change
↓
Observe
↓
Accept/reject hypothesis
```

------------------------------------------------------------------------

## 21.97 Scenario 92 --- Evidence Hierarchy

Useful evidence includes:

``` text
metrics
logs
Kafka metadata
consumer-group state
configuration
network traces
application traces
recent changes
```

Correlate evidence across layers.

------------------------------------------------------------------------

## 21.98 Scenario 93 --- Logs vs Metrics vs Traces

Logs answer:

``` text
What happened?
```

Metrics answer:

``` text
How often?
How much?
When?
```

Traces answer:

``` text
Where did latency travel?
```

Use all three when necessary.

------------------------------------------------------------------------

## 21.99 Scenario 94 --- Certification Time Management

For a scenario question:

### First 10 seconds

Identify the domain.

### Next 20 seconds

Identify the symptom.

### Next 30 seconds

Eliminate answers from the wrong subsystem.

Then choose the answer requiring the fewest unsupported assumptions.

------------------------------------------------------------------------

## 21.100 The Certification Elimination Method

Suppose:

``` text
A. Increase JVM heap
B. Change ACL
C. Inspect advertised.listeners
D. Increase partitions
```

Symptom:

``` text
bootstrap succeeds, broker connections timeout
```

Eliminate:

``` text
A — no evidence of GC
B — authorization occurs later
D — unrelated
```

Choose:

``` text
C
```

------------------------------------------------------------------------

## 21.101 CCDAK Scenario Pattern

Developer questions often emphasize:

``` text
producer
consumer
serialization
schemas
delivery semantics
transactions
Streams
Connect
application design
```

Think:

``` text
correctness
+
application behavior
```

------------------------------------------------------------------------

## 21.102 CCAAK Scenario Pattern

Administrator questions often emphasize:

``` text
brokers
replication
partitions
KRaft
security
networking
configuration
observability
operations
disaster recovery
```

Think:

``` text
cluster state
+
operational safety
```

------------------------------------------------------------------------

## 21.103 Mixed Scenario Pattern

Some questions cross both domains.

For example:

``` text
Consumer lag increases
```

Possible causes span:

``` text
consumer code
broker performance
network
database
partition distribution
security
```

Do not assume the question belongs to only one subsystem.

------------------------------------------------------------------------

## 21.104 The 12 High-Value Diagnostic Questions

Memorize these:

1.  What changed?
2.  What is the blast radius?
3.  Is it one partition or many?
4.  Is the broker healthy?
5.  Is the network healthy?
6.  Is metadata correct?
7.  Is authentication successful?
8.  Is authorization successful?
9.  Is replication healthy?
10. Is the client healthy?
11. Is a downstream dependency slow?
12. Can the system recover without intervention?

------------------------------------------------------------------------

## 21.105 Final 20-Second Mental Model

When the exam gives you a Kafka incident:

``` text
SYMPTOM
   ↓
SCOPE
   ↓
LAYER
   ↓
EVIDENCE
   ↓
HYPOTHESIS
   ↓
LOWEST-RISK ACTION
   ↓
VERIFY
```

Do not jump directly from symptom to configuration change.

------------------------------------------------------------------------

## 21.106 Certification Trap Matrix

  ------------------------------------------------------------------------------
  Symptom                 Common Wrong Answer     Better Direction
  ----------------------- ----------------------- ------------------------------
  Bootstrap works, broker Restart client          advertised listeners
  fails                                           

  Auth succeeds, write    Fix TLS                 ACL
  denied                                          

  One partition lagging   Add consumers           key/partition skew

  ISR shrinks             Increase RF             broker health

  Disk fills              Delete log files        retention/storage analysis

  More consumers than     Add more consumers      partition parallelism
  partitions                                      

  High heap               Increase heap           investigate GC/page cache

  Low throughput          Increase threads        identify bottleneck

  Duplicate processing    Kafka is broken         delivery semantics

  Lag after DB slowdown   Tune Kafka              fix downstream dependency

  Topic config ignored    Edit server.properties  inspect dynamic override

  Broker added but load   Add more brokers        rebalance replicas
  remains                                         

  TLS external failure    Change ACL              certificate/listener/network

  KRaft issue             Inspect consumer lag    inspect metadata quorum
  ------------------------------------------------------------------------------

------------------------------------------------------------------------

## 21.107 Final Exam Checklist

Before selecting an answer:

``` text
[ ] What component is failing?
[ ] What evidence is provided?
[ ] Is the failure connectivity, security, Kafka state, or application?
[ ] Is the symptom global or localized?
[ ] What is the least invasive diagnostic step?
[ ] Is the proposed fix addressing the cause?
[ ] Could the proposed fix reduce durability?
[ ] Could it cause data loss?
[ ] Could it increase recovery pressure?
[ ] Is there a safer alternative?
```

------------------------------------------------------------------------

## 21.108 Master Cheat Sheet

## Producer

``` text
connect
→ metadata
→ partition
→ send
→ acknowledgement
```

Investigate:

``` text
network
metadata
partition leader
acks
ISR
broker
buffer
batching
```

## Consumer

``` text
connect
→ metadata
→ group coordination
→ assignment
→ fetch
→ process
→ commit
```

Investigate:

``` text
group
assignment
lag
poll/processing
broker fetch
downstream
offsets
```

## Broker

``` text
network
request handling
disk
replication
leadership
metadata
```

## Security

``` text
TCP
→ TLS
→ SASL
→ Kafka protocol
→ ACL
```

## Replication

``` text
leader
replicas
ISR
min.insync.replicas
acks
```

## KRaft

``` text
controllers
→ metadata quorum
→ cluster metadata
```

------------------------------------------------------------------------

## 21.109 Senior-Level Principle

The strongest answer in a Kafka certification scenario is rarely:

> Change X.

It is usually:

> First establish whether X is actually the bottleneck or failure
> domain, using the evidence provided.

That distinction separates configuration memorization from engineering
judgment.

------------------------------------------------------------------------

## 21.110 Chapter Summary

The core method is:

``` text
Understand the symptom
        ↓
Determine scope
        ↓
Identify the subsystem
        ↓
Gather evidence
        ↓
Form a hypothesis
        ↓
Choose the safest effective action
        ↓
Verify recovery
```

For CCDAK, prioritize:

``` text
producer
consumer
delivery semantics
schemas
transactions
Streams
Connect
application correctness
```

For CCAAK, prioritize:

``` text
cluster
replication
KRaft
security
networking
configuration
observability
troubleshooting
operations
```

The ultimate certification skill is not remembering the most commands.

It is being able to look at a Kafka symptom and immediately ask:

> Which layer could produce this symptom, what evidence would
> distinguish the possibilities, and what is the safest next action?

------------------------------------------------------------------------

## 21.111 Final Challenge --- Senior Certification Drill

For each incident below, answer in under 60 seconds.

### A

``` text
Producer connects to bootstrap but fails after metadata retrieval.
```

Identify the most likely layer and first diagnostic.

### B

``` text
Consumer group lag is huge only on partition 7.
```

Identify likely causes and first diagnostic.

### C

``` text
Authentication succeeds but producer receives authorization denied.
```

Identify the security layer.

### D

``` text
RF=3, min.insync.replicas=2, ISR=1.
```

Identify why writes may fail.

### E

``` text
Broker disk latency increases after a broker failure.
```

Identify why recovery may be responsible.

### F

``` text
Topic retention was changed in server.properties but effective topic behavior did not change.
```

Identify what to inspect.

### G

``` text
20 consumers, 4 partitions.
```

Identify the parallelism limit.

### H

``` text
Kafka broker metrics are healthy but consumer processing latency is 10× higher.
```

Identify where to investigate.

### I

``` text
TLS works internally but fails for external clients.
```

Identify likely areas.

### J

``` text
AdminClient topic creation times out.
```

Identify why blindly retrying may be unsafe.

------------------------------------------------------------------------

## 21.112 Answers to the Final Challenge

### A

Likely:

``` text
metadata / advertised endpoint / network
```

First inspect:

``` text
advertised.listeners
DNS
TCP reachability to returned broker endpoints
```

### B

Likely:

``` text
hot partition
key skew
slow processing
poison record
```

First inspect:

``` text
partition assignment + application processing
```

### C

``` text
authorization / ACL
```

Authentication has already succeeded.

### D

Only one ISR remains while two are required. With sufficiently strong
producer acknowledgement requirements, writes can fail because the
durability condition cannot be satisfied.

### E

Broker recovery can generate:

``` text
disk IO
network IO
replication work
```

which competes with normal traffic.

### F

Inspect:

``` text
topic-level dynamic configuration
```

and effective configuration.

### G

At most approximately:

``` text
4 active partition assignments
```

for that topic within the group.

### H

Investigate:

``` text
application
downstream dependencies
CPU
GC
thread pools
database/HTTP calls
```

before tuning Kafka.

### I

Inspect:

``` text
external listener
advertised hostname
certificate SAN
external CA trust
load balancer
firewall/network path
```

### J

The request may have succeeded while the response was lost.

Therefore:

``` text
timeout
→ inspect current state
→ reconcile
```

rather than blindly assuming failure.

------------------------------------------------------------------------

## 21.113 Next Chapter

## Chapter 22 --- Kafka Certification Mock Exam #1: Developer Fundamentals + Producer/Consumer

The next chapter switches from learning mode to exam mode.

It will contain:

-   50 certification-style questions
-   CCDAK-oriented difficulty
-   producer questions
-   consumer questions
-   partitions
-   offsets
-   consumer groups
-   serialization
-   schemas
-   delivery semantics
-   idempotent producers
-   transactions
-   ordering
-   retries
-   error handling
-   troubleshooting
-   detailed answer explanations
-   certification traps
-   score interpretation
-   senior-level reasoning

Recommended exam procedure:

``` text
Round 1:
Answer without looking at explanations.

Round 2:
Review incorrect answers.

Round 3:
Explain why each wrong option is wrong.

Target:
≥ 80% before moving to the next mock exam.
```

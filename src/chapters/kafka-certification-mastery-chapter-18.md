# Chapter 18 — Kafka Production Configuration, Tuning & Configuration Reference

## Kafka Developer (CCDAK) & Kafka Administrator (CCAAK) Certification Book

> **Certification focus:** production configuration, 
> performance tuning, capacity planning, reliability, networking, 
> dynamic configuration, and scenario-based troubleshooting.

---

## 17.1 Learning Objectives

By the end of this chapter, you should be able to:

- Explain Kafka configuration layers and precedence.
- Distinguish KRaft configuration from legacy ZooKeeper-era configuration.
- Design storage, retention, segmentation, and compaction settings.
- Reason about replication factor, ISR, `min.insync.replicas`, `acks`, and unclean leader election.
- Configure listeners and understand `listeners` versus `advertised.listeners`.
- Tune producer batching, compression, buffering, and in-flight requests.
- Tune consumer fetch behavior.
- Understand Kafka's JVM heap and operating-system page-cache relationship.
- Perform capacity planning for brokers, partitions, disks, network, and recovery.
- Diagnose hot partitions and consumer-group scaling limits.
- Use observability before changing configuration.
- Apply dynamic broker configuration safely.
- Recognize certification traps involving configuration trade-offs.

---

## 17.2 The Golden Rule of Kafka Tuning

> **Do not tune Kafka because a configuration looks suboptimal. Tune Kafka because measurements demonstrate a bottleneck or because a business requirement requires a specific behavior.**

A useful engineering loop is:

```text
Requirement
   ↓
Architecture
   ↓
Capacity
   ↓
Configuration
   ↓
Observability
   ↓
Measurement
   ↓
Controlled tuning
   ↓
Validation
```

---

## 17.3 Kafka Configuration Layers

Kafka configuration exists at multiple scopes:

- Broker configuration
- Topic configuration
- Producer configuration
- Consumer configuration
- Dynamic cluster/broker configuration

Typical broker properties:

```properties
num.network.threads
num.io.threads
log.dirs
num.partitions
default.replication.factor
min.insync.replicas
auto.create.topics.enable
```

Typical topic properties:

```properties
retention.ms
retention.bytes
segment.bytes
cleanup.policy
min.insync.replicas
compression.type
```

Typical producer properties:

```properties
acks
batch.size
linger.ms
compression.type
buffer.memory
delivery.timeout.ms
```

Typical consumer properties:

```properties
fetch.min.bytes
fetch.max.wait.ms
fetch.max.bytes
max.partition.fetch.bytes
```

For applicable KRaft settings, dynamic configuration can take precedence over static configuration. Always verify the exact property and Kafka version.

---

## 17.4 KRaft Configuration

Modern Kafka deployments use KRaft rather than ZooKeeper.

Important concepts:

```text
node.id
process.roles
controller quorum
listeners
advertised.listeners
```

Combined node:

```properties
process.roles=broker,controller
```

Dedicated broker:

```properties
process.roles=broker
```

Dedicated controller:

```properties
process.roles=controller
```

### Certification distinction

| KRaft | Legacy ZooKeeper |
|---|---|
| `node.id` | `broker.id` |
| KRaft controller quorum | ZooKeeper ensemble |
| `process.roles` | Broker role |
| No ZooKeeper dependency | `zookeeper.connect` |
| Controller quorum metadata | ZooKeeper metadata |

---

## 17.5 Storage Configuration

Kafka is a disk-backed distributed log.

Key property:

```properties
log.dirs=/var/lib/kafka/data
```

Multiple directories can be configured:

```properties
log.dirs=/data1/kafka,/data2/kafka,/data3/kafka
```

Storage planning must consider:

- capacity
- throughput
- latency
- filesystem performance
- replication traffic
- retention
- compaction
- recovery
- headroom

Disk capacity alone does not prove sufficient performance.

---

## 17.6 Disk Capacity Planning

A simplified model:

```text
Required physical storage
≈
Logical retained data
×
Replication factor
+
Operational headroom
```

Example:

```text
Logical retention = 4 TB
Replication factor = 3

Replica storage ≈ 12 TB
```

Then add headroom for:

- segment lifecycle
- recovery
- uneven distribution
- broker failure
- maintenance
- growth

Avoid designing production storage to run continuously near full capacity.

---

## 17.7 Retention

Common retention settings:

```properties
retention.ms
retention.bytes
```

Retention is segment-oriented.

Therefore:

```text
record age
≠
exact deletion time
```

A record is part of a segment, and eligible segments are cleaned asynchronously.

---

## 17.8 Log Segments

Important settings:

```properties
segment.bytes
segment.ms
```

Smaller segments can make cleanup more responsive but increase file and metadata overhead.

Larger segments reduce file-management overhead but can delay cleanup.

Mental model:

```text
Segment roll
     ↓
Segment becomes eligible
     ↓
Deletion / compaction
```

---

## 17.9 Cleanup Policies

Possible policies include:

```properties
cleanup.policy=delete
```

```properties
cleanup.policy=compact
```

```properties
cleanup.policy=compact,delete
```

### Delete

Old data is removed according to retention.

### Compact

Kafka eventually retains the latest value for each key according to compaction semantics.

### Compact + delete

Combines key-based compaction with retention-based deletion.

---

## 17.10 Log Compaction

Compaction is useful for state topics.

Example:

```text
user-123 -> ACTIVE
user-123 -> PREMIUM
user-123 -> SUSPENDED
```

Older values may eventually be removed so the latest state remains.

Compaction is asynchronous.

Important concepts:

- active segment
- cleaner threads
- compaction backlog
- dirty ratio
- tombstones

A tombstone represents deletion of a key.

---

## 17.11 Reliability: Replication Factor

Replication factor defines how many replicas of a partition exist.

Example:

```text
RF = 3

Leader
Follower 1
Follower 2
```

Higher RF generally increases durability but also increases:

- disk usage
- network traffic
- replication work
- recovery traffic
- storage cost

---

## 17.12 ISR — In-Sync Replicas

ISR means **In-Sync Replicas**.

With:

```text
RF = 3
```

the ISR may be:

```text
Leader
Follower A
Follower B
```

After a follower falls behind:

```text
ISR = {Leader, Follower A}
```

Remember:

```text
Replica count = configured replicas
ISR count     = currently in-sync replicas
```

---

## 17.13 `min.insync.replicas`

Example:

```text
RF = 3
min.insync.replicas = 2
acks = all
```

Normal state:

```text
ISR = 3
```

One broker fails:

```text
ISR = 2
```

Writes can continue.

A second failure:

```text
ISR = 1
```

Writes requiring two ISR replicas can fail.

Key distinction:

```text
RF ≠ min.insync.replicas
```

---

## 17.14 `acks`

Producer acknowledgment modes:

```text
acks=0
acks=1
acks=all
```

### `acks=0`

No broker acknowledgment is awaited.

Low latency is possible, but durability guarantees are weak.

### `acks=1`

The leader acknowledges after accepting the record.

Potential failure:

```text
Leader accepts record
       ↓
Leader fails before replication
       ↓
Record may be lost
```

### `acks=all`

The leader waits for the required ISR acknowledgments.

Strong durability requires reasoning about:

```text
RF
ISR
min.insync.replicas
leader election
failure domains
```

`acks=all` alone is not a universal no-data-loss guarantee.

---

## 17.15 Unclean Leader Election

Key property:

```properties
unclean.leader.election.enable=false
```

Clean election favors durability:

```text
data safety
    ↓
possible partition unavailability
```

Unclean election favors availability:

```text
availability
    ↓
possible data loss
```

Certification mental model:

```text
Clean election
→ durability first

Unclean election
→ availability first
```

---

## 17.16 Failure-Domain Awareness

RF alone does not guarantee resilience against an entire rack or availability-zone failure.

Bad:

```text
Broker 1 → AZ-A
Broker 2 → AZ-A
Broker 3 → AZ-A
```

Better:

```text
Broker 1 → AZ-A
Broker 2 → AZ-B
Broker 3 → AZ-C
```

Therefore:

```text
Replication factor
+
Failure-domain awareness
=
meaningful fault tolerance
```

---

## 17.17 Auto Topic Creation

Many production environments prefer:

```properties
auto.create.topics.enable=false
```

This prevents accidental topic creation caused by client typos.

Explicit provisioning improves governance.

---

## 17.18 Listener Architecture

Two concepts are essential:

```text
listeners
advertised.listeners
```

### `listeners`

Where the broker binds.

Example:

```properties
listeners=INTERNAL://0.0.0.0:9092
```

### `advertised.listeners`

The addresses returned to clients.

Example:

```properties
advertised.listeners=INTERNAL://broker-1.kafka.local:9092
```

These are not interchangeable.

---

## 17.19 Classic `advertised.listeners` Failure

A client can bootstrap successfully and then fail to connect:

```text
bootstrap server
      ↓
metadata request
      ↓
Kafka returns broker addresses
      ↓
client connects to advertised broker
      ↓
FAIL
```

Possible causes:

- incorrect hostname
- DNS failure
- unreachable private IP
- firewall
- NAT
- container networking
- TLS hostname mismatch

Successful bootstrap therefore does not prove end-to-end connectivity.

---

## 17.20 Internal and External Listeners

Example:

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
```

```properties
advertised.listeners=INTERNAL://broker-1.kafka.local:9092,EXTERNAL://kafka.example.com:9094
```

Internal clients use the internal address.

External clients use the external address.

This is common in cloud and containerized deployments.

---

## 17.21 Listener Security Protocol Mapping

Example:

```properties
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:SSL
```

This maps:

```text
INTERNAL → PLAINTEXT
EXTERNAL → SSL
```

Other production designs may use:

```text
SSL
SASL_SSL
```

---

## 17.22 Broker Threads

Important settings:

```properties
num.network.threads
num.io.threads
```

Do not increase them blindly.

If disk I/O is the bottleneck, more network threads will not solve the problem.

If CPU is already saturated, adding threads may increase contention.

---

## 17.23 CPU

Kafka CPU can be consumed by:

- request processing
- compression
- decompression
- TLS
- replication
- compaction
- networking
- garbage collection
- background operations

Compression illustrates the trade-off:

```text
Compression
=
CPU ↔ network/storage
```

---

## 17.24 Producer Configuration

Important settings:

```properties
acks
compression.type
batch.size
linger.ms
buffer.memory
max.in.flight.requests.per.connection
request.timeout.ms
delivery.timeout.ms
max.request.size
```

Producer performance is strongly influenced by batching and compression.

---

## 17.25 `linger.ms`

`linger.ms` allows the producer to wait briefly for more records so batches can become larger.

Conceptually:

```text
record arrives
      ↓
brief wait
      ↓
more records arrive
      ↓
larger batch
      ↓
send
```

Higher values can improve batching and throughput but may increase latency.

Kafka defaults are version-dependent. For example, Kafka 4.0 changed the default `linger.ms` from 0 ms to 5 ms.

---

## 17.26 `batch.size`

`batch.size` controls the target maximum size of a producer batch per partition.

Larger batches can improve:

- compression
- network efficiency
- throughput

But can also increase:

- memory usage
- latency
- inefficiency under sparse traffic

Increasing `batch.size` does not guarantee larger batches if traffic is sparse.

---

## 17.27 Producer Buffer Memory

`buffer.memory` provides producer memory for records waiting to be sent.

When the producer cannot send quickly enough:

```text
application
    ↓
producer buffer
    ↓
buffer fills
    ↓
backpressure
```

Producer-side latency can therefore be caused by a slow broker or network.

---

## 17.28 In-Flight Requests

`max.in.flight.requests.per.connection` limits unacknowledged requests outstanding on one connection.

Higher concurrency can improve throughput.

But ordering and retry behavior must be considered together with:

- idempotence
- retries
- ordering requirements
- delivery semantics

---

## 17.29 Consumer Fetch Configuration

Important settings:

```properties
fetch.min.bytes
fetch.max.wait.ms
fetch.max.bytes
max.partition.fetch.bytes
```

Main trade-off:

```text
latency
versus
throughput
```

---

## 17.30 `fetch.min.bytes`

Larger values can improve throughput by reducing request frequency.

Potential downside:

```text
larger minimum
→ potentially more waiting
→ higher latency
```

---

## 17.31 `fetch.max.wait.ms`

If enough data is not immediately available to satisfy `fetch.min.bytes`, the broker can wait up to `fetch.max.wait.ms`.

Consider both settings together.

---

## 17.32 `max.partition.fetch.bytes`

This limits the amount of data returned for a partition in a fetch.

It matters for workloads with large records or high per-partition throughput.

The effective fetch behavior depends on multiple total and per-partition limits.

---

## 17.33 Message Size Limits

Large messages require coordinated configuration across:

```text
Producer
Broker
Topic
Consumer
```

Relevant settings can include:

```text
producer max.request.size
broker message.max.bytes
topic max.message.bytes
consumer max.partition.fetch.bytes
```

Always verify exact limits and relationships for the Kafka version being used.

---

## 17.34 JVM Heap and Page Cache

Kafka memory is not simply:

```text
RAM = JVM heap
```

Consider:

```text
JVM heap
+
OS page cache
+
native memory
+
network buffers
+
other process memory
```

Kafka benefits heavily from OS page caching.

Therefore:

> Excessively increasing JVM heap can reduce Kafka performance by starving the page cache.

---

## 17.35 Garbage Collection

Excessive heap pressure can cause:

```text
GC activity
    ↓
CPU consumption
    ↓
request latency
    ↓
replication delays
    ↓
ISR shrink
    ↓
consumer lag
```

Do not automatically increase heap.

Investigate:

- allocation rate
- GC pause time
- heap occupancy
- object churn
- page-cache pressure
- workload characteristics

---

## 17.36 File Descriptors

Kafka opens many files and network connections.

Requirements grow with:

- partitions
- log segments
- broker connections
- replicas
- clients
- internal operations

Production systems should configure sufficient OS limits and monitor them.

---

## 17.37 Connection Capacity

Kafka brokers handle connections from:

- producers
- consumers
- AdminClients
- replication
- monitoring
- Connect workers
- Streams applications

Connection count is therefore a capacity-planning dimension.

---

## 17.38 Capacity Planning

Include:

```text
Ingress
Egress
Replication
Disk
CPU
Memory
Network
Partitions
Connections
Recovery
Growth
```

If a topic receives:

```text
500 MB/s
```

with RF=3, the cluster must handle substantially more internal traffic than producer ingress alone because replicas also receive data.

---

## 17.39 Partition Capacity Planning

Suppose:

```text
Required throughput = 600 MB/s
Validated partition capacity = 20 MB/s
```

Initial estimate:

```text
600 / 20 = 30 partitions
```

But validate:

- key distribution
- producer parallelism
- consumer parallelism
- broker placement
- recovery time
- partition overhead
- growth
- hot-key behavior

---

## 17.40 Hot Partitions

If one key dominates:

```text
key = customer-123
```

traffic may become highly skewed:

```text
Partition 0 → 90%
Partition 1 → 3%
Partition 2 → 2%
Partition 3 → 5%
```

Adding consumers does not solve the hot partition.

The partitioning strategy must be investigated.

Potential solutions include:

- changing the partitioning key
- controlled key sharding
- workload redesign
- increasing partitions where appropriate

Changing partitions can affect key mapping and ordering assumptions.

---

## 17.41 Consumer Group Scaling

Fundamental rule:

```text
Maximum active consumer parallelism
≤
number of partitions
```

Example:

```text
12 partitions
20 consumers
```

Only up to 12 consumers can actively own partitions.

Therefore:

```text
More consumers
≠
automatically more throughput
```

---

## 17.42 Replication and Recovery Capacity

Kafka must be sized for degraded operation as well as normal operation.

Failure can trigger:

```text
Broker failure
      ↓
Replica recovery
      ↓
Network traffic
      ↓
Disk writes
      ↓
CPU utilization
```

If normal utilization is already near maximum, recovery can overload the remaining brokers.

This creates:

```text
failure
 ↓
recovery traffic
 ↓
resource saturation
 ↓
slower recovery
 ↓
longer degraded period
```

---

## 17.43 Headroom

Headroom protects against:

- traffic spikes
- broker failure
- rebalancing
- replica recovery
- compaction
- maintenance
- rolling upgrades
- unexpected client behavior

A realistic capacity model is:

```text
normal traffic
+
growth
+
failure scenario
+
operational margin
```

---

## 17.44 Performance Tuning Order

A strong sequence is:

```text
1. Architecture
2. Failure domains
3. Partitioning
4. Replication
5. Storage
6. Network
7. Producer batching/compression
8. Consumer fetching
9. Broker threads
10. JVM/OS
11. Micro-optimizations
```

---

## 17.45 Producer Throughput Investigation

Investigate:

```text
Producer CPU
Producer network
Broker BytesIn
Request latency
Request queue time
Compression ratio
Batch size
Record size
Retries
Error rate
Buffer exhaustion
```

Possible diagnoses:

```text
Low batch size
→ insufficient batching

High compression CPU
→ producer CPU bottleneck

High broker request time
→ broker bottleneck

High buffer wait
→ broker/network slower than producer
```

---

## 17.46 Consumer Throughput Investigation

Investigate:

```text
consumer lag
fetch latency
fetch size
consumer CPU
deserialization
application processing
broker BytesOut
partition distribution
rebalances
```

If application processing is slow, increasing broker fetch size may not solve the bottleneck.

---

## 17.47 Observability Before Tuning

Useful metric categories:

### Traffic

```text
BytesInPerSec
BytesOutPerSec
MessagesInPerSec
```

### Request performance

```text
RequestQueueTime
LocalTime
TotalTime
```

### Network

```text
NetworkProcessorAvgIdlePercent
```

### Request handling

```text
RequestHandlerAvgIdlePercent
```

### Replication

```text
UnderReplicatedPartitions
IsrShrinksPerSec
IsrExpandsPerSec
```

### Availability

```text
OfflinePartitionsCount
```

### Consumer

```text
Consumer lag
```

### JVM

```text
GC pauses
heap usage
allocation rate
```

### Storage

```text
disk utilization
disk latency
disk throughput
filesystem capacity
```

---

## 17.48 Request Handler Idle

Low request-handler idle time can indicate broker request-processing pressure.

Do not immediately increase threads.

Determine why requests are expensive first:

- disk
- CPU
- compression
- large requests
- replication
- overloaded partitions

---

## 17.49 Network Processor Idle

Low network processor idle time can indicate network-thread pressure.

Investigate:

- connection count
- request rate
- payload size
- network throughput
- TLS overhead
- thread configuration

---

## 17.50 Replication Throttling

Replication traffic can compete with client traffic.

During:

- partition reassignment
- broker recovery
- cluster expansion

replication traffic may need throttling.

The objective is:

```text
Recovery speed
versus
Client availability
```

Aggressive recovery can saturate disks and networks and hurt production traffic.

---

## 17.51 Dynamic Configuration

Kafka supports dynamic configuration for many properties.

Typical commands:

```bash
kafka-configs.sh   --bootstrap-server broker:9092   --entity-type brokers   --entity-name 1   --alter   --add-config '...'
```

Inspect:

```bash
kafka-configs.sh   --bootstrap-server broker:9092   --entity-type brokers   --entity-name 1   --describe
```

Remove an override:

```bash
kafka-configs.sh   --bootstrap-server broker:9092   --entity-type brokers   --entity-name 1   --alter   --delete-config '...'
```

Not every configuration is dynamically changeable.

---

## 17.52 Production Configuration Baseline

### Architecture

- KRaft topology
- broker/controller roles
- failure domains
- networking
- security

### Storage

- `log.dirs`
- disk type
- capacity
- filesystem
- retention
- segment policy

### Reliability

- replication factor
- `min.insync.replicas`
- `acks`
- unclean election policy
- failure-domain awareness

### Networking

- listeners
- advertised listeners
- security protocol mapping
- DNS
- firewall rules

### Performance

- producer batching
- compression
- consumer fetch configuration
- broker thread configuration

### Operations

- metrics
- alerting
- DR strategy
- upgrade procedure
- capacity thresholds

---

## 17.53 Configuration Change Procedure

A production configuration change should follow:

```text
1. Define objective
2. Capture baseline metrics
3. Identify expected side effects
4. Determine rollback
5. Change smallest necessary scope
6. Monitor
7. Compare metrics
8. Validate functional behavior
9. Document
```

---

## 17.54 Scenario Drill — Producer Latency

Metrics:

```text
Broker request queue time: high
Network idle: healthy
Producer CPU: healthy
Disk latency: high
```

Diagnosis:

```text
storage pressure
```

Do not immediately increase `linger.ms`.

Investigate:

- disk latency
- disk utilization
- compaction
- replication recovery
- retention cleanup
- overloaded partitions

---

## 17.55 Scenario Drill — Disk Bottleneck

Symptoms:

```text
disk utilization ≈ 100%
disk latency = high
CPU = moderate
network = moderate
```

Diagnosis:

```text
storage bottleneck
```

Possible actions:

- improve storage
- distribute workload
- review partition placement
- review compaction
- review retention
- check recovery/reassignment

Increasing network threads is unlikely to solve the root cause.

---

## 17.56 Scenario Drill — Network Bottleneck

Symptoms:

```text
network utilization = very high
disk = healthy
CPU = moderate
```

Investigate:

- producer traffic
- consumer traffic
- replication
- recovery
- large records
- network capacity

Compression may reduce network usage but increases CPU cost.

---

## 17.57 Scenario Drill — Hot Partition

One partition has dramatically higher traffic and corresponding consumer lag.

Diagnosis:

```text
partition-level skew
```

Wrong answer:

```text
Add more consumers.
```

The hot partition remains a single partition assignment within the consumer group.

---

## 17.58 Scenario Drill — Broker Failure

Configuration:

```text
RF=3
min.insync.replicas=2
acks=all
```

One broker fails:

```text
ISR: 3 → 2
```

Writes can continue.

A second failure may produce:

```text
ISR: 2 → 1
```

Writes requiring two ISR replicas can fail.

---

## 17.59 Scenario Drill — Two Failures

Configuration:

```text
RF=3
min.insync.replicas=2
acks=all
unclean.leader.election.enable=false
```

Two brokers fail.

If only one ISR remains:

```text
ISR=1
```

Kafka prioritizes data safety.

Potential result:

```text
partition unavailable
```

rather than electing an out-of-sync replica and risking data loss.

---

## 17.60 Scenario Drill — Retention Seems Late

A topic has:

```text
retention.ms=1 hour
```

A record older than one hour is still on disk.

Diagnosis:

- retention is segment-oriented
- cleanup is asynchronous
- the segment may still be active or not yet eligible

Check:

- segment roll
- active segments
- cleanup activity
- disk state

---

## 17.61 Scenario Drill — Bootstrap Works, Broker Connections Fail

Symptoms:

```text
Bootstrap succeeds
Metadata succeeds
Broker connection fails
```

First suspect:

```text
advertised.listeners
```

Check:

- hostname
- DNS
- routing
- firewall
- NAT
- container networking
- TLS certificate names

---

## 17.62 Scenario Drill — `server.properties` Appears Ignored

Static configuration:

```properties
some.property=value-A
```

Runtime behavior:

```text
value-B
```

Investigate dynamic configuration.

Always determine whether the property has a dynamic override before assuming the static file is authoritative.

---

## 17.63 Scenario Drill — Increasing Heap Hurts Performance

Possible explanation:

```text
larger heap
→ less OS page cache
→ more disk reads
→ worse I/O behavior
```

Kafka memory must be considered as:

```text
heap
+
page cache
+
native memory
+
network buffers
```

---

## 17.64 Certification Traps

1. **More partitions always improve performance.**  
   False. They add parallelism and overhead.

2. **Higher replication is free.**  
   False. It increases storage, network, and recovery costs.

3. **Increase JVM heap whenever Kafka is slow.**  
   False. The bottleneck may be page cache, disk, network, CPU, or partition skew.

4. **Increase all broker thread counts.**  
   False. Thread tuning should follow measurements.

5. **`listeners` and `advertised.listeners` are the same.**  
   False.

6. **`min.insync.replicas` is the same as RF.**  
   False.

7. **`acks=all` guarantees no data loss under every failure.**  
   False.

8. **Retention deletes records exactly when their timestamp expires.**  
   False.

9. **More consumers always improve throughput.**  
   False.

10. **Dynamic configuration always overrides everything.**  
    False. Scope and property support matter.

---

## 17.65 Production Readiness Checklist

### Architecture

- [ ] KRaft topology documented
- [ ] Controller quorum designed
- [ ] Broker placement documented
- [ ] Failure domains considered
- [ ] Capacity growth model defined

### Storage

- [ ] Disk throughput tested
- [ ] Disk latency monitored
- [ ] Capacity threshold defined
- [ ] Retention defined
- [ ] Segment strategy reviewed
- [ ] Compaction workloads understood

### Reliability

- [ ] RF selected intentionally
- [ ] ISR monitored
- [ ] `min.insync.replicas` reviewed
- [ ] Producer `acks` reviewed
- [ ] Unclean leader election policy documented
- [ ] Failure-domain awareness configured

### Networking

- [ ] Listeners documented
- [ ] Advertised listeners tested from each client network
- [ ] DNS verified
- [ ] Firewall rules verified
- [ ] TLS/SASL configuration tested

### Performance

- [ ] Producer batching measured
- [ ] Compression evaluated
- [ ] Consumer fetch behavior measured
- [ ] Broker thread utilization monitored
- [ ] CPU headroom verified
- [ ] Network headroom verified
- [ ] Storage headroom verified

### Operations

- [ ] Metrics available
- [ ] Consumer lag monitored
- [ ] ISR shrink alerts configured
- [ ] Offline partition alerts configured
- [ ] Disk alerts configured
- [ ] GC monitored
- [ ] Recovery procedures tested
- [ ] Configuration changes documented

---

## 17.66 Certification Master Matrix

| Concept | What to Remember | Typical Trap |
|---|---|---|
| `node.id` | KRaft node identity | Confusing with `broker.id` |
| `process.roles` | KRaft node roles | Mixing KRaft and ZooKeeper |
| `log.dirs` | Broker storage | Ignoring disk performance |
| `retention.ms` | Time-based retention | Assuming exact deletion |
| `retention.bytes` | Size-based retention | Ignoring segments |
| `cleanup.policy` | Delete/compact combinations | Assuming compaction is immediate |
| RF | Number of replicas | Confusing with ISR |
| ISR | In-sync replicas | Assuming all replicas are in sync |
| `min.insync.replicas` | Minimum ISR requirement | Treating it as RF |
| `acks` | Producer acknowledgment | Assuming `all` solves every failure |
| Unclean election | Availability vs durability | Ignoring data-loss risk |
| `listeners` | Bind addresses | Confusing with advertised addresses |
| `advertised.listeners` | Client-visible addresses | Bootstrap-success trap |
| `batch.size` | Producer batch target | Assuming larger is always better |
| `linger.ms` | Producer batching delay | Ignoring latency |
| `buffer.memory` | Producer buffering | Ignoring backpressure |
| `fetch.min.bytes` | Consumer fetch batching | Ignoring latency |
| `fetch.max.wait.ms` | Fetch wait bound | Considering it independently |
| Heap | JVM memory | Ignoring page cache |
| Partitions | Parallelism | Assuming unlimited benefit |
| Consumers | Group parallelism | Ignoring partition count |
| Replication | Fault tolerance | Ignoring recovery traffic |
| Dynamic config | Runtime overrides | Assuming static config always wins |

---

## 17.67 Final Cheat Sheet

### KRaft

```text
node.id
process.roles
controller quorum
```

### Storage

```text
log.dirs
retention.ms
retention.bytes
segment.bytes
segment.ms
```

### Reliability

```text
RF
ISR
min.insync.replicas
acks
unclean.leader.election.enable
```

### Networking

```text
listeners
advertised.listeners
listener.security.protocol.map
```

### Producer

```text
batch.size
linger.ms
compression.type
buffer.memory
acks
delivery.timeout.ms
```

### Consumer

```text
fetch.min.bytes
fetch.max.wait.ms
fetch.max.bytes
max.partition.fetch.bytes
```

### Broker processing

```text
num.network.threads
num.io.threads
```

### Memory

```text
JVM heap
OS page cache
native memory
network buffers
```

### Capacity

```text
ingress
egress
replication
disk
CPU
network
partitions
connections
recovery
growth
```

### Troubleshooting

```text
measure
→ identify bottleneck
→ change one thing
→ observe
→ validate
```

---

## 17.68 Senior-Level Mental Model

Kafka configuration is a set of trade-offs:

```text
                 Reliability
                     ▲
                     │
                     │
Performance ◄────────┼────────► Cost
                     │
                     │
                     ▼
                 Complexity
```

Examples:

```text
Higher RF
→ reliability ↑
→ storage cost ↑
→ replication traffic ↑
→ recovery work ↑
```

```text
Larger batches
→ throughput potential ↑
→ compression efficiency ↑
→ latency may ↑
→ memory usage ↑
```

```text
More partitions
→ parallelism ↑
→ metadata/operational overhead ↑
→ recovery complexity ↑
```

```text
More heap
→ heap capacity ↑
→ page cache ↓
→ potentially worse Kafka I/O
```

This trade-off model is more valuable than memorizing isolated numbers.

---

## 17.69 Chapter Summary

Remember:

1. Kafka tuning must be measurement-driven.
2. KRaft configuration differs from legacy ZooKeeper configuration.
3. Storage capacity and storage performance are separate concerns.
4. Retention is segment-oriented and asynchronous.
5. RF is not the same as ISR.
6. `min.insync.replicas` works together with producer acknowledgment settings.
7. `acks=all` is strongest when combined with appropriate replication and ISR policy.
8. Unclean leader election trades durability for availability.
9. Failure-domain awareness matters as much as replication factor.
10. `listeners` and `advertised.listeners` solve different problems.
11. Producer batching is a latency/throughput trade-off.
12. Consumer fetch settings are also latency/throughput trade-offs.
13. Kafka uses the OS page cache heavily.
14. Partitions provide parallelism but do not automatically solve hot keys.
15. Consumer groups cannot have more active partition owners than partitions.
16. Recovery traffic must be included in capacity planning.
17. Observability should come before configuration changes.
18. Dynamic configuration must be understood before assuming static files are authoritative.
19. Version-specific defaults matter.
20. Production Kafka configuration is an engineering optimization problem, not a magic-number exercise.

---

# Official Reference Material

For current configuration details and version-specific defaults, use the official Apache Kafka documentation for the Kafka version being studied.

Recommended references:

- Apache Kafka Configuration
- Apache Kafka Broker Configs
- Apache Kafka Topic Configs
- Apache Kafka Producer Configs
- Apache Kafka Consumer Configs
- Apache Kafka KRaft documentation

**Certification rule:** when a question asks for a default value, determine the Kafka version first.

---

# Next Chapter

## Chapter 18 — Kafka Networking, Listeners, Protocols & Connectivity Deep Dive

Topics:

- TCP/IP fundamentals for Kafka
- DNS and hostname resolution
- `listeners` and `advertised.listeners`
- listener security protocols
- SSL/TLS
- SASL
- authentication versus authorization
- internal/external connectivity
- NAT and load balancers
- Docker/Kubernetes networking
- cloud networking
- packet-flow troubleshooting
- common connectivity failures
- certification scenario drills

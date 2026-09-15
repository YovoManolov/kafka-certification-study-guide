# Chapter 17 — Kafka CLI, AdminClient & Certification Command Mastery

> Kafka Developer & Administrator Certification Preparation
> Based on the security concepts covered in *Kafka: The Definitive Guide*, with certification-oriented explanations,
> operational examples, troubleshooting scenarios, and exam traps.

## 1. Learning Objectives

By the end of this chapter, you should be able to:

- Select the correct Kafka CLI for an operational task.
- Create, inspect, modify, and delete topics safely.
- Understand topic-level and broker-level configuration overrides.
- Diagnose consumer-group lag and assignment problems.
- Inspect cluster metadata and KRaft controller state.
- Understand replicas, ISR, leaders, and partitions.
- Perform partition reassignment conceptually and operationally.
- Use `kafka-configs` for dynamic configuration.
- Use `kafka-acls` for authorization administration.
- Use the Java `AdminClient` for programmatic administration.
- Distinguish modern KRaft administration from legacy
  ZooKeeper-oriented workflows.
- Interpret command output rather than merely memorizing syntax.
- Recognize common CCDAK/CCAAK certification traps.

------------------------------------------------------------------------

## 2. The Kafka Administration Mindset

Kafka administration is about managing several layers:

``` mermaid
flowchart LR
    Cluster["Kafka Cluster"]
    
    Cluster --> Controllers["Controllers<br/>(KRaft Metadata Quorum)"]
    Cluster --> Brokers["Brokers"]
    Cluster --> Topics["Topics"]
    Cluster --> ConsumerGroups["Consumer Groups"]
    
    Brokers --> BrokerConfig["Broker Configuration"]
    Brokers --> Listeners["Listeners"]
    Brokers --> Storage["Storage"]
    Brokers --> Replication["Replication"]
    
    Topics --> Partitions["Partitions"]
    Topics --> Replicas["Replicas"]
    Topics --> ISR["ISR"]
    Topics --> TopicConfig["Topic Configuration"]
    
    ConsumerGroups --> Members["Members"]
    ConsumerGroups --> Assignments["Assignments"]
    ConsumerGroups --> Offsets["Offsets"]
    ConsumerGroups --> Lag["Lag"]
    
    classDef cluster fill:#FF4757,color:#FFF,stroke:#C0392B,stroke-width:3px,font-weight:bold
    classDef mainNode fill:#2ED593,color:#000,stroke:#1A8A4A,stroke-width:2px,font-weight:bold
    classDef subNode fill:#FFA502,color:#000,stroke:#D68910,stroke-width:2px,font-weight:bold
    classDef leafNode fill:#3742FA,color:#FFF,stroke:#1E2A8A,stroke-width:2px,font-weight:bold
    
    class Cluster cluster
    class Controllers,Brokers,Topics,ConsumerGroups mainNode
    class BrokerConfig,Listeners,Storage,Replication,Partitions,Replicas,ISR,TopicConfig,Members,Assignments,Offsets,Lag leafNode
```

The key certification skill is:

> **Identify the Kafka object → identify the required operation → choose
> the correct CLI/API → understand the consequence.**

------------------------------------------------------------------------

## 3. Core Kafka CLI Toolkit

  -----------------------------------------------------------------------
CLI Primary purpose
  ----------------------------------- -----------------------------------
`kafka-topics`                      Topic and partition administration

`kafka-configs`                     Entity configuration

`kafka-consumer-groups`             Consumer-group inspection and
offset administration

`kafka-console-producer`            Produce test records

`kafka-console-consumer`            Consume test records

`kafka-acls`                        ACL administration

`kafka-reassign-partitions`         Replica reassignment

`kafka-cluster`                     Cluster-level operations

`kafka-metadata-quorum`             KRaft metadata quorum inspection

`kafka-storage`                     KRaft storage operations

`kafka-dump-log`                    Low-level log inspection

`kafka-features`                    Feature-level administration
-----------------------------------------------------------------------

Always validate release-specific syntax with:

``` bash
bin/kafka-topics.sh --help
```

------------------------------------------------------------------------

## 4. `kafka-topics`

Typical operations:

- create a topic
- list topics
- describe a topic
- increase partitions
- delete a topic

### 4.1. List

``` bash
bin/kafka-topics.sh   --bootstrap-server localhost:9092   --list
```

The bootstrap server is an initial connection/discovery endpoint. It is
not permanently responsible for the topic.

### 4.2. Create

``` bash
bin/kafka-topics.sh   \
  --bootstrap-server localhost:9092   \
  --create   \
  --topic orders   \
  --partitions 12   \
  --replication-factor 3
```

This means:

``` text
Topic               = orders
Partitions          = 12
Replication factor = 3
```

------------------------------------------------------------------------

## 5. Partition Count vs Replication Factor

These are fundamentally different.

For:

``` text
Partitions = 6
Replication factor = 3
```

there are:

``` text
6 partitions
18 replica assignments
```

because:

``` text
6 × 3 = 18
```

Remember:

``` text
Partitions → parallelism / ordering unit

Replication factor → number of replicas per partition
```

Replication factor does not directly provide consumer parallelism.

------------------------------------------------------------------------

## 6. Increasing Partitions

Example:

``` bash
bin/kafka-topics.sh   \
  --bootstrap-server localhost:9092   \
  --alter   \
  --topic orders   \
  --partitions 18
```

This increases the topic from 6 to 18 partitions.

A normal Kafka administration operation does not reduce a topic's partition count.

### 6.1. Why?

Partition count is fundamental to:

- offsets
- ordering
- consumer assignment
- key-based partitioning
- application parallelism

### 6.2. Certification trap

If a question asks how to reduce a topic from 20 partitions to 10, do
not search for a `--partitions 10` solution. Kafka does not provide a
normal partition-shrink operation.

------------------------------------------------------------------------

## 7. Partition Expansion and Key Ordering

If a producer uses:

``` text
key = customerId
```

the key determines partition selection.

Changing the number of partitions can change the mapping for future
records.

Conceptually:

``` text
Before:
customer-123 → partition 2 of 6

After:
customer-123 → potentially another partition of 18
```

Therefore:

> Increasing partitions can affect assumptions about key-based ordering
> and partition affinity.

------------------------------------------------------------------------

## 8. Describing Topics

``` bash
bin/kafka-topics.sh   --bootstrap-server localhost:9092   --describe   --topic orders
```

You should understand fields such as:

- partition
- leader
- replicas
- ISR

Example:

``` text
Partition  Leader  Replicas  ISR
0          2       2,1,3     2,1,3
1          1       1,3,2     1,3,2
2          3       3,2,1     3,2,1
```

------------------------------------------------------------------------

## 9. Leader, Replicas and ISR

### 9.1. Leader

The broker currently acting as leader for a partition.

### 9.2. Replicas

All assigned replicas.

``` text
Replicas = [1,2,3]
```

### 9.3. ISR

The In-Sync Replica set.

``` text
Replicas = [1,2,3]
ISR      = [1,2]
```

Broker 3 is assigned but currently not in sync.

The key relationship is:

``` text
ISR ⊆ Replicas
```

------------------------------------------------------------------------

## 10. Under-Replicated Partitions

A partition is under-replicated when:

``` text
ISR count < replica count
```

Example:

``` text
Replication factor = 3
ISR count = 2
```

Typical causes:

- broker failure
- network problems
- slow disks
- replication lag
- overloaded brokers
- resource saturation

The correct response is normally investigation and recovery, not
immediately changing application offsets.

------------------------------------------------------------------------

## 11. `kafka-configs`

`kafka-configs` manages configuration entities.

Example:

``` bash
bin/kafka-configs.sh   \
  --bootstrap-server localhost:9092   \
  --entity-type topics   \
  --entity-name orders   \
  --alter   \
  --add-config retention.ms=86400000
```

This creates/changes a topic-level override.

------------------------------------------------------------------------

## 12. Broker Defaults vs Topic Overrides

Kafka configuration can be viewed conceptually as:

``` text
Broker/default configuration
            ↓
      Topic override
            ↓
Effective topic configuration
```

Example:

``` text
Broker retention.ms = 7 days
Topic orders retention.ms = 1 day
```

The effective setting for `orders` is one day.

This distinction is critical when troubleshooting unexpected behavior.

------------------------------------------------------------------------

## 13. Inspecting Topic Configuration

``` bash
bin/kafka-configs.sh   \
  --bootstrap-server localhost:9092   \
  --entity-type topics   \
  --entity-name orders   \
  --describe
```

When investigating configuration, always distinguish:

``` text
  Default value
       vs
Explicit override
       vs
 Effective value
```

------------------------------------------------------------------------

## 14. Removing a Topic Override

If the objective is to make the topic inherit the default again, remove
the override:

``` bash
bin/kafka-configs.sh   \
  --bootstrap-server localhost:9092   \
  --entity-type topics   \
  --entity-name orders   \
  --alter   \
  --delete-config retention.ms
```

Conceptually:

``` text
  Override removed
         ↓
Topic inherits default
```

This is different from explicitly setting the topic to the current
default value.

------------------------------------------------------------------------

## 15. Static vs Dynamic Configuration

A critical distinction:

``` text
             Static configuration
                      ↓
Usually requires configuration-file change/restart

            Dynamic configuration
                      ↓
Can be changed through Kafka administration mechanisms
```

Not every Kafka property is dynamically changeable.

### Certification trap

Do not assume:

> "Because `kafka-configs` exists, every Kafka property can be changed
> dynamically."

That is **false**.

------------------------------------------------------------------------

## 16. `kafka-consumer-groups`

Describe a consumer group:

``` bash
bin/kafka-consumer-groups.sh   --bootstrap-server localhost:9092   --describe   --group orders-service
```

Typical output contains:

``` text
GROUP          TOPIC    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders-service orders   0          1200            1250            50
orders-service orders   1          1500            1500            0
```

------------------------------------------------------------------------

## 17. Understanding Consumer Lag

A useful simplified model is:

``` text
lag = log-end-offset - consumer position
```

Example:

``` text
Log end offset = 10,000
Consumer position = 9,700

Lag = 300
```

Lag means the consumer is behind the current end of the log.

It does not automatically mean the consumer is broken.

Investigate:

- processing latency
- throughput
- number of consumers
- partition assignment
- broker/network health
- downstream dependencies
- rebalances

------------------------------------------------------------------------

## 18. Consumer Group Parallelism

Suppose:

``` text
Topic partitions = 6
Consumers in group = 10
```

At most six consumers can actively own partitions at a time.

``` text
    6 partitions
         ↓
  6 active consumers

4 additional consumers
        ↓
      idle
```

### Certification trap

More consumers do not automatically mean more throughput.

For one consumer group:

``` text
Active partition-level consumers ≤ partition count
```

------------------------------------------------------------------------

## 19. Resetting Consumer Group Offsets

Kafka provides administrative offset-reset operations.

Example concept:

``` bash
bin/kafka-consumer-groups.sh   \
  --bootstrap-server localhost:9092   \
  --group orders-service   \
  --topic orders   \
  --reset-offsets   \
  --to-earliest
```

Depending on the Kafka release and command usage, you may need a
dry-run/execute workflow.

Possible reset strategies include:

- earliest
- latest
- timestamp
- specific offset
- relative shift

### Production warning

Resetting offsets is a business-impacting operation.

It can cause ``reprocessing or data skipping``

Always understand the desired recovery semantics before executing it.

------------------------------------------------------------------------

## 20. `kafka-console-producer`

Useful for smoke tests:

``` bash
bin/kafka-console-producer.sh \  
  --bootstrap-server localhost:9092 \  
  --topic orders
```

For key/value testing:

``` bash
bin/kafka-console-producer.sh   \
  --bootstrap-server localhost:9092 \  
  --topic orders   \
  --property parse.key=true \   
  --property key.separator=:
```

Then:

``` text
customer-1:order-100
customer-2:order-101
```

Use it for:

- connectivity tests
- basic partitioning tests
- smoke tests
- reproducing simple scenarios

It is not a replacement for a production producer.

------------------------------------------------------------------------

## 21 `kafka-console-consumer`

Basic consumption:

``` bash
bin/kafka-console-consumer.sh   
  --bootstrap-server localhost:9092   
  --topic orders
```

From the beginning:

``` bash
bin/kafka-console-consumer.sh   
  --bootstrap-server localhost:9092   
  --topic orders   
  --from-beginning
```

With a group:

``` bash
bin/kafka-console-consumer.sh   
  --bootstrap-server localhost:9092   
  --topic orders   
  --group debug-orders
```

### Important

`--from-beginning` should not be interpreted as:

> Ignore existing committed offsets.

For an existing consumer group, its committed position matters.

------------------------------------------------------------------------

## 22 `kafka-acls`

Authorization administration uses `kafka-acls`.

Example:

``` bash
bin/kafka-acls.sh   
  --bootstrap-server localhost:9092   
  --add   
  --allow-principal User:alice   
  --operation Read   
  --topic orders
```

Depending on the security configuration, ACLs can govern resources such
as:

- topics
- consumer groups
- clusters
- transactional IDs

------------------------------------------------------------------------

## 23 Authentication vs Authorization

Keep these concepts separate:

``` text
Authentication
    ↓
Who are you?

Authorization
    ↓
What are you allowed to do?
```

Examples of authentication mechanisms include:

``` text
TLS
SASL
OAuth
Kerberos
```

Authorization can be enforced through the Kafka authorizer/ACL model.

Therefore:

``` text
Authenticated client
        ≠
Authorized client
```

A client can authenticate successfully and still receive:

``` text
TOPIC_AUTHORIZATION_FAILED
```

------------------------------------------------------------------------

## 24 `kafka-reassign-partitions`

Partition reassignment moves replicas between brokers.

Typical reasons:

- rebalance storage
- decommission a broker
- correct uneven replica distribution
- move replicas after capacity changes
- maintenance

It changes the relationship:

``` text
partition → replica broker set
```

It is different from increasing the partition count.

------------------------------------------------------------------------

## 25 Partition Expansion vs Replica Reassignment

Problem Appropriate operation
  ----------------------------------- ---------------------------------
Need more application parallelism Increase partitions
Need to move replicas Reassignment
Need to decommission a broker Reassignment
Need different retention Topic configuration
Need to change consumer position Consumer-group offset operation
Need to change authorization ACL operation

The certification skill is recognizing the Kafka object being changed.

------------------------------------------------------------------------

## 26 Broker Decommissioning

A safe conceptual workflow is:

``` text
  Identify broker replicas
          ↓
 Generate reassignment plan
          ↓
     Move replicas
          ↓
  Verify ISR/distribution
          ↓
      Stop broker
          ↓
  Verify cluster health
```

The exact operational workflow depends on the Kafka deployment and
version.

The principle is:

> **Move responsibility before removing capacity.**

------------------------------------------------------------------------

## 27 KRaft Administration

Modern Kafka uses KRaft rather than ZooKeeper.

Conceptually:

``` mermaid
flowchart TB
    subgraph Controllers["Controller Quorum (KRaft)"]
        direction LR
        C1["Controller 1"] --- C2["Controller 2"] --- C3["Controller 3"]
    end
    
    Controllers -->|"Metadata sync<br/>(Raft consensus)"| Brokers
    
    subgraph Brokers["Kafka Brokers"]
        direction LR
        B1["Broker 1"] --- B2["Broker 2"] --- B3["Broker 3"]
    end
    
    classDef controller fill:#4A90D9,color:#fff,stroke:#2C5F8A,stroke-width:2px
    classDef broker fill:#2ECC71,color:#fff,stroke:#1A8A4A,stroke-width:2px
    classDef cluster fill:#F39C12,color:#fff,stroke:#B8770E,stroke-width:2px,stroke-dasharray:5 5
    
    class C1,C2,C3 controller
    class B1,B2,B3 broker
    class Controllers,Brokers cluster
```

The controller quorum manages Kafka metadata.

------------------------------------------------------------------------

## 28 `kafka-metadata-quorum`

Use KRaft quorum tooling to inspect metadata quorum state.

Example:

``` bash
bin/kafka-metadata-quorum.sh   --bootstrap-server localhost:9092   describe --status
```

Depending on Kafka version, output can expose information related to:

- leader
- voters
- observers
- high watermark
- quorum state

This is an important CCAAK administration area.

------------------------------------------------------------------------

## 29 Controller Quorum vs Broker Cluster

Do not confuse:

``` text
Kafka brokers
```

with:

``` text
KRaft controllers
```

A deployment can use:

``` text
combined broker/controller nodes
```

or:

``` text
dedicated controllers
+
dedicated brokers
```

For larger production environments, role separation can be useful.

------------------------------------------------------------------------

## 30 `kafka-storage`

KRaft storage initialization involves the `kafka-storage` tool.

Typical operations include:

``` bash
kafka-storage.sh random-uuid
```

and storage formatting according to the deployment procedure.

### Critical warning

Storage formatting is not a routine restart operation.

Always verify:

- node identity
- cluster ID
- storage directory
- deployment mode

before formatting.

Formatting the wrong storage directory can destroy expected Kafka state.

------------------------------------------------------------------------

## 31 `kafka-dump-log`

`kafka-dump-log` is a low-level diagnostic tool for Kafka log segments.

It can help investigate:

- corrupted records
- segment structure
- unexpected log contents
- low-level storage problems

It is an advanced troubleshooting tool rather than a routine
administration command.

------------------------------------------------------------------------

## 32 AdminClient

Kafka administration can be performed programmatically through the Java
`AdminClient`.

Conceptually:

``` text
Automation / Application
        ↓
     AdminClient
        ↓
 Kafka Admin Protocol
        ↓
 Kafka Cluster
```

Typical use cases:

- provisioning
- automation
- integration tests
- operational workflows
- custom control planes

------------------------------------------------------------------------

## 33 Creating an AdminClient

Java example:

``` java
Properties props = new Properties();

props.put(
    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
    "localhost:9092"
);

try (Admin admin = Admin.create(props)) {
    // Administrative operations
}
```

The AdminClient communicates using Kafka's administrative protocol.

------------------------------------------------------------------------

## 34 Creating a Topic with AdminClient

Example:

``` java
NewTopic topic =
    new NewTopic("orders", 12, (short) 3);

CreateTopicsResult result =
    admin.createTopics(List.of(topic));

result.all().get();
```

Conceptually:

``` text
NewTopic
   ↓
AdminClient
   ↓
CreateTopics request
   ↓
Kafka controller
   ↓
Topic metadata
```

------------------------------------------------------------------------

## 35 Describing Topics with AdminClient

Example:

``` java
TopicDescription description =
    admin.describeTopics(List.of("orders"))
         .topicNameValues()
         .get("orders")
         .get();
```

You can inspect:

- partitions
- leaders
- replicas
- ISR

This is useful for operational tooling.

------------------------------------------------------------------------

## 36 AdminClient and Configuration

AdminClient can inspect and modify Kafka configuration.

Modern applications should understand both full and incremental
configuration APIs, with incremental alteration being especially useful
when making targeted changes.

Conceptually:

``` text
ConfigResource
      ↓
describeConfigs()
      ↓
Kafka configuration
```

and:

``` text
AlterConfigOp
      ↓
incrementalAlterConfigs()
      ↓
Kafka configuration
```

------------------------------------------------------------------------

## 37 AdminClient and Consumer Groups

AdminClient APIs can support:

- listing groups
- describing groups
- listing committed offsets
- altering offsets
- inspecting group members
- inspecting group state

This enables custom operational tools such as:

``` text
Kafka Operations Dashboard
        ↓
      AdminClient
        ↓
  Topics / Groups / Brokers
```

------------------------------------------------------------------------

## 38 CLI vs AdminClient

Use CLI tools for:

``` text
Human operator
+
Interactive troubleshooting
+
One-off administration
```

Use AdminClient for:

``` text
Automation
+
Repeated workflows
+
Programmatic control
+
Custom tooling
```

Examples:

``` text
Manual lag investigation
    → kafka-consumer-groups

Automated lag monitor
    → AdminClient

Manual topic creation
    → kafka-topics

Automated provisioning
    → AdminClient / infrastructure automation
```

------------------------------------------------------------------------

## 39 Certification Command Decision Matrix

Scenario Primary tool
  ---------------------------------- ------------------------------------
List topics                        `kafka-topics`
Inspect partition leaders          `kafka-topics --describe`
Increase partitions                `kafka-topics --alter`
Change topic retention             `kafka-configs`
Inspect topic override             `kafka-configs --describe`
Inspect consumer lag               `kafka-consumer-groups --describe`
Reset group offsets                `kafka-consumer-groups`
Test producing                     `kafka-console-producer`
Test consuming                     `kafka-console-consumer`
Manage authorization               `kafka-acls`
Move replicas                      `kafka-reassign-partitions`
Inspect KRaft quorum               `kafka-metadata-quorum`
Inspect/initialize KRaft storage   `kafka-storage`
Low-level log inspection           `kafka-dump-log`
Programmatic administration        `AdminClient`

------------------------------------------------------------------------

## 40 Important Relationships to Memorize

``` text
partition count
    ≠
replication factor
```

``` text
replicas
    ⊇
ISR
```

``` text
ISR count < replica count
    =
under-replicated partition
```

``` text
consumer lag
    ≈
log end offset - consumer position
```

``` text
authentication
    ≠
authorization
```

``` text
broker
    ≠
KRaft controller
```

``` text
topic configuration
    ≠
broker default
```

------------------------------------------------------------------------

## 41 Certification Scenario Drills

## Scenario 1 --- High Consumer Lag

A partition has:

``` text
Current position = 9,500
Log end offset = 12,000
```

### Answer

Approximate lag:

``` text
2,500
```

Investigate processing latency, throughput, assignment, consumer
capacity, broker/network health, and downstream dependencies.

Do not immediately reset offsets.

------------------------------------------------------------------------

## Scenario 2 --- Under-Replicated Partition

``` text
Replicas = [1,2,3]
ISR      = [1,2]
```

### Answer

Broker 3 is assigned but not currently in ISR.

Investigate broker 3, disk/network health, replication lag, and resource
saturation.

------------------------------------------------------------------------

## Scenario 3 --- Need More Parallelism

A topic has four partitions and the application requires more consumer
parallelism.

### Answer

Increase the partition count.

Changing replication factor does not solve this problem.

------------------------------------------------------------------------

## Scenario 4 --- Remove a Broker

Broker 3 must be permanently removed.

### Answer

Move its replicas through an appropriate reassignment process, verify
the cluster, then shut down the broker.

------------------------------------------------------------------------

## Scenario 5 --- Unexpected Retention

Broker default:

``` text
retention.ms = 7 days
```

Topic:

``` text
payments = 1 day
```

### Answer

Inspect topic-level configuration overrides.

------------------------------------------------------------------------

## Scenario 6 --- Authentication Works, Produce Fails

The client authenticates successfully but receives:

``` text
TOPIC_AUTHORIZATION_FAILED
```

### Answer

Investigate authorization/ACLs for the client identity and target topic.

------------------------------------------------------------------------

## Scenario 7 --- KRaft Metadata Problem

Brokers appear healthy, but metadata operations are failing.

### Answer

Investigate the KRaft controller quorum and metadata state.

Healthy brokers do not automatically imply a healthy metadata quorum.

------------------------------------------------------------------------

## Scenario 8 --- Existing Consumer Group

A console consumer is started with:

``` bash
--group orders-debug
--from-beginning
```

but the group already has committed offsets.

### Answer

Inspect the group's committed offsets and understand the command's
offset-reset semantics. Do not assume `--from-beginning` unconditionally
overrides an existing group's position.

------------------------------------------------------------------------

## 42 Practical Lab --- Three-Broker KRaft Cluster

Create:

``` text
Controller/Broker 1
Controller/Broker 2
Controller/Broker 3
```

Create:

``` text
Topic: orders
Partitions: 6
Replication factor: 3
```

Then practice:

### Step 1 --- List

``` bash
kafka-topics.sh   --bootstrap-server localhost:9092   --list
```

### Step 2 --- Describe

``` bash
kafka-topics.sh   --bootstrap-server localhost:9092   --describe   --topic orders
```

### Step 3 --- Produce

``` bash
kafka-console-producer.sh   --bootstrap-server localhost:9092   --topic orders
```

### Step 4 --- Consume

``` bash
kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic orders   --from-beginning
```

### Step 5 --- Create a group

``` bash
kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic orders   --group orders-lab
```

### Step 6 --- Inspect the group

``` bash
kafka-consumer-groups.sh   --bootstrap-server localhost:9092   --describe   --group orders-lab
```

### Step 7 --- Change retention

``` bash
kafka-configs.sh   --bootstrap-server localhost:9092   --entity-type topics   --entity-name orders   --alter   --add-config retention.ms=86400000
```

### Step 8 --- Inspect configuration

``` bash
kafka-configs.sh   --bootstrap-server localhost:9092   --entity-type topics   --entity-name orders   --describe
```

------------------------------------------------------------------------

## 43 Failure Injection Lab

With three brokers, create a topic with:

``` text
RF = 3
```

Stop one broker.

Observe:

``` text
leader changes
ISR changes
under-replication
consumer behavior
```

Restart the broker.

Observe:

``` text
replica recovery
ISR restoration
cluster convergence
```

The objective is to connect the CLI output with actual cluster behavior.

------------------------------------------------------------------------

## 44 Consumer Lag Lab

Create:

``` text
orders
12 partitions
```

Start:

``` text
2 consumers
```

Produce records rapidly.

Then increase consumers:

``` text
2 → 4 → 8 → 12 → 16
```

Observe:

- assignment
- lag
- throughput
- idle consumers

The objective is to demonstrate:

``` text
partition count limits active parallelism within a group
```

------------------------------------------------------------------------

## 45 Configuration Override Lab

Set a broker/default retention of:

``` text
7 days
```

Set a topic override:

``` text
orders = 1 day
```

Inspect the topic configuration.

Then remove the topic override.

Verify that the topic returns to inherited/default behavior.

------------------------------------------------------------------------

## 46 Replica Reassignment Lab

Create topics with:

``` text
RF = 3
```

Inspect their replica placement.

Prepare a reassignment that moves replicas away from one broker.

Verify:

``` text
Replica distribution
ISR
Leadership
Cluster health
```

Do not stop at executing the command. Be able to explain what changed
and why.

------------------------------------------------------------------------

## 47. AdminClient Lab

Build a small Java program that:

1. Connects to Kafka.
2. Lists topics.
3. Describes `orders`.
4. Lists consumer groups.
5. Describes `orders-lab`.
6. Reads topic configuration.
7. Creates a topic if it does not exist.

Suggested structure:

``` text
KafkaAdminLab
      |
      +-- TopicInspector
      |
      +-- ConsumerGroupInspector
      |
      +-- ConfigInspector
      |
      +-- TopicProvisioner
      |
      +-- AdminClient
```

The goal is to become comfortable moving between CLI administration and
programmatic administration.

------------------------------------------------------------------------

## 48. Certification Master Checklist

Before moving to the next chapter, you should be able to answer these
without documentation.

### Topics

- How do you list topics?
- How do you describe a topic?
- How do you create a topic?
- How do you increase partitions?
- Why can't you normally reduce partitions?
- What is replication factor?
- What is ISR?

### Configuration

- How do you inspect topic overrides?
- How do you change a dynamic topic configuration?
- How do you remove an override?
- What is a broker default?
- What is a topic override?
- Which settings require restart?

### Consumer Groups

- How do you inspect lag?
- What is a committed offset?
- What is log-end offset?
- Why can consumers be idle?
- How do you reset offsets?
- What are the risks?

### Security

- Authentication vs authorization?
- How do ACLs work?
- What does `TOPIC_AUTHORIZATION_FAILED` indicate?

### Replication

- What is a leader?
- What are replicas?
- What is ISR?
- What is an under-replicated partition?
- Why can a replica leave ISR?
- How do you move replicas?

### KRaft

- What is the metadata quorum?
- What does a controller do?
- What does `kafka-metadata-quorum` inspect?
- What is the role of `kafka-storage`?

### AdminClient

- What is AdminClient?
- When should you use it?
- How can it manage topics?
- How can it inspect groups?
- How can it inspect configurations?

------------------------------------------------------------------------

## 49. Final Certification Cheat Sheet

``` text
TOPIC
  kafka-topics

CONFIGURATION
  kafka-configs

CONSUMER GROUP
  kafka-consumer-groups

AUTHORIZATION
  kafka-acls

REPLICA MOVEMENT
  kafka-reassign-partitions

KRAFT QUORUM
  kafka-metadata-quorum

KRAFT STORAGE
  kafka-storage

LOW-LEVEL LOG
  kafka-dump-log

PRODUCE TEST DATA
  kafka-console-producer

CONSUME TEST DATA
  kafka-console-consumer

PROGRAMMATIC ADMINISTRATION
  AdminClient
```

Central decision pattern:

``` text
PROBLEM
   ↓
KAFKA OBJECT
   ↓
OPERATION
   ↓
CLI / ADMIN API
   ↓
EXPECTED EFFECT
   ↓
POSSIBLE SIDE EFFECT
```

------------------------------------------------------------------------

## 50. Chapter Summary

Kafka administration becomes much easier once the problem is mapped to
the correct Kafka resource.

The most important mental model is:

``` text
                    Kafka Cluster
                         |
        +----------------+----------------+
        |                |                |
      Topics          Consumer         Security
        |              Groups              |
        |                |                ACLs
  Partitions          Offsets
  Replicas              Lag
  ISR
        |
   Configuration

                         |
                       KRaft
                         |
                  Metadata Quorum
                         |
                    Controllers
```

For certification purposes, do not memorize commands as isolated
strings.

Memorize:

``` text
Problem
  ↓
Kafka object
  ↓
Operation
  ↓
CLI/API
  ↓
Expected effect
  ↓
Side effects
```

**Next chapter:** Chapter 17 --- Kafka Production Configuration, Tuning
& Configuration Reference

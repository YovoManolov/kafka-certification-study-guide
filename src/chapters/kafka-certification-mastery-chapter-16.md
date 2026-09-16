# Chapter 16 — Kafka Transactions & Exactly-Once Semantics Deep Dive

> Kafka Developer & Administrator Certification Preparation
> Based on the security concepts covered in *Kafka: The Definitive Guide*, with certification-oriented explanations,
> operational examples, troubleshooting scenarios, and exam traps.

---

## 1. Learning Objectives

By the end of this chapter, you should be able to explain and troubleshoot:

1. [x] Kafka producer idempotence
2. [x] Kafka transactions
3. [x] transactional.id
4. [x] producer IDs and epochs
5. [x] transaction coordinators
6. [x] transaction markers
7. [x] read_committed
8. [x] read_uncommitted
9. [x] consumer offset commits inside transactions
10. [x] exactly-once semantics
11. [x] exactly-once processing
12. [x] at-most-once semantics
13. [x] at-least-once semantics
14. [x] duplicate records
15. [x] zombie producers
16. [x] fencing
17. [x] transaction timeouts
18. [x] aborted transactions
19. [x] isolation levels
20. [x] Kafka Streams exactly-once processing
21. [x] EOS vs external side effects
22. [x] transactional outbox
23. [x] common certification traps
24. [x] production failure scenarios

## 2. Why Transactions Matter

Kafka provides durable event storage, but distributed processing introduces a fundamental problem.

```text
    Kafka Topic A
         |
         v
      Consumer
         |
         v
 Business Processing
         |
         v
    Kafka Topic B
```

The consumer reads ``A[offset=100]`` and produces ``B[offset=?]``, then the consumer needs to commit the input offset.

We now have two operations:

1. Produce output
2. Commit input offset

What happens if only one succeeds?

## 3. The Classic Failure Window

```text
Read A[100]
    |
    v
 Produce B
    |
    X
Consumer crashes before committing A[100]
```

After restart:

```text
Read A[100] again
       |
       v
 Produce B again
```

Now B contains a duplicate. This is the classic **at-least-once** processing problem.

## 4. The Opposite Failure

Now reverse the order:

```text
    Read A[100]
        |
        v
 Commit offset 100
        |
        v
     Produce B
        |
        X
 Producer crashes
```

The input is marked as processed, but the output was never produced, after restart ``A[100]``
will not necessarily be processed again. Result is **data loss**, this resembles **at-most-once** processing.

## 5. The Fundamental Atomicity Problem

Without transactions, ``Produce output + Commit input offset`` are separate operations.

We want:

```text
+--------------------------------+
|          Transaction           |
|                                |
|         output records         |
|       input offset commit      |
|                                |
+--------------------------------+
```

Either both become visible/committed, or neither does.

## 6. Kafka Transactions

Kafka transactions allow a producer to atomically write multiple records and commit consumer offsets as part of the same
transaction.

Conceptually:

````text
       Input Topic
            |
            v
         Consumer
            |
            v
   Transactional Producer
            |
            +-----------> Output Topic
            |
            +-----------> Offset commit
````

The output and offset can be committed together.

## 7. Transactional Processing

The desired flow becomes:

```text
                    Read input
                        |
                        v
               Begin transaction
                        |
                        v
                 Process record
                        |
                        v
                  Produce output
                        |
                        v
     Commit input offset as part of transaction
                        |
                        v
                Commit transaction
```

If the transaction aborts, ``output + offset commit`` are not committed as a successful unit.

## 8. Idempotent Producer vs Transactions

These are related but not the same.

1. **Idempotent producer**: Protects against **duplicate writes** caused by producer **retries**
2. **Transactions**: Provide atomicity across multiple Kafka writes and offset commits.

````text
              Idempotence
                   |
                   v
duplicate protection for producer retries


              Transactions
                   |
                   +--> atomic multiple writes
                   +--> atomic offset commit
                   +--> transactional visibility
````

## 9. Idempotent Producer

Suppose a producer sends ``Record X``, the broker writes it successfully, but the acknowledgement is lost:

```text
Producer ---> Broker
                |
                | write X
                |
            X ACK lost
```

- Without idempotence, duplicate data can occur.
- With idempotence, Kafka can identify the retry and avoid creating a duplicate logical record.

### 9.1. Producer Identity

Kafka uses producer identity information to track producer writes.

```text
 Producer
    |
    +--> Producer ID
    |
    +--> sequence numbers
```

The broker can determine whether a record is:

1. [ ] new
2. [ ] duplicate
3. [ ] out of order

### 9.2. Sequence Numbers

For each partition, producer records have sequence information.

```text
Partition 0

   seq 0
   seq 1
   seq 2
   seq 3
```

If a retry repeats ``seq 2``, kafka can detect that it has already accepted the record.

This is one mechanism behind **idempotent producer behavior**.

### 9.3. What Idempotence Does NOT Provide

Idempotence alone does not make this atomic ``Topic A offset commit + Topic B output``
It primarily protects producer retries.

> **idempotence != transaction**

This distinction is heavily tested in certification questions.

## 10. Transactions

A Kafka transaction can contain multiple records.

```text
Transaction T1

  Topic A
    |
    +--> record 1

  Topic B
    |
    +--> record 2

  Topic C
    |
    +--> record 3
```

The transaction is committed as a unit.

### 10.1. Atomicity

The key property is:

All records in a committed transaction become visible together according to transactional isolation
semantics.

If the transaction aborts:

```text
T1
|
+--> record A
+--> record B
+--> record C

ABORT
```

Consumers using read_committed should not treat those transactional records as committed application data.

### 10.2. Transactional Producer

A producer used for transactions needs a stable transactional identity. meaning Producer configuration

```properties
transactional.id=order-processor-1
```

Then in the application code:

```text
producer.initTransactions()

producer.beginTransaction()

producer.send(...)

producer.commitTransaction()

or:

producer.abortTransaction()
```

#### 10.2.1 Typical Java Flow

```java
producer.initTransactions();

producer.

beginTransaction();

producer.

send(record1);
producer.

send(record2);

producer.

commitTransaction();
```

On failure:

```java
producer.abortTransaction();
```

The actual production implementation must also correctly handle exceptions and producer lifecycle.

### 10.3. transactional.id

``transactional.id`` identifies the transactional producer.
It is particularly important because Kafka can use it to detect when a newer
producer instance takes ownership of the same transactional identity.

```text
transactional.id = payment-service-instance-A
```

If another producer starts with the same identity:

```text
    Producer A
        |
        v
transactional.id = X
        
    Producer B
        |
        v
transactional.id = X
```

Kafka can fence the older producer.

### 10.4. Producer Fencing

Fencing prevents an old producer instance from continuing to write when a newer instance has taken over its
transactional identity.

This protects against **zombie producers**

A **zombie producer** is an old process that believes it is still active after another process has taken over.

#### 10.4.1. Zombie Producer Scenario

```text
Producer A
    |
    | transactional.id = order-processor
    |
network partition
    |
    X
```

Producer A is still alive but isolated. Then a replacement starts:

```text
 Producer B
    |
    | same transactional.id
    |
    v
  Kafka

```

Kafka recognizes the newer producer incarnation. **Producer A can be fenced**.

## 11. Producer Epoch

Kafka uses producer epochs along with producer identity to distinguish producer incarnations.

```text

Producer ID
    |
    +--> epoch 0
    |
    +--> epoch 1
    |
    +--> epoch 2
```

A newer epoch supersedes an older one. This is part of the mechanism used for producer fencing.

## 12. Transaction Coordinator

Kafka uses a transaction coordinator to manage transactional state.

```text
  Transactional Producer
            |
            v
  Transaction Coordinator
            |
            +----> Transaction state
            |
            +----> Transaction metadata
```

The coordinator is associated with a Kafka broker.

The producer discovers the appropriate coordinator for its transactional identity.

## 13. Transaction Lifecycle

```text
     Producer
        |
        v
initTransactions()
        |
        v
Begin transaction
        |
        v
   Write records
        |
        v
   Send offsets
        |
        v
Commit transaction
```

Or:

```text
Begin
  |
  v
Write
  |
  X
Failure
  |
  v
Abort
```

## 14. Transaction Markers

Kafka uses transaction markers to communicate transaction commit/abort state to partitions.

```text
Partition

record
record
record
COMMIT marker
```

or:

```text
record
record
record
ABORT marker
```

These markers allow consumers to determine transactional visibility.

## 15. read_uncommitted

Consumers using ``isolation.level=read_uncommitted`` can read records without waiting for transactions to be committed.

This means they can observe records belonging to transactions that later abort.

```text
Transaction
    |
    +--> record
    |
    +--> ABORT

           read_uncommitted
                 |
                 v
    may observe transactional record
```

## 16. read_committed

Consumers using ``isolation.level=read_committed`` only return records from committed transactions.

Aborted transactional records are filtered out.

```text
Committed transaction
         |
         +--> visible


Aborted transaction
        |
        +--> hidden
```

## 17. Critical Certification Difference

Memorize:

1. read_uncommitted = can see uncommitted transactional data
2. read_committed = only committed transactional data

This is a very common certification topic.

## 18. Important: Read Committed Does Not Mean Exactly Once

This is a major trap. Using ```isolation.level=read_committed``` does not automatically make your application
**exactly-once**.

It only controls transactional visibility. **Exactly-once** processing requires a broader design involving:

> idempotent producer + transactions + correct offset management + appropriate consumer isolation

and, for **external side effects**, additional considerations.

## 19. Consuming Transactional Data

Consider:

```text
 Producer
    |
    | transaction
    v
  Topic B
    |
    v
 Consumer
```

With:

- **read_committed** the consumer sees only committed transactional records.
- **read_uncommitted** the consumer may see records from aborted transactions.

## 20. Last Stable Offset

Transactional consumers need to distinguish records that are safe to return from records that are still part of open
transactions.

Kafka exposes the concept of the **Last Stable Offset (LSO)**

For ``read_committed`` consumers, records beyond the **LSO** may not yet be considered safe to expose.

## 21. LSO Mental Model

Imagine:

```text
Offset:

100  committed
101  committed
102  transaction open
103  transaction open
104  non-transactional
```

The read-committed consumer cannot simply assume that everything after 101 is safe. The open transaction affects the
stable visibility boundary.

```text

        LSO
         |
         v
100 101 | 102 103 104
--------+-------------
safe      transactional uncertainty
```

## 22. High Watermark vs LSO

Do not confuse:

* **High Watermark**: The offset boundary associated with replication/commit visibility in Kafka's log model.
* **Last Stable Offset**: Important for transactional consumers because it accounts for open transactions.

## 23. sendOffsetsToTransaction

This is one of the most important APIs for Kafka transactional processing. The application can associate consumed
offsets with the producer transaction.

Conceptually:

```java
producer.beginTransaction();

producer.

send(output);

producer.

sendOffsetsToTransaction(offsets, consumerGroupMetadata);

producer.

commitTransaction();
```

Now ``output + input offset`` are committed atomically.

## 24. The Read-Process-Write Pattern

A classic transactional consume-transform-produce pipeline:

```text
         Topic A
            |
            v
         Consumer
            |
            v
         Process
            |
            v
   Transactional Producer
            |
            +------> Topic B
            |
            +------> consumed offset
```

The transaction contains ``output record(s) + consumer offset(s)``

## 25. Failure Scenario

Suppose:

```text
            Read A[100]
                |
                v
         Begin transaction
                |
                v
             Produce B
                |
                v
     sendOffsetsToTransaction()
                |
                X
        Crash before commit
```

The transaction is not committed. After restart ``A[100]`` can be processed again.

The old transactional output remains aborted/invisible to read_committed.

The new transaction can successfully produce the output.

This is the core mechanism behind Kafka's exactly-once processing model for Kafka-to-Kafka workflows.

## 26. Why Duplicates Are Not Simply "Never Produced"

This distinction is important. An aborted transaction may have written records to Kafka's log.

Those records may physically exist.

But ``read_committed`` filters them from application-visible results.

Therefore, exactly-once semantics are about the observable processing result, not necessarily "the bytes were never
written."

## 27. Exactly-Once Semantics

Exactly-once semantics mean that, within the supported transactional boundary, the effect of processing a record is made
visible once as a successful committed result.

For Kafka-to-Kafka processing:

```text
   input record
        |
        v
    processing
        |
        v
  output record
        |
        v
  offset commit
```

can be made atomic.

## 28. Exactly-Once Is a Scope

This is one of the most important senior-level concepts. Kafka transactions can provide atomicity for Kafka operations.

But imagine:

```text
      Kafka
        |
        v
    Application
        |
        +----> Kafka
        |
        +----> PostgreSQL
        |
        +----> REST API
        |
        +----> Email
```

A Kafka transaction cannot automatically make all of these external operations atomic.

## 29. Kafka EOS vs External Side Effects

Suppose:

```text
          Consume Kafka
                |
                v
        Charge credit card
                |
                v
        Produce Kafka event
```

A Kafka transaction cannot automatically roll back the credit-card charge.

If:

```text
charge succeeds
Kafka transaction aborts
```

the money has still been charged.

> Kafka exactly-once semantics do not automatically mean exactly-once side effects in external systems.

## 40. Transactional Outbox

A common solution for **database + Kafka consistency** is the transactional **outbox pattern**.

````text
        Application
            |
            v
    Database transaction
            |
            +--> Business state
            |
            +--> Outbox event
            |
            v
      Outbox Publisher
            |
            v
          Kafka
````

The database transaction atomically updates ``business state + outbox record``

Then an independent publisher sends the outbox event to Kafka.

## 41. Outbox + Kafka Transactions

The publisher can optionally use Kafka transactions for ``Kafka output + source offsets``.

But the database transaction and Kafka transaction are still different transactional systems.

There is no automatic distributed ACID transaction across both unless a separate distributed transaction mechanism is
introduced.

## 42. Exactly Once vs Idempotent Business Logic

Even with Kafka transactions, business logic should ideally be safe to retry because:

* crash
* retry
* rebalance
* restart

can happen. A robust system combines ``Kafka transactional guarantees + idempotent application behavior`` where
appropriate.

## 43. Transaction Timeout

Transactions cannot remain open forever. Kafka has transaction timeout mechanisms.

Conceptually:

```text
 Begin transaction
        |
        |
        |
        | too long
        v
transaction timeout
        |
        v
   abort/failure
```

A long-running transaction can therefore fail.

## 44. Why Long Transactions Are Dangerous

Long transactions can:

1. [x] increase resource usage
2. [x] delay transactional visibility
3. [x] increase recovery complexity
4. [x] affect consumer progress
5. [x] increase latency

Keep transaction boundaries appropriate for the workload.

## 45. transaction.timeout.ms

The producer-side transaction timeout controls how long a transaction can remain open before being considered timed out.

The broker also enforces limits, ``producer timeout + broker maximum`` must be compatible.

## 46. transaction.state.log

Kafka stores transaction state in an internal topic ``__transaction_state``. This topic is used by Kafka's transaction
machinery.

Administrators should recognize it as a critical internal Kafka component.

## 47. Transaction State Topic

Conceptually:

```text
Kafka Cluster

__transaction_state
        |
        +--> transaction metadata
        +--> producer transactional state
```

It is not an application topic. Do not casually manipulate internal Kafka topics.

## 48. Transaction Coordinator Failover

The transaction coordinator is a broker, if that broker fails Kafka can elect/recover the coordinator for affected
transactional identities.

The transactional protocol is designed to tolerate broker failures within Kafka's availability model.

## 49. Producer Restart

Suppose:

```text
Producer A
transactional.id = payments-1
```

crashes.

A new producer starts using:

```text
transactional.id = payments-1
```

Kafka can use the transactional identity to establish the new producer incarnation and fence the old one.

This is one reason stable transactional IDs matter.

## 50. Transactional ID Design

Avoid accidentally assigning the same transactional ID to multiple independent active producers unless that is
specifically part of your fencing/ownership model.

Think:

```text
instance A -> transactional.id A
instance B -> transactional.id B
```

when each is an independent transactional producer.

But for failover/replacement of the same logical producer identity:

```text
old instance -> transactional.id X
new instance -> transactional.id X
```

allows fencing semantics to protect against zombies.

## 51. Kafka Streams and EOS

Kafka Streams provides higher-level **exactly-once** processing capabilities.

A Streams topology might look like:

```text
Input Topic
    |
    v
 KStream
    |
    v
filter/map
    |
    v
aggregate
    |
    v
Output Topic
```

With **exactly-once** processing enabled, Kafka Streams uses Kafka's transactional capabilities to coordinate processing
and output.

## 52. Kafka Streams Processing Guarantee

Modern Kafka Streams supports processing guarantee configurations such as ``exactly_once_v2``

This is the modern **exactly-once** processing model to know for current Kafka environments.

## 53. EOS in Kafka Streams

Conceptually:

```text
     Consume
        |
     Process
        |
   Update state
        |
  Produce output
        |
  Commit offsets
        |
Commit transaction
```

Kafka Streams coordinates these operations so that a failed processing attempt does not expose an inconsistent committed
result.

## 54. State Stores

Kafka Streams may maintain local state:

```text
  Input
    |
    v
 Processor
    |
    +--> State Store
    |
    +--> Output Topic
```

State stores can be backed by changelog topics.

**Exactly-once** processing needs to consider

> input consumption + state updates + output records + offsets

as part of the processing model.

## 55. Changelog Topics

Kafka Streams state stores can be backed by Kafka changelog topics.

Conceptually:

```text
   State Store
        |
        v
 Changelog Topic
```

If the local state is lost:

```text
    Changelog
        |
        v
 State restoration
```

This makes Kafka itself part of the state durability mechanism.

## 56. EOS and Reprocessing

A key advantage of Kafka's transactional processing model is that failed attempts can be retried without exposing
aborted results to ``read_committed`` consumers.

```text
   Attempt 1
      |
      X
    ABORT

   Attempt 2
      |
      v
    COMMIT
```

Consumers see the committed result.

## 57. Certification Traps

### 57.1. Exactly Once Means No Retries

**False**. **Exactly-once** processing can involve retries.

The important property is that failed attempts do not create multiple committed observable results within the
transactional boundary.

### 57.2. Exactly Once Means No Duplicate Bytes in the Log

**False**. Aborted transactional records may exist physically in Kafka's log.

**read_committed** consumers do not expose them as committed application data.

### 57.3. Idempotent Producer = Exactly Once

**False**. Idempotence protects producer retries.

Transactions provide atomicity across multiple Kafka operations.

### 57.4. read_committed Enables Transactions

**False**. ``read_committed`` controls what a consumer sees.

It does not make producers transactional.

### 57.5. Every Consumer Must Use read_committed

**Not necessarily**. It depends on whether the application needs transactional isolation.

For applications consuming transactional data and requiring only committed results ``read_committed`` is typically
appropriate.

### 57.6. Kafka Transactions Cover Databases

**False**. Kafka transactions operate within Kafka's transactional model.

External databases require their own transaction/coordination strategy.

## 58. Certification Scenarios

### 58.1. Duplicate Output

Input:

> A[100]

Processing:

> produce B
> crash

Offset was not committed. **What happens with at-least-once processing?**

> A[100]

may be processed again.

Potentially:

```text
B
B
```

### 58.2. Transactional Processing

Same scenario:

````text
      A[100]
        |
    transaction
        |
     produce B
        |
crash before commit
````

The transaction is aborted/recovered.

A ``read_committed`` consumer will not treat the aborted output as committed. The input can be processed again.

### 58.3. read_uncommitted

A transactional producer writes:

```text
B1
B2
```

then aborts. A consumer uses ``read_uncommitted``

Question: **Can it observe those transactional records?**

**Yes**, transactional isolation does not filter them from this consumer.

### 58.4. read_committed

Same transaction aborts. Consumer:

```properties
isolation.level=read_committed
```

Question: **Will the aborted transactional records be returned as normal committed records?**

No.

### 58.5. Old Producer

```text
Producer A:

transactional.id = orders
```

becomes isolated.

Producer B starts with:

```text
transactional.id = orders
```

Question: **Why can Producer A be fenced?**

Because Kafka uses the transactional identity and producer epoch/incarnation to ensure that the old producer cannot
continue operating as the current owner.

### 58.6. Two Outputs

A transaction writes:

````text
Topic A -> record X
Topic B -> record Y
````

Question: **Can these be committed atomically?**

**Yes**, when both are part of the same Kafka transaction.

### 58.7. Output + Offset

Consumer processes ```Input A[50]``` Producer writes ``Output B`` and calls ``sendOffsetsToTransaction(...)``

Then commits the transaction.

Question: **What is the benefit?**

The output and consumed offset are committed atomically as part of the transaction.

### 58.8. Transaction Timeout

Application begins a transaction **T1**  but processing takes longer than allowed.

Question: **What can happen?**

The transaction can time out and fail/abort according to Kafka's transaction timeout rules.

### 58.9. External Database

Application:

```text
   Kafka input
        |
        v
Update PostgreSQL
        |
        v
Produce Kafka output
```

Question: **Does Kafka EOS guarantee** exactly-once **PostgreSQL updates?**

**No**. The database is outside Kafka's transaction boundary.

### 58.10. Credit Card

Application:

```text
      Kafka
        |
        v
   charge card
        |
        v
 Kafka transaction
```

Question: **Can aborting the Kafka transaction automatically reverse the card charge?**

**No**. External side effects need their own idempotency/reconciliation strategy.

## 59. Producer Configuration Concepts

Important configuration areas include:

1. [x] enable.idempotence
2. [x] transactional.id
3. [x] transaction.timeout.ms
4. [x] acks

For transactional producers, the configuration must satisfy Kafka's transactional requirements.

### 59.1. enable.idempotence

Conceptually:

````properties
enable.idempotence=true
````

enables producer idempotence.

Modern Kafka clients have idempotence enabled by default under normal compatible configuration.

Certification questions may test both the concept and how producer configuration interacts with retries/acks.

### 59.2. acks

Producer acknowledgements determine how the producer waits for broker confirmation.

For strong durability, commonly ``acks=all`` is used.

Idempotence also imposes compatible producer configuration constraints.

## 60. Transactions and acks

Do not think ``acks=all`` alone means **exactly once**, it does not.

These are different guarantees:

1. [x] **acks** = acknowledgement/durability behavior
2. [x] **idempotence** = duplicate retry protection
3. [x] **transactions** = atomic transactional processing

## 61. Transactional Producer Error Handling

A transactional application must distinguish between **retryable errors** and **fatal/transactional errors**

Some errors require **abort transaction** others may require:

```text
close producer
create new producer
```

The exact handling depends on the exception and client behavior.

## 62. Don't Continue After Fatal Transaction Errors

A common production mistake is:

````text
    transaction failure
            |
            v
     ignore exception
            |
            v
   continue using producer
````

This can violate transactional guarantees. Applications should follow the Kafka producer API's error-handling contract.

## 63. Transaction Boundaries

The size of a transaction matters.

1. Very small **one record = one transaction** can produce excessive overhead.
2. Very large **millions of records = one transaction** can increase:

* latency
* memory
* recovery cost
* transaction timeout risk

Choose transaction boundaries carefully.

## 64. Throughput Trade-Off

Transactions introduce coordination overhead.

```text
Non-transactional

send -> ack


Transactional

  begin
    |
   send
    |
 offsets
    |
  commit
```

**Exactly-once** guarantees are **not free**.

## 65. Latency Trade-Off

If transactions remain open longer:

```text
    begin
    |
    | processing
    |
    | processing
    |
    commit
```

the visibility of results can be delayed. **Transaction duration is an important performance parameter.**

## 66. Operational Monitoring

Administrators should monitor:

1. [x] transaction abort rate
2. [x] transaction commit latency
3. [x] transaction duration
4. [x] transaction timeout errors
5. [x] producer fencing errors
6. [x] transaction coordinator availability
7. [x] producer errors
8. [x] consumer lag

## 67. Transaction Abort Rate

A high abort rate may indicate:

1. [x] application failures
2. [x] timeouts
3. [x] rebalances
4. [x] producer fencing
5. [x] downstream problems

A transaction that constantly aborts can severely reduce throughput.

## 68. Fencing Errors

Repeated fencing errors may indicate:

1. [ ] duplicate transactional IDs
2. [ ] unstable producer ownership
3. [ ] bad instance identity
4. [ ] application lifecycle problems

This should be investigated rather than simply retried forever.

## 69. Consumer Lag with Transactions

Transactions can affect consumer progress.

For ``read_committed`` consumers **open transactions** can affect what records are visible.

> lag metrics should be interpreted carefully when transactional workloads are involved.

## 70. Transaction Coordinator Failure

If the coordinator fails:

```text
 Producer
    |
    v
Coordinator
    |
    X
```

Kafka can recover coordination through broker failover.

Applications may experience temporary failures/retries during coordinator transition.

## 71. Disaster Recovery

Transactions introduce additional state and timing considerations.

A DR design must consider:

1. [x] transaction state
2. [x] producer identities
3. [x] consumer offsets
4. [x] application state
5. [x] Kafka data

When moving workloads between clusters, transactional IDs and application ownership must be carefully designed.

## 72. Cross-Cluster Transactions

Do not assume:

```text
Cluster A transaction
        |
        v
     Cluster B
```

automatically provides one atomic transaction across both Kafka clusters.

Replication systems and Kafka transactions are separate concepts.

## 73. MirrorMaker and Transactions

Replication tools can replicate Kafka records, but you must understand how transactional semantics and consumer-visible
behavior are preserved by the specific replication architecture/version.

Never assume ``replicated bytes = identical distributed transaction boundary``

This is an important administrator-level distinction.

## 74. Exactly Once and Disaster Recovery

Suppose:

```text
  Primary Cluster
        |
        v
    DR Cluster
```

If the application fails over, you need to reason about:

1. [x] which records were committed
2. [x] which transactions completed
3. [x] consumer offsets
4. [x] producer identity
5. [x] duplicate processing

**Exactly-once** guarantees must be analyzed across the entire failover design.

## 75. Exactly Once Is Not a Magic Button

A configuration such as ``exactly_once_v2`` does not magically make an entire distributed system exactly-once.

It provides a defined processing guarantee within the Kafka Streams/Kafka transactional model.

Outside that boundary:

* database
* HTTP
* filesystem
* email
* payment system

need their own guarantees.

## 76. Exactly-Once Architecture

A strong Kafka-to-Kafka pipeline:

```text
                +-------------+
                | Input Topic |
                +------+------+
                       |
                       v
                +-------------+
                |  Consumer   |
                +------+------+
                       |
                       v
                +-------------+
                | Processing  |
                +------+------+
                       |
                       v
                +-------------+
                | Transaction |
                +------+------+
                       |
              +------------------+
              |                  |
              v                  v
        Output Topic       Input Offset
              |                  |
              +--------+---------+
                       |
                       v
                Commit Transaction
```

## 77. Exactly-Once with External Database

For database integration:

````text
      Kafka
        |
        v
    Application
        |
        +--> DB transaction
        |
        +--> Outbox
        |
        v
    Publisher
        |
        v
      Kafka
````

This is often easier to reason about than attempting to make Kafka and a relational database participate in one
distributed transaction.

## 78. Idempotency as a Second Defense

Even with transactions ``idempotency key + transactional processing`` can provide stronger protection
against repeated business operations.

Example:

> eventId = 8f2...

Store:

> eventId -> processed

Then:

```text
  duplicate event
        |
        v
 already processed
        |
        v
      ignore
```

## 79. Transaction vs Idempotency

These solve different problems.

1. **Transaction** protects atomicity.
2. **Idempotency** protects repeated execution.

A robust system can use both ``transaction + idempotent business operation``

## 80. Certification-Level Mental Model

Memorize this:

```text
                 Kafka Processing Guarantees
                            |
             +--------------+--------------+
             |              |              |
        At-most-once   At-least-once   Exactly-once
             |              |              |
          loss risk    duplicate risk   transaction
                                         semantics
```

```text
            Idempotence
                |
                v
     Producer retry protection
            
            Transactions
                |
                +--> atomic writes
                +--> offset commits
                +--> transactional visibility
            
           read_committed
                 |
                 v
 hide aborted transactional records
```

## 81. The Most Important Distinctions

1. **Idempotence** duplicate producer retry
2. **Idempotency** safe repeated business operation
3. **Transaction** atomic group of Kafka operations
4. ``read_committed`` visibility of committed transactional records
5. **Exactly-once** ``processing + output + offset`` within Kafka transactional boundary

These concepts are related but not interchangeable.

## 82. Certification Questions

### 82.1. Question 1

A producer retries a record because the acknowledgement was lost. **What feature prevents duplicate writes?**

<details>
<summary>Answer</summary>
Producer idempotence.
</details>

### 82.2. Question 2

A consumer reads Topic A and produces Topic B. **Which feature allows output records and consumed offsets to be
committed atomically?**

<details>
<summary>Answer</summary>
Kafka transactions
</details>

### 82.3. Question 3

**Which consumer setting hides aborted transactional records?**

<details>
<summary>Answer</summary>
isolation.level=read_committed
</details>

### 82.4. Question 4

**What can a ``read_uncommitted`` consumer see?**

<details>
<summary>Answer</summary>
It can see records from transactions that have not committed, including records from transactions that later abort.
</details>

### 82.5. Question 5

**What is transactional.id used for?**

<details>
<summary>Answer</summary>
It provides a stable transactional identity that enables Kafka to manage producer 
incarnations and fence older producers.
</details>

### 82.6. Question 6

**What is a zombie producer?**

<details>
<summary>Answer</summary>
An old producer instance that continues operating after a newer producer has taken 
over the same transactional identity.
</details>

### 82.7. Question 7

**How does Kafka prevent zombie producers?**

<details>
<summary>Answer</summary>
Producer epochs/fencing associated with transactional producer identity.
</details>

### 82.8. Question 8

**Does ``read_committed`` make a producer transactional?**

<details>
<summary>Answer</summary>
No.
</details>

### 82.9. Question 9

**Does idempotence alone provide exactly-once processing?**

<details>
<summary>Answer</summary>
No.
</details>

### 82.10. Question 10

**Can Kafka transactions automatically roll back a PostgreSQL transaction?**

<details>
<summary>Answer</summary>
No.
</details>

### 82.11. Question 11

**Why is sendOffsetsToTransaction**() important?

<details>
<summary>Answer</summary>
It allows consumed offsets to be committed as part of the same Kafka transaction as the output records.
</details>

### 82.12. Question 12

**What happens to transactional output if the transaction aborts?**

<details>
<summary>Answer</summary>
It is not exposed as committed transactional data to read_committed consumers.
</details>

### 82.13. Question 13

**Can abort transactional records physically exist in the Kafka log?**

<details>
<summary>Answer</summary>
Yes.
</details>

### 82.14. Question 14

**What is the main risk of at-most-once processing?**

<details>
<summary>Answer</summary>
Data loss.
</details>

### 82.15. Question 15

**What is the main risk of at-least-once processing?**

<details>
<summary>Answer</summary>
Duplicate processing.
</details>

### 82.16. Question 16

**What is the main benefit of exactly-once Kafka processing?**

<details>
<summary>Answer</summary>
Atomic coordination of processing results, Kafka output and consumed offsets within the Kafka transactional boundary.
</details>

### 82.17. Question 17

**Why can long transactions be problematic?**

<details>
<summary>Answer</summary>
They can increase 

* latency
* resource usage
* transaction timeout risk
* recovery complexity

</details>

### 82.18. Question 18

**What is the transaction coordinator?**

<details>
<summary>Answer</summary>
The Kafka broker-side component responsible for managing transaction coordination/state 
for transactional producers.
</details>

### 82.19. Question 19

**What is the Last Stable Offset?**

<details>
<summary>Answer</summary>
A transactional visibility boundary used by consumers, particularly read_committed, to determine which records are 
safely visible with respect to open transactions.
</details>

### 82.20. Question 20

**Does exactly-once Kafka processing mean an external REST API is invoked exactly once?**

<details>
<summary>Answer</summary>
No.
</details>

## 83. Scenario Exercise

```text
    orders
       |
       v
Order Processor
       |
       +----> payments
       |
       +----> order-status
```

The processor consumes ``orders[100]`` and performs

1. produce payment event
2. produce status event
3. commit consumed offset

Without a transaction:

```text
payment event ✓
status event ✓
offset commit ✗
```

After restart:

```text
payment event
status event
```

may be produced again.

## 84. Transactional Solution

Use:

````java
beginTransaction()

produce payment
event
produce status

event

sendOffsetsToTransaction()

commitTransaction()
````

Now:

```text
+-----------------------------+
|     Kafka Transaction       |
|                             |
|       payment event         |
|       status event          |
|       input offset          |
|                             |
+-----------------------------+
```

on failure **ABORT**, on success **COMMIT**

## 85. What About the Payment System?

If the payment event triggers:

````text
   payment-service
         |
         v
    external bank
````

Kafka EOS does not automatically make the bank operation exactly-once.

You need **idempotency key** or another business-level mechanism.

For example ``paymentId = 12345``the payment system ensures ``paymentId 12345`` is processed
once even if the event is delivered more than once.

## 86. Production Pattern

A mature architecture can therefore look like:

```text
             Kafka
               |
               v
          Transactional
           Processing
               |
        +------+------+
        |             |
        v             v
      Kafka        Database
        |             |
        |         idempotency
        |             |
        +------+------+
               |
               v
         Business Result

```

The key is to define the transactional boundary explicitly.

## 87. Administrator Checklist

For a transactional Kafka environment, verify:

1. [ ] Idempotent producer enabled
2. [ ] transactional.id correctly designed
3. [ ] transaction timeout configured
4. [ ] broker transaction limits understood
5. [ ] consumer isolation configured correctly
6. [ ] transaction coordinator healthy
7. [ ] fencing errors monitored
8. [ ] transaction aborts monitored
9. [ ] transaction latency monitored
10. [ ] consumer lag monitored
11. [ ] internal transaction state protected
12. [ ] DR strategy documented
13. [ ] external side effects handled separately

## 88. Developer Checklist

When implementing Kafka transactions:

1. [ ] initTransactions ()
2. [ ] beginTransaction ()
3. [ ] produce output
4. [ ] sendOffsetsToTransaction ()
5. [ ] commitTransaction ()
6. [ ] abort on appropriate failures
7. [ ] handle fatal transaction errors correctly
8. [ ] use stable transactional identity
9. [ ] configure consumers appropriately
10. [ ] test crashes
11. [ ] test retries
12. [ ] test rebalances
13. [ ] test fencing
14. [ ] test transaction timeout

## 89. Failure Injection Tests

A production-grade test suite should deliberately simulate:

1. [ ] producer crash
2. [ ] consumer crash
3. [ ] network failure
4. [ ] broker failure
5. [ ] transaction timeout
6. [ ] rebalance
7. [ ] duplicate processing
8. [ ] fenced producer
9. [ ] aborted transaction
10. [ ] Schema Registry failure

For exactly-once systems:

> Failure testing is more important than the happy path.

## 90. Final Chapter Cheat Sheet

```text
Idempotent producer
        |
        +--> protects against duplicate producer retries
```

```text
Transactions
    |
    +--> atomic Kafka writes
    +--> atomic consumed offset commit
    +--> transactional visibility
```

```text
 transactional.id
        |
        +--> stable transactional identity
        +--> enables fencing

```

```text
Producer epoch
      |
      +--> producer incarnation
      +--> fencing
```

```text
Zombie producer
      |
      +--> old producer still running
```

```text
 read_uncommitted
        |
        +--> may expose aborted transactional records
```

```text
read_committed
      |
      +--> only committed transactional records
```

```text
sendOffsetsToTransaction()
            |
            +--> offsets become part of transaction
```

> **at-most-once** -> possible loss
> **at-least-once** -> possible duplicates
> **exactly-once** -> atomic Kafka processing result

> Kafka EOS != external-system EOS

## 91. Final Certification Mental Model

When you see a certification scenario, ask these questions in order:

1. **Is the producer idempotent?**
              |
              v
2. **Is there a Kafka transaction?**
              |
              v
3. **Are output records inside the transaction?**
              |
              v
4. **Are consumed offsets committed through
   sendOffsetsToTransaction()?**
              |
              v
5. **Is the consumer using read_committed?**
              |
              v
6. **Could a producer be fenced?**
              |
              v
7. **Is there an external side effect?**
              |
              v
8. **If yes, Kafka EOS does NOT automatically
   cover it.**
              |
              v
9. **Is business-level idempotency required?**

The single most important formula for the Developer certification is:

>Consume + Process + Produce + sendOffsetsToTransaction() + Commit Transaction = Kafka exactly-once processing

while remembering:

>Kafka exactly-once ≠ exactly-once across arbitrary external systems

That distinction is one of **the most important concepts** separating basic Kafka knowledge from **senior-level Kafka certification** knowledge.
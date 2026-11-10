# ADR 0001: Redpanda as the broker, and keying every topic by `shipment_id`

- Status: Accepted
- Date: 2026-11-10

## Context

FreightPulse ingests two independent feeds per load:
- lifecycle events from the TMS and carrier APIs;
- GPS telemetry from ELDs.

The processor must apply each shipment's events in a sensible order. It must also scale out
horizontally. And the whole stack must run on a laptop with `make up`.

## Decision

1. **Use Redpanda (Kafka API) in Docker.** It is a single binary with no ZooKeeper or KRaft quorum
   to manage, and it fits in about 1 GB of RAM. Because it speaks the Kafka protocol, the same
   `confluent-kafka` client and semantics (consumer groups, offsets, idempotent producer) carry
   over to Apache Kafka, MSK or Confluent Cloud without code changes.
2. **Key every message by `shipment_id`**, on `freight.shipment.events` (6 partitions),
   `freight.telemetry.gps` (6), `freight.alerts` (3), and `freight.dlq` (original key, 1 partition).
   Kafka guarantees order only within a partition, and the default partitioner hashes the key.
   So every event of a given shipment lands on the same partition, in production order.
3. **Give both input topics the same partition count and use `partition.assignment.strategy=range`.**
   With equal partition counts and the same key, a shipment's lifecycle events and GPS pings land
   on the same partition *number* in both topics. The range assignor hands partition *n* of both
   topics to the same consumer. So each shipment has a single writer (one processor instance)
   during steady state, and the optimistic version check (ADR 0002) only has to catch races during
   rebalances.
4. Topics are created explicitly by `bootstrap` with fixed partition counts. Nothing relies on
   broker auto-create.

## Consequences

- Per-shipment ordering *within a topic* is guaranteed. Ordering *across* the two topics is not:
  a ping can be processed before a slightly older `stop.departed`. The processor handles this with
  event-time rules (ADR 0003), not by assuming arrival order.
- Throughput scales with the number of partitions (6), so up to 6 processor instances. Going
  further means repartitioning. Increasing partition counts remaps keys, so it would be done with a
  new topic generation and a cut-over, not in place.
- Measured scaling (docs/benchmarks.md): 3 instances drained a 500k-event backlog 2.15x faster
  than 1 instance, on a laptop where all instances share one Postgres in a 4-CPU Docker VM.
- A very hot shipment cannot be spread across consumers. This doesn't matter for this workload:
  one truck sends at most one ping every ~30 s of simulated time.
- The DLQ has 1 partition because volume is tiny and a single ordered stream is easier to inspect
  and replay.

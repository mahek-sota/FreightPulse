# ADR 0004: Daily range partitioning, BRIN for time scans, retention by DROP

- Status: Accepted
- Date: 2026-11-10

## Context

`gps_pings` is by far the largest table: 500 trucks pinging every ~75 s of simulated time
produces millions of rows a day at 60x. `shipment_events` grows more slowly but is append-only too.
Queries are either per shipment (timeline, breadcrumbs) or time-range scans over the fleet
(rollups, analytics). Old telemetry must be removable cheaply.

## Decision

1. **Declarative `PARTITION BY RANGE (event_time)`, one partition per UTC day**, for both
   `shipment_events` and `gps_pings`. The primary key is `(event_id, event_time)`, because a
   unique constraint on a partitioned table must include the partition key. Idempotency by
   `event_id` is still exact because the `event_time` of an event never changes.
2. **Partition manager** (`storage/partitions.py`):
   - creates partitions from yesterday through **7 days ahead**, hourly, from the jobs service and
     at bootstrap;
   - **drops partitions older than `FREIGHTPULSE_PARTITION_RETENTION_DAYS`** (default 90) with
     `DROP TABLE`, which is instant, generates no WAL churn and leaves no bloat, unlike `DELETE`;
   - the processor additionally creates partitions on demand for every event day in a batch
     (cached), so late or accelerated-clock events never fail on a missing partition;
   - DDL is serialised with `pg_advisory_xact_lock`.
   - No DEFAULT partition: rows in a default partition would block creating the matching day
     partition later, and on-demand creation makes a default partition unnecessary.
3. **Indexes**, each justified with measurements in `docs/sql_performance.md`:
   - `(shipment_id, event_time)` B-tree on both tables, for the timeline and breadcrumbs;
   - **BRIN on `gps_pings(event_time)`** with `pages_per_range = 32`. Pings arrive almost in
     time order, so block ranges have tight min/max values and BRIN is orders of magnitude smaller
     than a B-tree while still skipping irrelevant blocks.
4. **No foreign key from the event tables to `shipments`.** These are the hottest write paths.
   The processor guarantees the shipment exists before appending (unknown shipments are parked),
   and an FK would add a lookup per inserted row.

## Consequences

- Queries that filter on `event_time` get partition pruning (shown in the SQL case studies).
  Queries that don't, such as the per-shipment timeline, touch one index per partition, so
  retention keeps the partition count bounded (about 98 partitions per table at the defaults).
- Changing granularity (for example to hourly) means a new partition naming scheme. The manager
  only manages names matching `<table>_pYYYYMMDD`.
- `make seed-large` generates data over the last 30 days and creates its partitions explicitly.

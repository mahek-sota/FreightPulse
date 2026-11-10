# ADR 0002: At-least-once consumption + idempotent writes = effectively-once results

- Status: Accepted
- Date: 2026-11-10

## Context

Kafka delivers at least once. Producers retry, consumers crash between writing and committing,
and the simulator injects duplicates on purpose. Postgres is the system of record. We need every
event reflected exactly once in the database, with no loss and no double-counting, without using
Kafka transactions (they don't span Postgres).

## Decision

1. `enable.auto.commit=false` and `enable.auto.offset.store=false`. Offsets are committed manually
   and synchronously, **only after the database transaction has committed**.
2. **One Postgres transaction per batch.** It contains:
   - shipments, stops, and the lane upsert for new shipments;
   - the event-log appends;
   - parked events;
   - every `shipment_current_state` update;
   - stop backfills;
   - the stream watermarks.
3. **Idempotency by `event_id`.** Before applying a batch, the processor:
   - drops repeated `event_id`s within the batch;
   - looks up which `event_id`s are already in `shipment_events` or `gps_pings` (the lookup is
     bounded by the batch's event-time range so Postgres prunes partitions) and skips them;
   - appends with `INSERT ... ON CONFLICT (event_id, event_time) DO NOTHING` as a final guard.
   So a redelivered event never runs through the reducer twice.
4. **Optimistic concurrency.** State updates are one bulk
   `UPDATE ... FROM jsonb_to_recordset(...) WHERE version = expected` that sets `version + 1`.
   If any row was changed by another writer (possible only during a rebalance overlap), the whole
   transaction rolls back and the batch is retried, up to 5 times.
5. **Derived outputs are at-least-once with deterministic keys.** Alerts and DLQ records are
   produced and **flushed before the DB commit**. If the DB commit then fails, the batch is
   redelivered and the same records are produced again:
   - alerts carry `dedupe_key` and `alert_id = uuid5(dedupe_key)`, so the alert sink upserts
     them idempotently;
   - DLQ duplicates are harmless (replay is idempotent through rule 3).
   Producing *after* the commit was rejected: a crash between commit and produce would lose the
   alert forever, because the state change that triggered it is already persisted and won't fire
   again.
6. **Unknown shipments are parked durably.** An event whose `shipment.created` hasn't arrived yet
   is stored in `parked_events` inside the batch transaction. Its offset can then be committed
   without losing it. Every batch with new shipments, and a periodic sweep, pulls back resolvable
   parked events (with `FOR UPDATE SKIP LOCKED`). Events still unknown after
   `unknown_shipment_retry_s` (default 30 s) go to the DLQ as `UNKNOWN_SHIPMENT`.

7. **Shared reference rows are upserted *before* the batch transaction.** New `lanes` (an
   origin/destination pair shared by many shipments) are upserted in their own short transaction,
   in sorted order, just before the batch transaction. Upserting them inside the long batch
   transaction let two processor instances lock the same new lanes in different orders and
   **deadlock**. The 3-instance benchmark found this: 2 of 3 processors crashed with
   `DeadlockDetected`. A lane row with no shipment yet (if the batch then fails) is harmless and
   idempotent, so this does not weaken rule 2 for the event data.
8. **Transient conflicts are retried, not fatal.** A `VersionConflictError`, `DeadlockDetected`
   or `SerializationFailure` rolls back the transaction and retries the batch (up to 5 times,
   with a short backoff). Expired-parking deletes use `FOR UPDATE SKIP LOCKED` so instances never
   contend on them.

## Verification

`tests/integration/test_processor.py` covers:
- **Duplicates.** Every event sent 3 times produces exactly one row each.
- **Crash before DB commit.** The processor runs as a subprocess and is hard-killed with
  `os._exit(137)` in batch 3 before the commit. A restarted processor ends with exactly the
  expected rows: no loss, no duplicates.
- **Crash after DB commit, before the offset commit.** On restart the batch is redelivered and
  recognised as already processed (the duplicate counter is > 0), with an exact row count.
- **SIGKILL** at an arbitrary moment, then restart, gives exact row counts.
- **Three processor subprocesses** over shared lanes all exit cleanly, share the work, and give
  exact row counts (regression test for the lane deadlock).

## Consequences

- An extra indexed lookup per batch (step 3). It is cheap because the lookup is bounded by
  partition and uses the primary key.
- Alerts and DLQ are at-least-once on the wire. Consumers must dedupe: the sink does it by
  `dedupe_key`.
- A failure to flush the producer aborts the batch. Kafka unavailability therefore stops progress
  instead of silently dropping alerts.

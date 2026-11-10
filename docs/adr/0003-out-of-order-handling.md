# ADR 0003: Out-of-order and late events, and event time vs processing time

- Status: Accepted
- Date: 2026-11-10

## Context

ELD and telematics feeds are messy. Events arrive late (a cellular dead zone), duplicated, or
out of order. Two topics are involved (lifecycle and GPS), and Kafka only orders messages within a
partition of *one* topic, so a ping is routinely processed before a slightly older
`stop.departed` from the other topic. The simulator adds 3% emission delays of 1 to 10 minutes on
top of that.

## Decision

### 1. The event log is append-only, whatever the arrival order

Every valid, non-duplicate event is appended to `shipment_events` or `gps_pings`. Only the
*derived* state in `shipment_current_state` depends on order rules.

### 2. Each aspect of state has its own event-time guard

| Aspect | Updates only when | Guard column |
|---|---|---|
| Status (lifecycle) | `event_time >= status_event_time` | `status_event_time` |
| Position, speed EWMA, HOS, ETA | `event_time > last_ping_at` | `last_ping_at` |
| Carrier / rate | `event_time >= carrier_event_time / rate_event_time` | those columns |
| Stop timestamps | always (backfill) | `LEAST(arrived)`, `GREATEST(departed)` |
| `last_event_time` | max of all | n/a (drives the lateness metric) |

The spec says position updates "only if `event_time > last_event_time`". With two independently
consumed topics, one shared guard would discard valid pings that happen to be processed right
after a newer lifecycle event, so I split it per aspect (spec deviation, noted in PROGRESS.md).
Each row also carries a `version` column for optimistic concurrency (ADR 0002).

### 3. Status never regresses

Statuses have ranks along the main path:
CREATED 0, TENDERED/REJECTED 1, ACCEPTED 2, AT_PICKUP 3, IN_TRANSIT 4, AT_DELIVERY 5, DELIVERED 6.

- An *older* lifecycle event (event time before `status_event_time`) is recorded and backfills
  stop timestamps, but never changes status. The one exception: it may advance the status
  *underneath* an EXCEPTION overlay, so that resolving the exception lands where an in-order
  sequence would have.
- A *newer* event moving to a **lower** rank is invalid. For example, `stop.departed` stamped
  after `shipment.delivered` goes to the DLQ as `INVALID_TRANSITION`.
- A *newer* event moving to a **higher** rank is valid even if it skips stages (for example,
  ACCEPTED to IN_TRANSIT after a missed geofence). This is required for order independence: when
  a newer event arrives before an older one, the processor cannot yet know the gap will be filled.

### 4. Lifecycle events recompute ETA at the newest known position time

When a stop or status changes, the ETA is recomputed as of `max(event_time, last_ping_at)`.

### 5. Scanners and the API run on event time

The detention and stale-tracking scanners, and "today" in `/metrics/summary`, use the **fleet
clock**: `max(max_event_time)` from `stream_watermarks`, which the processor advances per
consumed partition inside its batch transaction. Wall time would be wrong in two cases:
- with the 60x simulator, two simulated hours of detention pass in two real minutes;
- when replaying historical data, the alerts must reflect that data's timeline.

### 6. Impossible GPS jumps re-anchor instead of cascading

A ping implying more than 100 mph since the last accepted ping is dead-lettered as
`IMPOSSIBLE_GPS_JUMP`. A corrupt *first* fix, though, has nothing to be checked against. Once it
becomes the anchor, every genuine ping after it would look impossible: this happened in the
live demo, with 15 good pings dead-lettered for one truck. So a rejected ping is remembered as a
*suspect* (`suspect_lat/lon/at`, migration 0004). If the next ping agrees with the suspect
(at most 100 mph between them, within 30 min) rather than with the anchor, the anchor was the
outlier and the track re-anchors on the new ping. An isolated teleport between good pings is
still rejected, because the following good ping agrees with the anchor, not the suspect. The cost:
at most one good ping is dead-lettered when the first fix is bad, and rejections now update this
bookkeeping (never business state).

### 7. Metrics

- `events_out_of_order_total` counts events older than the newest event already seen for their
  shipment, across both topics.
- `event_lateness_seconds` is a histogram of how far behind they were, in event time.
- Cross-topic interleaving makes the out-of-order count higher than the 3% injected delay rate.
  That is expected.

## Verification

- A Hypothesis property: random event sequences never regress status.
- A Hypothesis property: any shuffle of a valid lifecycle converges to the same
  order-independent state (status, stops, position, carrier, rate) as the ordered sequence.
- An integration test on Redpanda: a shuffled shipment ends in exactly the same database state as
  the ordered one.

## Consequences

- Speed EWMA (and therefore ETA) depends on which pings arrived in order. Pings that arrive late
  are logged but don't feed the EWMA. That is acceptable: the next in-order ping corrects it.
- A real backward move that arrives *before* its predecessor (for example a bogus departure
  processed before the delivery) cannot be detected at that moment. It is accepted, and the
  delivery that arrives later is treated as out of order. This is the cost of order independence.
  A per-shipment reorder buffer could catch it at the price of extra latency.

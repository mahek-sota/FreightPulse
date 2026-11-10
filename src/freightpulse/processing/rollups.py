"""Analytics rollups: facility dwell, ``lane_metrics_hourly``, ``carrier_scorecard_daily``.

Each rollup recomputes a recent window (default 48h of the fleet clock) with one
``INSERT ... SELECT ... ON CONFLICT DO UPDATE``, so it is idempotent and safe to run on a
schedule or re-run after late data arrives.

Definitions:
* delivered at = arrival at the final stop; on time = that arrival <= its ``appt_end``;
* transit hours = final arrival - departure from the first stop;
* detention minutes per stop = max(0, dwell - 120);
* tracking compliance = share of a carrier's shipments delivered that day that never raised a
  STALE_TRACKING alert.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import Connection, text

LANE_METRICS_SQL = text(
    """
    WITH last_stop AS (
        SELECT DISTINCT ON (shipment_id) shipment_id, arrived_at, appt_end
        FROM shipment_stops ORDER BY shipment_id, stop_sequence DESC
    ), first_stop AS (
        SELECT shipment_id, departed_at FROM shipment_stops WHERE stop_sequence = 1
    ), detention AS (
        SELECT shipment_id,
               avg(GREATEST(0, extract(epoch FROM departed_at - arrived_at) / 60 - 120))
                   AS detention_min
        FROM shipment_stops WHERE departed_at IS NOT NULL AND arrived_at IS NOT NULL
        GROUP BY shipment_id
    ), delivered AS (
        SELECT s.lane_id,
               date_trunc('hour', ls.arrived_at) AS hour_bucket,
               ls.arrived_at <= ls.appt_end AS on_time,
               extract(epoch FROM ls.arrived_at - fs.departed_at) / 3600 AS transit_h,
               d.detention_min
        FROM shipments s
        JOIN last_stop ls USING (shipment_id)
        JOIN first_stop fs USING (shipment_id)
        LEFT JOIN detention d USING (shipment_id)
        WHERE s.status = 'DELIVERED'
          AND ls.arrived_at >= :since AND ls.arrived_at < :until
          AND fs.departed_at IS NOT NULL
    )
    INSERT INTO lane_metrics_hourly AS l (lane_id, hour_bucket, shipments_delivered, on_time_pct,
        avg_transit_hours, p90_transit_hours, avg_detention_min)
    SELECT lane_id, hour_bucket, count(*),
           round(100.0 * avg(on_time::int), 2),
           round(avg(transit_h)::numeric, 2),
           round(percentile_cont(0.9) WITHIN GROUP (ORDER BY transit_h)::numeric, 2),
           round(avg(detention_min)::numeric, 2)
    FROM delivered
    GROUP BY lane_id, hour_bucket
    ON CONFLICT (lane_id, hour_bucket) DO UPDATE SET
        shipments_delivered = EXCLUDED.shipments_delivered,
        on_time_pct = EXCLUDED.on_time_pct,
        avg_transit_hours = EXCLUDED.avg_transit_hours,
        p90_transit_hours = EXCLUDED.p90_transit_hours,
        avg_detention_min = EXCLUDED.avg_detention_min
    """
)

CARRIER_SCORECARD_SQL = text(
    """
    WITH tenders AS (
        SELECT payload->>'carrier_id' AS carrier_id,
               (event_time AT TIME ZONE 'UTC')::date AS day,
               count(*) FILTER (WHERE event_type = 'shipment.tendered') AS tenders,
               count(*) FILTER (WHERE event_type = 'shipment.tender_accepted') AS accepted,
               count(*) FILTER (WHERE event_type = 'shipment.tender_rejected') AS rejected
        FROM shipment_events
        WHERE event_type IN ('shipment.tendered', 'shipment.tender_accepted',
                             'shipment.tender_rejected')
          AND event_time >= :since AND event_time < :until
        GROUP BY 1, 2
    ), pickups AS (
        SELECT s.carrier_id, (st.arrived_at AT TIME ZONE 'UTC')::date AS day,
               avg((st.arrived_at <= st.appt_end)::int) AS on_time
        FROM shipment_stops st JOIN shipments s USING (shipment_id)
        WHERE st.stop_type = 'PICKUP' AND s.carrier_id IS NOT NULL
          AND st.arrived_at >= :since AND st.arrived_at < :until
        GROUP BY 1, 2
    ), deliveries AS (
        SELECT s.carrier_id, (st.arrived_at AT TIME ZONE 'UTC')::date AS day,
               avg((st.arrived_at <= st.appt_end)::int) AS on_time,
               avg((NOT EXISTS (
                   SELECT 1 FROM alerts a
                   WHERE a.shipment_id = s.shipment_id AND a.alert_type = 'STALE_TRACKING'
               ))::int) AS compliant
        FROM shipment_stops st JOIN shipments s USING (shipment_id)
        WHERE st.stop_type = 'DELIVERY' AND s.status = 'DELIVERED' AND s.carrier_id IS NOT NULL
          AND st.stop_sequence = (SELECT max(stop_sequence) FROM shipment_stops x
                                  WHERE x.shipment_id = st.shipment_id)
          AND st.arrived_at >= :since AND st.arrived_at < :until
        GROUP BY 1, 2
    ), keys AS (
        SELECT carrier_id, day FROM tenders
        UNION SELECT carrier_id, day FROM pickups
        UNION SELECT carrier_id, day FROM deliveries
    )
    INSERT INTO carrier_scorecard_daily AS c (carrier_id, day, tenders, acceptance_rate,
        on_time_pickup_pct, on_time_delivery_pct, tracking_compliance_pct)
    SELECT k.carrier_id, k.day, COALESCE(t.tenders, 0),
           round(100.0 * t.accepted / NULLIF(t.accepted + t.rejected, 0), 2),
           round(100.0 * p.on_time, 2),
           round(100.0 * d.on_time, 2),
           round(100.0 * d.compliant, 2)
    FROM keys k
    JOIN carriers USING (carrier_id)
    LEFT JOIN tenders t USING (carrier_id, day)
    LEFT JOIN pickups p USING (carrier_id, day)
    LEFT JOIN deliveries d USING (carrier_id, day)
    ON CONFLICT (carrier_id, day) DO UPDATE SET
        tenders = EXCLUDED.tenders,
        acceptance_rate = EXCLUDED.acceptance_rate,
        on_time_pickup_pct = EXCLUDED.on_time_pickup_pct,
        on_time_delivery_pct = EXCLUDED.on_time_delivery_pct,
        tracking_compliance_pct = EXCLUDED.tracking_compliance_pct
    """
)

FACILITY_DWELL_SQL = text(
    """
    UPDATE facilities f SET avg_dwell_min = d.avg_min
    FROM (
        SELECT facility_id,
               round(avg(extract(epoch FROM departed_at - arrived_at) / 60)::numeric, 2) AS avg_min
        FROM shipment_stops
        WHERE arrived_at IS NOT NULL AND departed_at IS NOT NULL
          AND departed_at >= :since AND departed_at < :until
        GROUP BY facility_id
    ) d
    WHERE f.facility_id = d.facility_id
    """
)


def run_rollups(
    conn: Connection, now: datetime, window: timedelta = timedelta(hours=48)
) -> dict[str, int]:
    """Recompute rollups for [now - window, now + 1h). Returns rows touched per rollup."""
    since = now - window
    until = now + timedelta(hours=1)
    # Whole days for the daily scorecard so a partial window never overwrites a full day.
    day_since = datetime(since.year, since.month, since.day, tzinfo=since.tzinfo)
    params = {"since": since, "until": until}
    return {
        "facility_dwell": conn.execute(
            FACILITY_DWELL_SQL, {"since": now - timedelta(days=30), "until": until}
        ).rowcount,
        "lane_metrics_hourly": conn.execute(LANE_METRICS_SQL, params).rowcount,
        "carrier_scorecard_daily": conn.execute(
            CARRIER_SCORECARD_SQL, {"since": day_since, "until": until}
        ).rowcount,
    }

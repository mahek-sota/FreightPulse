from __future__ import annotations

import json

from prometheus_client import REGISTRY

from freightpulse.observability.metrics import update_lag_from_stats


def test_consumer_lag_from_librdkafka_stats() -> None:
    stats = {
        "topics": {
            "freight.telemetry.gps": {
                "partitions": {
                    "0": {"consumer_lag": 42},
                    "1": {"consumer_lag": -1},  # unknown: ignored
                    "-1": {"consumer_lag": 999},  # internal UA partition: ignored
                }
            }
        }
    }
    update_lag_from_stats(json.dumps(stats))
    get = REGISTRY.get_sample_value
    assert get("consumer_lag", {"topic": "freight.telemetry.gps", "partition": "0"}) == 42
    assert get("consumer_lag", {"topic": "freight.telemetry.gps", "partition": "-1"}) is None
    update_lag_from_stats("{}")  # no topics: no-op

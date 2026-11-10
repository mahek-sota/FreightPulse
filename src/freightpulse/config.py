"""Runtime configuration, loaded from environment variables (prefix ``FREIGHTPULSE_``).

Every service reads the same ``Settings`` object so that topic names, database URL and
tuning knobs stay consistent across the simulator, processor, alert sink, jobs and API.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FREIGHTPULSE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- infrastructure -------------------------------------------------------------------
    database_url: str = "postgresql+psycopg://freightpulse:freightpulse@localhost:5433/freightpulse"
    kafka_bootstrap_servers: str = "localhost:19092"

    # --- topics ---------------------------------------------------------------------------
    # A prefix lets tests and benchmarks run on isolated topics against the same broker.
    topic_prefix: str = ""
    shipment_events_topic_name: str = "freight.shipment.events"
    telemetry_topic_name: str = "freight.telemetry.gps"
    alerts_topic_name: str = "freight.alerts"
    dlq_topic_name: str = "freight.dlq"
    shipment_events_partitions: int = 6
    telemetry_partitions: int = 6
    alerts_partitions: int = 3
    dlq_partitions: int = 1

    # --- processor ------------------------------------------------------------------------
    processor_group_id: str = "freightpulse-processor"
    processor_batch_size: int = Field(default=1000, ge=1)
    processor_batch_timeout_s: float = Field(default=0.25, gt=0)
    processor_max_conflict_retries: int = 5
    # Unknown shipment_id: park the event durably and retry for this long (wall clock)
    # before routing it to the DLQ, because ``shipment.created`` may lag.
    unknown_shipment_retry_s: float = 30.0
    parking_sweep_interval_s: float = 2.0
    processor_metrics_port: int = 9100
    # How long the group waits for a crashed member before reassigning its partitions.
    kafka_session_timeout_ms: int = 45_000

    # --- alert sink -----------------------------------------------------------------------
    alert_sink_group_id: str = "freightpulse-alert-sink"
    alert_sink_metrics_port: int = 9101

    # --- periodic jobs (scanners, rollups, partition maintenance) ---------------------------
    scanner_interval_s: float = 60.0
    rollup_interval_s: float = 120.0
    partition_maintenance_interval_s: float = 3600.0
    jobs_metrics_port: int = 9102
    partition_days_ahead: int = 7
    partition_retention_days: int = 90

    # --- API ------------------------------------------------------------------------------
    api_default_page_size: int = 50
    api_max_page_size: int = 500

    @property
    def shipment_events_topic(self) -> str:
        return f"{self.topic_prefix}{self.shipment_events_topic_name}"

    @property
    def telemetry_topic(self) -> str:
        return f"{self.topic_prefix}{self.telemetry_topic_name}"

    @property
    def alerts_topic(self) -> str:
        return f"{self.topic_prefix}{self.alerts_topic_name}"

    @property
    def dlq_topic(self) -> str:
        return f"{self.topic_prefix}{self.dlq_topic_name}"

    def topic_partitions(self) -> dict[str, int]:
        return {
            self.shipment_events_topic: self.shipment_events_partitions,
            self.telemetry_topic: self.telemetry_partitions,
            self.alerts_topic: self.alerts_partitions,
            self.dlq_topic: self.dlq_partitions,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

"""
Runtime settings loaded from environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class RuntimeSettings:
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_key: Optional[str] = None

    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: Optional[str] = None
    mqtt_password: Optional[str] = None
    mqtt_topic_filter: str = "factory/+/+/+/telemetry"
    mqtt_tls_enabled: bool = False
    enable_mqtt: bool = True

    influxdb_url: str = "http://localhost:8086"
    influxdb_token: str = ""
    influxdb_org: str = "factory-stream"
    influxdb_bucket: str = "ot-data"

    timescale_host: str = "localhost"
    timescale_port: int = 5432
    timescale_db: str = "factory_stream"
    timescale_user: str = "postgres"
    timescale_password: str = "postgres"

    registry_path: Optional[str] = None
    event_store_path: str = "/tmp/factory-stream/events.jsonl"
    runtime_state_path: str = "/tmp/factory-stream/runtime-state.json"

    queue_maxsize: int = 10_000
    anomaly_window: int = 60
    worker_heartbeat_ttl_s: int = 30
    worker_poll_sleep_s: float = 1.0

    pipeline_version: str = "phase3-runtime-v1"
    detector_version: str = "rules-v1"

    @classmethod
    def from_env(cls) -> "RuntimeSettings":
        return cls(
            api_host=os.getenv("API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("API_PORT", 8000)),
            api_key=os.getenv("API_KEY") or None,
            mqtt_host=os.getenv("MQTT_HOST", "localhost"),
            mqtt_port=int(os.getenv("MQTT_PORT", 1883)),
            mqtt_username=os.getenv("MQTT_USERNAME") or None,
            mqtt_password=os.getenv("MQTT_PASSWORD") or None,
            mqtt_topic_filter=os.getenv("MQTT_TOPIC_FILTER", "factory/+/+/+/telemetry"),
            mqtt_tls_enabled=_env_bool("MQTT_TLS_ENABLED", False),
            enable_mqtt=_env_bool("ENABLE_MQTT", True),
            influxdb_url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
            influxdb_token=os.getenv("INFLUXDB_TOKEN", ""),
            influxdb_org=os.getenv("INFLUXDB_ORG", "factory-stream"),
            influxdb_bucket=os.getenv("INFLUXDB_BUCKET", "ot-data"),
            timescale_host=os.getenv("TIMESCALE_HOST", "localhost"),
            timescale_port=int(os.getenv("TIMESCALE_PORT", 5432)),
            timescale_db=os.getenv("TIMESCALE_DB", "factory_stream"),
            timescale_user=os.getenv("TIMESCALE_USER", "postgres"),
            timescale_password=os.getenv("TIMESCALE_PASSWORD", "postgres"),
            registry_path=os.getenv("REGISTRY_PATH") or None,
            event_store_path=os.getenv("EVENT_STORE_PATH", "/tmp/factory-stream/events.jsonl"),
            runtime_state_path=os.getenv(
                "RUNTIME_STATE_PATH", "/tmp/factory-stream/runtime-state.json"
            ),
            queue_maxsize=int(os.getenv("QUEUE_MAXSIZE", 10_000)),
            anomaly_window=int(os.getenv("ANOMALY_WINDOW", 60)),
            worker_heartbeat_ttl_s=int(os.getenv("WORKER_HEARTBEAT_TTL_S", 30)),
            worker_poll_sleep_s=float(os.getenv("WORKER_POLL_SLEEP_S", 1.0)),
            pipeline_version=os.getenv("PIPELINE_VERSION", "phase3-runtime-v1"),
            detector_version=os.getenv("DETECTOR_VERSION", "rules-v1"),
        )


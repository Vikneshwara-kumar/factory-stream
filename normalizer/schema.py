"""
Canonical MachineReading schema.
All protocol adapters normalize their raw payloads into this unified model.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class SourceProtocol(str, Enum):
    MQTT = "mqtt"
    MODBUS = "modbus"
    OPCUA = "opcua"


class MachineStatus(str, Enum):
    RUNNING = "running"
    IDLE = "idle"
    FAULT = "fault"
    MAINTENANCE = "maintenance"
    UNKNOWN = "unknown"


class AnomalyFlag(str, Enum):
    SPIKE = "SPIKE"
    DRIFT = "DRIFT"
    FLATLINE = "FLATLINE"
    OUT_OF_RANGE = "OUT_OF_RANGE"


class MetricPayload(BaseModel):
    """Flexible metric container — keys are metric names, values are floats."""

    temperature: Optional[float] = Field(None, description="Temperature in Celsius")
    vibration: Optional[float] = Field(None, description="Vibration amplitude (g)")
    rpm: Optional[float] = Field(None, description="Rotational speed (RPM)")
    pressure: Optional[float] = Field(None, description="Pressure in kPa")
    power_kw: Optional[float] = Field(None, description="Power consumption (kW)")
    current_a: Optional[float] = Field(None, description="Current in Amperes")
    voltage_v: Optional[float] = Field(None, description="Voltage in Volts")
    humidity: Optional[float] = Field(None, description="Relative humidity (%)")
    flow_rate: Optional[float] = Field(None, description="Flow rate (L/min)")

    # Allow extra metrics not in the fixed schema
    model_config = {"extra": "allow"}

    def to_dict(self) -> Dict[str, float]:
        return {k: v for k, v in self.model_dump().items() if v is not None}


class MachineReading(BaseModel):
    """
    Unified normalized reading from any OT device.
    This is the canonical schema used throughout the pipeline.
    """

    machine_id: str = Field(..., description="Unique identifier for the machine")
    plant_id: Optional[str] = Field(None, description="Plant / facility identifier")
    line_id: Optional[str] = Field(None, description="Production line identifier")
    source_protocol: SourceProtocol = Field(..., description="Originating protocol")
    event_id: Optional[str] = Field(None, description="Stable idempotency key for the reading")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of the reading",
    )
    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when the pipeline accepted the reading",
    )
    metrics: MetricPayload = Field(..., description="Sensor metrics")
    status: MachineStatus = Field(
        default=MachineStatus.UNKNOWN, description="Machine operational status"
    )
    raw_payload: Optional[Dict[str, Any]] = Field(
        None, description="Original raw payload for debugging"
    )

    # Anomaly fields — populated by anomaly detector
    anomaly_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Composite anomaly score (0=normal, 1=critical)",
    )
    anomaly_flags: List[AnomalyFlag] = Field(
        default_factory=list, description="Specific anomaly types detected"
    )
    pipeline_version: str = Field(
        default="phase3-runtime-v1",
        description="Pipeline version that processed the reading",
    )
    detector_version: str = Field(
        default="rules-v1",
        description="Detector version that produced the anomaly metadata",
    )

    @field_validator("timestamp", "ingested_at", mode="before")
    @classmethod
    def ensure_utc(cls, v):
        if isinstance(v, str):
            v = datetime.fromisoformat(v)
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v

    @model_validator(mode="after")
    def ensure_event_id(self) -> "MachineReading":
        if not self.event_id:
            self.event_id = self.build_event_id()
        return self

    def build_event_id(self) -> str:
        payload = {
            "machine_id": self.machine_id,
            "plant_id": self.plant_id,
            "line_id": self.line_id,
            "source_protocol": self.source_protocol.value,
            "timestamp": self.timestamp.isoformat(),
            "metrics": self.metrics.to_dict(),
            "status": self.status.value,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def is_anomalous(self, threshold: float = 0.5) -> bool:
        return self.anomaly_score >= threshold

    def to_influx_point(self) -> Dict[str, Any]:
        """Format for InfluxDB line protocol ingestion."""
        return {
            "measurement": "machine_readings",
            "tags": {
                "event_id": self.event_id,
                "machine_id": self.machine_id,
                "plant_id": self.plant_id or "unknown",
                "line_id": self.line_id or "unknown",
                "protocol": self.source_protocol.value,
                "status": self.status.value,
            },
            "fields": {
                **self.metrics.to_dict(),
                "anomaly_score": self.anomaly_score,
                "anomaly_flags": ",".join(self.anomaly_flags) or "none",
                "pipeline_version": self.pipeline_version,
                "detector_version": self.detector_version,
            },
            "time": self.timestamp.isoformat(),
        }

    def to_timescale_row(self) -> Dict[str, Any]:
        """Format for TimescaleDB INSERT."""
        return {
            "event_id": self.event_id,
            "ingested_at": self.ingested_at,
            "time": self.timestamp,
            "machine_id": self.machine_id,
            "plant_id": self.plant_id,
            "line_id": self.line_id,
            "protocol": self.source_protocol.value,
            "status": self.status.value,
            "anomaly_score": self.anomaly_score,
            "anomaly_flags": [flag.value for flag in self.anomaly_flags],
            "pipeline_version": self.pipeline_version,
            "detector_version": self.detector_version,
            **self.metrics.to_dict(),
        }

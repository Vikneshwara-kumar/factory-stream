"""
Normalization engine.
Transforms raw protocol-specific payloads into canonical MachineReading objects.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from normalizer.schema import (
    MachineReading,
    MachineStatus,
    MetricPayload,
    SourceProtocol,
)

logger = logging.getLogger(__name__)


class NormalizationError(Exception):
    pass


class MQTTNormalizer:
    """
    Normalizes MQTT payloads.

    Expected topic pattern: factory/{plant_id}/{line_id}/{machine_id}/telemetry
    Expected payload (JSON):
    {
        "ts": 1709900000,
        "temp": 72.4,
        "vib": 0.03,
        "rpm": 1450,
        "pres": 101.3,
        "pwr": 3.2,
        "status": "running"
    }
    """

    METRIC_MAP = {
        "temp": "temperature",
        "temperature": "temperature",
        "vib": "vibration",
        "vibration": "vibration",
        "rpm": "rpm",
        "speed": "rpm",
        "pres": "pressure",
        "pressure": "pressure",
        "pwr": "power_kw",
        "power": "power_kw",
        "power_kw": "power_kw",
        "current": "current_a",
        "current_a": "current_a",
        "voltage": "voltage_v",
        "voltage_v": "voltage_v",
        "humidity": "humidity",
        "hum": "humidity",
        "flow": "flow_rate",
        "flow_rate": "flow_rate",
    }

    def normalize(self, topic: str, payload: bytes | str | Dict) -> MachineReading:
        try:
            if isinstance(payload, (bytes, str)):
                data = json.loads(payload)
            else:
                data = payload

            # Parse topic: factory/{plant_id}/{line_id}/{machine_id}/telemetry
            parts = topic.strip("/").split("/")
            machine_id = parts[3] if len(parts) >= 4 else "unknown"
            plant_id = parts[1] if len(parts) >= 2 else None
            line_id = parts[2] if len(parts) >= 3 else None

            # Extract timestamp
            ts = data.get("ts") or data.get("timestamp")
            if ts:
                timestamp = datetime.fromtimestamp(float(ts), tz=timezone.utc) if isinstance(ts, (int, float)) else datetime.fromisoformat(ts)
            else:
                timestamp = datetime.now(timezone.utc)

            # Normalize metrics
            metrics = {}
            for raw_key, value in data.items():
                normalized_key = self.METRIC_MAP.get(raw_key.lower())
                if normalized_key and isinstance(value, (int, float)):
                    metrics[normalized_key] = float(value)

            # Parse status
            raw_status = str(data.get("status", "unknown")).lower()
            status = MachineStatus(raw_status) if raw_status in MachineStatus._value2member_map_ else MachineStatus.UNKNOWN

            return MachineReading(
                machine_id=machine_id,
                plant_id=plant_id,
                line_id=line_id,
                source_protocol=SourceProtocol.MQTT,
                timestamp=timestamp,
                metrics=MetricPayload(**metrics),
                status=status,
                raw_payload=data,
            )

        except Exception as e:
            raise NormalizationError(f"MQTT normalization failed: {e}") from e


class ModbusNormalizer:
    """
    Normalizes Modbus register reads.

    Register map (configurable):
    40001 → temperature (x0.1)
    40002 → vibration (x0.001)
    40003 → rpm (x1)
    40004 → pressure (x0.1)
    40005 → power_kw (x0.01)
    40006 → status code
    """

    REGISTER_MAP = {
        40001: ("temperature", 0.1),
        40002: ("vibration", 0.001),
        40003: ("rpm", 1.0),
        40004: ("pressure", 0.1),
        40005: ("power_kw", 0.01),
        40006: ("current_a", 0.01),
        40007: ("voltage_v", 0.1),
    }

    STATUS_REGISTER = 40010
    STATUS_MAP = {
        0: MachineStatus.IDLE,
        1: MachineStatus.RUNNING,
        2: MachineStatus.FAULT,
        3: MachineStatus.MAINTENANCE,
    }

    def normalize(
        self,
        machine_id: str,
        registers: Dict[int, int],
        plant_id: Optional[str] = None,
        line_id: Optional[str] = None,
    ) -> MachineReading:
        try:
            metrics = {}
            for reg_addr, (metric_name, scale) in self.REGISTER_MAP.items():
                if reg_addr in registers:
                    metrics[metric_name] = registers[reg_addr] * scale

            status_code = registers.get(self.STATUS_REGISTER, 0)
            status = self.STATUS_MAP.get(status_code, MachineStatus.UNKNOWN)

            return MachineReading(
                machine_id=machine_id,
                plant_id=plant_id,
                line_id=line_id,
                source_protocol=SourceProtocol.MODBUS,
                timestamp=datetime.now(timezone.utc),
                metrics=MetricPayload(**metrics),
                status=status,
                raw_payload={"registers": registers},
            )

        except Exception as e:
            raise NormalizationError(f"Modbus normalization failed: {e}") from e


class OPCUANormalizer:
    """
    Normalizes OPC-UA node reads.

    Expected node structure:
    {
        "ns=2;i=1001": {"name": "Temperature", "value": 72.4, "status": "Good"},
        "ns=2;i=1002": {"name": "Vibration", "value": 0.03, "status": "Good"},
        ...
    }
    """

    NODE_NAME_MAP = {
        "temperature": "temperature",
        "temp": "temperature",
        "vibration": "vibration",
        "vib": "vibration",
        "spindle_speed": "rpm",
        "rpm": "rpm",
        "pressure": "pressure",
        "power": "power_kw",
        "active_power": "power_kw",
        "current": "current_a",
        "voltage": "voltage_v",
        "humidity": "humidity",
        "flow_rate": "flow_rate",
    }

    def normalize(
        self,
        machine_id: str,
        nodes: Dict[str, Dict[str, Any]],
        plant_id: Optional[str] = None,
        line_id: Optional[str] = None,
    ) -> MachineReading:
        try:
            metrics = {}
            status = MachineStatus.UNKNOWN

            for node_id, node_data in nodes.items():
                name = node_data.get("name", "").lower().replace(" ", "_")
                value = node_data.get("value")
                quality = node_data.get("status", "Good")

                if quality != "Good":
                    logger.warning(f"OPC-UA node {node_id} has bad quality: {quality}")
                    continue

                if name == "machine_status":
                    status = MachineStatus(str(value).lower()) if str(value).lower() in MachineStatus._value2member_map_ else MachineStatus.UNKNOWN
                    continue

                mapped = self.NODE_NAME_MAP.get(name)
                if mapped and isinstance(value, (int, float)):
                    metrics[mapped] = float(value)

            return MachineReading(
                machine_id=machine_id,
                plant_id=plant_id,
                line_id=line_id,
                source_protocol=SourceProtocol.OPCUA,
                timestamp=datetime.now(timezone.utc),
                metrics=MetricPayload(**metrics),
                status=status,
                raw_payload={"nodes": nodes},
            )

        except Exception as e:
            raise NormalizationError(f"OPC-UA normalization failed: {e}") from e


class NormalizerRouter:
    """Routes raw payloads to the correct normalizer based on protocol."""

    def __init__(self):
        self.mqtt = MQTTNormalizer()
        self.modbus = ModbusNormalizer()
        self.opcua = OPCUANormalizer()

    def normalize_mqtt(self, topic: str, payload: Any) -> MachineReading:
        return self.mqtt.normalize(topic, payload)

    def normalize_modbus(self, machine_id: str, registers: Dict, **kwargs) -> MachineReading:
        return self.modbus.normalize(machine_id, registers, **kwargs)

    def normalize_opcua(self, machine_id: str, nodes: Dict, **kwargs) -> MachineReading:
        return self.opcua.normalize(machine_id, nodes, **kwargs)

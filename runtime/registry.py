"""
Device registry and collector configuration loader.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

try:
    import yaml

    HAS_YAML = True
except ImportError:  # pragma: no cover - optional fallback
    HAS_YAML = False


@dataclass(frozen=True)
class MQTTSourceConfig:
    enabled: bool = True
    broker_host: Optional[str] = None
    broker_port: Optional[int] = None
    topic_filter: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    tls_enabled: bool = False


@dataclass(frozen=True)
class ModbusDeviceConfig:
    machine_id: str
    host: str
    port: int = 502
    unit_id: int = 1
    plant_id: Optional[str] = None
    line_id: Optional[str] = None
    poll_interval: float = 1.0
    register_addresses: List[int] = field(default_factory=list)


@dataclass(frozen=True)
class OPCUADeviceConfig:
    machine_id: str
    endpoint: str
    plant_id: Optional[str] = None
    line_id: Optional[str] = None
    poll_interval: float = 1.0
    nodes: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DeviceRegistry:
    mqtt: Optional[MQTTSourceConfig] = None
    modbus_devices: List[ModbusDeviceConfig] = field(default_factory=list)
    opcua_devices: List[OPCUADeviceConfig] = field(default_factory=list)

    @classmethod
    def empty(cls) -> "DeviceRegistry":
        return cls()

    @classmethod
    def load(cls, path: Optional[str]) -> "DeviceRegistry":
        if not path:
            return cls.empty()

        registry_path = Path(path)
        if not registry_path.exists():
            return cls.empty()

        with registry_path.open("r", encoding="utf-8") as handle:
            if registry_path.suffix.lower() == ".json" or not HAS_YAML:
                payload = json.load(handle)
            else:
                payload = yaml.safe_load(handle) or {}

        mqtt_payload = payload.get("mqtt")
        mqtt = MQTTSourceConfig(**mqtt_payload) if mqtt_payload else None
        modbus_devices = [
            ModbusDeviceConfig(**device) for device in payload.get("modbus_devices", [])
        ]
        opcua_devices = [
            OPCUADeviceConfig(**device) for device in payload.get("opcua_devices", [])
        ]
        return cls(
            mqtt=mqtt,
            modbus_devices=modbus_devices,
            opcua_devices=opcua_devices,
        )

"""
Focused tests for the production-grade runtime upgrades.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from api.main import create_app
from anomaly.detector import AnomalyDetectionPipeline
from ingestion.adapters.modbus_adapter import ModbusPollingAdapter
from ingestion.adapters.opcua_adapter import OPCUAPollingAdapter
from normalizer.schema import AnomalyFlag, MachineReading, MetricPayload, SourceProtocol
from runtime.registry import DeviceRegistry, ModbusDeviceConfig, OPCUADeviceConfig
from runtime.services import FactoryStreamWorker, ReplayService
from runtime.settings import RuntimeSettings
from runtime.state import RuntimeStateStore
from storage.event_store import EventStore


def make_reading(
    *,
    machine_id: str = "CNC-001",
    source_protocol: SourceProtocol = SourceProtocol.MQTT,
    timestamp: datetime | None = None,
    temperature: float = 72.4,
    anomaly_score: float = 0.0,
    anomaly_flags: list[AnomalyFlag] | None = None,
) -> MachineReading:
    return MachineReading(
        machine_id=machine_id,
        source_protocol=source_protocol,
        timestamp=timestamp or datetime.now(timezone.utc),
        metrics=MetricPayload(temperature=temperature, rpm=1450.0),
        anomaly_score=anomaly_score,
        anomaly_flags=anomaly_flags or [],
    )


def test_event_store_persists_and_dedupes(tmp_path):
    path = tmp_path / "events.jsonl"
    store = EventStore(str(path))
    reading = make_reading(anomaly_score=0.8, anomaly_flags=[AnomalyFlag.SPIKE])

    assert store.append(reading) is True
    assert store.append(reading.model_copy(deep=True)) is False

    reopened = EventStore(str(path))
    stats = reopened.get_stats()
    assert stats["total_readings"] == 1
    assert stats["total_anomalies"] == 1
    latest = reopened.get_latest_reading(reading.machine_id)
    assert latest is not None
    assert latest.event_id == reading.event_id


def test_device_registry_loads_yaml(tmp_path):
    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(
        """
mqtt:
  enabled: true
  broker_host: broker
  broker_port: 1883
modbus_devices:
  - machine_id: MODBUS-001
    host: modbus
    port: 502
opcua_devices:
  - machine_id: OPCUA-001
    endpoint: opc.tcp://opcua:4840/factory/
    nodes:
      Temperature: ns=2;i=1001
""",
        encoding="utf-8",
    )

    registry = DeviceRegistry.load(str(registry_path))
    assert registry.mqtt is not None
    assert registry.mqtt.broker_host == "broker"
    assert len(registry.modbus_devices) == 1
    assert len(registry.opcua_devices) == 1


def test_modbus_polling_adapter_normalizes_fake_client():
    class FakeResult:
        registers = [724, 30, 1450, 0, 0, 0, 0, 0, 0, 1]

        @staticmethod
        def isError():
            return False

    class FakeClient:
        def connect(self):
            return True

        def read_holding_registers(self, address, count, slave):
            assert address == 0
            assert count == 10
            assert slave == 1
            return FakeResult()

        def close(self):
            return None

    device = ModbusDeviceConfig(
        machine_id="MODBUS-001",
        host="modbus",
        register_addresses=[40001, 40002, 40003, 40010],
    )
    captured = []
    adapter = ModbusPollingAdapter([device], on_reading=captured.append)
    reading = adapter.poll_device_once(device, client=FakeClient())

    assert reading is not None
    assert reading.metrics.temperature == 72.4
    assert reading.metrics.vibration == 0.03
    assert reading.metrics.rpm == 1450.0
    assert captured[0].machine_id == "MODBUS-001"


def test_opcua_polling_adapter_normalizes_fake_client():
    class FakeNode:
        def __init__(self, value):
            self.value = value

        def get_value(self):
            return self.value

    class FakeClient:
        def __init__(self):
            self.values = {
                "temp-node": FakeNode(72.4),
                "rpm-node": FakeNode(1450),
                "status-node": FakeNode("running"),
            }

        def connect(self):
            return True

        def get_node(self, node_id):
            return self.values[node_id]

        def disconnect(self):
            return None

    device = OPCUADeviceConfig(
        machine_id="OPCUA-001",
        endpoint="opc.tcp://opcua:4840/factory/",
        nodes={
            "Temperature": "temp-node",
            "RPM": "rpm-node",
            "Machine Status": "status-node",
        },
    )
    captured = []
    adapter = OPCUAPollingAdapter([device], on_reading=captured.append)
    reading = adapter.poll_device_once(device, client=FakeClient())

    assert reading is not None
    assert reading.metrics.temperature == 72.4
    assert reading.metrics.rpm == 1450.0
    assert reading.status.value == "running"
    assert captured[0].machine_id == "OPCUA-001"


def test_api_uses_event_store_and_api_key(tmp_path):
    event_store_path = tmp_path / "events.jsonl"
    runtime_state_path = tmp_path / "runtime.json"
    event_store = EventStore(str(event_store_path))
    event_store.append(make_reading())
    RuntimeStateStore(str(runtime_state_path)).write(
        {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "last_heartbeat": datetime.now(timezone.utc).isoformat(),
            "queue_depth": 0,
            "total_duplicates": 0,
            "failed_events": 0,
            "pipeline_version": "phase3-runtime-v1",
            "detector_version": "rules-v1",
        }
    )

    app = create_app(
        RuntimeSettings(
            api_key="secret",
            event_store_path=str(event_store_path),
            runtime_state_path=str(runtime_state_path),
        )
    )
    client = TestClient(app)

    assert client.get("/machines").status_code == 401
    assert client.get("/ready").status_code == 200
    authorized = client.get("/machines", headers={"x-api-key": "secret"})
    assert authorized.status_code == 200
    assert authorized.json()["count"] == 1
    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert "factory_stream_total_readings 1" in metrics.text


def test_replay_service_exports_versioned_results(tmp_path):
    path = tmp_path / "events.jsonl"
    store = EventStore(str(path))
    base = datetime.now(timezone.utc)
    store.append(make_reading(timestamp=base, temperature=70.0))
    store.append(make_reading(timestamp=base + timedelta(seconds=1), temperature=71.0))
    store.append(make_reading(timestamp=base + timedelta(seconds=2), temperature=250.0))

    output_path = tmp_path / "replay.jsonl"
    service = ReplayService(
        event_store=store,
        detector_version="rules-replay-v2",
        replay_pipeline_version="replay-v2",
    )
    result = service.replay(output_path=str(output_path))

    assert result["replayed"] == 3
    assert result["exported"] == 3
    exported = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    assert exported[0]["pipeline_version"] == "replay-v2"
    assert exported[0]["detector_version"] == "rules-replay-v2"
    assert any(record["anomaly_score"] > 0.0 for record in exported)


def test_worker_processes_once_and_records_state(tmp_path):
    class FakeWriter:
        def __init__(self):
            self.writes = []

        def write(self, reading):
            self.writes.append(reading.model_copy(deep=True))

        def close(self):
            return None

    event_store_path = tmp_path / "events.jsonl"
    runtime_state_path = tmp_path / "runtime.json"
    event_store = EventStore(str(event_store_path))
    state_store = RuntimeStateStore(str(runtime_state_path))
    influx_writer = FakeWriter()
    timescale_writer = FakeWriter()
    settings = RuntimeSettings(
        event_store_path=str(event_store_path),
        runtime_state_path=str(runtime_state_path),
        pipeline_version="phase3-test",
        detector_version="rules-test",
    )
    worker = FactoryStreamWorker(
        settings=settings,
        event_store=event_store,
        state_store=state_store,
        anomaly_pipeline=AnomalyDetectionPipeline(version="rules-test"),
        influx_writer=influx_writer,
        timescale_writer=timescale_writer,
        adapters=[],
    )

    reading = make_reading()
    assert worker.process_reading(reading) is True
    assert worker.process_reading(reading.model_copy(deep=True)) is False
    state = state_store.read()

    assert state["total_processed"] == 1
    assert state["total_duplicates"] == 1
    assert influx_writer.writes[0].pipeline_version == "phase3-test"
    assert influx_writer.writes[0].detector_version == "rules-test"

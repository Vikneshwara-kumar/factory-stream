"""
Unit tests for factory-stream core components.
"""

import json
import pytest
from datetime import datetime, timezone

from normalizer.schema import (
    AnomalyFlag, MachineReading, MachineStatus, MetricPayload, SourceProtocol
)
from normalizer.engine import (
    MQTTNormalizer, ModbusNormalizer, OPCUANormalizer, NormalizationError
)
from anomaly.detector import (
    AnomalyDetectionPipeline, RollingStats, MachineAnomalyDetector
)


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestMachineReading:
    def test_basic_creation(self):
        r = MachineReading(
            machine_id="CNC-001",
            source_protocol=SourceProtocol.MQTT,
            metrics=MetricPayload(temperature=72.4, rpm=1450.0),
        )
        assert r.machine_id == "CNC-001"
        assert r.anomaly_score == 0.0
        assert r.anomaly_flags == []
        assert r.status == MachineStatus.UNKNOWN

    def test_timestamp_defaults_to_utc(self):
        r = MachineReading(
            machine_id="X",
            source_protocol=SourceProtocol.MODBUS,
            metrics=MetricPayload(),
        )
        assert r.timestamp.tzinfo is not None

    def test_to_influx_point(self):
        r = MachineReading(
            machine_id="CNC-001",
            plant_id="plant-A",
            source_protocol=SourceProtocol.MQTT,
            metrics=MetricPayload(temperature=72.4, rpm=1450),
            status=MachineStatus.RUNNING,
        )
        point = r.to_influx_point()
        assert point["measurement"] == "machine_readings"
        assert point["tags"]["machine_id"] == "CNC-001"
        assert "temperature" in point["fields"]
        assert point["fields"]["temperature"] == 72.4

    def test_to_timescale_row(self):
        r = MachineReading(
            machine_id="CNC-001",
            source_protocol=SourceProtocol.OPCUA,
            metrics=MetricPayload(vibration=0.03),
        )
        row = r.to_timescale_row()
        assert row["machine_id"] == "CNC-001"
        assert row["protocol"] == "opcua"
        assert row["vibration"] == 0.03

    def test_is_anomalous(self):
        r = MachineReading(
            machine_id="X",
            source_protocol=SourceProtocol.MQTT,
            metrics=MetricPayload(),
            anomaly_score=0.8,
        )
        assert r.is_anomalous(threshold=0.5) is True
        assert r.is_anomalous(threshold=0.9) is False


class TestMetricPayload:
    def test_to_dict_excludes_none(self):
        m = MetricPayload(temperature=72.4, rpm=None)
        d = m.to_dict()
        assert "temperature" in d
        assert "rpm" not in d

    def test_extra_fields_allowed(self):
        m = MetricPayload(custom_metric=99.9)
        assert m.to_dict()["custom_metric"] == 99.9


# ---------------------------------------------------------------------------
# Normalizer tests
# ---------------------------------------------------------------------------

class TestMQTTNormalizer:
    def setup_method(self):
        self.norm = MQTTNormalizer()

    def test_basic_normalization(self):
        topic = "factory/plant-A/line-1/CNC-001/telemetry"
        payload = json.dumps({
            "ts": 1709900000,
            "temp": 72.4,
            "vib": 0.03,
            "rpm": 1450,
            "status": "running",
        })
        r = self.norm.normalize(topic, payload)
        assert r.machine_id == "CNC-001"
        assert r.plant_id == "plant-A"
        assert r.line_id == "line-1"
        assert r.source_protocol == SourceProtocol.MQTT
        assert r.metrics.temperature == 72.4
        assert r.metrics.vibration == 0.03
        assert r.status == MachineStatus.RUNNING

    def test_unknown_status_defaults(self):
        topic = "factory/p/l/M1/telemetry"
        payload = json.dumps({"temp": 50, "status": "mystery_mode"})
        r = self.norm.normalize(topic, payload)
        assert r.status == MachineStatus.UNKNOWN

    def test_bytes_payload(self):
        topic = "factory/p/l/M1/telemetry"
        payload = b'{"temp": 60.0}'
        r = self.norm.normalize(topic, payload)
        assert r.metrics.temperature == 60.0

    def test_invalid_json_raises(self):
        with pytest.raises(NormalizationError):
            self.norm.normalize("factory/p/l/M1/telemetry", b"not-json")


class TestModbusNormalizer:
    def setup_method(self):
        self.norm = ModbusNormalizer()

    def test_register_conversion(self):
        registers = {
            40001: 724,   # temp = 72.4
            40002: 30,    # vibration = 0.03
            40003: 1450,  # rpm = 1450
            40010: 1,     # running
        }
        r = self.norm.normalize("PRESS-001", registers)
        assert r.source_protocol == SourceProtocol.MODBUS
        assert r.metrics.temperature == pytest.approx(72.4, abs=0.1)
        assert r.metrics.vibration == pytest.approx(0.03, abs=0.001)
        assert r.metrics.rpm == 1450.0
        assert r.status == MachineStatus.RUNNING

    def test_fault_status(self):
        r = self.norm.normalize("M1", {40010: 2})
        assert r.status == MachineStatus.FAULT


class TestOPCUANormalizer:
    def setup_method(self):
        self.norm = OPCUANormalizer()

    def test_node_normalization(self):
        nodes = {
            "ns=2;i=1001": {"name": "Temperature", "value": 72.4, "status": "Good"},
            "ns=2;i=1002": {"name": "RPM", "value": 1450, "status": "Good"},
            "ns=2;i=1003": {"name": "Vibration", "value": 0.03, "status": "Good"},
        }
        r = self.norm.normalize("CNC-001", nodes)
        assert r.source_protocol == SourceProtocol.OPCUA
        assert r.metrics.temperature == 72.4
        assert r.metrics.rpm == 1450
        assert r.metrics.vibration == pytest.approx(0.03, abs=1e-5)

    def test_bad_quality_skipped(self):
        nodes = {
            "ns=2;i=1001": {"name": "Temperature", "value": 999.9, "status": "Bad"},
        }
        r = self.norm.normalize("M1", nodes)
        assert r.metrics.temperature is None


# ---------------------------------------------------------------------------
# Anomaly detection tests
# ---------------------------------------------------------------------------

class TestRollingStats:
    def test_zscore_returns_none_before_warmup(self):
        rs = RollingStats(window=60)
        for _ in range(9):
            rs.update(50.0)
        assert rs.zscore(50.0) is None

    def test_zscore_detects_spike(self):
        rs = RollingStats(window=60)
        for _ in range(50):
            rs.update(50.0)
        z = rs.zscore(200.0)
        assert z is not None
        assert z > 5.0

    def test_flatline_detection(self):
        rs = RollingStats(window=60)
        for _ in range(20):
            rs.update(50.0)
        assert rs.is_flatline(tolerance=0.001, window=10) is True

    def test_no_flatline_with_variance(self):
        rs = RollingStats(window=60)
        import random
        for _ in range(20):
            rs.update(50.0 + random.gauss(0, 2))
        assert rs.is_flatline(tolerance=0.001, window=10) is False


class TestAnomalyDetectionPipeline:
    def _make_reading(self, machine_id="CNC-001", temperature=70.0, anomaly_score=0.0):
        return MachineReading(
            machine_id=machine_id,
            source_protocol=SourceProtocol.MQTT,
            metrics=MetricPayload(temperature=temperature, rpm=1450.0),
        )

    def test_normal_reading_low_score(self):
        pipeline = AnomalyDetectionPipeline()
        # Warm up
        for _ in range(30):
            r = self._make_reading(temperature=70.0)
            pipeline.process(r)
        r = self._make_reading(temperature=71.0)
        result = pipeline.process(r)
        assert result.anomaly_score < 0.5

    def test_spike_detected(self):
        pipeline = AnomalyDetectionPipeline()
        for _ in range(30):
            r = self._make_reading(temperature=70.0)
            pipeline.process(r)
        # Inject massive spike
        r = self._make_reading(temperature=250.0)
        result = pipeline.process(r)
        assert result.anomaly_score > 0.5
        assert AnomalyFlag.OUT_OF_RANGE in result.anomaly_flags or AnomalyFlag.SPIKE in result.anomaly_flags

    def test_separate_detectors_per_machine(self):
        pipeline = AnomalyDetectionPipeline()
        for _ in range(20):
            r1 = self._make_reading(machine_id="M1", temperature=70.0)
            pipeline.process(r1)
        # M2 should have a fresh detector
        r2 = self._make_reading(machine_id="M2", temperature=70.0)
        result = pipeline.process(r2)
        assert result.anomaly_score == 0.0  # No prior data → no anomaly

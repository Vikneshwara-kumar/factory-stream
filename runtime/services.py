"""
Worker and replay services.
"""

from __future__ import annotations

import json
import logging
import queue
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from anomaly.detector import AnomalyDetectionPipeline
from ingestion.adapters.modbus_adapter import ModbusPollingAdapter
from ingestion.adapters.mqtt_adapter import MQTTAdapter
from ingestion.adapters.opcua_adapter import OPCUAPollingAdapter
from normalizer.schema import MachineReading
from runtime.registry import DeviceRegistry
from runtime.settings import RuntimeSettings
from runtime.state import RuntimeStateStore
from storage.event_store import EventStore
from storage.influxdb.writer import InfluxDBWriter
from storage.timescaledb.writer import TimescaleDBWriter

logger = logging.getLogger(__name__)


class FactoryStreamWorker:
    def __init__(
        self,
        settings: Optional[RuntimeSettings] = None,
        registry: Optional[DeviceRegistry] = None,
        event_store: Optional[EventStore] = None,
        state_store: Optional[RuntimeStateStore] = None,
        anomaly_pipeline: Optional[AnomalyDetectionPipeline] = None,
        influx_writer: Optional[InfluxDBWriter] = None,
        timescale_writer: Optional[TimescaleDBWriter] = None,
        adapters: Optional[List[object]] = None,
    ):
        self.settings = settings or RuntimeSettings.from_env()
        self.registry = registry or DeviceRegistry.load(self.settings.registry_path)
        self.event_store = event_store or EventStore(self.settings.event_store_path)
        self.state_store = state_store or RuntimeStateStore(self.settings.runtime_state_path)
        self.anomaly_pipeline = anomaly_pipeline or AnomalyDetectionPipeline(
            window=self.settings.anomaly_window,
            version=self.settings.detector_version,
        )
        self.influx_writer = influx_writer or InfluxDBWriter(
            url=self.settings.influxdb_url,
            token=self.settings.influxdb_token,
            org=self.settings.influxdb_org,
            bucket=self.settings.influxdb_bucket,
        )
        self.timescale_writer = timescale_writer or TimescaleDBWriter(
            host=self.settings.timescale_host,
            port=self.settings.timescale_port,
            database=self.settings.timescale_db,
            user=self.settings.timescale_user,
            password=self.settings.timescale_password,
        )
        self._queue: queue.Queue[MachineReading] = queue.Queue(
            maxsize=self.settings.queue_maxsize
        )
        self._running = False
        self._worker_thread = None
        self.adapters = adapters or self._build_adapters()
        self.total_received = 0
        self.total_processed = 0
        self.total_duplicates = 0
        self.total_anomalies = 0
        self.failed_events = 0
        self.readings_per_protocol: Dict[str, int] = {}
        self.started_at = datetime.now(timezone.utc)
        self.last_error: Optional[str] = None

    def start(self) -> None:
        self._running = True
        for adapter in self.adapters:
            adapter.start()
        import threading

        self._worker_thread = threading.Thread(
            target=self._process_loop,
            daemon=True,
            name="factory-stream-worker",
        )
        self._worker_thread.start()
        self._write_state()

    def stop(self) -> None:
        self._running = False
        for adapter in self.adapters:
            adapter.stop()
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=2.0)
        self.influx_writer.close()
        self.timescale_writer.close()
        self._write_state()

    def run_forever(self) -> None:
        self.start()
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("[WORKER] Stopping worker")
        finally:
            self.stop()

    def enqueue(self, reading: MachineReading) -> None:
        self.total_received += 1
        try:
            self._queue.put_nowait(reading)
        except queue.Full:
            self.failed_events += 1
            self.last_error = f"queue full for machine {reading.machine_id}"
            logger.warning("[WORKER] Queue full, dropping reading for %s", reading.machine_id)
            self._write_state()

    def process_reading(self, reading: MachineReading) -> bool:
        processed = reading.model_copy(deep=True)
        processed.ingested_at = datetime.now(timezone.utc)
        processed.pipeline_version = self.settings.pipeline_version
        processed = self.anomaly_pipeline.process(processed)
        if not self.event_store.append(processed):
            self.total_duplicates += 1
            self._write_state()
            return False

        self.influx_writer.write(processed)
        self.timescale_writer.write(processed)

        self.total_processed += 1
        if processed.anomaly_flags:
            self.total_anomalies += 1
        protocol = processed.source_protocol.value
        self.readings_per_protocol[protocol] = self.readings_per_protocol.get(protocol, 0) + 1
        self._write_state(last_event_id=processed.event_id)
        return True

    def _process_loop(self) -> None:
        while self._running or not self._queue.empty():
            try:
                reading = self._queue.get(timeout=self.settings.worker_poll_sleep_s)
            except queue.Empty:
                self._write_state()
                continue

            try:
                self.process_reading(reading)
            except Exception as exc:  # pragma: no cover - runtime resilience
                self.failed_events += 1
                self.last_error = str(exc)
                logger.exception("[WORKER] Processing failed: %s", exc)
            finally:
                self._queue.task_done()

    def _build_adapters(self) -> List[object]:
        adapters: List[object] = []
        mqtt_config = self.registry.mqtt
        if (mqtt_config and mqtt_config.enabled) or (
            mqtt_config is None and self.settings.enable_mqtt
        ):
            adapters.append(
                MQTTAdapter(
                    broker_host=(mqtt_config.broker_host if mqtt_config else None)
                    or self.settings.mqtt_host,
                    broker_port=(mqtt_config.broker_port if mqtt_config else None)
                    or self.settings.mqtt_port,
                    username=(mqtt_config.username if mqtt_config else None)
                    or self.settings.mqtt_username,
                    password=(mqtt_config.password if mqtt_config else None)
                    or self.settings.mqtt_password,
                    topic_filter=(mqtt_config.topic_filter if mqtt_config else None)
                    or self.settings.mqtt_topic_filter,
                    tls_enabled=(mqtt_config.tls_enabled if mqtt_config else False)
                    or self.settings.mqtt_tls_enabled,
                    on_reading=self.enqueue,
                )
            )
        if self.registry.modbus_devices:
            adapters.append(ModbusPollingAdapter(self.registry.modbus_devices, self.enqueue))
        if self.registry.opcua_devices:
            adapters.append(OPCUAPollingAdapter(self.registry.opcua_devices, self.enqueue))
        return adapters

    def _write_state(self, last_event_id: Optional[str] = None) -> None:
        snapshot = {
            "started_at": self.started_at.isoformat(),
            "last_heartbeat": datetime.now(timezone.utc).isoformat(),
            "queue_depth": self._queue.qsize(),
            "total_received": self.total_received,
            "total_processed": self.total_processed,
            "total_duplicates": self.total_duplicates,
            "total_anomalies": self.total_anomalies,
            "failed_events": self.failed_events,
            "last_error": self.last_error,
            "readings_per_protocol": self.readings_per_protocol,
            "active_detectors": self.anomaly_pipeline.active_machines,
            "pipeline_version": self.settings.pipeline_version,
            "detector_version": self.settings.detector_version,
            "last_event_id": last_event_id,
            "event_store": self.event_store.get_stats(),
        }
        self.state_store.write(snapshot)


class ReplayService:
    def __init__(
        self,
        event_store: EventStore,
        window: int = 60,
        detector_version: str = "rules-replay-v1",
        replay_pipeline_version: str = "replay-v1",
    ):
        self.event_store = event_store
        self.window = window
        self.detector_version = detector_version
        self.replay_pipeline_version = replay_pipeline_version

    def replay(
        self,
        machine_id: Optional[str] = None,
        since_iso: Optional[str] = None,
        until_iso: Optional[str] = None,
        limit: Optional[int] = None,
        detector_version: Optional[str] = None,
        output_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        version = detector_version or self.detector_version
        detector = AnomalyDetectionPipeline(window=self.window, version=version)
        readings = self.event_store.iter_readings(
            machine_id=machine_id,
            since_iso=since_iso,
            until_iso=until_iso,
            limit=limit,
        )

        exported = 0
        output_handle = None
        if output_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_handle = output_file.open("w", encoding="utf-8")

        try:
            replayed = 0
            anomalies = 0
            for reading in readings:
                replayed_reading = reading.model_copy(deep=True)
                replayed_reading.anomaly_score = 0.0
                replayed_reading.anomaly_flags = []
                replayed_reading.pipeline_version = self.replay_pipeline_version
                detector.process(replayed_reading)
                replayed += 1
                if replayed_reading.anomaly_flags:
                    anomalies += 1
                if output_handle is not None:
                    json.dump(
                        replayed_reading.model_dump(mode="json"),
                        output_handle,
                        sort_keys=True,
                    )
                    output_handle.write("\n")
                    exported += 1

            return {
                "replayed": replayed,
                "anomalies": anomalies,
                "machine_id": machine_id,
                "detector_version": version,
                "output_path": output_path,
                "exported": exported,
            }
        finally:
            if output_handle is not None:
                output_handle.close()


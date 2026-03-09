"""
Pipeline orchestrator.
Wires together: MQTT adapter → Normalizer → Anomaly Detector → Storage (InfluxDB + TimescaleDB) + REST API buffer.
"""

from __future__ import annotations

import asyncio
import logging
import os
import queue
import threading
from typing import Optional

from anomaly.detector import AnomalyDetectionPipeline
from api.main import ingest_reading as api_ingest
from ingestion.adapters.mqtt_adapter import MQTTAdapter
from normalizer.schema import MachineReading
from storage.influxdb.writer import InfluxDBWriter
from storage.timescaledb.writer import TimescaleDBWriter

logger = logging.getLogger(__name__)


class FactoryStreamPipeline:
    """
    Main pipeline:
    MQTT → Normalize → Anomaly Detect → [InfluxDB, TimescaleDB, REST API buffer]
    """

    def __init__(self):
        self._queue: queue.Queue = queue.Queue(maxsize=10_000)
        self._running = False

        # Components
        self.mqtt_adapter = MQTTAdapter(
            broker_host=os.getenv("MQTT_HOST", "localhost"),
            broker_port=int(os.getenv("MQTT_PORT", 1883)),
            on_reading=self._enqueue,
        )
        self.anomaly_pipeline = AnomalyDetectionPipeline(window=60)

        self.influx_writer = InfluxDBWriter(
            url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
            token=os.getenv("INFLUXDB_TOKEN", ""),
            org=os.getenv("INFLUXDB_ORG", "factory-stream"),
            bucket=os.getenv("INFLUXDB_BUCKET", "ot-data"),
        )

        self.timescale_writer = TimescaleDBWriter(
            host=os.getenv("TIMESCALE_HOST", "localhost"),
            port=int(os.getenv("TIMESCALE_PORT", 5432)),
            database=os.getenv("TIMESCALE_DB", "factory_stream"),
            user=os.getenv("TIMESCALE_USER", "postgres"),
            password=os.getenv("TIMESCALE_PASSWORD", "postgres"),
        )

    def _enqueue(self, reading: MachineReading):
        """Called by MQTT adapter on new normalized reading."""
        try:
            self._queue.put_nowait(reading)
        except queue.Full:
            logger.warning(f"[PIPELINE] Queue full, dropping reading from {reading.machine_id}")

    def _process_loop(self):
        """Worker thread: pulls from queue, runs anomaly detection, writes to all backends."""
        logger.info("[PIPELINE] Processing loop started")
        while self._running:
            try:
                reading = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                # Anomaly detection
                reading = self.anomaly_pipeline.process(reading)

                # Write to all backends
                self.influx_writer.write(reading)
                self.timescale_writer.write(reading)
                api_ingest(reading)

                logger.debug(
                    f"[PIPELINE] Processed {reading.machine_id} "
                    f"score={reading.anomaly_score:.3f} "
                    f"flags={reading.anomaly_flags}"
                )

            except Exception as e:
                logger.exception(f"[PIPELINE] Error processing reading: {e}")
            finally:
                self._queue.task_done()

    def start(self):
        self._running = True

        # Start MQTT adapter
        self.mqtt_adapter.start()

        # Start worker thread
        worker = threading.Thread(target=self._process_loop, daemon=True, name="pipeline-worker")
        worker.start()

        logger.info("[PIPELINE] Factory Stream pipeline started")
        return worker

    def stop(self):
        self._running = False
        self.mqtt_adapter.stop()
        self.influx_writer.close()
        self.timescale_writer.close()
        logger.info("[PIPELINE] Pipeline stopped")


if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    pipeline = FactoryStreamPipeline()
    pipeline.start()

    # Import the FastAPI app and run
    from api.main import app
    uvicorn.run(app, host="0.0.0.0", port=8000)

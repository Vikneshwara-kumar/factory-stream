"""
InfluxDB 2.x storage backend.
Writes normalized MachineReading objects as time-series data points.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from normalizer.schema import MachineReading

logger = logging.getLogger(__name__)

try:
    from influxdb_client import InfluxDBClient, WriteOptions
    from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
    from influxdb_client.domain.write_precision import WritePrecision
    HAS_INFLUX = True
except ImportError:
    HAS_INFLUX = False
    logger.warning("influxdb-client not installed. InfluxDB writer disabled.")


class InfluxDBWriter:
    """
    Writes MachineReading data points to InfluxDB 2.x.

    Measurement: machine_readings
    Tags: machine_id, plant_id, line_id, protocol, status
    Fields: all metrics + anomaly_score + anomaly_flags
    """

    def __init__(
        self,
        url: str = "http://localhost:8086",
        token: str = "",
        org: str = "factory-stream",
        bucket: str = "ot-data",
        batch_size: int = 100,
        flush_interval_ms: int = 1000,
    ):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self._client = None
        self._write_api = None

        if not HAS_INFLUX:
            logger.warning("[INFLUX] influxdb-client not available. Writes will be no-ops.")
            return

        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._write_api = self._client.write_api(
            write_options=WriteOptions(
                batch_size=batch_size,
                flush_interval=flush_interval_ms,
                write_type=ASYNCHRONOUS,
            )
        )
        logger.info(f"[INFLUX] Connected to {url}, org={org}, bucket={bucket}")

    def write(self, reading: MachineReading) -> None:
        """Write a single MachineReading to InfluxDB."""
        if not self._write_api:
            logger.debug(f"[INFLUX] (no-op) Would write: {reading.machine_id}")
            return

        try:
            point = reading.to_influx_point()
            self._write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point,
                write_precision=WritePrecision.NANOSECONDS,
            )
            logger.debug(f"[INFLUX] Wrote reading for {reading.machine_id}")
        except Exception as e:
            logger.error(f"[INFLUX] Write failed for {reading.machine_id}: {e}")

    def write_batch(self, readings: List[MachineReading]) -> None:
        """Write a batch of readings."""
        for reading in readings:
            self.write(reading)

    def query(self, machine_id: str, hours: int = 1) -> Optional[str]:
        """Query recent readings for a machine. Returns raw CSV."""
        if not self._client:
            return None

        try:
            query_api = self._client.query_api()
            flux = f'''
from(bucket: "{self.bucket}")
  |> range(start: -{hours}h)
  |> filter(fn: (r) => r["_measurement"] == "machine_readings")
  |> filter(fn: (r) => r["machine_id"] == "{machine_id}")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
            result = query_api.query_csv(flux, org=self.org)
            return result
        except Exception as e:
            logger.error(f"[INFLUX] Query failed: {e}")
            return None

    def close(self):
        if self._write_api:
            self._write_api.close()
        if self._client:
            self._client.close()
        logger.info("[INFLUX] Connection closed")

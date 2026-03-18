"""
TimescaleDB storage backend.
Writes normalized MachineReading rows into a PostgreSQL hypertable.
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional

from normalizer.schema import MachineReading

logger = logging.getLogger(__name__)

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    logger.warning("psycopg2 not installed. TimescaleDB writer disabled.")


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS machine_readings (
    event_id        TEXT,
    ingested_at     TIMESTAMPTZ,
    time            TIMESTAMPTZ     NOT NULL,
    machine_id      TEXT            NOT NULL,
    plant_id        TEXT,
    line_id         TEXT,
    protocol        TEXT            NOT NULL,
    status          TEXT,
    temperature     DOUBLE PRECISION,
    vibration       DOUBLE PRECISION,
    rpm             DOUBLE PRECISION,
    pressure        DOUBLE PRECISION,
    power_kw        DOUBLE PRECISION,
    current_a       DOUBLE PRECISION,
    voltage_v       DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    flow_rate       DOUBLE PRECISION,
    anomaly_score   DOUBLE PRECISION DEFAULT 0.0,
    anomaly_flags   TEXT[]          DEFAULT '{}',
    pipeline_version TEXT,
    detector_version TEXT
);
"""

ALTER_TABLE_SQL = """
ALTER TABLE machine_readings ADD COLUMN IF NOT EXISTS event_id TEXT;
ALTER TABLE machine_readings ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;
ALTER TABLE machine_readings ADD COLUMN IF NOT EXISTS pipeline_version TEXT;
ALTER TABLE machine_readings ADD COLUMN IF NOT EXISTS detector_version TEXT;
"""

CREATE_HYPERTABLE_SQL = """
SELECT create_hypertable(
    'machine_readings', 'time',
    if_not_exists => TRUE
);
"""

CREATE_INDEXES_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_machine_readings_event_id
    ON machine_readings (event_id);

CREATE INDEX IF NOT EXISTS idx_machine_readings_machine_id
    ON machine_readings (machine_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_machine_readings_anomaly
    ON machine_readings (anomaly_score, time DESC)
    WHERE anomaly_score > 0.5;
"""


INSERT_SQL = """
INSERT INTO machine_readings (
    event_id, ingested_at, time, machine_id, plant_id, line_id, protocol, status,
    temperature, vibration, rpm, pressure, power_kw,
    current_a, voltage_v, humidity, flow_rate,
    anomaly_score, anomaly_flags, pipeline_version, detector_version
) VALUES (
    %(event_id)s, %(ingested_at)s, %(time)s, %(machine_id)s, %(plant_id)s, %(line_id)s,
    %(protocol)s, %(status)s,
    %(temperature)s, %(vibration)s, %(rpm)s, %(pressure)s, %(power_kw)s,
    %(current_a)s, %(voltage_v)s, %(humidity)s, %(flow_rate)s,
    %(anomaly_score)s, %(anomaly_flags)s, %(pipeline_version)s, %(detector_version)s
) ON CONFLICT (event_id) DO NOTHING;
"""


class TimescaleDBWriter:
    """
    Writes MachineReading rows into a TimescaleDB hypertable.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "factory_stream",
        user: str = "postgres",
        password: str = "postgres",
    ):
        self.conn_params = {
            "host": host,
            "port": port,
            "dbname": database,
            "user": user,
            "password": password,
        }
        self._conn = None

        if not HAS_PSYCOPG2:
            logger.warning("[TIMESCALE] psycopg2 not available. Writes will be no-ops.")
            return

        self._connect()
        self._ensure_schema()

    def _connect(self):
        try:
            self._conn = psycopg2.connect(**self.conn_params)
            self._conn.autocommit = False
            logger.info(f"[TIMESCALE] Connected to {self.conn_params['host']}:{self.conn_params['port']}/{self.conn_params['dbname']}")
        except Exception as e:
            logger.error(f"[TIMESCALE] Connection failed: {e}")
            self._conn = None

    def _ensure_schema(self):
        if not self._conn:
            return
        try:
            with self._conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
                cur.execute(ALTER_TABLE_SQL)
                cur.execute(CREATE_HYPERTABLE_SQL)
                cur.execute(CREATE_INDEXES_SQL)
            self._conn.commit()
            logger.info("[TIMESCALE] Schema ready")
        except Exception as e:
            logger.error(f"[TIMESCALE] Schema creation failed: {e}")
            self._conn.rollback()

    def write(self, reading: MachineReading) -> None:
        """Write a single MachineReading row."""
        if not self._conn:
            logger.debug(f"[TIMESCALE] (no-op) Would write: {reading.machine_id}")
            return

        row = reading.to_timescale_row()
        # Ensure all columns have defaults
        for col in [
            "temperature",
            "vibration",
            "rpm",
            "pressure",
            "power_kw",
            "current_a",
            "voltage_v",
            "humidity",
            "flow_rate",
            "event_id",
            "ingested_at",
            "pipeline_version",
            "detector_version",
        ]:
            row.setdefault(col, None)

        try:
            with self._conn.cursor() as cur:
                cur.execute(INSERT_SQL, row)
            self._conn.commit()
            logger.debug(f"[TIMESCALE] Wrote reading for {reading.machine_id}")
        except Exception as e:
            logger.error(f"[TIMESCALE] Write failed: {e}")
            self._conn.rollback()

    def write_batch(self, readings: List[MachineReading]) -> None:
        """Batch insert multiple readings efficiently."""
        if not self._conn:
            return

        rows = []
        for reading in readings:
            row = reading.to_timescale_row()
            for col in [
                "temperature",
                "vibration",
                "rpm",
                "pressure",
                "power_kw",
                "current_a",
                "voltage_v",
                "humidity",
                "flow_rate",
                "event_id",
                "ingested_at",
                "pipeline_version",
                "detector_version",
            ]:
                row.setdefault(col, None)
            rows.append(row)

        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, INSERT_SQL, rows, page_size=100)
            self._conn.commit()
            logger.info(f"[TIMESCALE] Batch wrote {len(rows)} readings")
        except Exception as e:
            logger.error(f"[TIMESCALE] Batch write failed: {e}")
            self._conn.rollback()

    def query_recent(self, machine_id: str, limit: int = 100) -> List[dict]:
        """Query most recent readings for a machine."""
        if not self._conn:
            return []

        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT * FROM machine_readings
                    WHERE machine_id = %s
                    ORDER BY time DESC
                    LIMIT %s
                    """,
                    (machine_id, limit),
                )
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"[TIMESCALE] Query failed: {e}")
            return []

    def query_anomalies(self, threshold: float = 0.5, hours: int = 1) -> List[dict]:
        """Query recent anomalous readings above threshold."""
        if not self._conn:
            return []

        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT * FROM machine_readings
                    WHERE anomaly_score >= %s
                      AND time > NOW() - INTERVAL '%s hours'
                    ORDER BY anomaly_score DESC, time DESC
                    LIMIT 500
                    """,
                    (threshold, hours),
                )
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"[TIMESCALE] Anomaly query failed: {e}")
            return []

    def close(self):
        if self._conn:
            self._conn.close()
        logger.info("[TIMESCALE] Connection closed")

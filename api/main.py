"""
FastAPI REST API for factory-stream.
Exposes normalized readings, anomaly data, and pipeline health.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from normalizer.schema import MachineReading, MachineStatus, SourceProtocol

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory ring buffer — last N readings per machine
# ---------------------------------------------------------------------------
READING_BUFFER: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
RECENT_ANOMALIES: deque = deque(maxlen=1000)
PIPELINE_STATS: Dict[str, Any] = {
    "total_readings": 0,
    "total_anomalies": 0,
    "started_at": datetime.now(timezone.utc).isoformat(),
    "readings_per_protocol": defaultdict(int),
}


def ingest_reading(reading: MachineReading) -> None:
    """Called by pipeline workers to push readings into the API buffer."""
    READING_BUFFER[reading.machine_id].appendleft(reading)
    PIPELINE_STATS["total_readings"] += 1
    PIPELINE_STATS["readings_per_protocol"][reading.source_protocol.value] += 1

    if reading.anomaly_flags:
        RECENT_ANOMALIES.appendleft(reading)
        PIPELINE_STATS["total_anomalies"] += 1


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="factory-stream API",
    description="Real-time OT data pipeline REST API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------
class MachineReadingResponse(BaseModel):
    machine_id: str
    plant_id: Optional[str]
    line_id: Optional[str]
    source_protocol: str
    timestamp: str
    metrics: Dict[str, float]
    status: str
    anomaly_score: float
    anomaly_flags: List[str]


class PipelineStats(BaseModel):
    total_readings: int
    total_anomalies: int
    active_machines: int
    started_at: str
    readings_per_protocol: Dict[str, int]


def reading_to_response(r: MachineReading) -> MachineReadingResponse:
    return MachineReadingResponse(
        machine_id=r.machine_id,
        plant_id=r.plant_id,
        line_id=r.line_id,
        source_protocol=r.source_protocol.value,
        timestamp=r.timestamp.isoformat(),
        metrics=r.metrics.to_dict(),
        status=r.status.value,
        anomaly_score=r.anomaly_score,
        anomaly_flags=[f.value for f in r.anomaly_flags],
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Pipeline health check."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/stats", response_model=PipelineStats)
def get_stats():
    """Pipeline ingestion statistics."""
    return PipelineStats(
        total_readings=PIPELINE_STATS["total_readings"],
        total_anomalies=PIPELINE_STATS["total_anomalies"],
        active_machines=len(READING_BUFFER),
        started_at=PIPELINE_STATS["started_at"],
        readings_per_protocol=dict(PIPELINE_STATS["readings_per_protocol"]),
    )


@app.get("/machines")
def list_machines():
    """List all active machines."""
    machines = []
    for machine_id, buf in READING_BUFFER.items():
        if buf:
            latest = buf[0]
            machines.append({
                "machine_id": machine_id,
                "plant_id": latest.plant_id,
                "line_id": latest.line_id,
                "protocol": latest.source_protocol.value,
                "status": latest.status.value,
                "last_seen": latest.timestamp.isoformat(),
                "reading_count": len(buf),
            })
    return {"machines": machines, "count": len(machines)}


@app.get("/machines/{machine_id}/readings", response_model=List[MachineReadingResponse])
def get_machine_readings(
    machine_id: str,
    limit: int = Query(default=50, ge=1, le=500),
):
    """Get recent readings for a specific machine."""
    buf = READING_BUFFER.get(machine_id)
    if buf is None:
        raise HTTPException(status_code=404, detail=f"Machine '{machine_id}' not found")
    readings = list(buf)[:limit]
    return [reading_to_response(r) for r in readings]


@app.get("/machines/{machine_id}/latest", response_model=MachineReadingResponse)
def get_latest_reading(machine_id: str):
    """Get the most recent reading for a machine."""
    buf = READING_BUFFER.get(machine_id)
    if not buf:
        raise HTTPException(status_code=404, detail=f"Machine '{machine_id}' not found")
    return reading_to_response(buf[0])


@app.get("/anomalies", response_model=List[MachineReadingResponse])
def get_anomalies(
    limit: int = Query(default=50, ge=1, le=500),
    min_score: float = Query(default=0.0, ge=0.0, le=1.0),
):
    """Get recent anomalous readings."""
    anomalies = [r for r in RECENT_ANOMALIES if r.anomaly_score >= min_score]
    return [reading_to_response(r) for r in anomalies[:limit]]


@app.post("/ingest", response_model=MachineReadingResponse)
def ingest_reading_endpoint(reading: MachineReadingResponse):
    """
    Manually ingest a reading via REST (for testing or external systems).
    """
    mr = MachineReading(
        machine_id=reading.machine_id,
        plant_id=reading.plant_id,
        line_id=reading.line_id,
        source_protocol=SourceProtocol(reading.source_protocol),
        timestamp=datetime.fromisoformat(reading.timestamp),
        metrics=reading.metrics,  # type: ignore
        status=MachineStatus(reading.status),
        anomaly_score=reading.anomaly_score,
    )
    ingest_reading(mr)
    return reading_to_response(mr)


# ---------------------------------------------------------------------------
# WebSocket for live streaming
# ---------------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        logger.info(f"[WS] Client connected. Total: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)
        logger.info(f"[WS] Client disconnected. Total: {len(self.active)}")

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active.remove(ws)


ws_manager = ConnectionManager()


@app.websocket("/ws/readings")
async def websocket_readings(websocket: WebSocket):
    """WebSocket endpoint — streams live readings to connected clients."""
    await ws_manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(30)  # keepalive
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


async def broadcast_reading(reading: MachineReading):
    """Called by pipeline to push reading to WS clients."""
    if ws_manager.active:
        await ws_manager.broadcast(reading_to_response(reading).model_dump())

"""
FastAPI query and control plane for factory-stream.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from normalizer.schema import AnomalyFlag, MachineReading, MachineStatus, MetricPayload, SourceProtocol
from runtime.services import ReplayService
from runtime.settings import RuntimeSettings
from runtime.state import RuntimeStateStore
from storage.event_store import EventStore


class MachineReadingResponse(BaseModel):
    event_id: Optional[str] = None
    machine_id: str
    plant_id: Optional[str]
    line_id: Optional[str]
    source_protocol: str
    timestamp: str
    ingested_at: Optional[str] = None
    metrics: Dict[str, float]
    status: str
    anomaly_score: float
    anomaly_flags: List[str] = Field(default_factory=list)
    pipeline_version: Optional[str] = None
    detector_version: Optional[str] = None


class PipelineStats(BaseModel):
    total_readings: int
    total_anomalies: int
    active_machines: int
    started_at: str
    readings_per_protocol: Dict[str, int]
    total_duplicates: int = 0
    failed_events: int = 0
    pipeline_version: Optional[str] = None
    detector_version: Optional[str] = None


class ReplayRequest(BaseModel):
    machine_id: Optional[str] = None
    since: Optional[str] = None
    until: Optional[str] = None
    limit: Optional[int] = Field(default=None, ge=1, le=10_000)
    detector_version: Optional[str] = None


def reading_to_response(reading: MachineReading) -> MachineReadingResponse:
    return MachineReadingResponse(
        event_id=reading.event_id,
        machine_id=reading.machine_id,
        plant_id=reading.plant_id,
        line_id=reading.line_id,
        source_protocol=reading.source_protocol.value,
        timestamp=reading.timestamp.isoformat(),
        ingested_at=reading.ingested_at.isoformat(),
        metrics=reading.metrics.to_dict(),
        status=reading.status.value,
        anomaly_score=reading.anomaly_score,
        anomaly_flags=[flag.value for flag in reading.anomaly_flags],
        pipeline_version=reading.pipeline_version,
        detector_version=reading.detector_version,
    )


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _metrics_payload(
    event_store: EventStore,
    state_store: RuntimeStateStore,
    settings: RuntimeSettings,
) -> str:
    stats = event_store.get_stats()
    state = state_store.read()
    heartbeat = state.get("last_heartbeat")
    healthy = 0
    if heartbeat:
        age = (datetime.now(timezone.utc) - _parse_iso(heartbeat)).total_seconds()
        healthy = int(age <= settings.worker_heartbeat_ttl_s)
    lines = [
        "# HELP factory_stream_total_readings Total processed readings in the event store.",
        "# TYPE factory_stream_total_readings counter",
        f"factory_stream_total_readings {stats['total_readings']}",
        "# HELP factory_stream_total_anomalies Total anomalous readings in the event store.",
        "# TYPE factory_stream_total_anomalies counter",
        f"factory_stream_total_anomalies {stats['total_anomalies']}",
        "# HELP factory_stream_active_machines Number of active machines in the event store.",
        "# TYPE factory_stream_active_machines gauge",
        f"factory_stream_active_machines {stats['active_machines']}",
        "# HELP factory_stream_worker_healthy Whether the worker heartbeat is fresh.",
        "# TYPE factory_stream_worker_healthy gauge",
        f"factory_stream_worker_healthy {healthy}",
        "# HELP factory_stream_worker_queue_depth Worker queue depth.",
        "# TYPE factory_stream_worker_queue_depth gauge",
        f"factory_stream_worker_queue_depth {state.get('queue_depth', 0)}",
        "# HELP factory_stream_total_duplicates Duplicate readings skipped by the worker.",
        "# TYPE factory_stream_total_duplicates counter",
        f"factory_stream_total_duplicates {state.get('total_duplicates', 0)}",
        "# HELP factory_stream_failed_events Failed event count recorded by the worker.",
        "# TYPE factory_stream_failed_events counter",
        f"factory_stream_failed_events {state.get('failed_events', 0)}",
    ]
    for protocol, count in sorted(stats["readings_per_protocol"].items()):
        lines.append(
            f'factory_stream_readings_per_protocol{{protocol="{protocol}"}} {count}'
        )
    return "\n".join(lines) + "\n"


def create_app(settings: Optional[RuntimeSettings] = None) -> FastAPI:
    settings = settings or RuntimeSettings.from_env()
    event_store = EventStore(settings.event_store_path)
    state_store = RuntimeStateStore(settings.runtime_state_path)
    replay_service = ReplayService(
        event_store=event_store,
        window=settings.anomaly_window,
        detector_version=settings.detector_version,
        replay_pipeline_version=f"{settings.pipeline_version}-replay",
    )

    app = FastAPI(
        title="factory-stream API",
        description="Query and control API for the production-grade OT data pipeline",
        version="2.0.0",
    )
    app.state.settings = settings
    app.state.event_store = event_store
    app.state.state_store = state_store
    app.state.replay_service = replay_service

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def api_key_guard(request: Request, call_next):
        path = request.url.path
        public_paths = {"/health", "/ready", "/metrics", "/openapi.json", "/docs", "/redoc"}
        if not settings.api_key or path in public_paths:
            return await call_next(request)
        if request.headers.get("x-api-key") != settings.api_key:
            return JSONResponse(status_code=401, content={"detail": "Invalid API key"})
        return await call_next(request)

    @app.get("/health")
    def health():
        return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

    @app.get("/ready")
    def ready():
        state = state_store.read()
        heartbeat = state.get("last_heartbeat")
        if not heartbeat:
            return JSONResponse(
                status_code=503,
                content={"status": "degraded", "detail": "worker heartbeat missing"},
            )
        age = (datetime.now(timezone.utc) - _parse_iso(heartbeat)).total_seconds()
        if age > settings.worker_heartbeat_ttl_s:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "degraded",
                    "detail": "worker heartbeat stale",
                    "heartbeat_age_s": age,
                },
            )
        if not Path(settings.event_store_path).exists():
            return JSONResponse(
                status_code=503,
                content={"status": "degraded", "detail": "event store unavailable"},
            )
        return {
            "status": "ready",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "heartbeat_age_s": age,
        }

    @app.get("/stats", response_model=PipelineStats)
    def get_stats():
        stats = event_store.get_stats()
        state = state_store.read()
        return PipelineStats(
            total_readings=stats["total_readings"],
            total_anomalies=stats["total_anomalies"],
            active_machines=stats["active_machines"],
            started_at=state.get("started_at", datetime.now(timezone.utc).isoformat()),
            readings_per_protocol=stats["readings_per_protocol"],
            total_duplicates=state.get("total_duplicates", 0),
            failed_events=state.get("failed_events", 0),
            pipeline_version=state.get("pipeline_version"),
            detector_version=state.get("detector_version"),
        )

    @app.get("/machines")
    def list_machines():
        machines = event_store.list_machines()
        return {"machines": machines, "count": len(machines)}

    @app.get("/machines/{machine_id}/readings", response_model=List[MachineReadingResponse])
    def get_machine_readings(
        machine_id: str,
        limit: int = Query(default=50, ge=1, le=500),
    ):
        readings = event_store.get_machine_readings(machine_id, limit=limit)
        if not readings:
            raise HTTPException(status_code=404, detail=f"Machine '{machine_id}' not found")
        return [reading_to_response(reading) for reading in readings]

    @app.get("/machines/{machine_id}/latest", response_model=MachineReadingResponse)
    def get_latest_reading(machine_id: str):
        latest = event_store.get_latest_reading(machine_id)
        if latest is None:
            raise HTTPException(status_code=404, detail=f"Machine '{machine_id}' not found")
        return reading_to_response(latest)

    @app.get("/anomalies", response_model=List[MachineReadingResponse])
    def get_anomalies(
        limit: int = Query(default=50, ge=1, le=500),
        min_score: float = Query(default=0.0, ge=0.0, le=1.0),
    ):
        return [
            reading_to_response(reading)
            for reading in event_store.get_anomalies(limit=limit, min_score=min_score)
        ]

    @app.post("/ingest", response_model=MachineReadingResponse)
    def ingest_reading_endpoint(reading: MachineReadingResponse):
        ingested_at = _parse_iso(reading.ingested_at) if reading.ingested_at else datetime.now(timezone.utc)
        model = MachineReading(
            event_id=reading.event_id,
            machine_id=reading.machine_id,
            plant_id=reading.plant_id,
            line_id=reading.line_id,
            source_protocol=SourceProtocol(reading.source_protocol),
            timestamp=_parse_iso(reading.timestamp),
            ingested_at=ingested_at,
            metrics=MetricPayload(**reading.metrics),
            status=MachineStatus(reading.status),
            anomaly_score=reading.anomaly_score,
            anomaly_flags=[AnomalyFlag(flag) for flag in reading.anomaly_flags],
            pipeline_version=reading.pipeline_version or "manual-ingest",
            detector_version=reading.detector_version or settings.detector_version,
        )
        event_store.append(model)
        return reading_to_response(model)

    @app.post("/replay")
    def replay_readings(request: ReplayRequest):
        return replay_service.replay(
            machine_id=request.machine_id,
            since_iso=request.since,
            until_iso=request.until,
            limit=request.limit,
            detector_version=request.detector_version,
        )

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        return _metrics_payload(event_store, state_store, settings)

    @app.websocket("/ws/readings")
    async def websocket_readings(websocket: WebSocket):
        if settings.api_key and websocket.query_params.get("api_key") != settings.api_key:
            await websocket.close(code=4401)
            return

        await websocket.accept()
        last_event_id = websocket.query_params.get("last_event_id")
        try:
            while True:
                readings = event_store.get_readings_after(last_event_id, limit=100)
                for reading in readings:
                    await websocket.send_json(reading_to_response(reading).model_dump())
                    last_event_id = reading.event_id
                await asyncio.sleep(1.0)
        except WebSocketDisconnect:
            return

    return app


app = create_app()

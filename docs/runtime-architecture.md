# Runtime Architecture

This document describes the current production-style runtime that lives in this repo.

## Goals

The runtime is designed to separate ingestion/processing concerns from query concerns while keeping the implementation small enough for local development:

- `worker` owns collection, normalization, anomaly scoring, event persistence, and backend fan-out
- `api` owns read access, replay, readiness, and metrics
- durable local files provide a minimal event backbone inside this repository

## Runtime Modes

The CLI entrypoint is [`pipeline.py`](/home/vicky/factory-stream-1/pipeline.py):

- `python -m pipeline worker`
- `python -m pipeline api`
- `python -m pipeline replay`

## Core Components

### Worker

Implemented in [`runtime/services.py`](/home/vicky/factory-stream-1/runtime/services.py).

Responsibilities:
- load runtime settings from [`runtime/settings.py`](/home/vicky/factory-stream-1/runtime/settings.py)
- load the device registry from [`runtime/registry.py`](/home/vicky/factory-stream-1/runtime/registry.py)
- start enabled collectors
- normalize raw readings into [`MachineReading`](/home/vicky/factory-stream-1/normalizer/schema.py)
- score anomalies with [`AnomalyDetectionPipeline`](/home/vicky/factory-stream-1/anomaly/detector.py)
- persist durable events to [`storage/event_store.py`](/home/vicky/factory-stream-1/storage/event_store.py)
- write to InfluxDB and TimescaleDB
- publish worker heartbeat/runtime state to a JSON state file

### API

Implemented in [`api/main.py`](/home/vicky/factory-stream-1/api/main.py).

Responsibilities:
- serve query endpoints from the durable event store
- expose `/health`, `/ready`, `/stats`, `/machines`, `/anomalies`, `/metrics`
- provide replay through `POST /replay`
- stream new readings through `/ws/readings`
- optionally enforce `x-api-key` / websocket `api_key`

### Event Store

Implemented in [`storage/event_store.py`](/home/vicky/factory-stream-1/storage/event_store.py).

Properties:
- append-only JSONL
- durable across worker/API process boundaries
- dedupes by `event_id`
- supports machine views, anomaly views, cursor-based websocket/API reads, and replay input

### TimescaleDB

Implemented in [`storage/timescaledb/writer.py`](/home/vicky/factory-stream-1/storage/timescaledb/writer.py).

Runtime behavior:
- stores enriched readings
- preserves `event_id`, `ingested_at`, `pipeline_version`, and `detector_version`
- uses `ON CONFLICT (event_id) DO NOTHING` for idempotent writes

## Data Flow

```text
collector -> MachineReading -> anomaly scoring -> event store append
                                          \-> InfluxDB write
                                          \-> TimescaleDB write

api -> event store + runtime state -> query/readiness/metrics/replay
```

Event ordering is defined by append order in the event store. The websocket endpoint uses `event_id` cursors to stream forward from a known point.

## Collector Model

Collectors live in [`ingestion/adapters/`](/home/vicky/factory-stream-1/ingestion/adapters):

- MQTT subscriber
- Modbus polling adapter
- OPC-UA polling adapter

Registry configuration lives in [`config/device_registry.example.yaml`](/home/vicky/factory-stream-1/config/device_registry.example.yaml).

The registry controls:
- which source families are enabled
- connection endpoints
- poll cadence
- register address sets
- OPC-UA node maps

## Durable Reading Schema

`MachineReading` now includes:

- `event_id`
- `timestamp`
- `ingested_at`
- `pipeline_version`
- `detector_version`
- anomaly metadata

This allows replay and backend dedupe to operate over stable identifiers instead of transient in-memory state.

## Docker Compose Topology

The default Compose stack in [`docker-compose.yml`](/home/vicky/factory-stream-1/docker-compose.yml) runs:

- `mosquitto`
- `influxdb`
- `timescaledb`
- `mqtt-simulator`
- `modbus-simulator`
- `opcua-simulator`
- `worker`
- `api`

The `worker` and `api` containers share:

- `EVENT_STORE_PATH=/var/lib/factory-stream/events.jsonl`
- `RUNTIME_STATE_PATH=/var/lib/factory-stream/runtime-state.json`

Container names are project-scoped through `COMPOSE_PROJECT_NAME`, which lets integration tests boot isolated Compose stacks without colliding with a local dev environment.
Published host ports are also environment-overridable through variables such as `API_PUBLISH_PORT` and `TIMESCALE_PUBLISH_PORT`.

## Operational Endpoints

### `/health`

Process-level liveness.

### `/ready`

Checks:
- worker heartbeat freshness
- event store presence

### `/metrics`

Prometheus-style text metrics derived from:
- durable event-store counts
- runtime-state counters
- worker heartbeat health

### `POST /replay`

Re-runs anomaly scoring over historical event-store records with an optional detector-version override.

## Test Strategy

### Unit Tests

Unit tests validate:
- schema behavior
- protocol normalizers
- anomaly logic
- event-store behavior
- worker/replay/API service helpers

### Compose Integration Test

[`tests/integration/test_compose_runtime.py`](/home/vicky/factory-stream-1/tests/integration/test_compose_runtime.py) is the end-to-end Docker Compose test for the runtime split.

It verifies that:
- Compose boots `worker` and `api` as separate services
- the API becomes ready
- the worker produces durable readings
- the API can query those readings through live HTTP calls
- metrics and replay endpoints reflect the running stack

Run it with:

```bash
FACTORY_STREAM_RUN_COMPOSE_TESTS=1 python3 -m unittest -v tests.integration.test_compose_runtime
```

## Known Gaps

- Event storage is file-backed rather than a replicated event bus
- Backend writer reconnection is still basic
- The example Compose deployment is single-node and not TLS-hardened
- OPC-UA still needs real deployment node maps before enabling it in the default registry

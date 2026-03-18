# factory-stream

Real-time OT data pipeline for manufacturing digitization.

`factory-stream` now runs as a split runtime:
- a `worker` process ingests MQTT and registry-configured collectors, normalizes readings, scores anomalies, and persists a durable event log
- an `api` process serves query, replay, metrics, and readiness endpoints from that durable runtime state

The current repo implementation is production-oriented in shape, while still staying small enough to run locally with Docker Compose.

## What It Does

- Ingests OT telemetry from MQTT and polling collectors
- Normalizes raw payloads into a unified `MachineReading` schema
- Annotates readings with anomaly metadata plus `event_id`, `pipeline_version`, and `detector_version`
- Persists durable events to a local append-only event store
- Fan-outs processed data to InfluxDB and TimescaleDB
- Exposes a FastAPI control/query plane with replay, metrics, and readiness endpoints

## Runtime Architecture

High-level flow:

```text
[MQTT Broker] ----\
[Modbus Pollers] --- > [worker] -> [Anomaly Pipeline] -> [Event Store JSONL]
[OPC-UA Pollers] --/             \-> [InfluxDB]
                                 \-> [TimescaleDB]

[api] -> reads [Event Store JSONL] + [Runtime State JSON]
      -> /machines /stats /anomalies /replay /metrics /ready
```

Compose topology:

```text
mosquitto
influxdb
timescaledb
mqtt-simulator
modbus-simulator
opcua-simulator
worker  -> writes /var/lib/factory-stream/events.jsonl
api     -> reads  /var/lib/factory-stream/events.jsonl
```

Detailed architecture notes live in [docs/runtime-architecture.md](/home/vicky/factory-stream-1/docs/runtime-architecture.md).

## Project Layout

```text
factory-stream/
├── api/                      # FastAPI query and control plane
├── anomaly/                  # Rules-based anomaly detection
├── config/                   # Example config and device registry
├── docker/                   # Runtime container definitions
├── ingestion/
│   ├── adapters/             # MQTT, Modbus, OPC-UA collectors
│   └── simulators/           # Local protocol simulators
├── normalizer/               # Unified schema and protocol normalizers
├── runtime/                  # Settings, registry, worker/replay services
├── storage/
│   ├── event_store.py        # Durable append-only event log
│   ├── influxdb/             # InfluxDB writer
│   └── timescaledb/          # TimescaleDB writer
├── tests/
│   ├── unit/                 # Fast unit and service tests
│   └── integration/          # Docker Compose integration coverage
├── docker-compose.yml
└── pipeline.py               # CLI entrypoint: worker, api, replay
```

## Quick Start

### Prerequisites

- Docker
- Docker Compose v2
- Python 3.11+ if you want to run the app outside containers

### Start the full stack

```bash
docker compose up --build
```

This starts:
- protocol infrastructure and simulators
- the `worker` service
- the `api` service on `http://localhost:8000`

If those host ports are already in use, Compose supports published-port overrides such as:

```bash
API_PUBLISH_PORT=18080 TIMESCALE_PUBLISH_PORT=15432 docker compose up --build
```

Useful endpoints:
- `GET /health`
- `GET /ready`
- `GET /stats`
- `GET /machines`
- `GET /anomalies`
- `POST /replay`
- `GET /metrics`
- `GET /docs`

### Run the split runtime locally without Compose

Worker:

```bash
python -m pipeline worker
```

API:

```bash
python -m pipeline api
```

Replay:

```bash
python -m pipeline replay --limit 100
```

## Configuration

Runtime configuration is environment-driven via [`runtime/settings.py`](/home/vicky/factory-stream-1/runtime/settings.py).

Common settings:
- `API_HOST`, `API_PORT`, `API_KEY`
- `API_PUBLISH_PORT`, `MQTT_PUBLISH_PORT`, `MQTT_WS_PUBLISH_PORT`
- `INFLUXDB_PUBLISH_PORT`, `TIMESCALE_PUBLISH_PORT`, `MODBUS_PUBLISH_PORT`, `OPCUA_PUBLISH_PORT`
- `MQTT_HOST`, `MQTT_PORT`, `MQTT_USERNAME`, `MQTT_PASSWORD`, `MQTT_TOPIC_FILTER`, `MQTT_TLS_ENABLED`
- `REGISTRY_PATH`
- `EVENT_STORE_PATH`, `RUNTIME_STATE_PATH`
- `INFLUXDB_URL`, `INFLUXDB_TOKEN`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET`
- `TIMESCALE_HOST`, `TIMESCALE_PORT`, `TIMESCALE_DB`, `TIMESCALE_USER`, `TIMESCALE_PASSWORD`
- `PIPELINE_VERSION`, `DETECTOR_VERSION`

Registry-driven collectors are defined in [`config/device_registry.example.yaml`](/home/vicky/factory-stream-1/config/device_registry.example.yaml).

Current example registry:
- MQTT enabled
- Modbus polling enabled for `MODBUS-001`
- OPC-UA devices declared as empty until real node ids are available

## Protocol Support

Implemented end-to-end today:
- MQTT via [`ingestion/adapters/mqtt_adapter.py`](/home/vicky/factory-stream-1/ingestion/adapters/mqtt_adapter.py)
- Modbus polling via [`ingestion/adapters/modbus_adapter.py`](/home/vicky/factory-stream-1/ingestion/adapters/modbus_adapter.py)
- OPC-UA polling via [`ingestion/adapters/opcua_adapter.py`](/home/vicky/factory-stream-1/ingestion/adapters/opcua_adapter.py)

The Compose demo registry currently enables MQTT and Modbus. OPC-UA support is implemented in the runtime, but the example registry leaves it disabled by default because a real deployment needs actual node ids.

## Durable Event Model

Each `MachineReading` now carries:
- `event_id` for idempotency
- `ingested_at` for pipeline timing
- `pipeline_version`
- `detector_version`

The worker writes processed readings to:
- a local append-only event store
- InfluxDB
- TimescaleDB with `event_id`-based dedupe

The API reads from the event store instead of holding its own in-memory buffer.

## Testing

Unit tests:

```bash
docker run --rm -v "$PWD":/app -w /app factory-stream-1-pipeline pytest -q
```

Docker Compose integration test for the worker/API split:

```bash
FACTORY_STREAM_RUN_COMPOSE_TESTS=1 python3 -m unittest -v tests.integration.test_compose_runtime
```

That integration test:
- boots an isolated Compose project
- waits for the `worker` and `api` services to come up
- verifies the API can read data produced by the worker
- checks readiness, metrics, machine listing, anomalies, and replay behavior through the live API

## Current Limitations

- The durable event store is file-backed JSONL, not Kafka or NATS
- Timescale and Influx writes are still best-effort once the worker starts
- The example registry does not ship with real OPC-UA node mappings
- Security is limited to optional API-key auth and MQTT TLS settings, not full OT-grade identity and secret management

## License

MIT

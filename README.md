# 🏭 factory-stream

**Real-Time OT Data Pipeline for Manufacturing Digitization**

A protocol-agnostic ingestion engine that normalizes data from industrial machines (MQTT, Modbus, OPC-UA) into a unified schema, detects anomalies, stores to InfluxDB + TimescaleDB, and exposes a REST API.

---

## 🔧 Problem Solved

Factory machines speak dozens of protocols. Data arrives in inconsistent formats at different rates with no unified schema. This project solves:
- Protocol fragmentation (MQTT, Modbus, OPC-UA)
- Data normalization into a canonical `MachineReading` schema
- Dual time-series storage (InfluxDB + TimescaleDB)
- Real-time anomaly detection
- REST API for downstream consumers (dashboards, AI, ERP)

---

## 🏗️ Architecture

```
[Machine Simulators]
   MQTT Simulator ──────┐
   Modbus Simulator ────┤──▶ [Ingestion Engine] ──▶ [Normalizer] ──▶ [Router]
   OPC-UA Simulator ────┘                                                │
                                                                    ┌────┴─────┐
                                                               [InfluxDB] [TimescaleDB]
                                                                    └────┬─────┘
                                                                  [Anomaly Detector]
                                                                         │
                                                                    [REST API]
```

---

## 📁 Project Structure

```
factory-stream/
├── ingestion/
│   ├── adapters/         # Protocol adapters (MQTT, Modbus, OPC-UA)
│   └── simulators/       # Device simulators for testing
├── normalizer/           # Schema standardization engine
├── anomaly/              # Real-time anomaly detection
├── storage/
│   ├── influxdb/         # InfluxDB writer
│   └── timescaledb/      # TimescaleDB writer
├── api/                  # FastAPI REST endpoints
├── config/               # Configuration files
├── docker/               # Dockerfiles per service
├── tests/
│   ├── unit/
│   └── integration/
├── docker-compose.yml
└── requirements.txt
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+

### Run everything

```bash
git clone https://github.com/your-org/factory-stream.git
cd factory-stream
cp config/config.example.yaml config/config.yaml
docker-compose up --build
```

### Run simulators only (dev mode)

```bash
pip install -r requirements.txt
python -m ingestion.simulators.mqtt_simulator
python -m ingestion.simulators.modbus_simulator
python -m ingestion.simulators.opcua_simulator
```

### API docs

Visit `http://localhost:8000/docs` for interactive Swagger UI.

---

## 📡 Supported Protocols

| Protocol | Port | Use Case |
|----------|------|----------|
| MQTT     | 1883 | Sensors, PLCs publishing telemetry |
| Modbus TCP | 502 | Legacy PLCs, drives, meters |
| OPC-UA   | 4840 | Modern CNC, SCADA systems |

---

## 🗄️ Storage Backends

| Backend       | Type        | Best For |
|---------------|-------------|----------|
| InfluxDB      | Time-series | High-frequency sensor data, dashboards |
| TimescaleDB   | PostgreSQL  | Complex queries, joins, reporting |
| REST API      | HTTP/JSON   | Real-time consumers, ERP, AI models |

---

## 📊 Unified Schema (`MachineReading`)

All protocol data is normalized into:

```json
{
  "machine_id": "CNC-001",
  "source_protocol": "mqtt",
  "timestamp": "2025-03-09T10:23:44.123Z",
  "metrics": {
    "temperature": 72.4,
    "vibration": 0.03,
    "rpm": 1450,
    "pressure": 101.3,
    "power_kw": 3.2
  },
  "status": "running",
  "anomaly_score": 0.12,
  "anomaly_flags": []
}
```

---

## 🔍 Anomaly Detection

Uses Z-score based statistical detection with configurable thresholds per metric. Flags:
- `SPIKE` — sudden value jump
- `DRIFT` — gradual mean shift
- `FLATLINE` — sensor dropout / stuck value
- `OUT_OF_RANGE` — exceeds min/max bounds

---

## 🧪 Running Tests

```bash
pytest tests/ -v --cov=. --cov-report=html
```

---

## 📄 License

MIT
# factory-stream
# factory-stream
# factory-stream

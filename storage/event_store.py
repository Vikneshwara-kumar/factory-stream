"""
Durable append-only event store for processed machine readings.
"""

from __future__ import annotations

import json
import threading
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

from normalizer.schema import MachineReading


class EventStore:
    def __init__(
        self,
        path: str,
        max_recent_per_machine: int = 500,
        max_recent_anomalies: int = 1000,
    ):
        self.path = Path(path)
        self.max_recent_per_machine = max_recent_per_machine
        self.max_recent_anomalies = max_recent_anomalies
        self._lock = threading.RLock()
        self._offset = 0
        self._events: List[MachineReading] = []
        self._event_ids: Dict[str, int] = {}
        self._machine_indexes: Dict[str, Deque[int]] = defaultdict(
            lambda: deque(maxlen=self.max_recent_per_machine)
        )
        self._anomaly_indexes: Deque[int] = deque(maxlen=self.max_recent_anomalies)
        self._protocol_counts: Dict[str, int] = defaultdict(int)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self.refresh()

    def refresh(self) -> None:
        with self._lock:
            size = self.path.stat().st_size if self.path.exists() else 0
            if size < self._offset:
                self._reset()

            with self.path.open("r", encoding="utf-8") as handle:
                handle.seek(self._offset)
                for line in handle:
                    record = line.strip()
                    if not record:
                        continue
                    reading = MachineReading.model_validate(json.loads(record))
                    self._record(reading)
                self._offset = handle.tell()

    def append(self, reading: MachineReading) -> bool:
        with self._lock:
            self.refresh()
            if reading.event_id in self._event_ids:
                return False
            with self.path.open("a", encoding="utf-8") as handle:
                json.dump(reading.model_dump(mode="json"), handle, sort_keys=True)
                handle.write("\n")
                handle.flush()
                self._offset = handle.tell()
            self._record(reading)
            return True

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            self.refresh()
            return {
                "total_readings": len(self._events),
                "total_anomalies": len(self._anomaly_indexes),
                "active_machines": len(self._machine_indexes),
                "readings_per_protocol": dict(self._protocol_counts),
                "last_event_id": self._events[-1].event_id if self._events else None,
            }

    def list_machines(self) -> List[Dict[str, Any]]:
        with self._lock:
            self.refresh()
            machines: List[Dict[str, Any]] = []
            for machine_id, indexes in self._machine_indexes.items():
                if not indexes:
                    continue
                latest = self._events[indexes[-1]]
                machines.append(
                    {
                        "machine_id": machine_id,
                        "plant_id": latest.plant_id,
                        "line_id": latest.line_id,
                        "protocol": latest.source_protocol.value,
                        "status": latest.status.value,
                        "last_seen": latest.timestamp.isoformat(),
                        "reading_count": len(indexes),
                        "event_id": latest.event_id,
                    }
                )
            return sorted(machines, key=lambda item: item["machine_id"])

    def get_machine_readings(self, machine_id: str, limit: int = 50) -> List[MachineReading]:
        with self._lock:
            self.refresh()
            indexes = list(self._machine_indexes.get(machine_id, []))
            return [
                self._events[index].model_copy(deep=True)
                for index in reversed(indexes[-limit:])
            ]

    def get_latest_reading(self, machine_id: str) -> Optional[MachineReading]:
        readings = self.get_machine_readings(machine_id, limit=1)
        return readings[0] if readings else None

    def get_anomalies(self, limit: int = 50, min_score: float = 0.0) -> List[MachineReading]:
        with self._lock:
            self.refresh()
            anomalies = [
                self._events[index].model_copy(deep=True)
                for index in reversed(self._anomaly_indexes)
                if self._events[index].anomaly_score >= min_score
            ]
            return anomalies[:limit]

    def get_readings_after(
        self,
        event_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[MachineReading]:
        with self._lock:
            self.refresh()
            start_index = 0
            if event_id is not None and event_id in self._event_ids:
                start_index = self._event_ids[event_id] + 1
            return [
                reading.model_copy(deep=True)
                for reading in self._events[start_index : start_index + limit]
            ]

    def iter_readings(
        self,
        machine_id: Optional[str] = None,
        since_iso: Optional[str] = None,
        until_iso: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[MachineReading]:
        with self._lock:
            self.refresh()
            readings = self._events
            if machine_id:
                indexes = self._machine_indexes.get(machine_id, [])
                readings = [self._events[index] for index in indexes]

            filtered: List[MachineReading] = []
            for reading in readings:
                ts = reading.timestamp.isoformat()
                if since_iso and ts < since_iso:
                    continue
                if until_iso and ts > until_iso:
                    continue
                filtered.append(reading.model_copy(deep=True))
                if limit is not None and len(filtered) >= limit:
                    break
            return filtered

    def _reset(self) -> None:
        self._offset = 0
        self._events = []
        self._event_ids = {}
        self._machine_indexes = defaultdict(
            lambda: deque(maxlen=self.max_recent_per_machine)
        )
        self._anomaly_indexes = deque(maxlen=self.max_recent_anomalies)
        self._protocol_counts = defaultdict(int)

    def _record(self, reading: MachineReading) -> None:
        if reading.event_id in self._event_ids:
            return
        index = len(self._events)
        self._events.append(reading)
        self._event_ids[reading.event_id] = index
        self._machine_indexes[reading.machine_id].append(index)
        self._protocol_counts[reading.source_protocol.value] += 1
        if reading.anomaly_flags:
            self._anomaly_indexes.append(index)


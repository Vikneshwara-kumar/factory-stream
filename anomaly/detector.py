"""
Real-time anomaly detection engine.
Uses rolling statistics (Z-score, range checks, flatline detection) per machine per metric.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from normalizer.schema import AnomalyFlag, MachineReading

logger = logging.getLogger(__name__)


@dataclass
class MetricConfig:
    """Per-metric anomaly detection configuration."""
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    zscore_threshold: float = 3.0
    flatline_tolerance: float = 0.0001
    flatline_window: int = 10


DEFAULT_METRIC_CONFIGS: Dict[str, MetricConfig] = {
    "temperature": MetricConfig(min_value=0, max_value=200, zscore_threshold=3.0),
    "vibration": MetricConfig(min_value=0, max_value=5.0, zscore_threshold=3.5),
    "rpm": MetricConfig(min_value=0, max_value=10000, zscore_threshold=3.0),
    "pressure": MetricConfig(min_value=0, max_value=500, zscore_threshold=3.0),
    "power_kw": MetricConfig(min_value=0, max_value=100, zscore_threshold=3.0),
    "current_a": MetricConfig(min_value=0, max_value=200, zscore_threshold=3.0),
    "voltage_v": MetricConfig(min_value=200, max_value=500, zscore_threshold=2.5),
}


class RollingStats:
    """Maintains rolling mean and std for online Z-score computation."""

    def __init__(self, window: int = 60):
        self.window = window
        self._values: deque = deque(maxlen=window)

    def update(self, value: float) -> None:
        self._values.append(value)

    def zscore(self, value: float) -> Optional[float]:
        if len(self._values) < 10:
            return None  # not enough data yet
        mean = sum(self._values) / len(self._values)
        variance = sum((v - mean) ** 2 for v in self._values) / len(self._values)
        std = variance ** 0.5
        if std < 1e-9:
            return abs(value - mean)
        return abs(value - mean) / std

    def is_flatline(self, tolerance: float = 0.0001, window: int = 10) -> bool:
        if len(self._values) < window:
            return False
        recent = list(self._values)[-window:]
        return (max(recent) - min(recent)) < tolerance


class MachineAnomalyDetector:
    """Tracks rolling stats per metric for a single machine."""

    def __init__(
        self,
        machine_id: str,
        configs: Optional[Dict[str, MetricConfig]] = None,
        window: int = 60,
    ):
        self.machine_id = machine_id
        self.configs = configs or DEFAULT_METRIC_CONFIGS
        self._stats: Dict[str, RollingStats] = defaultdict(lambda: RollingStats(window))

    def analyze(self, reading: MachineReading) -> Tuple[float, List[AnomalyFlag]]:
        """
        Analyze a reading and return (anomaly_score, flags).
        anomaly_score is 0.0–1.0.
        """
        flags: List[AnomalyFlag] = []
        scores: List[float] = []

        metrics = reading.metrics.to_dict()

        for metric_name, value in metrics.items():
            stats = self._stats[metric_name]
            config = self.configs.get(metric_name, MetricConfig())

            # Range check
            if config.min_value is not None and value < config.min_value:
                flags.append(AnomalyFlag.OUT_OF_RANGE)
                scores.append(0.8)
            elif config.max_value is not None and value > config.max_value:
                flags.append(AnomalyFlag.OUT_OF_RANGE)
                scores.append(0.9)

            # Z-score spike detection
            z = stats.zscore(value)
            if z is not None:
                if z > config.zscore_threshold * 1.5:
                    flags.append(AnomalyFlag.SPIKE)
                    scores.append(min(1.0, z / (config.zscore_threshold * 3)))
                elif z > config.zscore_threshold:
                    flags.append(AnomalyFlag.DRIFT)
                    scores.append(min(0.7, z / (config.zscore_threshold * 2)))

            # Update stats after analysis
            stats.update(value)

            # Flatline detection should include the current sample.
            if stats.is_flatline(config.flatline_tolerance, config.flatline_window):
                flags.append(AnomalyFlag.FLATLINE)
                scores.append(0.4)

        # Deduplicate flags
        flags = list(set(flags))

        # Composite score = max of individual scores (or 0 if clean)
        anomaly_score = max(scores) if scores else 0.0
        return round(anomaly_score, 4), flags


class AnomalyDetectionPipeline:
    """
    Manages per-machine detectors and annotates MachineReadings in-place.
    """

    def __init__(
        self,
        window: int = 60,
        configs: Optional[Dict[str, MetricConfig]] = None,
        version: str = "rules-v1",
    ):
        self.window = window
        self.configs = configs or DEFAULT_METRIC_CONFIGS
        self._detectors: Dict[str, MachineAnomalyDetector] = {}
        self.version = version

    def _get_detector(self, machine_id: str) -> MachineAnomalyDetector:
        if machine_id not in self._detectors:
            self._detectors[machine_id] = MachineAnomalyDetector(
                machine_id, self.configs, self.window
            )
            logger.info(f"[ANOMALY] Created detector for machine {machine_id}")
        return self._detectors[machine_id]

    def process(self, reading: MachineReading) -> MachineReading:
        """Annotate a reading with anomaly score and flags. Returns the mutated reading."""
        detector = self._get_detector(reading.machine_id)
        score, flags = detector.analyze(reading)

        reading.anomaly_score = score
        reading.anomaly_flags = flags
        reading.detector_version = self.version

        if flags:
            logger.warning(
                f"[ANOMALY] {reading.machine_id} | score={score:.3f} | flags={flags}"
            )

        return reading

    @property
    def active_machines(self) -> List[str]:
        return list(self._detectors.keys())

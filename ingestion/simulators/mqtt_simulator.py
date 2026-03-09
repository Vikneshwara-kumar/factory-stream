"""
MQTT Device Simulator.
Simulates multiple factory machines publishing telemetry over MQTT.
"""

import asyncio
import json
import logging
import math
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


@dataclass
class SimulatedMachine:
    machine_id: str
    plant_id: str
    line_id: str
    base_temp: float = 65.0
    base_rpm: float = 1450.0
    base_vibration: float = 0.02
    base_pressure: float = 101.3
    base_power: float = 3.0
    fault_probability: float = 0.02  # 2% chance of fault per reading
    _tick: int = field(default=0, init=False)

    @property
    def topic(self) -> str:
        return f"factory/{self.plant_id}/{self.line_id}/{self.machine_id}/telemetry"

    def generate_reading(self) -> Dict:
        self._tick += 1
        t = self._tick

        # Simulate realistic sinusoidal drift + noise
        temp = self.base_temp + 5 * math.sin(t / 20) + random.gauss(0, 0.5)
        rpm = self.base_rpm + 50 * math.sin(t / 30) + random.gauss(0, 10)
        vibration = self.base_vibration + 0.005 * math.sin(t / 10) + random.gauss(0, 0.001)
        pressure = self.base_pressure + 2 * math.sin(t / 25) + random.gauss(0, 0.2)
        power = self.base_power + 0.5 * math.sin(t / 15) + random.gauss(0, 0.1)

        # Inject occasional spike anomaly
        if random.random() < 0.03:
            temp += random.uniform(15, 30)
            logger.info(f"[SIMULATOR] {self.machine_id}: injecting temperature spike")

        # Determine status
        is_fault = random.random() < self.fault_probability
        status = "fault" if is_fault else "running"

        if is_fault:
            vibration *= 5
            power *= 1.5

        return {
            "ts": int(time.time()),
            "temp": round(temp, 2),
            "rpm": round(max(0, rpm), 1),
            "vib": round(max(0, vibration), 4),
            "pres": round(pressure, 2),
            "pwr": round(max(0, power), 3),
            "status": status,
        }


class MQTTSimulator:
    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        publish_interval: float = 1.0,
        machines: Optional[List[SimulatedMachine]] = None,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.publish_interval = publish_interval
        self.machines = machines or self._default_machines()
        self._client = mqtt.Client(client_id="factory-stream-simulator")
        self._running = False

    def _default_machines(self) -> List[SimulatedMachine]:
        return [
            SimulatedMachine("CNC-001", "plant-A", "line-1", base_temp=70, base_rpm=2000),
            SimulatedMachine("CNC-002", "plant-A", "line-1", base_temp=68, base_rpm=1800),
            SimulatedMachine("PRESS-001", "plant-A", "line-2", base_temp=55, base_pressure=150),
            SimulatedMachine("WELD-001", "plant-B", "line-1", base_temp=90, base_power=8.0),
            SimulatedMachine("CONV-001", "plant-B", "line-2", base_rpm=300, base_power=1.5),
        ]

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"[MQTT SIM] Connected to broker at {self.broker_host}:{self.broker_port}")
        else:
            logger.error(f"[MQTT SIM] Connection failed with code {rc}")

    def connect(self):
        self._client.on_connect = self._on_connect
        self._client.connect(self.broker_host, self.broker_port, keepalive=60)
        self._client.loop_start()

    def disconnect(self):
        self._running = False
        self._client.loop_stop()
        self._client.disconnect()

    def run(self):
        """Blocking run loop — publishes readings for all machines."""
        self.connect()
        self._running = True
        logger.info(f"[MQTT SIM] Starting simulation for {len(self.machines)} machines")

        try:
            while self._running:
                for machine in self.machines:
                    payload = machine.generate_reading()
                    self._client.publish(
                        machine.topic,
                        json.dumps(payload),
                        qos=1,
                    )
                    logger.debug(f"[MQTT SIM] Published to {machine.topic}: {payload}")

                time.sleep(self.publish_interval)

        except KeyboardInterrupt:
            logger.info("[MQTT SIM] Stopped by user")
        finally:
            self.disconnect()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import os

    sim = MQTTSimulator(
        broker_host=os.getenv("MQTT_HOST", "localhost"),
        broker_port=int(os.getenv("MQTT_PORT", 1883)),
        publish_interval=float(os.getenv("PUBLISH_INTERVAL", 1.0)),
    )
    sim.run()

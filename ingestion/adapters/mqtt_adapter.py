"""
MQTT Ingestion Adapter.
Subscribes to MQTT topics and feeds normalized readings into the pipeline.
"""

import asyncio
import logging
from typing import Callable, Optional

import paho.mqtt.client as mqtt

from normalizer.engine import MQTTNormalizer, NormalizationError
from normalizer.schema import MachineReading

logger = logging.getLogger(__name__)


class MQTTAdapter:
    """
    Connects to an MQTT broker, subscribes to factory telemetry topics,
    normalizes payloads, and delivers MachineReading objects to a handler.
    """

    TOPIC_FILTER = "factory/+/+/+/telemetry"

    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        on_reading: Optional[Callable[[MachineReading], None]] = None,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.on_reading = on_reading
        self.normalizer = MQTTNormalizer()
        self._client = mqtt.Client(client_id="factory-stream-mqtt-adapter")
        self._running = False

        if username and password:
            self._client.username_pw_set(username, password)

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"[MQTT] Connected to {self.broker_host}:{self.broker_port}")
            client.subscribe(self.TOPIC_FILTER, qos=1)
            logger.info(f"[MQTT] Subscribed to {self.TOPIC_FILTER}")
        else:
            logger.error(f"[MQTT] Connection failed rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logger.warning(f"[MQTT] Unexpected disconnect rc={rc}. Will reconnect.")

    def _on_message(self, client, userdata, msg):
        try:
            reading = self.normalizer.normalize(msg.topic, msg.payload)
            logger.debug(f"[MQTT] Normalized reading from {reading.machine_id}")
            if self.on_reading:
                self.on_reading(reading)
        except NormalizationError as e:
            logger.error(f"[MQTT] Failed to normalize message on {msg.topic}: {e}")
        except Exception as e:
            logger.exception(f"[MQTT] Unexpected error: {e}")

    def start(self):
        self._client.connect(self.broker_host, self.broker_port, keepalive=60)
        self._client.loop_start()
        self._running = True
        logger.info("[MQTT] Adapter started")

    def stop(self):
        self._running = False
        self._client.loop_stop()
        self._client.disconnect()
        logger.info("[MQTT] Adapter stopped")

"""
MQTT ingestion adapter.
"""

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

    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        topic_filter: str = "factory/+/+/+/telemetry",
        tls_enabled: bool = False,
        on_reading: Optional[Callable[[MachineReading], None]] = None,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic_filter = topic_filter
        self.tls_enabled = tls_enabled
        self.on_reading = on_reading
        self.normalizer = MQTTNormalizer()
        self._client = mqtt.Client(client_id="factory-stream-mqtt-adapter")
        self._running = False
        self._connected = False

        if username and password:
            self._client.username_pw_set(username, password)
        if tls_enabled:
            self._client.tls_set()

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected = True
            logger.info(f"[MQTT] Connected to {self.broker_host}:{self.broker_port}")
            client.subscribe(self.topic_filter, qos=1)
            logger.info(f"[MQTT] Subscribed to {self.topic_filter}")
        else:
            logger.error(f"[MQTT] Connection failed rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        self._connected = False
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

    @property
    def connected(self) -> bool:
        return self._connected

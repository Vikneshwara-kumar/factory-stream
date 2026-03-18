"""
OPC-UA polling adapter.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable, List, Optional

from normalizer.engine import OPCUANormalizer
from normalizer.schema import MachineReading
from runtime.registry import OPCUADeviceConfig

logger = logging.getLogger(__name__)

try:
    from opcua import Client

    HAS_OPCUA = True
except ImportError:  # pragma: no cover - optional dependency
    HAS_OPCUA = False


class OPCUAPollingAdapter:
    def __init__(
        self,
        devices: List[OPCUADeviceConfig],
        on_reading: Optional[Callable[[MachineReading], None]] = None,
        client_factory: Optional[Callable[[OPCUADeviceConfig], object]] = None,
    ):
        self.devices = devices
        self.on_reading = on_reading
        self.client_factory = client_factory or self._default_client_factory
        self.normalizer = OPCUANormalizer()
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []

    def start(self) -> None:
        self._stop_event.clear()
        for device in self.devices:
            worker = threading.Thread(
                target=self._poll_device_loop,
                args=(device,),
                daemon=True,
                name=f"opcua-{device.machine_id}",
            )
            worker.start()
            self._threads.append(worker)

    def stop(self) -> None:
        self._stop_event.set()
        for worker in self._threads:
            worker.join(timeout=2.0)
        self._threads.clear()

    def poll_device_once(
        self,
        device: OPCUADeviceConfig,
        client: Optional[object] = None,
    ) -> Optional[MachineReading]:
        owns_client = client is None
        client = client or self.client_factory(device)
        try:
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect()

            nodes = {}
            for name, node_id in device.nodes.items():
                node = client.get_node(node_id)
                value = node.get_value()
                nodes[node_id] = {"name": name, "value": value, "status": "Good"}

            reading = self.normalizer.normalize(
                machine_id=device.machine_id,
                nodes=nodes,
                plant_id=device.plant_id,
                line_id=device.line_id,
            )
            if self.on_reading:
                self.on_reading(reading)
            return reading
        finally:
            if owns_client:
                disconnect = getattr(client, "disconnect", None)
                if callable(disconnect):
                    disconnect()

    def _poll_device_loop(self, device: OPCUADeviceConfig) -> None:
        while not self._stop_event.is_set():
            try:
                self.poll_device_once(device)
            except Exception as exc:  # pragma: no cover - runtime resilience
                logger.exception("[OPCUA] Poll failed for %s: %s", device.machine_id, exc)
            self._stop_event.wait(device.poll_interval)

    @staticmethod
    def _default_client_factory(device: OPCUADeviceConfig) -> object:
        if not HAS_OPCUA:
            raise RuntimeError("opcua is not installed")
        return Client(device.endpoint)


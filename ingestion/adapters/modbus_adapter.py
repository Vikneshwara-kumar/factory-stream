"""
Modbus polling adapter.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, List, Optional

from normalizer.engine import ModbusNormalizer
from normalizer.schema import MachineReading
from runtime.registry import ModbusDeviceConfig

logger = logging.getLogger(__name__)

try:
    from pymodbus.client import ModbusTcpClient

    HAS_PYMODBUS = True
except ImportError:  # pragma: no cover - optional dependency
    HAS_PYMODBUS = False


class ModbusPollingAdapter:
    def __init__(
        self,
        devices: List[ModbusDeviceConfig],
        on_reading: Optional[Callable[[MachineReading], None]] = None,
        client_factory: Optional[Callable[[ModbusDeviceConfig], object]] = None,
    ):
        self.devices = devices
        self.on_reading = on_reading
        self.client_factory = client_factory or self._default_client_factory
        self.normalizer = ModbusNormalizer()
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []

    def start(self) -> None:
        self._stop_event.clear()
        for device in self.devices:
            worker = threading.Thread(
                target=self._poll_device_loop,
                args=(device,),
                daemon=True,
                name=f"modbus-{device.machine_id}",
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
        device: ModbusDeviceConfig,
        client: Optional[object] = None,
    ) -> Optional[MachineReading]:
        owns_client = client is None
        client = client or self.client_factory(device)
        try:
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect()

            addresses = device.register_addresses or list(self.normalizer.REGISTER_MAP.keys()) + [
                self.normalizer.STATUS_REGISTER
            ]
            start_address = min(addresses)
            count = (max(addresses) - start_address) + 1
            result = client.read_holding_registers(
                start_address - 40001,
                count=count,
                slave=device.unit_id,
            )
            is_error = getattr(result, "isError", None)
            if callable(is_error) and is_error():
                logger.warning("[MODBUS] Read error for %s", device.machine_id)
                return None

            registers = {}
            values = getattr(result, "registers", [])
            for offset, value in enumerate(values):
                absolute_address = start_address + offset
                if absolute_address in addresses:
                    registers[absolute_address] = value

            reading = self.normalizer.normalize(
                machine_id=device.machine_id,
                registers=registers,
                plant_id=device.plant_id,
                line_id=device.line_id,
            )
            if self.on_reading:
                self.on_reading(reading)
            return reading
        finally:
            if owns_client:
                close = getattr(client, "close", None)
                if callable(close):
                    close()

    def _poll_device_loop(self, device: ModbusDeviceConfig) -> None:
        while not self._stop_event.is_set():
            try:
                self.poll_device_once(device)
            except Exception as exc:  # pragma: no cover - runtime resilience
                logger.exception("[MODBUS] Poll failed for %s: %s", device.machine_id, exc)
            self._stop_event.wait(device.poll_interval)

    @staticmethod
    def _default_client_factory(device: ModbusDeviceConfig) -> object:
        if not HAS_PYMODBUS:
            raise RuntimeError("pymodbus is not installed")
        return ModbusTcpClient(host=device.host, port=device.port)


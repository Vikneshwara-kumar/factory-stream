"""
Modbus TCP Device Simulator.
Simulates a Modbus TCP server with realistic register values.
"""

import logging
import math
import random
import time
import threading
from typing import Dict, Optional

logger = logging.getLogger(__name__)

try:
    from pymodbus.server import StartTcpServer
    from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
    HAS_PYMODBUS = True
except ImportError:
    HAS_PYMODBUS = False
    logger.warning("pymodbus not installed. ModbusServer will run in mock mode.")


class ModbusRegisterSimulator:
    """
    Generates realistic Modbus register values for a simulated machine.

    Register Map (Holding Registers):
    40001 → temperature (raw = actual * 10)
    40002 → vibration (raw = actual * 1000)
    40003 → rpm (raw = actual)
    40004 → pressure (raw = actual * 10)
    40005 → power_kw (raw = actual * 100)
    40006 → current_a (raw = actual * 100)
    40007 → voltage_v (raw = actual * 10)
    40010 → status (0=idle, 1=running, 2=fault, 3=maintenance)
    """

    def __init__(self, machine_id: str, base_values: Optional[Dict] = None):
        self.machine_id = machine_id
        self.tick = 0
        self.base = base_values or {
            "temperature": 65.0,
            "vibration": 0.02,
            "rpm": 1450.0,
            "pressure": 101.3,
            "power_kw": 3.0,
            "current_a": 15.0,
            "voltage_v": 400.0,
        }

    def get_registers(self) -> Dict[int, int]:
        self.tick += 1
        t = self.tick

        temp = self.base["temperature"] + 5 * math.sin(t / 20) + random.gauss(0, 0.5)
        vib = self.base["vibration"] + 0.005 * math.sin(t / 10) + random.gauss(0, 0.001)
        rpm = self.base["rpm"] + 50 * math.sin(t / 30) + random.gauss(0, 10)
        pressure = self.base["pressure"] + 2 * math.sin(t / 25) + random.gauss(0, 0.2)
        power = self.base["power_kw"] + 0.5 * math.sin(t / 15) + random.gauss(0, 0.1)
        current = self.base["current_a"] + 1 * math.sin(t / 15) + random.gauss(0, 0.2)
        voltage = self.base["voltage_v"] + random.gauss(0, 1.0)

        is_fault = random.random() < 0.02
        status_code = 2 if is_fault else 1

        return {
            40001: max(0, int(temp * 10)),
            40002: max(0, int(vib * 1000)),
            40003: max(0, int(rpm)),
            40004: max(0, int(pressure * 10)),
            40005: max(0, int(power * 100)),
            40006: max(0, int(current * 100)),
            40007: max(0, int(voltage * 10)),
            40010: status_code,
        }


class ModbusSimulatorServer:
    """
    Runs a Modbus TCP server that continuously updates register values.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 502,
        machine_id: str = "MODBUS-001",
        update_interval: float = 1.0,
    ):
        self.host = host
        self.port = port
        self.machine_id = machine_id
        self.update_interval = update_interval
        self.sim = ModbusRegisterSimulator(machine_id)

    def run_mock(self):
        """Mock mode — just print registers to stdout for testing without pymodbus."""
        logger.info(f"[MODBUS SIM] Mock mode: simulating {self.machine_id}")
        try:
            while True:
                registers = self.sim.get_registers()
                logger.info(f"[MODBUS SIM] {self.machine_id} registers: {registers}")
                time.sleep(self.update_interval)
        except KeyboardInterrupt:
            logger.info("[MODBUS SIM] Stopped")

    def run(self):
        if not HAS_PYMODBUS:
            logger.warning("pymodbus not available, running in mock mode")
            self.run_mock()
            return

        # Build datastore with initial values
        sim = self.sim
        update_interval = self.update_interval

        def build_block():
            regs = sim.get_registers()
            # ModbusSequentialDataBlock needs a flat list starting at address 1
            # We use offset: register 40001 → index 0
            max_addr = max(regs.keys()) - 40000
            block = [0] * (max_addr + 1)
            for addr, val in regs.items():
                block[addr - 40001] = val
            return block

        block = build_block()
        store = ModbusSlaveContext(hr=ModbusSequentialDataBlock(1, block))
        context = ModbusServerContext(slaves=store, single=True)

        def update_loop():
            while True:
                regs = sim.get_registers()
                values = [0] * 20
                for addr, val in regs.items():
                    idx = addr - 40001
                    if 0 <= idx < len(values):
                        values[idx] = val
                context[0x00].setValues(3, 1, values)
                time.sleep(update_interval)

        updater = threading.Thread(target=update_loop, daemon=True)
        updater.start()

        logger.info(f"[MODBUS SIM] Starting TCP server on {self.host}:{self.port}")
        StartTcpServer(context=context, address=(self.host, self.port))


if __name__ == "__main__":
    import os
    logging.basicConfig(level=logging.INFO)
    server = ModbusSimulatorServer(
        host=os.getenv("MODBUS_HOST", "0.0.0.0"),
        port=int(os.getenv("MODBUS_PORT", 502)),
        machine_id=os.getenv("MODBUS_MACHINE_ID", "MODBUS-001"),
    )
    server.run()

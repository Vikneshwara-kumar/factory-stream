"""
OPC-UA Device Simulator.
Simulates an OPC-UA server exposing machine telemetry nodes.
"""

import logging
import math
import random
import time
import threading
from typing import Dict, Optional

logger = logging.getLogger(__name__)

try:
    from opcua import Server, ua
    HAS_OPCUA = True
except ImportError:
    HAS_OPCUA = False
    logger.warning("opcua package not installed. Running in mock mode.")


class OPCUASimulatorServer:
    """
    OPC-UA server exposing machine telemetry nodes.

    Node structure:
    Objects/
      Factory/
        Plant-A/
          Line-1/
            CNC-001/
              Temperature
              Vibration
              RPM
              Pressure
              PowerKW
              CurrentA
              VoltageV
              MachineStatus
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 4840,
        update_interval: float = 1.0,
    ):
        self.host = host
        self.port = port
        self.update_interval = update_interval
        self.tick = 0

    def _generate_values(self) -> Dict[str, float]:
        self.tick += 1
        t = self.tick
        return {
            "Temperature": round(65 + 5 * math.sin(t / 20) + random.gauss(0, 0.5), 2),
            "Vibration": round(max(0, 0.02 + 0.005 * math.sin(t / 10) + random.gauss(0, 0.001)), 4),
            "RPM": round(max(0, 1450 + 50 * math.sin(t / 30) + random.gauss(0, 10)), 1),
            "Pressure": round(101.3 + 2 * math.sin(t / 25) + random.gauss(0, 0.2), 2),
            "PowerKW": round(max(0, 3.0 + 0.5 * math.sin(t / 15) + random.gauss(0, 0.1)), 3),
            "CurrentA": round(max(0, 15.0 + random.gauss(0, 0.3)), 2),
            "VoltageV": round(400.0 + random.gauss(0, 1.0), 1),
            "MachineStatus": "fault" if random.random() < 0.02 else "running",
        }

    def run_mock(self):
        """Print node values to stdout — no OPC-UA library needed."""
        logger.info("[OPC-UA SIM] Mock mode: simulating OPC-UA server")
        try:
            while True:
                values = self._generate_values()
                nodes = {
                    f"ns=2;i={1000+i}": {"name": k, "value": v, "status": "Good"}
                    for i, (k, v) in enumerate(values.items())
                }
                logger.info(f"[OPC-UA SIM] Nodes: {nodes}")
                time.sleep(self.update_interval)
        except KeyboardInterrupt:
            logger.info("[OPC-UA SIM] Stopped")

    def run(self):
        if not HAS_OPCUA:
            logger.warning("opcua not available, running in mock mode")
            self.run_mock()
            return

        server = Server()
        server.set_endpoint(f"opc.tcp://{self.host}:{self.port}/factory/")
        server.set_server_name("FactoryStream OPC-UA Server")

        uri = "http://factory-stream.io"
        idx = server.register_namespace(uri)

        objects = server.get_objects_node()
        factory = objects.add_object(idx, "Factory")
        plant = factory.add_object(idx, "Plant-A")
        line = plant.add_object(idx, "Line-1")
        machine = line.add_object(idx, "CNC-001")

        # Add variable nodes
        node_map = {}
        node_map["Temperature"] = machine.add_variable(idx, "Temperature", 0.0)
        node_map["Vibration"] = machine.add_variable(idx, "Vibration", 0.0)
        node_map["RPM"] = machine.add_variable(idx, "RPM", 0.0)
        node_map["Pressure"] = machine.add_variable(idx, "Pressure", 0.0)
        node_map["PowerKW"] = machine.add_variable(idx, "PowerKW", 0.0)
        node_map["CurrentA"] = machine.add_variable(idx, "CurrentA", 0.0)
        node_map["VoltageV"] = machine.add_variable(idx, "VoltageV", 0.0)
        node_map["MachineStatus"] = machine.add_variable(idx, "MachineStatus", "unknown")

        for node in node_map.values():
            node.set_writable()

        server.start()
        logger.info(f"[OPC-UA SIM] Server running at opc.tcp://{self.host}:{self.port}")

        try:
            while True:
                values = self._generate_values()
                for name, value in values.items():
                    if name in node_map:
                        node_map[name].set_value(value)
                time.sleep(self.update_interval)
        except KeyboardInterrupt:
            logger.info("[OPC-UA SIM] Stopped")
        finally:
            server.stop()


if __name__ == "__main__":
    import os
    logging.basicConfig(level=logging.INFO)
    sim = OPCUASimulatorServer(
        host=os.getenv("OPCUA_HOST", "0.0.0.0"),
        port=int(os.getenv("OPCUA_PORT", 4840)),
    )
    sim.run()

"""
Docker Compose integration test for the worker/api runtime split.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
RUN_COMPOSE_TESTS = os.getenv("FACTORY_STREAM_RUN_COMPOSE_TESTS") == "1"
HAS_DOCKER = shutil.which("docker") is not None


@unittest.skipUnless(
    RUN_COMPOSE_TESTS and HAS_DOCKER,
    "Set FACTORY_STREAM_RUN_COMPOSE_TESTS=1 on a host with Docker to run Compose integration tests.",
)
class ComposeRuntimeIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.project_name = f"factory-stream-it-{os.getpid()}"
        cls.api_port = str(18000 + (os.getpid() % 1000))
        cls.compose_env = os.environ.copy()
        cls.compose_env["COMPOSE_PROJECT_NAME"] = cls.project_name
        cls.compose_env["API_PUBLISH_PORT"] = cls.api_port
        cls.compose_env["MQTT_PUBLISH_PORT"] = str(19000 + (os.getpid() % 1000))
        cls.compose_env["MQTT_WS_PUBLISH_PORT"] = str(19100 + (os.getpid() % 1000))
        cls.compose_env["INFLUXDB_PUBLISH_PORT"] = str(18080 + (os.getpid() % 1000))
        cls.compose_env["TIMESCALE_PUBLISH_PORT"] = str(15432 + (os.getpid() % 1000))
        cls.compose_env["MODBUS_PUBLISH_PORT"] = str(15020 + (os.getpid() % 1000))
        cls.compose_env["OPCUA_PUBLISH_PORT"] = str(14840 + (os.getpid() % 1000))

        cls._run_compose("up", "-d", "--build", "api", "mqtt-simulator")
        cls.base_url = f"http://127.0.0.1:{cls.api_port}"
        cls._wait_for_http_status(f"{cls.base_url}/ready", 200, timeout=180)
        cls._wait_for_service_data(timeout=180)

    @classmethod
    def tearDownClass(cls) -> None:
        if RUN_COMPOSE_TESTS and HAS_DOCKER:
            cls._run_compose("down", "-v", "--remove-orphans", check=False)

    def test_worker_and_api_services_are_running(self):
        result = self._run_compose("ps", "--services", "--status", "running")
        running = set(result.stdout.strip().splitlines())
        self.assertIn("worker", running)
        self.assertIn("api", running)

    def test_api_reads_live_worker_data(self):
        stats = self._get_json(f"{self.base_url}/stats")
        self.assertGreater(stats["total_readings"], 0)
        self.assertIn("modbus", stats["readings_per_protocol"])

        machines = self._get_json(f"{self.base_url}/machines")
        self.assertGreaterEqual(machines["count"], 1)
        machine_id = machines["machines"][0]["machine_id"]

        latest = self._get_json(f"{self.base_url}/machines/{machine_id}/latest")
        self.assertEqual(latest["machine_id"], machine_id)
        self.assertIn("event_id", latest)
        self.assertIn("pipeline_version", latest)
        self.assertIn("detector_version", latest)

    def test_metrics_and_replay_endpoints_work(self):
        metrics = self._get_text(f"{self.base_url}/metrics")
        self.assertIn("factory_stream_total_readings", metrics)
        self.assertIn("factory_stream_worker_healthy 1", metrics)

        replay = self._post_json(f"{self.base_url}/replay", {"limit": 5})
        self.assertGreater(replay["replayed"], 0)
        self.assertIn("detector_version", replay)

    @classmethod
    def _run_compose(cls, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), *args],
            cwd=REPO_ROOT,
            env=cls.compose_env,
            text=True,
            capture_output=True,
        )
        if check and result.returncode != 0:
            raise AssertionError(
                f"docker compose {' '.join(args)} failed\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )
        return result

    @classmethod
    def _wait_for_http_status(
        cls,
        url: str,
        expected_status: int,
        timeout: float,
    ) -> None:
        deadline = time.time() + timeout
        last_error = None
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=5) as response:
                    if response.status == expected_status:
                        return
            except Exception as exc:  # pragma: no cover - retry loop
                last_error = exc
            time.sleep(2)
        raise AssertionError(f"{url} did not return {expected_status}: {last_error}")

    @classmethod
    def _wait_for_service_data(cls, timeout: float) -> None:
        deadline = time.time() + timeout
        last_payload = None
        while time.time() < deadline:
            try:
                payload = cls._get_json(f"{cls.base_url}/stats")
                last_payload = payload
                if payload["total_readings"] > 0:
                    return
            except Exception:
                pass
            time.sleep(2)
        raise AssertionError(f"worker did not produce readings in time: {last_payload}")

    @staticmethod
    def _get_json(url: str) -> dict:
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    @staticmethod
    def _get_text(url: str) -> str:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.read().decode("utf-8")

    @staticmethod
    def _post_json(url: str, payload: dict) -> dict:
        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))

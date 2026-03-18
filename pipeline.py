"""
CLI entrypoint for the factory-stream worker, API, and replay modes.
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Optional

from api.main import create_app
from runtime.services import FactoryStreamWorker, ReplayService
from runtime.settings import RuntimeSettings
from storage.event_store import EventStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="factory-stream runtime")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("worker", help="Run the ingestion and processing worker")
    subparsers.add_parser("api", help="Run the query/control API")

    replay = subparsers.add_parser("replay", help="Replay historical readings")
    replay.add_argument("--machine-id")
    replay.add_argument("--since")
    replay.add_argument("--until")
    replay.add_argument("--limit", type=int)
    replay.add_argument("--detector-version")
    replay.add_argument("--output-path")
    return parser


def run_worker(settings: RuntimeSettings) -> int:
    worker = FactoryStreamWorker(settings=settings)
    worker.run_forever()
    return 0


def run_api(settings: RuntimeSettings) -> int:
    import uvicorn

    app = create_app(settings)
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
    return 0


def run_replay(settings: RuntimeSettings, args: argparse.Namespace) -> int:
    service = ReplayService(
        event_store=EventStore(settings.event_store_path),
        window=settings.anomaly_window,
        detector_version=settings.detector_version,
        replay_pipeline_version=f"{settings.pipeline_version}-replay",
    )
    result = service.replay(
        machine_id=args.machine_id,
        since_iso=args.since,
        until_iso=args.until,
        limit=args.limit,
        detector_version=args.detector_version,
        output_path=args.output_path,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "worker"
    settings = RuntimeSettings.from_env()

    if command == "worker":
        return run_worker(settings)
    if command == "api":
        return run_api(settings)
    if command == "replay":
        return run_replay(settings, args)
    parser.error(f"Unknown command: {command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

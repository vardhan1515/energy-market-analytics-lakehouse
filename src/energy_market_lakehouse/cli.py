"""Command-line entry points for source ingestion."""

from __future__ import annotations

import argparse
import logging
from datetime import date

from dotenv import load_dotenv

from .config import Settings
from .ingestion.http import IngestionError
from .ingestion.miso_history import ingest_history
from .ingestion.miso_realtime import ingest_realtime
from .ingestion.weather import ingest_weather


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("use YYYY-MM-DD") from exc


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="energy-lakehouse")
    commands = root.add_subparsers(dest="command", required=True)
    history = commands.add_parser("ingest-history")
    history.add_argument("--date", type=_date, required=True)
    history.add_argument("--batch-id")
    realtime = commands.add_parser("ingest-realtime")
    realtime.add_argument("--batch-id")
    weather = commands.add_parser("ingest-weather")
    weather.add_argument("--start-date", type=_date, required=True)
    weather.add_argument("--end-date", type=_date, required=True)
    weather.add_argument("--batch-id")
    return root


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parser().parse_args(argv)
    settings = Settings.from_environment()
    try:
        if args.command == "ingest-history":
            path = ingest_history(args.date, settings, args.batch_id)
        elif args.command == "ingest-realtime":
            path = ingest_realtime(settings, args.batch_id)
        else:
            path = ingest_weather(args.start_date, args.end_date, settings, args.batch_id)
        logging.info("Batch written to %s", path.resolve())
        return 0
    except (IngestionError, OSError, ValueError) as exc:
        logging.error("Ingestion failed: %s", exc)
        return 1

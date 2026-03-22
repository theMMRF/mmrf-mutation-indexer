import logging
import platform
import uuid
from logging import handlers
from typing import Any
import pathlib
import time

from pythonjsonlogger import jsonlogger


class DatadogLogFormatter(jsonlogger.JsonFormatter):
    def __init__(self, service_name: str) -> None:
        super().__init__(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={
                "levelname": "level",
                "asctime": "timestamp",
                "name": "logger.name",
            },
            static_fields={"service": service_name},
        )

    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ) -> None:
        super().add_fields(log_record, record, message_dict)

        if record.exc_info:
            exc_type, exception, _ = record.exc_info
            log_record["error.stack"] = log_record.pop("exc_info", None)

            if exc_type and exception:
                log_record["error.message"] = f"{exception}"
                log_record["error.kind"] = f"{exc_type.__module__}.{exc_type.__name__}"

        log_record["host"] = platform.node()


log_formatter = DatadogLogFormatter("mutation_indexer")


def configure() -> None:
    # log_dir = pathlib.Path.home() / "logs" / "mmrf-mutation-indexer"
    log_dir = pathlib.Path("/home/ssm-user/logs/mmrf-mutation-indexer/")
    log_dir.mkdir(parents=True, exist_ok=True)

    run_id = time.strftime("%Y-%m-%d_%H-%M-%S")
    log_path = log_dir / f"mutation_indexer-{run_id}.log"

    latest = log_dir / "current.log"
    try:
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(log_path.name)   # relative symlink inside the same dir
    except OSError:
        pass  # ignore if FS doesn't support symlinks

    log_handler = handlers.WatchedFileHandler(
        log_path, mode="a+"
    )

    log_handler.setFormatter(log_formatter)
    logging.basicConfig(handlers=(log_handler,), level=logging.INFO, force=True)


def add_build_id(build_id: uuid.UUID) -> None:
    log_formatter.static_fields["build_id"] = build_id

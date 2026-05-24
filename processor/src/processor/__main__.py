from __future__ import annotations

import asyncio
import os
from pathlib import Path

import structlog

from .config_verifier import ConfigurationError, ConfigurationVerifier
from .scheduler import NoopPipelineExecutor, PipelineScheduler

LOGGER = structlog.get_logger(__name__)


def _configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),
        cache_logger_on_first_use=True,
    )


def _resolve_startup_paths() -> tuple[Path, Path]:
    config_path = Path(os.getenv("PROCESSOR_CONFIG_PATH", "/app/config/config.yaml")).resolve()
    mapping_root = Path(os.getenv("PROCESSOR_MAPPING_ROOT", "/etc/mapping")).resolve()
    return config_path, mapping_root


async def _run() -> None:
    config_path, mapping_root = _resolve_startup_paths()
    verifier = ConfigurationVerifier(mapping_root=mapping_root)

    try:
        parsed_config = verifier.load_and_validate(config_path=config_path)
    except ConfigurationError as exc:
        LOGGER.error("configuration_validation_failed", error=str(exc))
        raise SystemExit(1) from exc

    scheduler = PipelineScheduler(
        config=parsed_config,
        executor=NoopPipelineExecutor(),
    )

    LOGGER.info("processor_started")
    LOGGER.info(
        "database_url_resolved",
        processor_database_url=os.getenv("PROCESSOR_DATABASE_URL", "<not-set>"),
    )
    LOGGER.info("config_path_resolved", config_path=str(config_path))
    LOGGER.info("mapping_root_resolved", mapping_root=str(mapping_root))

    await scheduler.run_forever()


def main() -> None:
    _configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
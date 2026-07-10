from __future__ import annotations

import asyncio
from dataclasses import replace
import os
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from alembic import command
from alembic.config import Config
from processor.pipelines.siri_et_light_pipeline import SiriEtLightPipeline
from processor.repository.intf_timeline_repository import TimelineRepositoryInterface
import structlog
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .common.runtime_config_service import RuntimeConfigService
from .config_verifier import ConfigurationError, ConfigurationVerifier
from .exports import TimelineExport, TimelineExportExecutor
from .loading.loading_service import LoadingService
from .mapping.mapping_service import MappingService
from .pipelines import GtfsNominalPipeline, GtfsRtTripUpdatesPipeline, TimelinePipelineExecutor
from .repository import SqlAlchemyTimelineRepository
from .scheduler import PipelineScheduler

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


def _resolve_processor_timezone_name() -> str:
    timezone_name = os.getenv("PROCESSOR_TIMEZONE", "UTC").strip() or "UTC"
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        LOGGER.error("processor_timezone_invalid", processor_timezone=timezone_name)
        raise SystemExit(1) from exc
    return timezone_name


def _create_session_factory(database_url: str) -> sessionmaker[Session]:
    engine = create_engine(database_url, pool_pre_ping=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _run_migrations() -> None:
    alembic_ini_path = Path(os.getenv("PROCESSOR_ALEMBIC_INI_PATH", "/app/alembic.ini")).resolve()

    if not alembic_ini_path.exists():
        fallback_ini_path = Path(__file__).resolve().parents[2] / "alembic.ini"
        if fallback_ini_path.exists():
            alembic_ini_path = fallback_ini_path
        else:
            LOGGER.error("alembic_ini_missing", alembic_ini_path=str(alembic_ini_path))
            raise SystemExit(1)

    LOGGER.info("database_migration_started", alembic_ini_path=str(alembic_ini_path))
    alembic_config = Config(str(alembic_ini_path))
    alembic_config.set_main_option(
        "script_location",
        str((alembic_ini_path.parent / "alembic").resolve()),
    )
    try:
        command.upgrade(alembic_config, "head")
    except Exception:
        LOGGER.exception("database_migration_failed")
        raise SystemExit(1)
    LOGGER.info("database_migration_completed")


async def _run() -> None:
    config_path, mapping_root = _resolve_startup_paths()
    processor_timezone_name = _resolve_processor_timezone_name()
    verifier = ConfigurationVerifier(mapping_root=mapping_root)

    try:
        parsed_config = verifier.load_and_validate(config_path=config_path)
    except ConfigurationError as exc:
        LOGGER.error("configuration_validation_failed", error=str(exc))
        raise SystemExit(1) from exc

    parsed_config = replace(parsed_config, timezone_name=processor_timezone_name)
    RuntimeConfigService.initialize(parsed_config)

    processor_database_url = os.getenv("PROCESSOR_DATABASE_URL")
    if not processor_database_url:
        LOGGER.error("processor_database_url_missing")
        raise SystemExit(1)

    _run_migrations()

    session_factory = _create_session_factory(processor_database_url)
    repository: TimelineRepositoryInterface = SqlAlchemyTimelineRepository(session_factory=session_factory)
    loading_service: LoadingService = LoadingService(repository=repository)
    mapping_service: MappingService = MappingService()

    gtfs_nominal_pipeline = GtfsNominalPipeline(
        loading_service=loading_service,
        mapping_service=mapping_service,
        processor_timezone_name=processor_timezone_name,
    )
    gtfs_realtime_pipeline = GtfsRtTripUpdatesPipeline(
        loading_service=loading_service,
        mapping_service=mapping_service,
        processor_timezone_name=processor_timezone_name,
    )
    siri_et_light_pipeline = SiriEtLightPipeline(
        loading_service=loading_service,
        mapping_service=mapping_service,
        processor_timezone_name=processor_timezone_name,
    )

    scheduler = PipelineScheduler(
        config=parsed_config,
        executor=TimelinePipelineExecutor(
            mapping_service=mapping_service,
            gtfs_nominal_pipeline=gtfs_nominal_pipeline,
            gtfs_realtime_pipeline=gtfs_realtime_pipeline,
            siri_et_light_pipeline=siri_et_light_pipeline
        ),
        export_executor=TimelineExportExecutor(
            timeline_export=TimelineExport(repository=repository),
        ),
    )

    LOGGER.info("processor_started")
    LOGGER.info("database_url_resolved", processor_database_url=processor_database_url)
    LOGGER.info("config_path_resolved", config_path=str(config_path))
    LOGGER.info("mapping_root_resolved", mapping_root=str(mapping_root))
    LOGGER.info("processor_timezone_resolved", processor_timezone=processor_timezone_name)

    await scheduler.run_forever()


def main() -> None:
    _configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
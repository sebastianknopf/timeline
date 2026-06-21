from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

PipelineType = Literal["nominal", "realtime"]
PipelinePolicy = Literal["startupAndSchedule", "schedule"]
FilterType = Literal["include", "exclude"]


@dataclass(frozen=True, slots=True)
class ExportProcessingConfig:
    directory: Path | None = None


@dataclass(frozen=True, slots=True)
class ExportPeriodConfig:
    from_day: int
    to_day: int


@dataclass(frozen=True, slots=True)
class ExportConfig:
    id: str
    name: str
    cron: str
    period: ExportPeriodConfig
    processing: ExportProcessingConfig = field(default_factory=ExportProcessingConfig)


@dataclass(frozen=True, slots=True)
class AuthenticationConfig:
    token: str | None = None
    username: str | None = None
    password: str | None = None


@dataclass(frozen=True, slots=True)
class MappingConfig:
    stops: Path | None = None
    routes: Path | None = None


@dataclass(frozen=True, slots=True)
class FilterEntryConfig:
    match: str
    type: FilterType
    mapping: MappingConfig | None = None


@dataclass(frozen=True, slots=True)
class FilterConfig:
    routes: tuple[FilterEntryConfig, ...] = field(default_factory=tuple)
    operators: tuple[FilterEntryConfig, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    id: str
    name: str
    type: PipelineType
    cron: str
    endpoint: str
    policy: PipelinePolicy = "schedule"
    # Optional per-pipeline source timezone used for feed-local schedule fields.
    timezone: str = "UTC"
    authentication: AuthenticationConfig | None = None
    parameters: dict[str, object] = field(default_factory=dict)
    filter: FilterConfig | None = None
    mapping: MappingConfig | None = None


@dataclass(frozen=True, slots=True)
class InstanceConfig:
    id: str
    pipelines: tuple[PipelineConfig, ...]
    exports: tuple[ExportConfig, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class ProcessorConfig:
    instances: tuple[InstanceConfig, ...]
    timezone_name: str = "UTC"

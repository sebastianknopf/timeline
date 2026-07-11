from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from croniter import croniter

from .runtime_config import (
    AuthenticationConfig,
    ExportConfig,
    ExportPeriodConfig,
    ExportProcessingConfig,
    FilterConfig,
    FilterEntryConfig,
    InstanceConfig,
    MappingConfig,
    PipelineConfig,
    ProcessorConfig,
)


class ConfigurationError(ValueError):
    """Raised when processor configuration fails validation."""


class ConfigurationVerifier:
    def __init__(
        self,
        mapping_root: Path,
        known_pipeline_names: set[str] | None = None,
        known_export_names: set[str] | None = None,
    ) -> None:
        self._mapping_root = mapping_root.resolve()
        self._known_pipeline_names = known_pipeline_names or {
            "gtfs",
            "gtfsrt-tripupdates",
            "siri-et-light"
        }
        self._known_export_names = known_export_names or {
            "timeline-export",
        }

    def load_and_validate(self, config_path: Path) -> ProcessorConfig:
        payload = self._read_yaml(config_path)
        instance_payloads = payload.get("instance")

        if not isinstance(instance_payloads, list) or not instance_payloads:
            raise ConfigurationError("Top-level 'instance' must be a non-empty list.")

        instances: list[InstanceConfig] = []
        for raw_instance in instance_payloads:
            instances.append(self._parse_instance(raw_instance))

        return ProcessorConfig(instances=tuple(instances))

    def _read_yaml(self, config_path: Path) -> dict[str, Any]:
        if not config_path.exists() or not config_path.is_file():
            raise ConfigurationError(f"Configuration file not found: {config_path}")

        try:
            with config_path.open("r", encoding="utf-8") as file_obj:
                loaded = yaml.safe_load(file_obj)
        except yaml.YAMLError as exc:
            raise ConfigurationError(f"YAML parse error in {config_path}: {exc}") from exc

        if not isinstance(loaded, dict):
            raise ConfigurationError("Configuration root must be a YAML object.")

        return loaded

    def _parse_instance(self, raw_instance: Any) -> InstanceConfig:
        if not isinstance(raw_instance, dict):
            raise ConfigurationError("Each instance entry must be a YAML object.")

        instance_id = self._require_non_empty_str(raw_instance.get("id"), "instance.id")

        raw_pipelines = raw_instance.get("pipeline")
        if not isinstance(raw_pipelines, list) or not raw_pipelines:
            raise ConfigurationError(
                f"Instance '{instance_id}' must define at least one pipeline."
            )

        pipelines: list[PipelineConfig] = []
        for raw_pipeline in raw_pipelines:
            pipelines.append(self._parse_pipeline(instance_id, raw_pipeline))

        exports: list[ExportConfig] = []
        raw_exports = raw_instance.get("export")
        if raw_exports is not None:
            if not isinstance(raw_exports, list):
                raise ConfigurationError(
                    f"Instance '{instance_id}' export must be a list."
                )
            for raw_export in raw_exports:
                exports.append(self._parse_export(instance_id, raw_export))

        return InstanceConfig(
            id=instance_id,
            pipelines=tuple(pipelines),
            exports=tuple(exports),
        )

    def _parse_pipeline(self, instance_id: str, raw_pipeline: Any) -> PipelineConfig:
        if not isinstance(raw_pipeline, dict):
            raise ConfigurationError(
                f"Pipelines in instance '{instance_id}' must be YAML objects."
            )

        pipeline_id = self._require_non_empty_str(raw_pipeline.get("id"), "pipeline.id")
        pipeline_name = self._require_non_empty_str(
            raw_pipeline.get("name"),
            f"pipeline.name ({pipeline_id})",
        )

        if pipeline_name not in self._known_pipeline_names:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' has unknown name '{pipeline_name}'."
            )

        pipeline_type = self._require_non_empty_str(
            raw_pipeline.get("type"),
            f"pipeline.type ({pipeline_id})",
        )
        if pipeline_type not in {"nominal", "realtime"}:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' type must be 'nominal' or 'realtime'."
            )

        cron_expression = self._require_non_empty_str(
            raw_pipeline.get("cron"),
            f"pipeline.cron ({pipeline_id})",
        )
        if not croniter.is_valid(cron_expression):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' has invalid cron expression '{cron_expression}'."
            )

        policy = self._parse_policy(raw_pipeline.get("policy"), pipeline_id)

        endpoint = self._require_non_empty_str(
            raw_pipeline.get("endpoint"),
            f"pipeline.endpoint ({pipeline_id})",
        )

        priority: int = raw_pipeline.get("priority", 0)
        if not isinstance(priority, int) or priority < 0:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' priority must be an integer >= 0."
            )
        
        timezone_name = self._parse_timezone(raw_pipeline.get("timezone"), pipeline_id)

        parameters = raw_pipeline.get("parameters", {})
        if not isinstance(parameters, dict):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' parameters must be a YAML object."
            )

        authentication = self._parse_authentication(
            raw_pipeline.get("authentication"),
            pipeline_id,
        )

        filter_config = self._parse_filter(raw_pipeline.get("filter"), pipeline_id)
        mapping_config = self._parse_pipeline_mapping(
            raw_pipeline.get("mapping"),
            pipeline_id,
        )

        return PipelineConfig(
            id=pipeline_id,
            name=pipeline_name,
            type=pipeline_type,
            cron=cron_expression,
            endpoint=endpoint,
            policy=policy,
            priority=priority,
            timezone=timezone_name,
            authentication=authentication,
            parameters=parameters,
            filter=filter_config,
            mapping=mapping_config,
        )

    def _parse_pipeline_mapping(
        self,
        raw_mapping: Any,
        pipeline_id: str,
    ) -> MappingConfig | None:
        if raw_mapping is None:
            return None

        if not isinstance(raw_mapping, dict):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' mapping must be a YAML object."
            )

        allowed_keys = {"stops", "routes"}
        unexpected = set(raw_mapping.keys()) - allowed_keys
        if unexpected:
            unexpected_keys = ", ".join(sorted(unexpected))
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' mapping contains unsupported keys: {unexpected_keys}."
            )

        stops_path = self._validate_mapping_csv(
            self._resolve_mapping_path(
                raw_mapping.get("stops"),
                pipeline_id,
                "stops",
            )
        )
        routes_path = self._validate_mapping_csv(
            self._resolve_mapping_path(
                raw_mapping.get("routes"),
                pipeline_id,
                "routes",
            )
        )

        if stops_path is None and routes_path is None:
            return None

        return MappingConfig(stops=stops_path, routes=routes_path)

    def _parse_export(self, instance_id: str, raw_export: Any) -> ExportConfig:
        if not isinstance(raw_export, dict):
            raise ConfigurationError(
                f"Exports in instance '{instance_id}' must be YAML objects."
            )

        export_id = self._require_non_empty_str(raw_export.get("id"), "export.id")
        export_name = self._require_non_empty_str(
            raw_export.get("name"),
            f"export.name ({export_id})",
        )

        if export_name not in self._known_export_names:
            raise ConfigurationError(
                f"Export '{export_id}' has unknown name '{export_name}'."
            )

        cron_expression = self._require_non_empty_str(
            raw_export.get("cron"),
            f"export.cron ({export_id})",
        )
        if not croniter.is_valid(cron_expression):
            raise ConfigurationError(
                f"Export '{export_id}' has invalid cron expression '{cron_expression}'."
            )

        period = self._parse_export_period(raw_export.get("period"), export_id)
        processing = self._parse_export_processing(raw_export.get("processing"), export_id)

        return ExportConfig(
            id=export_id,
            name=export_name,
            cron=cron_expression,
            period=period,
            processing=processing,
        )

    def _parse_export_period(self, raw_period: Any, export_id: str) -> ExportPeriodConfig:
        if raw_period is None:
            raise ConfigurationError(
                f"Export '{export_id}' is missing required key 'period'."
            )

        if not isinstance(raw_period, dict):
            raise ConfigurationError(
                f"Export '{export_id}' period must be a YAML object."
            )

        raw_from = raw_period.get("from")
        raw_to = raw_period.get("to")

        if not isinstance(raw_from, int):
            raise ConfigurationError(
                f"Export '{export_id}' period.from must be an integer."
            )
        if not isinstance(raw_to, int):
            raise ConfigurationError(
                f"Export '{export_id}' period.to must be an integer."
            )
        if raw_from >= raw_to:
            raise ConfigurationError(
                f"Export '{export_id}' period.from must be less than period.to."
            )

        return ExportPeriodConfig(from_day=raw_from, to_day=raw_to)

    def _parse_export_processing(
        self,
        raw_processing: Any,
        export_id: str,
    ) -> ExportProcessingConfig:
        if raw_processing is None:
            return ExportProcessingConfig()

        if not isinstance(raw_processing, dict):
            raise ConfigurationError(
                f"Export '{export_id}' processing must be a YAML object."
            )

        raw_directory = raw_processing.get("directory")
        if raw_directory is not None:
            if not isinstance(raw_directory, str) or not raw_directory.strip():
                raise ConfigurationError(
                    f"Export '{export_id}' processing.directory must be a non-empty string path."
                )
            return ExportProcessingConfig(directory=Path(raw_directory.strip()))

        return ExportProcessingConfig()

    def _parse_policy(self, raw_policy: Any, pipeline_id: str) -> str:
        if raw_policy is None:
            return "schedule"

        if not isinstance(raw_policy, str):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' policy must be a string."
            )

        normalized_policy = raw_policy.strip()
        if normalized_policy not in {"startupAndSchedule", "schedule"}:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' policy must be 'startupAndSchedule' or 'schedule'."
            )

        return normalized_policy

    def _parse_timezone(self, raw_timezone: Any, pipeline_id: str) -> str:
        if raw_timezone is None:
            return "UTC"

        if not isinstance(raw_timezone, str) or not raw_timezone.strip():
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' timezone must be a non-empty string."
            )

        timezone_name = raw_timezone.strip()
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' has invalid timezone '{timezone_name}'."
            ) from exc

        return timezone_name

    def _parse_authentication(
        self,
        raw_authentication: Any,
        pipeline_id: str,
    ) -> AuthenticationConfig | None:
        if raw_authentication is None:
            return None

        if not isinstance(raw_authentication, dict):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' authentication must be a YAML object."
            )

        token = raw_authentication.get("token")
        username = raw_authentication.get("username")
        password = raw_authentication.get("password")
        cert: str | None = raw_authentication.get("cert")
        key: str | None = raw_authentication.get("key")

        has_token: bool = isinstance(token, str) and bool(token.strip())
        has_basic: bool = (
            isinstance(username, str)
            and bool(username.strip())
            and isinstance(password, str)
            and bool(password.strip())
        )
        has_mtls: bool = (
            isinstance(cert, str)
            and bool(cert.strip())
            and isinstance(key, str)
            and bool(key.strip())
        )

        # check that exactly one authentication method is used
        if has_token + has_basic + has_mtls != 1:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' authentication must be either token, username/password or cert/key/chain, but not a combination of them."
            )
        
        # check whether all mentioned mTLS authentication files exist if mTLS is used
        if has_mtls and not os.path.isfile(cert):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' authentication cert file '{cert}' does not exist."
            )
        if has_mtls and not os.path.isfile(key):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' authentication key file '{key}' does not exist."
            )

        # return AuthenticationConfig object
        return AuthenticationConfig(
            token=token.strip() if has_token else None,
            username=username.strip() if has_basic else None,
            password=password.strip() if has_basic else None,
            cert=cert.strip() if isinstance(cert, str) and cert.strip() else None,
            key=key.strip() if isinstance(key, str) and key.strip() else None
        )

    def _parse_filter(self, raw_filter: Any, pipeline_id: str) -> FilterConfig | None:
        if raw_filter is None:
            return None

        if not isinstance(raw_filter, dict):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' filter must be a YAML object."
            )

        allowed_keys = {"routes", "operators"}
        unexpected = set(raw_filter.keys()) - allowed_keys
        if unexpected:
            unexpected_keys = ", ".join(sorted(unexpected))
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' filter contains unsupported keys: {unexpected_keys}."
            )

        has_routes = "routes" in raw_filter
        has_operators = "operators" in raw_filter
        if has_routes == has_operators:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' filter must define exactly one of 'routes' or 'operators'."
            )

        filter_name = "routes" if has_routes else "operators"
        raw_filters = raw_filter.get(filter_name)

        if not isinstance(raw_filters, list) or not raw_filters:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' filter.{filter_name} must be a non-empty list."
            )

        entries: list[FilterEntryConfig] = []
        for index, raw_entry in enumerate(raw_filters, start=1):
            entries.append(
                self._parse_filter_entry(raw_entry, pipeline_id, filter_name, index)
            )

        if filter_name == "routes":
            return FilterConfig(routes=tuple(entries))

        return FilterConfig(operators=tuple(entries))

    def _parse_filter_entry(
        self,
        raw_entry: Any,
        pipeline_id: str,
        filter_name: str,
        index: int,
    ) -> FilterEntryConfig:
        if not isinstance(raw_entry, dict):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' filter.{filter_name}[{index}] must be a YAML object."
            )

        allowed_keys = {"match", "type", "mapping"}
        unexpected = set(raw_entry.keys()) - allowed_keys
        if unexpected:
            unexpected_keys = ", ".join(sorted(unexpected))
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' filter.{filter_name}[{index}] contains unsupported keys: {unexpected_keys}."
            )

        match = self._require_non_empty_str(
            raw_entry.get("match"),
            f"pipeline.filter.{filter_name}[{index}].match ({pipeline_id})",
        )

        filter_type = self._require_non_empty_str(
            raw_entry.get("type"),
            f"pipeline.filter.{filter_name}[{index}].type ({pipeline_id})",
        )
        if filter_type not in {"include", "exclude"}:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' filter.{filter_name}[{index}] type must be 'include' or 'exclude'."
            )

        raw_mapping = raw_entry.get("mapping")
        mapping_config: MappingConfig | None = None
        if raw_mapping is not None:
            mapping_config = self._parse_filter_mapping(
                raw_mapping,
                pipeline_id,
                filter_name,
                index,
                match,
            )

        return FilterEntryConfig(
            match=match,
            type=filter_type,
            mapping=mapping_config,
        )

    def _parse_filter_mapping(
        self,
        raw_mapping: Any,
        pipeline_id: str,
        filter_name: str,
        index: int,
        match: str,
    ) -> MappingConfig | None:
        if not isinstance(raw_mapping, dict):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' filter.{filter_name}[{index}] mapping must be a YAML object."
            )

        allowed_keys = {"stops", "routes"}
        unexpected = set(raw_mapping.keys()) - allowed_keys
        if unexpected:
            unexpected_keys = ", ".join(sorted(unexpected))
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' filter.{filter_name}[{index}] mapping contains unsupported keys: {unexpected_keys}."
            )

        stops_path = self._validate_mapping_csv(
            self._resolve_mapping_path(
                raw_mapping.get("stops"),
                pipeline_id,
                f"filter.{filter_name}[{index}].mapping.stops ({match})",
            )
        )
        routes_path = self._validate_mapping_csv(
            self._resolve_mapping_path(
                raw_mapping.get("routes"),
                pipeline_id,
                f"filter.{filter_name}[{index}].mapping.routes ({match})",
            )
        )

        if stops_path is None and routes_path is None:
            return None

        return MappingConfig(stops=stops_path, routes=routes_path)

    def _resolve_mapping_path(
        self,
        raw_path: Any,
        pipeline_id: str,
        field_name: str,
    ) -> Path | None:
        if raw_path is None:
            return None

        if not isinstance(raw_path, str) or not raw_path.strip():
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' mapping.{field_name} must be a non-empty string path."
            )

        candidate = Path(raw_path)
        resolved = (
            (self._mapping_root / candidate).resolve()
            if not candidate.is_absolute()
            else candidate.resolve()
        )

        try:
            resolved.relative_to(self._mapping_root)
        except ValueError as exc:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' mapping.{field_name} must be under '{self._mapping_root}'."
            ) from exc

        return resolved

    def _validate_mapping_csv(self, csv_path: Path | None) -> Path | None:
        if csv_path is None:
            return None

        if not csv_path.exists() or not csv_path.is_file():
            raise ConfigurationError(f"Mapping file not found: {csv_path}")

        try:
            with csv_path.open("r", encoding="utf-8", newline="") as file_obj:
                reader = csv.DictReader(file_obj)
                field_names = reader.fieldnames or []

                missing_columns = {"key", "value"} - set(field_names)
                if missing_columns:
                    raise ConfigurationError(
                        f"Mapping file '{csv_path}' is missing columns: {', '.join(sorted(missing_columns))}."
                    )

                # Trigger parse errors early by iterating rows once at startup.
                for _ in reader:
                    pass
        except OSError as exc:
            raise ConfigurationError(f"Failed to read mapping file '{csv_path}': {exc}") from exc

        return csv_path

    def _require_non_empty_str(self, value: Any, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ConfigurationError(f"'{field_name}' must be a non-empty string.")
        return value.strip()

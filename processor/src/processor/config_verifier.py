from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import yaml
from croniter import croniter

from .runtime_config import (
    AuthenticationConfig,
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
    ) -> None:
        self._mapping_root = mapping_root.resolve()
        self._known_pipeline_names = known_pipeline_names or {
            "gtfs",
            "gtfsrt-tripupdates",
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

        return InstanceConfig(id=instance_id, pipelines=tuple(pipelines))

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

        endpoint = self._require_non_empty_str(
            raw_pipeline.get("endpoint"),
            f"pipeline.endpoint ({pipeline_id})",
        )

        policy = self._parse_policy(raw_pipeline.get("policy"), pipeline_id)

        parameters = raw_pipeline.get("parameters", {})
        if not isinstance(parameters, dict):
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' parameters must be a YAML object."
            )

        authentication = self._parse_authentication(
            raw_pipeline.get("authentication"),
            pipeline_id,
        )

        mapping = self._parse_mapping(raw_pipeline.get("mapping"), pipeline_id)

        return PipelineConfig(
            id=pipeline_id,
            name=pipeline_name,
            type=pipeline_type,
            cron=cron_expression,
            endpoint=endpoint,
            policy=policy,
            authentication=authentication,
            parameters=parameters,
            mapping=mapping,
        )

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

        has_token = isinstance(token, str) and bool(token.strip())
        has_basic = (
            isinstance(username, str)
            and bool(username.strip())
            and isinstance(password, str)
            and bool(password.strip())
        )

        if has_token == has_basic:
            raise ConfigurationError(
                f"Pipeline '{pipeline_id}' authentication must be either token or username/password."
            )

        return AuthenticationConfig(
            token=token.strip() if has_token else None,
            username=username.strip() if has_basic else None,
            password=password.strip() if has_basic else None,
        )

    def _parse_mapping(self, raw_mapping: Any, pipeline_id: str) -> MappingConfig | None:
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
            self._resolve_mapping_path(raw_mapping.get("stops"), pipeline_id, "stops")
        )
        routes_path = self._validate_mapping_csv(
            self._resolve_mapping_path(raw_mapping.get("routes"), pipeline_id, "routes")
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

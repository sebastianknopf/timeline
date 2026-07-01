from __future__ import annotations

from ..runtime_config import ProcessorConfig


class RuntimeConfigService:
    """Stores the validated runtime configuration for shared lookup by components."""

    _config: ProcessorConfig | None = None

    @classmethod
    def initialize(cls, config: ProcessorConfig) -> None:
        cls._config = config

    @classmethod
    def clear(cls) -> None:
        cls._config = None

    @classmethod
    def get_pipeline_priority(cls, instance_id: str, pipeline_id: str) -> int | None:
        """Returns the priority of the specified pipeline for the given instance, or None if not found.
        
        Args:
            instance_id: The ID of the instance.
            pipeline_id: The ID of the pipeline.

        Returns:
            The priority of the pipeline, or None if not found.
        """

        config = cls._config

        if config is None:
            return None

        for instance in config.instances:
            if instance.id == instance_id:
                for pipeline in instance.pipelines:
                    if pipeline.id == pipeline_id:
                        return pipeline.priority

        return None

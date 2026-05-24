from __future__ import annotations

from typing import Protocol

from .runtime_config import InstanceConfig, PipelineConfig


class PipelineExecutorInterface(Protocol):
    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        """Execute one pipeline run for one instance."""

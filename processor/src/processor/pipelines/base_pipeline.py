from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from ..runtime_config import PipelineConfig, InstanceConfig


class PipelineBase(ABC):
    """Abstract base class for all pipeline implementations."""

    @abstractmethod
    async def execute(
        self, 
        instance: InstanceConfig, 
        pipeline: PipelineConfig
    ) -> None:
        """Execute one pipeline run.

        Args:
            instance: The instance configuration the pipeline belongs to.
            pipeline: The pipeline configuration, including period and processing options.
        """


class NominalPipelineBase(PipelineBase):
    """Abstract base class for all nominal pipeline implementations."""

    def __init__(self) -> None:
        pass


class RealtimePipelineBase(PipelineBase):
    """Abstract base class for all realtime pipeline implementations."""

    def __init__(self) -> None:
        pass

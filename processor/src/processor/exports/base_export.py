from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from ..runtime_config import ExportConfig, InstanceConfig


class ExportBase(ABC):
    """Abstract base class for all export implementations."""

    @abstractmethod
    async def execute(
        self,
        instance: InstanceConfig,
        export: ExportConfig,
        current_date: date,
    ) -> None:
        """Execute one export run.

        Args:
            instance: The instance configuration the export belongs to.
            export: The export configuration, including period and processing options.
            current_date: The local date at the time the export was triggered by the scheduler.
                          Used as the reference point for resolving period offsets.
        """

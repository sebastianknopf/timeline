from __future__ import annotations

from datetime import date
from typing import Protocol

from ..runtime_config import ExportConfig, InstanceConfig


class ExportExecutorInterface(Protocol):
    async def execute(
        self,
        instance: InstanceConfig,
        export: ExportConfig,
        current_date: date,
    ) -> None:
        """Execute one export run for one instance."""

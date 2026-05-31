from __future__ import annotations

from datetime import date

import structlog

from ..runtime_config import ExportConfig, InstanceConfig
from .intf_export_executor import ExportExecutorInterface
from .timeline_export import TimelineExport

LOGGER = structlog.get_logger(__name__)


class TimelineExportExecutor(ExportExecutorInterface):
    def __init__(self, timeline_export: TimelineExport) -> None:
        self._timeline_export = timeline_export

    async def execute(
        self,
        instance: InstanceConfig,
        export: ExportConfig,
        current_date: date,
    ) -> None:
        if export.name == "timeline-export":
            await self._timeline_export.execute(
                instance=instance,
                export=export,
                current_date=current_date,
            )
            return

        raise ValueError(
            f"No export implementation registered for export name '{export.name}'."
        )

from __future__ import annotations

import unittest
from datetime import date

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.exports.export_executor import TimelineExportExecutor
from processor.exports.models import ExportDataSet
from processor.exports.timeline_export import TimelineExport
from processor.runtime_config import (
    ExportConfig,
    ExportPeriodConfig,
    ExportProcessingConfig,
    InstanceConfig,
    PipelineConfig,
)


class RecordingTimelineExport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, date]] = []

    async def execute(
        self,
        instance: InstanceConfig,
        export: ExportConfig,
        current_date: date,
    ) -> None:
        self.calls.append((instance.id, export.id, current_date))


def _make_export(name: str = "timeline-export") -> ExportConfig:
    return ExportConfig(
        id="daily-trip-export",
        name=name,
        cron="0 3 * * *",
        period=ExportPeriodConfig(from_day=-1, to_day=0),
        processing=ExportProcessingConfig(),
    )


def _make_instance(export: ExportConfig) -> InstanceConfig:
    pipeline = PipelineConfig(
        id="nominal-main",
        name="gtfs",
        type="nominal",
        cron="0 2 * * *",
        endpoint="https://example.test/schedule",
    )
    return InstanceConfig(id="demo", pipelines=(pipeline,), exports=(export,))


class TimelineExportExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_dispatches_to_timeline_export(self) -> None:
        recording_export = RecordingTimelineExport()
        executor = TimelineExportExecutor(timeline_export=recording_export)  # type: ignore[arg-type]
        export = _make_export("timeline-export")
        instance = _make_instance(export)
        current_date = date(2026, 5, 31)

        await executor.execute(instance=instance, export=export, current_date=current_date)

        self.assertEqual(len(recording_export.calls), 1)
        self.assertEqual(recording_export.calls[0], ("demo", "daily-trip-export", current_date))

    async def test_executor_raises_for_unknown_export_name(self) -> None:
        recording_export = RecordingTimelineExport()
        executor = TimelineExportExecutor(timeline_export=recording_export)  # type: ignore[arg-type]
        export = _make_export("unknown-export")
        instance = _make_instance(export)

        with self.assertRaises(ValueError):
            await executor.execute(
                instance=instance,
                export=export,
                current_date=date(2026, 5, 31),
            )

        self.assertEqual(len(recording_export.calls), 0)


if __name__ == "__main__":
    unittest.main()

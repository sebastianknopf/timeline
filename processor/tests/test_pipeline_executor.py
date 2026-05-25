from __future__ import annotations

import unittest

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.pipelines.pipeline_executor import TimelinePipelineExecutor
from processor.runtime_config import InstanceConfig, PipelineConfig


class RecordingMappingService:
    def __init__(self) -> None:
        self.registrations: list[tuple[str, str]] = []

    def register_pipeline_mapping(self, instance_id: str, pipeline: PipelineConfig) -> None:
        self.registrations.append((instance_id, pipeline.id))


class RecordingNominalPipeline:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        self.calls.append((instance.id, pipeline.id))


class RecordingRealtimePipeline:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        self.calls.append((instance.id, pipeline.id))


class TimelinePipelineExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_dispatches_to_realtime_pipeline(self) -> None:
        mapping_service = RecordingMappingService()
        nominal_pipeline = RecordingNominalPipeline()
        realtime_pipeline = RecordingRealtimePipeline()

        executor = TimelinePipelineExecutor(
            mapping_service=mapping_service,
            gtfs_nominal_pipeline=nominal_pipeline,
            gtfs_realtime_pipeline=realtime_pipeline,
        )

        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        await executor.execute(instance=instance, pipeline=pipeline)

        self.assertEqual([("demo", "realtime-main")], mapping_service.registrations)
        self.assertEqual([], nominal_pipeline.calls)
        self.assertEqual([("demo", "realtime-main")], realtime_pipeline.calls)


if __name__ == "__main__":
    unittest.main()

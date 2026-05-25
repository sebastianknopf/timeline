from __future__ import annotations

import asyncio
import unittest

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.runtime_config import InstanceConfig, PipelineConfig, ProcessorConfig
from processor.scheduler import PipelineScheduler


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.first_call_event = asyncio.Event()

    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        self.calls.append((instance.id, pipeline.id))
        self.first_call_event.set()


class BlockingRecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.first_call_event = asyncio.Event()
        self.release_event = asyncio.Event()

    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        self.calls.append((instance.id, pipeline.id))
        self.first_call_event.set()
        await self.release_event.wait()


class PipelineSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_scheduler_triggers_pipeline_execution(self) -> None:
        config = ProcessorConfig(
            instances=(
                InstanceConfig(
                    id="demo",
                    pipelines=(
                        PipelineConfig(
                            id="realtime-main",
                            name="gtfsrt-tripupdates",
                            type="realtime",
                            cron="*/1 * * * * *",
                            endpoint="https://example.test/realtime",
                        ),
                    ),
                ),
            ),
        )

        executor = RecordingExecutor()
        scheduler = PipelineScheduler(config=config, executor=executor)

        scheduler_task = asyncio.create_task(scheduler.run_forever())

        try:
            await asyncio.wait_for(executor.first_call_event.wait(), timeout=3)
        finally:
            await scheduler.shutdown()
            await asyncio.wait_for(scheduler_task, timeout=5)

        self.assertGreaterEqual(len(executor.calls), 1)

    async def test_scheduler_does_not_run_pipeline_immediately_for_schedule_policy(self) -> None:
        config = ProcessorConfig(
            instances=(
                InstanceConfig(
                    id="demo",
                    pipelines=(
                        PipelineConfig(
                            id="realtime-main",
                            name="gtfsrt-tripupdates",
                            type="realtime",
                            cron="0 0 1 1 *",
                            endpoint="https://example.test/realtime",
                            policy="schedule",
                        ),
                    ),
                ),
            ),
        )

        executor = RecordingExecutor()
        scheduler = PipelineScheduler(config=config, executor=executor)

        scheduler_task = asyncio.create_task(scheduler.run_forever())

        try:
            await asyncio.sleep(0.2)
            self.assertEqual([], executor.calls)
        finally:
            await scheduler.shutdown()
            await asyncio.wait_for(scheduler_task, timeout=5)

    async def test_scheduler_runs_pipeline_immediately_for_startup_and_schedule_policy(self) -> None:
        config = ProcessorConfig(
            instances=(
                InstanceConfig(
                    id="demo",
                    pipelines=(
                        PipelineConfig(
                            id="realtime-main",
                            name="gtfsrt-tripupdates",
                            type="realtime",
                            cron="0 0 1 1 *",
                            endpoint="https://example.test/realtime",
                            policy="startupAndSchedule",
                        ),
                    ),
                ),
            ),
        )

        executor = RecordingExecutor()
        scheduler = PipelineScheduler(config=config, executor=executor)

        scheduler_task = asyncio.create_task(scheduler.run_forever())

        try:
            await asyncio.wait_for(executor.first_call_event.wait(), timeout=2)
            self.assertEqual([("demo", "realtime-main")], executor.calls)
        finally:
            await scheduler.shutdown()
            await asyncio.wait_for(scheduler_task, timeout=5)

    async def test_scheduler_skips_overlapping_runs_for_same_pipeline(self) -> None:
        config = ProcessorConfig(
            instances=(
                InstanceConfig(
                    id="demo",
                    pipelines=(
                        PipelineConfig(
                            id="realtime-main",
                            name="gtfsrt-tripupdates",
                            type="realtime",
                            cron="*/1 * * * * *",
                            endpoint="https://example.test/realtime",
                        ),
                    ),
                ),
            ),
        )

        executor = BlockingRecordingExecutor()
        scheduler = PipelineScheduler(config=config, executor=executor)

        scheduler_task = asyncio.create_task(scheduler.run_forever())

        try:
            await asyncio.wait_for(executor.first_call_event.wait(), timeout=3)
            await asyncio.sleep(2.2)
            self.assertEqual(len(executor.calls), 1)
        finally:
            executor.release_event.set()
            await scheduler.shutdown()
            await asyncio.wait_for(scheduler_task, timeout=5)


if __name__ == "__main__":
    unittest.main()

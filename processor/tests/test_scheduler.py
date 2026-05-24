from __future__ import annotations

import asyncio
import unittest

from processor.runtime_config import InstanceConfig, PipelineConfig, ProcessorConfig
from processor.scheduler import PipelineScheduler


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.first_call_event = asyncio.Event()

    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        self.calls.append((instance.id, pipeline.id))
        self.first_call_event.set()


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
                            cron="* * * * *",
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
            await asyncio.wait_for(executor.first_call_event.wait(), timeout=65)
        finally:
            await scheduler.shutdown()
            await asyncio.wait_for(scheduler_task, timeout=5)

        self.assertGreaterEqual(len(executor.calls), 1)


if __name__ == "__main__":
    unittest.main()

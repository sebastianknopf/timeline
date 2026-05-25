from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter
import structlog

from .intf_pipeline_executor import PipelineExecutorInterface
from .runtime_config import InstanceConfig, PipelineConfig, ProcessorConfig

LOGGER = structlog.get_logger(__name__)


class PipelineScheduler:
    def __init__(
        self,
        config: ProcessorConfig,
        executor: PipelineExecutorInterface,
    ) -> None:
        self._config = config
        self._executor = executor
        self._timezone = ZoneInfo(config.timezone_name)
        self._stop_event = asyncio.Event()
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._running_jobs: set[asyncio.Task[None]] = set()

    async def run_forever(self) -> None:
        self._stop_event.clear()

        for instance in self._config.instances:
            for pipeline in instance.pipelines:
                worker = asyncio.create_task(self._pipeline_worker(instance, pipeline))
                self._worker_tasks.append(worker)

        if not self._worker_tasks:
            raise RuntimeError("No pipelines available for scheduling.")

        LOGGER.info(
            "scheduler_started",
            worker_count=len(self._worker_tasks),
            scheduler_timezone=self._config.timezone_name,
        )

        await self._stop_event.wait()
        await self._stop_workers()

    async def shutdown(self) -> None:
        self._stop_event.set()
        await self._stop_workers()

    async def _stop_workers(self) -> None:
        for worker in self._worker_tasks:
            worker.cancel()

        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()

        if self._running_jobs:
            await asyncio.gather(*self._running_jobs, return_exceptions=True)

        LOGGER.info("scheduler_stopped")

    async def _pipeline_worker(
        self,
        instance: InstanceConfig,
        pipeline: PipelineConfig,
    ) -> None:
        base_time = datetime.now(self._timezone)
        cron = croniter(pipeline.cron, base_time)
        active_task: asyncio.Task[None] | None = None

        try:
            if pipeline.policy == "startupAndSchedule":
                active_task = self._create_pipeline_task(instance, pipeline)

            while not self._stop_event.is_set():
                next_run = cron.get_next(datetime)
                now = datetime.now(self._timezone)
                delay = max((next_run - now).total_seconds(), 0.0)

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                    break
                except TimeoutError:
                    pass

                if active_task is not None and not active_task.done():
                    LOGGER.info(
                        "pipeline_run_skipped_overlap",
                        instance_id=instance.id,
                        pipeline_id=pipeline.id,
                        pipeline_type=pipeline.type,
                    )
                    continue

                active_task = self._create_pipeline_task(instance, pipeline)
        except asyncio.CancelledError:
            LOGGER.debug(
                "scheduler_worker_cancelled",
                instance_id=instance.id,
                pipeline_id=pipeline.id,
            )
            raise

    def _create_pipeline_task(
        self,
        instance: InstanceConfig,
        pipeline: PipelineConfig,
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(self._run_pipeline(instance, pipeline))
        self._running_jobs.add(task)
        task.add_done_callback(self._running_jobs.discard)
        return task

    async def _run_pipeline(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        LOGGER.info(
            "pipeline_run_started",
            instance_id=instance.id,
            pipeline_id=pipeline.id,
            pipeline_type=pipeline.type,
        )
        try:
            await self._executor.execute(instance, pipeline)
        except Exception:
            LOGGER.exception(
                "pipeline_run_failed",
                instance_id=instance.id,
                pipeline_id=pipeline.id,
            )

from __future__ import annotations

import structlog

from ..intf_pipeline_executor import PipelineExecutorInterface
from ..mapping.intf_mapping_service import MappingServiceInterface
from ..runtime_config import InstanceConfig, PipelineConfig
from .gtfs_pipeline import GtfsNominalPipeline

LOGGER = structlog.get_logger(__name__)


class TimelinePipelineExecutor(PipelineExecutorInterface):
    def __init__(
        self,
        mapping_service: MappingServiceInterface,
        gtfs_nominal_pipeline: GtfsNominalPipeline,
    ) -> None:
        self._mapping_service = mapping_service
        self._gtfs_nominal_pipeline = gtfs_nominal_pipeline

    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        self._mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

        if pipeline.name == "gtfs":
            await self._gtfs_nominal_pipeline.execute(instance=instance, pipeline=pipeline)
            return

        if pipeline.name == "gtfsrt-tripupdates":
            LOGGER.info(
                "realtime_pipeline_not_implemented",
                instance_id=instance.id,
                pipeline_id=pipeline.id,
                pipeline_name=pipeline.name,
            )
            return

        raise ValueError(
            f"No pipeline implementation registered for pipeline name '{pipeline.name}'."
        )

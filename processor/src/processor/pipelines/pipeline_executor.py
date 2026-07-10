from __future__ import annotations

from processor.pipelines.siri_et_light_pipeline import SiriEtLightPipeline
import structlog

from .intf_pipeline_executor import PipelineExecutorInterface
from ..mapping.intf_mapping_service import MappingServiceInterface
from ..runtime_config import InstanceConfig, PipelineConfig
from .gtfs_pipeline import GtfsNominalPipeline
from .gtfsrt_tripupdates_pipeline import GtfsRtTripUpdatesPipeline

LOGGER = structlog.get_logger(__name__)


class TimelinePipelineExecutor(PipelineExecutorInterface):
    def __init__(
        self,
        mapping_service: MappingServiceInterface,
        gtfs_nominal_pipeline: GtfsNominalPipeline,
        gtfs_realtime_pipeline: GtfsRtTripUpdatesPipeline,
        siri_et_light_pipeline: SiriEtLightPipeline
    ) -> None:
        self._mapping_service = mapping_service
        self._gtfs_nominal_pipeline = gtfs_nominal_pipeline
        self._gtfs_realtime_pipeline = gtfs_realtime_pipeline
        self._siri_et_light_pipeline = siri_et_light_pipeline

    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        self._mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

        if pipeline.name == "gtfs":
            await self._gtfs_nominal_pipeline.execute(instance=instance, pipeline=pipeline)
            return

        if pipeline.name == "gtfsrt-tripupdates":
            await self._gtfs_realtime_pipeline.execute(instance=instance, pipeline=pipeline)
            return

        if pipeline.name == "siri-et-light":
            await self._siri_et_light_pipeline.execute(instance=instance, pipeline=pipeline)
            return

        raise ValueError(
            f"No pipeline implementation registered for pipeline name '{pipeline.name}'."
        )

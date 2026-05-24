from __future__ import annotations

from typing import Protocol

from ..loading.models import StopTimeRecord, TripRecord
from ..runtime_config import PipelineConfig


class MappingServiceInterface(Protocol):
    def register_pipeline_mapping(self, instance_id: str, pipeline: PipelineConfig) -> None:
        """Load and cache mapping data for one instance pipeline."""

    async def map_route_id(self, instance_id: str, pipeline_id: str, route_id: str) -> str:
        """Map one route ID for a pipeline context."""

    async def map_stop_id(self, instance_id: str, pipeline_id: str, stop_id: str) -> str:
        """Map one stop ID for a pipeline context."""

    async def map_records_for_loading(
        self,
        instance_id: str,
        pipeline_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> tuple[TripRecord, list[StopTimeRecord]]:
        """Return mapped records that can be passed to LoadingService."""

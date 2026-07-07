from __future__ import annotations

from abc import ABC, abstractmethod
import base64
from datetime import date

from processor.common.quality_report_service import QualityReportService
from processor.loading.loading_service import LoadingService

from ..runtime_config import AuthenticationConfig, PipelineConfig, InstanceConfig


class PipelineBase(ABC):
    """Abstract base class for all pipeline implementations."""

    @abstractmethod
    async def execute(
        self, 
        instance: InstanceConfig, 
        pipeline: PipelineConfig
    ) -> None:
        """Execute one pipeline run.

        Args:
            instance: The instance configuration the pipeline belongs to.
            pipeline: The pipeline configuration, including period and processing options.
        """

    def _build_auth_headers(self, authentication: AuthenticationConfig | None) -> dict[str, str]:
        if authentication is None:
            return {}

        if authentication.token:
            return {"Authorization": f"Bearer {authentication.token}"}

        if authentication.username and authentication.password:
            raw = f"{authentication.username}:{authentication.password}".encode("utf-8")
            encoded = base64.b64encode(raw).decode("ascii")
            return {"Authorization": f"Basic {encoded}"}

        return {}


class NominalPipelineBase(PipelineBase):
    """Abstract base class for all nominal pipeline implementations."""

    def __init__(self) -> None:
        pass


class RealtimePipelineBase(PipelineBase):
    """Abstract base class for all realtime pipeline implementations."""

    def __init__(
            self, 
            loading_service: LoadingService,
        ) -> None:
        self._loading_service: LoadingService = loading_service

        # quality report service MUST BE instantiated in the base constructor and NOT
        # on __main__() level for injection because it is required to work on pipeline instance
        # level, not globally!
        self._quality_report_service: QualityReportService = QualityReportService()

    def report_request(
            self, 
            instance: InstanceConfig,
            pipeline: PipelineConfig,
            timestamp: date, 
            num_entities: int, 
            loaded_direct_trip_count: int = 0,
            loaded_matched_trip_count: int = 0,
            age_seconds: int = 0, 
            status_code: int = 200
        ) -> None:
        """Report a request to the quality report service."""
        
        self._quality_report_service.report_request(
            timestamp=timestamp,
            instance=instance,
            pipeline=pipeline,
            num_entities=num_entities,
            loaded_direct_trip_count=loaded_direct_trip_count,
            loaded_matched_trip_count=loaded_matched_trip_count,
            age_seconds=age_seconds,
            status_code=status_code
        )

    def report_quality_issue(
            self,
            instance: InstanceConfig,
            pipeline: PipelineConfig,
            timestamp: date,
            entity_id: str,
            issue_type_id: int,
            concessionaire_id: str | None = None,
            concessionaire_name: str | None = None,
            operator_id: str | None = None,
            operator_name: str | None = None,
            assessment_value: str | None = None
        ) -> None:
        """Report a quality issue to the quality report service."""

        self._quality_report_service.report_quality_issue(
            timestamp=timestamp,
            instance=instance,
            pipeline=pipeline,
            entity_id=entity_id,
            issue_type_id=issue_type_id,
            concessionaire_id=concessionaire_id,
            concessionaire_name=concessionaire_name,
            operator_id=operator_id,
            operator_name=operator_name,
            assessment_value=assessment_value
        )

    async def submit_quality_report(self, instance: InstanceConfig) -> None:
        """Submit the quality report to the timeline repository."""

        await self._loading_service.load_request(
            instance.id,
            self._quality_report_service.get_request(),
        )

        await self._loading_service.load_quality_issues_batch(
            instance.id,
            self._quality_report_service.get_quality_issues(),
        )

        self._quality_report_service.clear()

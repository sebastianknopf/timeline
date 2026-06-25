from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from processor.common.quality_report_service import QualityReportService
from processor.loading.loading_service import LoadingService

from ..runtime_config import PipelineConfig, InstanceConfig


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


class NominalPipelineBase(PipelineBase):
    """Abstract base class for all nominal pipeline implementations."""

    def __init__(self) -> None:
        pass


class RealtimePipelineBase(PipelineBase):
    """Abstract base class for all realtime pipeline implementations."""

    def __init__(self, loading_service: LoadingService) -> None:
        self._quality_report_service: QualityReportService = QualityReportService()

        self._loading_service = loading_service

    def report_request(
            self, 
            instance: InstanceConfig,
            pipeline: PipelineConfig,
            timestamp: date, 
            num_entities: int, 
            age_seconds: int, 
            status_code: int = 200
        ) -> None:
        """Report a request to the quality report service."""
        
        self._quality_report_service.report_request(
            timestamp=timestamp,
            instance=instance,
            pipeline=pipeline,
            num_entities=num_entities,
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

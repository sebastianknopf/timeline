from __future__ import annotations

from datetime import datetime
from hashlib import md5

from ..runtime_config import InstanceConfig, PipelineConfig
from ..common.quality_issues import QualityIssue
from ..loading.models import QualityIssueRecord, RequestRecord


class QualityReportService:
    """Service for generating quality reports."""

    def __init__(self) -> None:
        
        self._request: RequestRecord | None = None
        self._quality_issues: list[QualityIssueRecord] = []

    def report_request(
            self, 
            instance: InstanceConfig,
            pipeline: PipelineConfig,
            timestamp: datetime,
            num_entities: int,
            age_seconds: int,
            status_code: int = 200
        ) -> None:
        """
        Add a request record to the quality report.
        
        Args:
            timestamp (datetime): The timestamp of the request.
            num_entities (int): The number of entities processed in the request.
            age_seconds (int): The age of the request in seconds.
            status_code (int, optional): The HTTP status code of the request. Defaults to 200.
        """

        request: RequestRecord = RequestRecord(
            instance_id=instance.id,
            request_id=self._compute_request_id(pipeline.id, timestamp),
            pipeline_id=pipeline.id,
            timestamp=timestamp,
            num_entities=num_entities,
            age_seconds=age_seconds,
            status_code=status_code
        )

        self._request = request

    def report_quality_issue(
            self,
            instance: InstanceConfig,
            pipeline: PipelineConfig,
            timestamp: datetime,
            entity_id: str,
            issue_type_id: QualityIssue,
            concessionaire_id: str | None = None,
            concessionaire_name: str | None = None,
            operator_id: str | None = None,
            operator_name: str | None = None,
            assessment_value: str | None = None
        ) -> None:
        """
        Add a quality issue record to the quality report.
        
        Args:
            timestamp (datetime): The timestamp of the quality issue.
            entity_id (str): The ID of the entity with the quality issue.
            issue_type_id (QualityIssue): The type of the quality issue.
            concessionaire_id (str, optional): The ID of the concessionaire. Defaults to None.
            concessionaire_name (str, optional): The name of the concessionaire. Defaults to None.
            operator_id (str, optional): The ID of the operator. Defaults to None.
            operator_name (str, optional): The name of the operator. Defaults to None.
            assessment_value (str, optional): The assessment value. Defaults to None.
        """

        quality_issue: QualityIssueRecord = QualityIssueRecord(
            instance_id=instance.id,
            issue_id=self._compute_quality_issue_id(pipeline.id, timestamp, entity_id, issue_type_id),
            pipeline_id=pipeline.id,
            timestamp=timestamp,
            entity_id=entity_id,
            issue_type_id=issue_type_id.value,
            concessionaire_id=concessionaire_id,
            concessionaire_name=concessionaire_name,
            operator_id=operator_id,
            operator_name=operator_name,
            assessment_value=assessment_value,
            num_affected_values=1 if assessment_value is not None else 0
        )

        # check here, if a quality issue with the same issue_id already exists
        # if so, add the the assessment_value separated by a comma and increment the num_affected_values
        if any(q.issue_id == quality_issue.issue_id for q in self._quality_issues):
            existing_issue = next(q for q in self._quality_issues if q.issue_id == quality_issue.issue_id)
            if existing_issue.assessment_value is None:
                object.__setattr__(existing_issue, "assessment_value", quality_issue.assessment_value)
            else:
                object.__setattr__(
                    existing_issue,
                    "assessment_value",
                    f"{existing_issue.assessment_value}, {quality_issue.assessment_value}",
                )

            object.__setattr__(existing_issue, "num_affected_values", existing_issue.num_affected_values + 1)
        else:
            self._quality_issues.append(quality_issue)

    def get_request(self) -> RequestRecord | None:
        """
        Get the first request record in the quality report.
        
        Returns:
            RequestRecord | None: The first request record, or None if no requests have been reported.
        """

        return self._request
    
    def get_quality_issues(self) -> list[QualityIssueRecord]:
        """
        Get all quality issue records in the quality report.
        
        Returns:
            list[QualityIssueRecord]: A list of all quality issue records.
        """

        return self._quality_issues
    
    def _compute_request_id(self, pipeline_id: str, timestamp: datetime) -> str:
        """
        Compute a unique record ID based on the pipeline ID and timestamp.
        
        Args:
            pipeline_id (str): The ID of the pipeline.
            timestamp (datetime): The timestamp of the record.

        Returns:
            str: The computed record ID.
        """

        return md5(f"{pipeline_id}-{timestamp.strftime('%Y%m%d%H%M%S')}".encode()).hexdigest()
    
    def _compute_quality_issue_id(
            self, 
            pipeline_id: str, 
            timestamp: datetime,
            entity_id: str,
            issue_type_id: QualityIssue
        ) -> str:
        """
        Compute a unique quality issue ID based on the pipeline ID, timestamp, entity ID, and issue type ID.
        
        Args:
            pipeline_id (str): The ID of the pipeline.
            timestamp (datetime): The timestamp of the quality issue.
            entity_id (str): The ID of the entity with the quality issue.
            issue_type_id (QualityIssue): The type of the quality issue.

        Returns:
            str: The computed quality issue ID.
        """

        return md5(f"{pipeline_id}-{timestamp.strftime('%Y%m%d%H%M%S')}-{entity_id}-{issue_type_id.value}".encode()).hexdigest()
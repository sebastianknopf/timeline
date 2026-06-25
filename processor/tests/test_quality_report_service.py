from __future__ import annotations

import unittest
from datetime import datetime, timezone

from processor.common.quality_issues import QualityIssue
from processor.common.quality_report_service import QualityReportService
from processor.runtime_config import InstanceConfig, PipelineConfig


class QualityReportServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.instance = InstanceConfig(
            id="demo",
            pipelines=(
                PipelineConfig(
                    id="gtfsrt-tripupdates",
                    name="gtfsrt-tripupdates",
                    type="realtime",
                    cron="0 * * * *",
                    endpoint="https://example.test/feed",
                ),
            ),
        )
        self.pipeline = self.instance.pipelines[0]
        self.service = QualityReportService(self.instance, self.pipeline)

    def test_add_request_populates_instance_id_and_request_fields(self) -> None:
        timestamp = datetime(2026, 6, 23, 12, 0, tzinfo=timezone.utc)

        self.service.report_request(
            timestamp=timestamp,
            num_entities=3,
            age_seconds=42,
            status_code=200,
        )

        request = self.service.get_request()
        self.assertIsNotNone(request)
        self.assertEqual("demo", request.instance_id)
        self.assertEqual(self.pipeline.id, request.pipeline_id)
        self.assertEqual(timestamp, request.timestamp)
        self.assertEqual(3, request.num_entities)
        self.assertEqual(42, request.age_seconds)
        self.assertEqual(200, request.status_code)
        self.assertTrue(request.request_id)

    def test_add_quality_issue_populates_instance_id_and_issue_fields(self) -> None:
        timestamp = datetime(2026, 6, 23, 12, 5, tzinfo=timezone.utc)

        self.service.report_quality_issue(
            timestamp=timestamp,
            entity_id="trip-1",
            issue_type_id=QualityIssue.OperatorIdIsNull,
            concessionaire_id="con-1",
            concessionaire_name="Concessionaire",
            operator_id="op-1",
            operator_name="Operator",
            assessment_value="HIGH",
            num_affected_values=2,
        )

        issues = self.service.get_quality_issues()
        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual("demo", issue.instance_id)
        self.assertEqual(self.pipeline.id, issue.pipeline_id)
        self.assertEqual(timestamp, issue.timestamp)
        self.assertEqual("trip-1", issue.entity_id)
        self.assertEqual(QualityIssue.OperatorIdIsNull.value, issue.issue_type_id)
        self.assertEqual("con-1", issue.concessionaire_id)
        self.assertEqual("Operator", issue.operator_name)
        self.assertEqual("HIGH", issue.assessment_value)
        self.assertEqual(1, issue.num_affected_values)
        self.assertTrue(issue.issue_id)

    def test_add_quality_issue_merges_duplicate_issue_ids(self) -> None:
        timestamp = datetime(2026, 6, 23, 12, 10, tzinfo=timezone.utc)

        self.service.report_quality_issue(
            timestamp=timestamp,
            entity_id="trip-2",
            issue_type_id=QualityIssue.RouteIdIsNull,
            assessment_value="LOW",
        )
        self.service.report_quality_issue(
            timestamp=timestamp,
            entity_id="trip-2",
            issue_type_id=QualityIssue.RouteIdIsNull,
            assessment_value="MEDIUM",
        )

        issues = self.service.get_quality_issues()
        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual(2, issue.num_affected_values)
        self.assertEqual("LOW, MEDIUM", issue.assessment_value)


if __name__ == "__main__":
    unittest.main()

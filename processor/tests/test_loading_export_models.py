from __future__ import annotations

import unittest
from datetime import date, datetime

from processor.exports.models import (
    ExportDataSet,
    ExportIssueTypeRow,
    ExportQualityIssueRow,
    ExportRequestRow,
    ExportStopTimeRow,
    ExportTripRow,
)
from processor.loading.models import (
    IssueTypeRecord,
    QualityIssueRecord,
    RequestRecord,
)


class LoadingAndExportModelsTests(unittest.TestCase):
    def test_new_domain_records_have_monitoring_fields(self) -> None:
        issue_type = IssueTypeRecord(issue_type_id=1, code="OperatorIdIsNull")
        request = RequestRecord(
            instance_id="demo",
            request_id="req-1",
            pipeline_id="pipeline-a",
            timestamp=datetime(2026, 6, 23, 12, 0, tzinfo=datetime.now().astimezone().tzinfo),
            num_entities=3,
            loaded_direct_trip_count=2,
            loaded_matched_trip_count=1,
            age_seconds=42,
        )
        quality_issue = QualityIssueRecord(
            instance_id="demo",
            issue_id="issue-1",
            pipeline_id="pipeline-a",
            timestamp=datetime(2026, 6, 23, 12, 0, tzinfo=datetime.now().astimezone().tzinfo),
            entity_id="entity-1",
            issue_type_id=1,
        )

        self.assertEqual(1, issue_type.issue_type_id)
        self.assertEqual("OperatorIdIsNull", issue_type.code)
        self.assertEqual("req-1", request.request_id)
        self.assertEqual(2, request.loaded_direct_trip_count)
        self.assertEqual(1, request.loaded_matched_trip_count)
        self.assertEqual(200, request.status_code)
        self.assertEqual("issue-1", quality_issue.issue_id)
        self.assertEqual(1, quality_issue.issue_type_id)

    def test_export_rows_cover_new_monitoring_models_and_delay_fields(self) -> None:
        stop_time = ExportStopTimeRow(
            operation_day_date=date(2026, 6, 23),
            trip_id="trip-1",
            stop_id="stop-1",
            stop_sequence=1,
            distance_from_start=0.0,
            nom_arrival_time=datetime(2026, 6, 23, 8, 0, tzinfo=datetime.now().astimezone().tzinfo),
            nom_departure_time=datetime(2026, 6, 23, 8, 1, tzinfo=datetime.now().astimezone().tzinfo),
            act_arrival_time=None,
            act_departure_time=None,
            schedule_relationship="UNKNOWN",
            arrival_delay_seconds=15,
            departure_delay_seconds=20,
        )
        trip = ExportTripRow(
            operation_day_date=date(2026, 6, 23),
            trip_id="trip-1",
            route_id="route-1",
            concessionaire_id=None,
            concessionaire_name=None,
            operator_id=None,
            operator_name=None,
            nom_start_time=datetime(2026, 6, 23, 8, 0, tzinfo=datetime.now().astimezone().tzinfo),
            nom_end_time=datetime(2026, 6, 23, 8, 30, tzinfo=datetime.now().astimezone().tzinfo),
            act_start_time=None,
            act_end_time=None,
            nom_start_stop_id="stop-1",
            nom_end_stop_id="stop-2",
            nom_total_distance=12.5,
            act_total_distance=None,
            schedule_relationship="UNKNOWN",
            realtime_assignment_method="MATCHING",
        )
        request = ExportRequestRow(
            request_id="req-1",
            pipeline_id="pipeline-a",
            timestamp=datetime(2026, 6, 23, 12, 0, tzinfo=datetime.now().astimezone().tzinfo),
            num_entities=3,
            age_seconds=42,
            status_code=200,
            loaded_direct_trip_count=2,
            loaded_matched_trip_count=1,
        )
        issue_type = ExportIssueTypeRow(issue_type_id=1, code="OperatorIdIsNull")
        quality_issue = ExportQualityIssueRow(
            issue_id="issue-1",
            pipeline_id="pipeline-a",
            timestamp=datetime(2026, 6, 23, 12, 0, tzinfo=datetime.now().astimezone().tzinfo),
            entity_id="entity-1",
            issue_type_id=1,
        )
        dataset = ExportDataSet()

        self.assertEqual(15, stop_time.arrival_delay_seconds)
        self.assertEqual(20, stop_time.departure_delay_seconds)
        self.assertEqual("MATCHING", trip.realtime_assignment_method)
        self.assertEqual("req-1", request.request_id)
        self.assertEqual(2, request.loaded_direct_trip_count)
        self.assertEqual(1, request.loaded_matched_trip_count)
        self.assertEqual(1, issue_type.issue_type_id)
        self.assertEqual("issue-1", quality_issue.issue_id)
        self.assertEqual([], dataset.requests)
        self.assertEqual([], dataset.quality_issues)

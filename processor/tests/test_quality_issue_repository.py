from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.loading.loading_service import LoadingService
from processor.loading.models import QualityIssueRecord
from processor.repository.sqlalchemy_timeline_repository import SqlAlchemyTimelineRepository


class RecordingRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[QualityIssueRecord]]] = []

    async def insert_quality_issues(
        self,
        instance_id: str,
        quality_issues: list[QualityIssueRecord],
    ) -> None:
        self.calls.append((instance_id, quality_issues))


class _FakeSession:
    def __init__(self) -> None:
        self.executed: list[tuple[Any, Any]] = []

    def begin(self) -> Any:
        return self

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, statement: Any, params: Any = None) -> Any:
        self.executed.append((statement, params))
        return SimpleNamespace()


class LoadingServiceQualityIssueTests(unittest.IsolatedAsyncioTestCase):
    async def test_loading_service_delegates_quality_issue_batches(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)  # type: ignore[arg-type]
        issue = QualityIssueRecord(
            instance_id="demo",
            issue_id="issue-1",
            pipeline_id="gtfsrt-tripupdates",
            timestamp=datetime(2026, 5, 31, 8, 0, tzinfo=timezone.utc),
            entity_id="trip-1",
            issue_type_id=7,
            concessionaire_id="con-1",
            concessionaire_name="Concessionaire",
            operator_id="op-1",
            operator_name="Operator",
            assessment_value="HIGH",
            num_affected_values=2,
        )

        await service.load_quality_issues_batch(instance_id="demo", quality_issues=[issue])

        self.assertEqual(repository.calls, [("demo", [issue])])

    def test_repository_inserts_quality_issue_rows_without_upsert(self) -> None:
        session = _FakeSession()
        repository = SqlAlchemyTimelineRepository(session_factory=lambda: session)
        issue = QualityIssueRecord(
            instance_id="demo",
            issue_id="issue-2",
            pipeline_id="gtfsrt-tripupdates",
            timestamp=datetime(2026, 5, 31, 9, 0, tzinfo=timezone.utc),
            entity_id="trip-2",
            issue_type_id=9,
            concessionaire_id="con-2",
            concessionaire_name="Concessionaire 2",
            operator_id="op-2",
            operator_name="Operator 2",
            assessment_value="LOW",
            num_affected_values=1,
        )

        import asyncio

        asyncio.run(repository.upsert_quality_issues(instance_id="demo", quality_issues=[issue]))

        self.assertEqual(len(session.executed), 1)
        statement, params = session.executed[0]
        self.assertIsNotNone(statement)
        self.assertEqual(params, [{
            "instance_id": "demo",
            "issue_id": "issue-2",
            "pipeline_id": "gtfsrt-tripupdates",
            "timestamp": issue.timestamp,
            "entity_id": "trip-2",
            "issue_type_id": 9,
            "concessionaire_id": "con-2",
            "concessionaire_name": "Concessionaire 2",
            "operator_id": "op-2",
            "operator_name": "Operator 2",
            "assessment_value": "LOW",
            "num_affected_values": 1,
        }])
        self.assertNotIn("ON CONFLICT", str(statement))


if __name__ == "__main__":
    unittest.main()

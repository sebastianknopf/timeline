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
from processor.loading.models import RequestRecord
from processor.repository.sqlalchemy_timeline_repository import SqlAlchemyTimelineRepository


class RecordingRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, RequestRecord]] = []

    async def insert_request(
        self,
        instance_id: str,
        request: RequestRecord,
    ) -> None:
        self.calls.append((instance_id, request))


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


class LoadingServiceRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_loading_service_delegates_request_inserts(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)  # type: ignore[arg-type]
        request = RequestRecord(
            instance_id="demo",
            request_id="req-1",
            pipeline_id="nominal-main",
            timestamp=datetime(2026, 5, 31, 8, 0, tzinfo=timezone.utc),
            num_entities=3,
            age_seconds=30,
            status_code=200,
        )

        await service.load_request(instance_id="demo", request=request)

        self.assertEqual(repository.calls, [("demo", request)])

    def test_repository_inserts_request_rows_without_upsert(self) -> None:
        session = _FakeSession()
        repository = SqlAlchemyTimelineRepository(session_factory=lambda: session)
        request = RequestRecord(
            instance_id="demo",
            request_id="req-2",
            pipeline_id="nominal-main",
            timestamp=datetime(2026, 5, 31, 9, 0, tzinfo=timezone.utc),
            num_entities=5,
            age_seconds=90,
            status_code=200,
        )

        import asyncio

        asyncio.run(repository.insert_request(instance_id="demo", request=request))

        self.assertEqual(len(session.executed), 1)
        statement, params = session.executed[0]
        self.assertIsNotNone(statement)
        self.assertEqual(params, [{
            "instance_id": "demo",
            "request_id": "req-2",
            "pipeline_id": "nominal-main",
            "timestamp": request.timestamp,
            "num_entities": 5,
            "age_seconds": 90,
            "status_code": 200,
        }])
        self.assertNotIn("ON CONFLICT", str(statement))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.common.runtime_config_service import RuntimeConfigService
from processor.runtime_config import InstanceConfig, PipelineConfig, ProcessorConfig


class RuntimeConfigServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        RuntimeConfigService.clear()

    def test_get_pipeline_priority_returns_none_when_service_is_not_initialized(self) -> None:
        self.assertIsNone(RuntimeConfigService.get_pipeline_priority("demo", "missing-pipeline"))

    def test_get_pipeline_priority_returns_none_for_unknown_pipeline_id(self) -> None:
        config = ProcessorConfig(
            instances=(
                InstanceConfig(
                    id="demo",
                    pipelines=(
                        PipelineConfig(
                            id="nominal-main",
                            name="gtfs",
                            type="nominal",
                            cron="0 2 * * *",
                            endpoint="https://example.test/nominal",
                            priority=10,
                        ),
                    ),
                ),
            )
        )

        RuntimeConfigService.initialize(config)

        self.assertIsNone(RuntimeConfigService.get_pipeline_priority("demo", "unknown-pipeline"))

    def test_get_pipeline_priority_returns_priority_for_matching_pipeline_id(self) -> None:
        config = ProcessorConfig(
            instances=(
                InstanceConfig(
                    id="demo",
                    pipelines=(
                        PipelineConfig(
                            id="realtime-main",
                            name="gtfsrt-tripupdates",
                            type="realtime",
                            cron="* * * * *",
                            endpoint="https://example.test/realtime",
                            priority=42,
                        ),
                        PipelineConfig(
                            id="nominal-main",
                            name="gtfs",
                            type="nominal",
                            cron="0 2 * * *",
                            endpoint="https://example.test/nominal",
                            priority=7,
                        ),
                    ),
                ),
            )
        )

        RuntimeConfigService.initialize(config)

        self.assertEqual(42, RuntimeConfigService.get_pipeline_priority("demo", "realtime-main"))
        self.assertEqual(7, RuntimeConfigService.get_pipeline_priority("demo", "nominal-main"))


if __name__ == "__main__":
    unittest.main()

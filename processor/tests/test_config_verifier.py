from __future__ import annotations

import tempfile
from pathlib import Path
import unittest

try:
  from . import _test_bootstrap
except ImportError:
  import _test_bootstrap

from processor.config_verifier import ConfigurationError, ConfigurationVerifier


class ConfigurationVerifierTests(unittest.TestCase):
    def test_load_and_validate_accepts_valid_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            mapping_root = tmp_dir / "mapping"
            mapping_root.mkdir()

            stops_file = mapping_root / "stops.csv"
            stops_file.write_text("key,value\nA,Stop A\n", encoding="utf-8")

            routes_file = mapping_root / "routes.csv"
            routes_file.write_text("key,value\nR1,Route 1\n", encoding="utf-8")

            config_file = tmp_dir / "config.yaml"
            config_file.write_text(
                """
instance:
  - id: demo
    pipeline:
      - id: nominal-main
        name: gtfs
        type: nominal
        cron: "0 2 * * *"
        endpoint: "https://example.test/nominal"
        mapping:
          stops: "stops.csv"
          routes: "routes.csv"
      - id: realtime-main
        name: gtfsrt-tripupdates
        type: realtime
        cron: "* * * * *"
        endpoint: "https://example.test/realtime"
        authentication:
          token: "abc"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=mapping_root)
            parsed = verifier.load_and_validate(config_file)

            self.assertEqual(1, len(parsed.instances))
            self.assertEqual("demo", parsed.instances[0].id)
            self.assertEqual(2, len(parsed.instances[0].pipelines))
            self.assertEqual("schedule", parsed.instances[0].pipelines[0].policy)

    def test_load_and_validate_accepts_second_based_cron_expression(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            config_file = tmp_dir / "config.yaml"
            config_file.write_text(
                """
instance:
  - id: demo
    pipeline:
      - id: realtime-fast
        name: gtfsrt-tripupdates
        type: realtime
        cron: "*/10 * * * * *"
        endpoint: "https://example.test/realtime"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)
            parsed = verifier.load_and_validate(config_file)

            self.assertEqual("*/10 * * * * *", parsed.instances[0].pipelines[0].cron)

    def test_load_and_validate_accepts_pipeline_startup_and_schedule_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            config_file = tmp_dir / "config.yaml"
            config_file.write_text(
                """
instance:
  - id: demo
    pipeline:
      - id: nominal-main
        name: gtfs
        type: nominal
        cron: "0 2 * * *"
        endpoint: "https://example.test/nominal"
        policy: "startupAndSchedule"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)
            parsed = verifier.load_and_validate(config_file)

            self.assertEqual(
                "startupAndSchedule",
                parsed.instances[0].pipelines[0].policy,
            )

    def test_load_and_validate_rejects_invalid_pipeline_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            config_file = tmp_dir / "config.yaml"
            config_file.write_text(
                """
instance:
  - id: demo
    pipeline:
      - id: nominal-main
        name: gtfs
        type: nominal
        cron: "0 2 * * *"
        endpoint: "https://example.test/nominal"
        policy: "startup"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)

            with self.assertRaises(ConfigurationError):
                verifier.load_and_validate(config_file)

    def test_load_and_validate_rejects_invalid_authentication_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            config_file = tmp_dir / "config.yaml"
            config_file.write_text(
                """
instance:
  - id: demo
    pipeline:
      - id: realtime-main
        name: gtfsrt-tripupdates
        type: realtime
        cron: "* * * * *"
        endpoint: "https://example.test/realtime"
        authentication:
          token: "abc"
          username: "user"
          password: "pw"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)

            with self.assertRaises(ConfigurationError):
                verifier.load_and_validate(config_file)

    def test_load_and_validate_rejects_mapping_path_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            mapping_root = tmp_dir / "mapping"
            mapping_root.mkdir()

            outside_file = tmp_dir / "outside.csv"
            outside_file.write_text("key,value\nA,B\n", encoding="utf-8")

            config_file = tmp_dir / "config.yaml"
            config_file.write_text(
                f"""
instance:
  - id: demo
    pipeline:
      - id: nominal-main
        name: gtfs
        type: nominal
        cron: "0 2 * * *"
        endpoint: "https://example.test/nominal"
        mapping:
          stops: "{outside_file.as_posix()}"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=mapping_root)

            with self.assertRaises(ConfigurationError):
                verifier.load_and_validate(config_file)


if __name__ == "__main__":
    unittest.main()

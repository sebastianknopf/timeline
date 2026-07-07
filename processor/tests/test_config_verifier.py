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
    def test_load_and_validate_defaults_pipeline_timezone_to_utc(self) -> None:
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
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)
            parsed = verifier.load_and_validate(config_file)

            self.assertEqual("UTC", parsed.instances[0].pipelines[0].timezone)

    def test_load_and_validate_accepts_pipeline_timezone(self) -> None:
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
        timezone: "Europe/Berlin"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)
            parsed = verifier.load_and_validate(config_file)

            self.assertEqual("Europe/Berlin", parsed.instances[0].pipelines[0].timezone)

    def test_load_and_validate_rejects_invalid_pipeline_timezone(self) -> None:
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
        timezone: "Mars/OlympusMons"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)

            with self.assertRaises(ConfigurationError):
                verifier.load_and_validate(config_file)

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
        filter:
          routes:
            - match: "*-route1"
              type: include
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
        filter:
          operators:
            - match: "*-operator1"
              type: exclude
              mapping:
                stops: "stops.csv"
                routes: "routes.csv"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=mapping_root)
            parsed = verifier.load_and_validate(config_file)

            self.assertEqual(1, len(parsed.instances))
            self.assertEqual("demo", parsed.instances[0].id)
            self.assertEqual(2, len(parsed.instances[0].pipelines))
            self.assertEqual("schedule", parsed.instances[0].pipelines[0].policy)
            nominal_filter = parsed.instances[0].pipelines[0].filter
            self.assertIsNotNone(nominal_filter)
            self.assertEqual(1, len(nominal_filter.routes))
            self.assertEqual("*-route1", nominal_filter.routes[0].match)
            self.assertIsNotNone(nominal_filter.routes[0].mapping)
            self.assertEqual(stops_file.resolve(), nominal_filter.routes[0].mapping.stops)
            self.assertEqual(routes_file.resolve(), nominal_filter.routes[0].mapping.routes)

            realtime_filter = parsed.instances[0].pipelines[1].filter
            self.assertIsNotNone(realtime_filter)
            self.assertEqual(1, len(realtime_filter.operators))
            self.assertEqual("exclude", realtime_filter.operators[0].type)

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
        filter:
          operators:
            - match: "*-operator1"
              type: include
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)
            parsed = verifier.load_and_validate(config_file)

            self.assertEqual("*/10 * * * * *", parsed.instances[0].pipelines[0].cron)
            self.assertEqual(
              "include",
              parsed.instances[0].pipelines[0].filter.operators[0].type,
            )

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
        filter:
          routes:
            - match: "*-route1"
              type: exclude
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
        filter:
          routes:
            - match: "*-route1"
              type: include
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
        filter:
          operators:
            - match: "*-operator1"
              type: include
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)

            with self.assertRaises(ConfigurationError):
                verifier.load_and_validate(config_file)

        def test_load_and_validate_authentication_accepts_exactly_one_method(self) -> None:
          with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            cert_file = tmp_dir / "client.crt"
            key_file = tmp_dir / "client.key"
            cert_file.write_text("dummy-cert", encoding="utf-8")
            key_file.write_text("dummy-key", encoding="utf-8")

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)

            valid_auth_blocks = {
              "token": 'token: "abc"',
              "basic": 'username: "user"\n          password: "pw"',
              "mtls": (
                f'cert: "{cert_file.as_posix()}"\n'
                f'          key: "{key_file.as_posix()}"'
              ),
            }

            for case_name, auth_block in valid_auth_blocks.items():
              with self.subTest(case=case_name):
                config_file = tmp_dir / "config.yaml"
                config_file.write_text(
                  f"""
      instance:
        - id: demo
        pipeline:
          - id: realtime-main
          name: gtfsrt-tripupdates
          type: realtime
          cron: "* * * * *"
          endpoint: "https://example.test/realtime"
          authentication:
            {auth_block}
      """.strip(),
                  encoding="utf-8",
                )

                parsed = verifier.load_and_validate(config_file)
                self.assertIsNotNone(parsed.instances[0].pipelines[0].authentication)

            invalid_auth_blocks = {
              "token_and_basic": (
                'token: "abc"\n'
                '          username: "user"\n'
                '          password: "pw"'
              ),
              "token_and_mtls": (
                f'token: "abc"\n'
                f'          cert: "{cert_file.as_posix()}"\n'
                f'          key: "{key_file.as_posix()}"'
              ),
              "basic_and_mtls": (
                'username: "user"\n'
                '          password: "pw"\n'
                f'          cert: "{cert_file.as_posix()}"\n'
                f'          key: "{key_file.as_posix()}"'
              ),
              "all_methods": (
                'token: "abc"\n'
                '          username: "user"\n'
                '          password: "pw"\n'
                f'          cert: "{cert_file.as_posix()}"\n'
                f'          key: "{key_file.as_posix()}"'
              ),
            }

            for case_name, auth_block in invalid_auth_blocks.items():
              with self.subTest(case=case_name):
                config_file = tmp_dir / "config.yaml"
                config_file.write_text(
                  f"""
      instance:
        - id: demo
        pipeline:
          - id: realtime-main
          name: gtfsrt-tripupdates
          type: realtime
          cron: "* * * * *"
          endpoint: "https://example.test/realtime"
          authentication:
            {auth_block}
      """.strip(),
                  encoding="utf-8",
                )

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
        filter:
          routes:
            - match: "*-route1"
              type: include
              mapping:
                stops: "{outside_file.as_posix()}"
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=mapping_root)

            with self.assertRaises(ConfigurationError):
                verifier.load_and_validate(config_file)

    def test_load_and_validate_accepts_pipeline_without_filter(self) -> None:
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
""".strip(),
                encoding="utf-8",
            )

            verifier = ConfigurationVerifier(mapping_root=tmp_dir)
            parsed = verifier.load_and_validate(config_file)

            self.assertEqual(1, len(parsed.instances))
            self.assertEqual(1, len(parsed.instances[0].pipelines))
            self.assertIsNone(parsed.instances[0].pipelines[0].filter)


if __name__ == "__main__":
    unittest.main()

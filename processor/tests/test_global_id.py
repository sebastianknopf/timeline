from __future__ import annotations

import unittest

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.common.global_id import GlobalId


class GlobalIdTests(unittest.TestCase):
    def test_is_global_id_returns_true_for_valid_input(self) -> None:
        self.assertTrue(GlobalId.is_global_id("de:0815:42"))

    def test_is_global_id_rejects_too_few_segments(self) -> None:
        self.assertFalse(GlobalId.is_global_id("de:0815"))

    def test_is_global_id_rejects_empty_segment(self) -> None:
        self.assertFalse(GlobalId.is_global_id("de::42"))

    def test_is_global_id_rejects_empty_segment_in_later_position(self) -> None:
        self.assertFalse(GlobalId.is_global_id("de:0815::42"))

    def test_is_global_id_rejects_whitespace_only_segments(self) -> None:
        self.assertFalse(GlobalId.is_global_id("de:0815: "))

    def test_is_global_id_rejects_non_alpha_namespace(self) -> None:
        self.assertFalse(GlobalId.is_global_id("d3:0815:42"))

    def test_level_returns_input_unchanged_for_non_global_id(self) -> None:
        self.assertEqual("not-a-global-id", GlobalId.level("not-a-global-id", 2))

    def test_level_reduces_global_id_to_requested_prefix_without_separator(self) -> None:
        self.assertEqual("de:0815", GlobalId.level("de:0815:42", 2))

    def test_level_returns_empty_string_for_level_zero(self) -> None:
        self.assertRaises(ValueError, GlobalId.level, "de:0815:42", 0)

    def test_level_returns_full_concatenation_for_large_level(self) -> None:
        self.assertEqual("de:0815:42", GlobalId.level("de:0815:42", 10))


if __name__ == "__main__":
    unittest.main()

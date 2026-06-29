from __future__ import annotations

import unittest

from processor.common import QualityIssue


class QualityIssueTests(unittest.TestCase):
    def test_quality_issue_enum_contains_documented_values(self) -> None:
        self.assertEqual(1, QualityIssue.OperatorIdIsNull.value)
        self.assertEqual(15, QualityIssue.EstimatedDepatureTimeBeforeArrivalTime.value)
        self.assertEqual("OperatorIdIsNull", QualityIssue.OperatorIdIsNull.code)

    def test_from_code_round_trips(self) -> None:
        self.assertEqual(QualityIssue.RouteIdIsNull, QualityIssue.from_code("RouteIdIsNull"))


if __name__ == "__main__":
    unittest.main()

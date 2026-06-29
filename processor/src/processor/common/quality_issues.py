from __future__ import annotations

from enum import IntEnum


class QualityIssue(IntEnum):
    """Canonical quality issue codes and their internal numeric IDs."""

    OperatorIdIsNull = 1
    RouteIdIsNull = 2
    OperationDayIsNull = 3
    RouteIdNonGlobal = 4
    StopIdNonGlobal = 5
    TripIdNonGlobal = 6
    TripNotMonitored = 7
    TripPredictionInaccurate = 8
    StartStopIdNull = 9
    DestinationStopIdNull = 10
    NotCompleteStopSequence = 11
    NoNominalTripFound = 12
    NoAmbiguousNominalTripFound = 13
    AimedDepartureTimeBeforeArrivalTime = 14
    EstimatedDepatureTimeBeforeArrivalTime = 15
    UnexpectedStopFound = 16
    ExpectedStopMissing = 17

    @classmethod
    def from_code(cls, code: str) -> "QualityIssue":
        try:
            return cls[code]
        except KeyError as exc:
            raise ValueError(f"Unknown quality issue code: {code}") from exc

    @property
    def code(self) -> str:
        return self.name

from __future__ import annotations

from datetime import date
from typing import Protocol

from .models import ExportDataSet


class ExportRepositoryInterface(Protocol):
    async def get_export_dataset(
        self,
        instance_id: str,
        from_date: date,
        to_date: date,
    ) -> ExportDataSet:
        """Fetch timeline data for the given instance and half-open date interval [from_date, to_date).

        Returns a consistent ExportDataSet where the dimension tables (trips, stops, routes)
        contain only the minimum set of rows referenced by the filtered stop_times.
        """

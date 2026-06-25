from .models import ExportDataSet, ExportRouteRow, ExportStopRow, ExportStopTimeRow, ExportTripRow

__all__ = [
    "ExportDataSet",
    "ExportExecutorInterface",
    "ExportRouteRow",
    "ExportStopRow",
    "ExportStopTimeRow",
    "ExportTripRow",
    "TimelineExport",
    "TimelineExportExecutor",
]


def __getattr__(name: str) -> object:
    if name == "ExportExecutorInterface":
        from .intf_export_executor import ExportExecutorInterface

        return ExportExecutorInterface
    if name == "TimelineExport":
        from .timeline_export import TimelineExport

        return TimelineExport
    if name == "TimelineExportExecutor":
        from .export_executor import TimelineExportExecutor

        return TimelineExportExecutor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

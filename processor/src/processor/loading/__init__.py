from .models import StopRecord, StopTimeRecord, TripRecord

__all__ = [
    "LoadingService",
    "StopRecord",
    "StopTimeRecord",
    "TripRecord",
]


def __getattr__(name: str) -> object:
    if name == "LoadingService":
        from .loading_service import LoadingService

        return LoadingService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

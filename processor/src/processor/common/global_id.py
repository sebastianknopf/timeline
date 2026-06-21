from __future__ import annotations

import structlog

LOGGER = structlog.get_logger(__name__)


class GlobalId:
    """Encapsules helper methods for working with global IDs in public transport."""

    @staticmethod
    def is_global_id(input: str) -> bool:
        """Returns True if input is a global ID."""

        if input is None:
            return False

        splitted: list[str] = input.split(":")
        return len(splitted) >= 3 and not any([True if e.strip() == "" else False for e in splitted]) and splitted[0].isalpha()
    
    @staticmethod
    def level(input: str, level: int) -> str:
        """Returns the reduced global ID to the desired level. If input is no global ID, input is returned again."""
        
        if not GlobalId.is_global_id(input):
            return input
        
        if level < 1:
            raise ValueError("Level for reducing a global ID must be at least 1!")
        
        splitted: list[str] = input.split(":")
        return ":".join(splitted[:level])
    

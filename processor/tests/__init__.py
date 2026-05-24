from __future__ import annotations

import logging
import sys
from pathlib import Path

import structlog

# Ensure src-layout package imports work with plain `python -m unittest discover`.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# Keep unit test output deterministic by suppressing processor runtime logs.
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.CRITICAL),
    cache_logger_on_first_use=True,
)
logging.getLogger("alembic").setLevel(logging.CRITICAL)
logging.getLogger("sqlalchemy").setLevel(logging.CRITICAL)

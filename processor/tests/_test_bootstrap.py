from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import structlog

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# Disable asyncio debug mode noise, for example "Executing <Task ...> took ... seconds".
os.environ["PYTHONASYNCIODEBUG"] = "0"

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.CRITICAL),
    cache_logger_on_first_use=True,
)

logging.getLogger().setLevel(logging.CRITICAL)
logging.getLogger("alembic").setLevel(logging.CRITICAL)
logging.getLogger("sqlalchemy").setLevel(logging.CRITICAL)
logging.getLogger("asyncio").setLevel(logging.CRITICAL)
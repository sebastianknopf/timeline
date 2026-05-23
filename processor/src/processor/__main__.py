from __future__ import annotations

import os


def main() -> None:
    database_url = os.getenv("DATABASE_URL", "<not-set>")
    print("timeline-processor started")
    print(f"DATABASE_URL={database_url}")


if __name__ == "__main__":
    main()
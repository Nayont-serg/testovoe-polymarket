from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg

from app.config import Settings

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


async def apply_migrations(database_url: str) -> None:
    connection = await asyncpg.connect(database_url)
    try:
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            await connection.execute(path.read_text())
    finally:
        await connection.close()


def main() -> None:
    settings = Settings.from_env()
    asyncio.run(apply_migrations(settings.database_url))


if __name__ == "__main__":
    main()

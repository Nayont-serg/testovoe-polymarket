import os

import asyncpg
import pytest

from app.db.migrate import apply_migrations


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://polymarket:polymarket@localhost:5433/polymarket_wallet_history",
    )


@pytest.fixture
async def pool():
    url = database_url()
    await apply_migrations(url)
    connection_pool = await asyncpg.create_pool(url, min_size=1, max_size=5)
    yield connection_pool
    async with connection_pool.acquire() as connection:
        await connection.execute(
            "TRUNCATE balance_checks, index_checkpoints, balance_events, assets, wallets RESTART IDENTITY CASCADE"
        )
    await connection_pool.close()

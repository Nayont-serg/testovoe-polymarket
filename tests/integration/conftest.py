import os
from collections.abc import AsyncIterator

import asyncpg
import pytest

from app.db.migrate import apply_migrations


def database_url() -> str:
    return os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql://polymarket:polymarket@localhost:5433/polymarket_wallet_history_test",
    )


async def ensure_database(url: str) -> None:
    server_url, database_name = url.rsplit("/", 1)
    connection = await asyncpg.connect(f"{server_url}/postgres")
    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", database_name
        )
        if not exists:
            await connection.execute(f'CREATE DATABASE "{database_name}"')
    finally:
        await connection.close()


@pytest.fixture
async def pool() -> AsyncIterator[asyncpg.Pool]:
    url = database_url()
    await ensure_database(url)
    await apply_migrations(url)
    connection_pool = await asyncpg.create_pool(url, min_size=1, max_size=5)
    yield connection_pool
    async with connection_pool.acquire() as connection:
        await connection.execute(
            "TRUNCATE balance_checks, index_checkpoints, balance_events, assets, "
            "wallets RESTART IDENTITY CASCADE"
        )
    await connection_pool.close()

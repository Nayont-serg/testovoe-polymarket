import pytest

pytestmark = pytest.mark.integration


async def test_migrations_create_expected_tables(pool):
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
    table_names = {row["table_name"] for row in rows}
    assert {
        "wallets",
        "assets",
        "balance_events",
        "index_checkpoints",
        "balance_checks",
    } <= table_names

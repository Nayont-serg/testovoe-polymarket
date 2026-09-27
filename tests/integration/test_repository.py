from datetime import UTC, datetime

import asyncpg
import pytest

from app.db.repository import LedgerRepository
from app.ledger.models import Asset, BalanceCheckResult, LedgerEntry

pytestmark = pytest.mark.integration

WALLET = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"


def make_entry(asset: Asset, tx_hash: str, log_index: int, delta: int) -> LedgerEntry:
    return LedgerEntry(
        wallet_address=WALLET,
        asset=asset,
        delta=delta,
        block_number=100,
        block_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        tx_hash=tx_hash,
        log_index=log_index,
        counterparty_address="0xother",
        event_type="DEPOSIT",
        source_event="Transfer",
    )


async def test_upsert_events_is_idempotent_on_repeated_insert(pool: asyncpg.Pool) -> None:
    repository = LedgerRepository(pool)
    await repository.ensure_wallet(WALLET)
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    asset_id = await repository.ensure_asset(asset)
    entry = make_entry(asset, "0xabc", 0, 1_000_000)
    await repository.upsert_events(asset_id, [entry])
    await repository.upsert_events(asset_id, [entry])
    assert await repository.sum_balance(WALLET, asset_id) == 1_000_000


async def test_sum_balance_nets_multiple_deposits_and_withdrawals(pool: asyncpg.Pool) -> None:
    repository = LedgerRepository(pool)
    await repository.ensure_wallet(WALLET)
    asset = Asset(
        kind="erc20", contract_address="0xusdc2", position_id=None, symbol="USDC", decimals=6
    )
    asset_id = await repository.ensure_asset(asset)
    await repository.upsert_events(
        asset_id,
        [
            make_entry(asset, "0xabc", 0, 1_000_000),
            make_entry(asset, "0xdef", 0, -400_000),
        ],
    )
    assert await repository.sum_balance(WALLET, asset_id) == 600_000


async def test_ensure_asset_is_idempotent_for_erc20_with_null_position_id(
    pool: asyncpg.Pool,
) -> None:
    repository = LedgerRepository(pool)
    asset = Asset(
        kind="erc20", contract_address="0xusdc5", position_id=None, symbol="USDC", decimals=6
    )
    first_asset_id = await repository.ensure_asset(asset)
    second_asset_id = await repository.ensure_asset(asset)
    assert first_asset_id == second_asset_id


async def test_checkpoint_roundtrip(pool: asyncpg.Pool) -> None:
    repository = LedgerRepository(pool)
    await repository.ensure_wallet(WALLET)
    asset = Asset(
        kind="erc20", contract_address="0xusdc3", position_id=None, symbol="USDC", decimals=6
    )
    asset_id = await repository.ensure_asset(asset)
    assert await repository.get_checkpoint(WALLET, asset_id) is None
    await repository.set_checkpoint(WALLET, asset_id, 12345)
    assert await repository.get_checkpoint(WALLET, asset_id) == 12345


async def test_save_balance_check_persists_result(pool: asyncpg.Pool) -> None:
    repository = LedgerRepository(pool)
    await repository.ensure_wallet(WALLET)
    asset = Asset(
        kind="erc20", contract_address="0xusdc4", position_id=None, symbol="USDC", decimals=6
    )
    asset_id = await repository.ensure_asset(asset)
    result = BalanceCheckResult(
        wallet_address=WALLET,
        asset=asset,
        computed_balance=500,
        onchain_balance=500,
        checked_at_block=99,
        matched=True,
    )
    await repository.save_balance_check(result, asset_id)

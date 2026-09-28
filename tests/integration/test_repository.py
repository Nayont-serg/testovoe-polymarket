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


async def test_list_erc1155_assets_for_wallet_excludes_checkpoint_sentinel(
    pool: asyncpg.Pool,
) -> None:
    repository = LedgerRepository(pool)
    await repository.ensure_wallet(WALLET)
    position_1 = Asset(
        kind="erc1155", contract_address="0xctf", position_id=1, symbol=None, decimals=0
    )
    position_2 = Asset(
        kind="erc1155", contract_address="0xctf", position_id=2, symbol=None, decimals=0
    )
    checkpoint_sentinel = Asset(
        kind="erc1155", contract_address="0xctf", position_id=None, symbol=None, decimals=None
    )
    position_1_id = await repository.ensure_asset(position_1)
    position_2_id = await repository.ensure_asset(position_2)
    checkpoint_id = await repository.ensure_asset(checkpoint_sentinel)
    await repository.upsert_events(position_1_id, [make_entry(position_1, "0xpos1", 0, 100)])
    await repository.upsert_events(position_2_id, [make_entry(position_2, "0xpos2", 0, 200)])
    await repository.upsert_events(
        checkpoint_id, [make_entry(checkpoint_sentinel, "0xcheckpoint", 0, 0)]
    )
    await repository.set_checkpoint(WALLET, checkpoint_id, 12345)

    assets = await repository.list_erc1155_assets_for_wallet(WALLET)

    assert {asset.position_id for asset in assets} == {1, 2}
    assert all(asset.position_id is not None for asset in assets)


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

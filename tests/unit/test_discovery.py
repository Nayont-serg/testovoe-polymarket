import asyncio
from typing import Any

from app.ledger.discovery import (
    AdaptiveConcurrencyLimiter,
    block_chunks,
    discover_erc20_transfers,
)
from app.ledger.models import Asset
from app.rpc.codec import TRANSFER_TOPIC, address_topic


def test_block_chunks_splits_full_range_into_fixed_windows() -> None:
    chunks = block_chunks(0, 999, 300)
    assert chunks == [(0, 299), (300, 599), (600, 899), (900, 999)]


def test_block_chunks_single_chunk_when_range_smaller_than_window() -> None:
    assert block_chunks(10, 20, 300) == [(10, 20)]


def test_current_limit_grows_over_time_up_to_max() -> None:
    fake_time = {"now": 0.0}
    limiter = AdaptiveConcurrencyLimiter(
        min_limit=2,
        start_limit=60,
        max_limit=100,
        grow_interval_seconds=180,
        clock=lambda: fake_time["now"],
    )
    assert limiter.current_limit() == 60
    fake_time["now"] = 180
    assert limiter.current_limit() == 61
    fake_time["now"] = 180 * 45
    assert limiter.current_limit() == 100


def test_degrade_forces_min_limit_until_expiry() -> None:
    fake_time = {"now": 0.0}
    limiter = AdaptiveConcurrencyLimiter(
        min_limit=2,
        start_limit=60,
        max_limit=100,
        grow_interval_seconds=180,
        clock=lambda: fake_time["now"],
    )
    limiter.degrade(30)
    assert limiter.current_limit() == 2
    fake_time["now"] = 31
    assert limiter.current_limit() == 60


async def test_acquire_blocks_at_limit_and_release_frees_slot() -> None:
    fake_time = {"now": 0.0}
    limiter = AdaptiveConcurrencyLimiter(
        min_limit=1,
        start_limit=1,
        max_limit=1,
        grow_interval_seconds=180,
        clock=lambda: fake_time["now"],
    )
    await limiter.acquire()
    acquired_second = False

    async def try_acquire() -> None:
        nonlocal acquired_second
        await limiter.acquire()
        acquired_second = True

    task = asyncio.ensure_future(try_acquire())
    await asyncio.sleep(0.05)
    assert acquired_second is False
    await limiter.release()
    await asyncio.wait_for(task, timeout=1)
    assert acquired_second is True


class FakeClient:
    def __init__(self, logs_by_direction: dict[int, list[dict[str, Any]]]) -> None:
        self._logs_by_direction = logs_by_direction

    async def call(
        self, _kind: str, _method: str, params: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        topics = params[0]["topics"]
        direction = 1 if topics[1] is not None else 2
        return self._logs_by_direction.get(direction, [])


async def test_discover_erc20_transfers_merges_in_and_out_and_decodes() -> None:
    wallet = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
    counterparty = "0x9999999999999999999999999999999999999999"
    outgoing_log = {
        "topics": [TRANSFER_TOPIC, address_topic(wallet), address_topic(counterparty)],
        "data": "0x" + format(500, "064x"),
        "blockNumber": hex(10),
        "transactionHash": "0xout",
        "logIndex": "0x0",
    }
    incoming_log = {
        "topics": [TRANSFER_TOPIC, address_topic(counterparty), address_topic(wallet)],
        "data": "0x" + format(700, "064x"),
        "blockNumber": hex(20),
        "transactionHash": "0xin",
        "logIndex": "0x1",
    }
    client = FakeClient({1: [outgoing_log], 2: [incoming_log]})
    limiter = AdaptiveConcurrencyLimiter(2, 60, 100, 180)
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    transfers = await discover_erc20_transfers(
        client, limiter, asset, wallet, start_block=0, end_block=100, window=1000
    )
    amounts = sorted(t.amount for t in transfers)
    assert amounts == [500, 700]
    assert {t.tx_hash for t in transfers} == {"0xout", "0xin"}

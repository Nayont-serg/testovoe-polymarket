from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from app.ledger.models import Asset, RawTransfer
from app.rpc.client import JsonRpcClient, LogQueryTooLargeError
from app.rpc.codec import (
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    TRANSFER_TOPIC,
    address_topic,
    decode_erc20_transfer,
    decode_transfer_batch,
    decode_transfer_single,
)

RpcLog = dict[str, Any]


class AdaptiveConcurrencyLimiter:
    def __init__(
        self,
        min_limit: int,
        start_limit: int,
        max_limit: int,
        grow_interval_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._min_limit = min_limit
        self._start_limit = start_limit
        self._max_limit = max_limit
        self._grow_interval_seconds = grow_interval_seconds
        self._clock = clock
        self._start_time = clock()
        self._in_flight = 0
        self._lock = asyncio.Lock()
        self._degraded_until: float | None = None

    def current_limit(self) -> int:
        now = self._clock()
        if self._degraded_until is not None and now < self._degraded_until:
            return self._min_limit
        elapsed = now - self._start_time
        steps = int(elapsed // self._grow_interval_seconds)
        return min(self._max_limit, self._start_limit + steps)

    def degrade(self, seconds: float) -> None:
        self._degraded_until = self._clock() + seconds

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                if self._in_flight < self.current_limit():
                    self._in_flight += 1
                    return
            await asyncio.sleep(0.01)

    async def release(self) -> None:
        async with self._lock:
            self._in_flight -= 1


def block_chunks(start_block: int, end_block: int, window: int) -> list[tuple[int, int]]:
    chunks: list[tuple[int, int]] = []
    current = start_block
    while current <= end_block:
        chunk_end = min(current + window - 1, end_block)
        chunks.append((current, chunk_end))
        current = chunk_end + 1
    return chunks


def _dedupe_logs(logs: list[RpcLog]) -> list[RpcLog]:
    seen: set[tuple[str, str]] = set()
    unique: list[RpcLog] = []
    for log in logs:
        key = (log["transactionHash"], log["logIndex"])
        if key not in seen:
            seen.add(key)
            unique.append(log)
    return unique


async def _fetch_wallet_logs(
    client: JsonRpcClient,
    limiter: AdaptiveConcurrencyLimiter,
    contract_address: str,
    topics: list[str | None],
    start_block: int,
    end_block: int,
    window: int,
) -> list[RpcLog]:
    async def fetch_range(chunk_start: int, chunk_end: int) -> list[RpcLog]:
        await limiter.acquire()
        try:
            return await client.call(
                "log",
                "eth_getLogs",
                [
                    {
                        "address": contract_address,
                        "topics": topics,
                        "fromBlock": hex(chunk_start),
                        "toBlock": hex(chunk_end),
                    }
                ],
            )
        finally:
            await limiter.release()

    async def fetch_chunk(chunk_start: int, chunk_end: int) -> list[RpcLog]:
        try:
            return await fetch_range(chunk_start, chunk_end)
        except LogQueryTooLargeError:
            if chunk_start == chunk_end:
                raise
            # Slot for the failed attempt is already released above: holding it across the
            # bisection would let concurrently-held ancestor slots starve out their own children.
            mid = (chunk_start + chunk_end) // 2
            left, right = await asyncio.gather(
                fetch_chunk(chunk_start, mid), fetch_chunk(mid + 1, chunk_end)
            )
            return left + right

    chunks = block_chunks(start_block, end_block, window)
    results = await asyncio.gather(
        *(fetch_chunk(chunk_start, chunk_end) for chunk_start, chunk_end in chunks)
    )
    return [log for chunk_logs in results for log in chunk_logs]


async def discover_erc20_transfers(
    client: JsonRpcClient,
    limiter: AdaptiveConcurrencyLimiter,
    asset: Asset,
    wallet_address: str,
    start_block: int,
    end_block: int,
    window: int,
) -> list[RawTransfer]:
    wallet_topic = address_topic(wallet_address)
    outgoing, incoming = await asyncio.gather(
        _fetch_wallet_logs(
            client,
            limiter,
            asset.contract_address,
            [TRANSFER_TOPIC, wallet_topic, None],
            start_block,
            end_block,
            window,
        ),
        _fetch_wallet_logs(
            client,
            limiter,
            asset.contract_address,
            [TRANSFER_TOPIC, None, wallet_topic],
            start_block,
            end_block,
            window,
        ),
    )
    transfers: list[RawTransfer] = []
    for log in _dedupe_logs(outgoing + incoming):
        from_address, to_address, amount = decode_erc20_transfer(log["topics"], log["data"])
        transfers.append(
            RawTransfer(
                contract_address=asset.contract_address,
                source_event="Transfer",
                from_address=from_address,
                to_address=to_address,
                position_id=None,
                amount=amount,
                block_number=int(log["blockNumber"], 16),
                tx_hash=log["transactionHash"],
                log_index=int(log["logIndex"], 16),
            )
        )
    return transfers


async def discover_ctf_transfers(
    client: JsonRpcClient,
    limiter: AdaptiveConcurrencyLimiter,
    ctf_address: str,
    wallet_address: str,
    start_block: int,
    end_block: int,
    window: int,
) -> list[RawTransfer]:
    wallet_topic = address_topic(wallet_address)
    single_out, single_in, batch_out, batch_in = await asyncio.gather(
        _fetch_wallet_logs(
            client,
            limiter,
            ctf_address,
            [TRANSFER_SINGLE_TOPIC, None, wallet_topic, None],
            start_block,
            end_block,
            window,
        ),
        _fetch_wallet_logs(
            client,
            limiter,
            ctf_address,
            [TRANSFER_SINGLE_TOPIC, None, None, wallet_topic],
            start_block,
            end_block,
            window,
        ),
        _fetch_wallet_logs(
            client,
            limiter,
            ctf_address,
            [TRANSFER_BATCH_TOPIC, None, wallet_topic, None],
            start_block,
            end_block,
            window,
        ),
        _fetch_wallet_logs(
            client,
            limiter,
            ctf_address,
            [TRANSFER_BATCH_TOPIC, None, None, wallet_topic],
            start_block,
            end_block,
            window,
        ),
    )
    transfers: list[RawTransfer] = []
    for log in _dedupe_logs(single_out + single_in):
        _, from_address, to_address, position_id, amount = decode_transfer_single(
            log["topics"], log["data"]
        )
        transfers.append(
            RawTransfer(
                contract_address=ctf_address,
                source_event="TransferSingle",
                from_address=from_address,
                to_address=to_address,
                position_id=position_id,
                amount=amount,
                block_number=int(log["blockNumber"], 16),
                tx_hash=log["transactionHash"],
                log_index=int(log["logIndex"], 16),
            )
        )
    for log in _dedupe_logs(batch_out + batch_in):
        _, from_address, to_address, pairs = decode_transfer_batch(log["topics"], log["data"])
        for position_id, amount in pairs:
            transfers.append(
                RawTransfer(
                    contract_address=ctf_address,
                    source_event="TransferBatch",
                    from_address=from_address,
                    to_address=to_address,
                    position_id=position_id,
                    amount=amount,
                    block_number=int(log["blockNumber"], 16),
                    tx_hash=log["transactionHash"],
                    log_index=int(log["logIndex"], 16),
                )
            )
    return transfers

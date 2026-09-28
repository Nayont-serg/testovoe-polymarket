from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from typing import Any

from app.ledger.models import Asset, RawTransfer
from app.rpc.client import JsonRpcClient, LogQueryTooLargeError, RpcAllEndpointsExhaustedError
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

_MAX_CHUNK_ATTEMPTS = 5
_CHUNK_RETRY_BACKOFF_SECONDS = 2.0
_LIMIT_RECHECK_SECONDS = 1.0


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
        self._condition = asyncio.Condition()
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

    @property
    def max_limit(self) -> int:
        return self._max_limit

    async def acquire(self) -> None:
        async with self._condition:
            while self._in_flight >= self.current_limit():
                # Timed wait: the limit also grows with time and recovers after degrade().
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._condition.wait(), _LIMIT_RECHECK_SECONDS)
            self._in_flight += 1

    async def release(self) -> None:
        async with self._condition:
            self._in_flight -= 1
            self._condition.notify()


def block_chunks(start_block: int, end_block: int, window: int) -> list[tuple[int, int]]:
    chunks: list[tuple[int, int]] = []
    current = start_block
    while current <= end_block:
        chunk_end = min(current + window - 1, end_block)
        chunks.append((current, chunk_end))
        current = chunk_end + 1
    return chunks


def _is_new(seen: set[tuple[str, str]], log: RpcLog) -> bool:
    key = (log["transactionHash"], log["logIndex"])
    if key in seen:
        return False
    seen.add(key)
    return True


async def _fetch_chunk(
    client: JsonRpcClient,
    contract_address: str,
    topics: list[str | None],
    chunk_start: int,
    chunk_end: int,
) -> list[RpcLog]:
    last_error: RpcAllEndpointsExhaustedError | None = None
    for attempt in range(_MAX_CHUNK_ATTEMPTS):
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
        except RpcAllEndpointsExhaustedError as exc:
            last_error = exc
            if attempt < _MAX_CHUNK_ATTEMPTS - 1:
                await asyncio.sleep(_CHUNK_RETRY_BACKOFF_SECONDS * (attempt + 1))
    assert last_error is not None
    raise last_error


async def _fetch_wallet_logs(
    client: JsonRpcClient,
    limiter: AdaptiveConcurrencyLimiter,
    contract_address: str,
    topics: list[str | None],
    start_block: int,
    end_block: int,
    window: int,
    sink: Callable[[list[RpcLog]], Awaitable[None]],
) -> None:
    # Fixed worker pool over a queue keeps task count bounded during deep bisection.
    queue: asyncio.Queue[tuple[int, int]] = asyncio.Queue()
    for chunk_start, chunk_end in block_chunks(start_block, end_block, window):
        queue.put_nowait((chunk_start, chunk_end))

    async def worker() -> None:
        while True:
            try:
                chunk_start, chunk_end = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            await limiter.acquire()
            try:
                try:
                    logs = await _fetch_chunk(
                        client, contract_address, topics, chunk_start, chunk_end
                    )
                except LogQueryTooLargeError:
                    if chunk_start == chunk_end:
                        raise
                    mid = (chunk_start + chunk_end) // 2
                    queue.put_nowait((chunk_start, mid))
                    queue.put_nowait((mid + 1, chunk_end))
                else:
                    if logs:
                        await sink(logs)
            finally:
                await limiter.release()

    workers = [asyncio.create_task(worker()) for _ in range(limiter.max_limit)]
    errors = await asyncio.gather(*workers, return_exceptions=True)
    for error in errors:
        if isinstance(error, BaseException):
            raise error


async def discover_erc20_transfers(
    client: JsonRpcClient,
    limiter: AdaptiveConcurrencyLimiter,
    asset: Asset,
    wallet_address: str,
    start_block: int,
    end_block: int,
    window: int,
    sink: Callable[[list[RawTransfer]], Awaitable[None]],
) -> None:
    wallet_topic = address_topic(wallet_address)
    seen: set[tuple[str, str]] = set()

    async def handle_logs(logs: list[RpcLog]) -> None:
        fresh: list[RawTransfer] = []
        for log in logs:
            if not _is_new(seen, log):
                continue
            from_address, to_address, amount = decode_erc20_transfer(log["topics"], log["data"])
            fresh.append(
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
        if fresh:
            await sink(fresh)

    results = await asyncio.gather(
        _fetch_wallet_logs(
            client,
            limiter,
            asset.contract_address,
            [TRANSFER_TOPIC, wallet_topic, None],
            start_block,
            end_block,
            window,
            handle_logs,
        ),
        _fetch_wallet_logs(
            client,
            limiter,
            asset.contract_address,
            [TRANSFER_TOPIC, None, wallet_topic],
            start_block,
            end_block,
            window,
            handle_logs,
        ),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            raise result


async def discover_ctf_transfers(
    client: JsonRpcClient,
    limiter: AdaptiveConcurrencyLimiter,
    ctf_address: str,
    wallet_address: str,
    start_block: int,
    end_block: int,
    window: int,
    sink: Callable[[list[RawTransfer]], Awaitable[None]],
) -> None:
    wallet_topic = address_topic(wallet_address)
    seen: set[tuple[str, str]] = set()

    async def handle_single(logs: list[RpcLog]) -> None:
        fresh: list[RawTransfer] = []
        for log in logs:
            if not _is_new(seen, log):
                continue
            _, from_address, to_address, position_id, amount = decode_transfer_single(
                log["topics"], log["data"]
            )
            fresh.append(
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
        if fresh:
            await sink(fresh)

    async def handle_batch(logs: list[RpcLog]) -> None:
        fresh: list[RawTransfer] = []
        for log in logs:
            if not _is_new(seen, log):
                continue
            _, from_address, to_address, pairs = decode_transfer_batch(log["topics"], log["data"])
            coalesced: dict[int, int] = {}
            for position_id, amount in pairs:
                coalesced[position_id] = coalesced.get(position_id, 0) + amount
            for position_id, amount in coalesced.items():
                fresh.append(
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
        if fresh:
            await sink(fresh)

    results = await asyncio.gather(
        _fetch_wallet_logs(
            client,
            limiter,
            ctf_address,
            [TRANSFER_SINGLE_TOPIC, None, wallet_topic, None],
            start_block,
            end_block,
            window,
            handle_single,
        ),
        _fetch_wallet_logs(
            client,
            limiter,
            ctf_address,
            [TRANSFER_SINGLE_TOPIC, None, None, wallet_topic],
            start_block,
            end_block,
            window,
            handle_single,
        ),
        _fetch_wallet_logs(
            client,
            limiter,
            ctf_address,
            [TRANSFER_BATCH_TOPIC, None, wallet_topic, None],
            start_block,
            end_block,
            window,
            handle_batch,
        ),
        _fetch_wallet_logs(
            client,
            limiter,
            ctf_address,
            [TRANSFER_BATCH_TOPIC, None, None, wallet_topic],
            start_block,
            end_block,
            window,
            handle_batch,
        ),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            raise result

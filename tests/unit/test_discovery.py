import asyncio
from typing import Any

import pytest

import app.ledger.discovery as discovery
from app.ledger.discovery import (
    AdaptiveConcurrencyLimiter,
    _fetch_chunk,
    _fetch_wallet_logs,
    block_chunks,
    discover_ctf_transfers,
    discover_erc20_transfers,
)
from app.ledger.models import Asset, RawTransfer
from app.rpc.client import LogQueryTooLargeError, RpcAllEndpointsExhaustedError
from app.rpc.codec import (
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    TRANSFER_TOPIC,
    address_topic,
)


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


class BisectingFakeClient:
    def __init__(self, threshold: int, found_log: dict[str, Any], found_block: int) -> None:
        self._threshold = threshold
        self._found_log = found_log
        self._found_block = found_block

    async def call(
        self, _kind: str, _method: str, params: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        from_block = int(params[0]["fromBlock"], 16)
        to_block = int(params[0]["toBlock"], 16)
        if to_block - from_block > self._threshold:
            raise LogQueryTooLargeError(
                "Query returned more than 20000 results. Try with this block range [0x0, 0x1]"
            )
        if from_block <= self._found_block <= to_block:
            return [self._found_log]
        return []


async def test_fetch_wallet_logs_bisects_wide_chunk_that_exceeds_provider_cap() -> None:
    found_log = {"blockNumber": hex(750), "transactionHash": "0xfound", "logIndex": "0x0"}
    client = BisectingFakeClient(threshold=10, found_log=found_log, found_block=750)
    limiter = AdaptiveConcurrencyLimiter(2, 60, 100, 180)
    collected: list[dict[str, Any]] = []

    async def sink(logs: list[dict[str, Any]]) -> None:
        collected.extend(logs)

    await _fetch_wallet_logs(
        client,
        limiter,
        "0xusdc",
        [TRANSFER_TOPIC, None, None],
        start_block=0,
        end_block=999,
        window=1000,
        sink=sink,
    )
    assert collected == [found_log]


class TaskCountingFakeClient:
    def __init__(self) -> None:
        self.max_tasks_observed = 0

    async def call(
        self, _kind: str, _method: str, params: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        self.max_tasks_observed = max(self.max_tasks_observed, len(asyncio.all_tasks()))
        from_block = int(params[0]["fromBlock"], 16)
        to_block = int(params[0]["toBlock"], 16)
        if from_block == to_block:
            return []
        raise LogQueryTooLargeError(
            "Query returned more than 20000 results. Try with this block range [0x0, 0x1]"
        )


async def test_fetch_wallet_logs_bounds_live_task_count_during_deep_bisection() -> None:
    client = TaskCountingFakeClient()
    worker_count = 4
    limiter = AdaptiveConcurrencyLimiter(worker_count, worker_count, worker_count, 180)
    collected: list[dict[str, Any]] = []

    async def sink(logs: list[dict[str, Any]]) -> None:
        collected.extend(logs)

    await _fetch_wallet_logs(
        client,
        limiter,
        "0xusdc",
        [TRANSFER_TOPIC, None, None],
        start_block=0,
        end_block=999,
        window=1000,
        sink=sink,
    )
    assert collected == []
    assert client.max_tasks_observed <= worker_count + 2


class TransientThenSuccessFakeClient:
    def __init__(self, failures_before_success: int, log: dict[str, Any]) -> None:
        self._failures_before_success = failures_before_success
        self._log = log
        self.call_count = 0

    async def call(
        self, _kind: str, _method: str, _params: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        self.call_count += 1
        if self.call_count <= self._failures_before_success:
            raise RpcAllEndpointsExhaustedError("log", "eth_getLogs")
        return [self._log]


async def test_fetch_chunk_retries_transient_failure_then_returns_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(discovery, "_CHUNK_RETRY_BACKOFF_SECONDS", 0.0)
    found_log = {"blockNumber": hex(5), "transactionHash": "0xok", "logIndex": "0x0"}
    client = TransientThenSuccessFakeClient(
        failures_before_success=discovery._MAX_CHUNK_ATTEMPTS - 1, log=found_log
    )
    logs = await _fetch_chunk(client, "0xusdc", [TRANSFER_TOPIC, None, None], 0, 10)
    assert logs == [found_log]
    assert client.call_count == discovery._MAX_CHUNK_ATTEMPTS


async def test_fetch_chunk_raises_after_exhausting_all_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(discovery, "_CHUNK_RETRY_BACKOFF_SECONDS", 0.0)
    client = TransientThenSuccessFakeClient(failures_before_success=999, log={})
    with pytest.raises(RpcAllEndpointsExhaustedError):
        await _fetch_chunk(client, "0xusdc", [TRANSFER_TOPIC, None, None], 0, 10)
    assert client.call_count == discovery._MAX_CHUNK_ATTEMPTS


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
    transfers: list[RawTransfer] = []

    async def sink(batch: list[RawTransfer]) -> None:
        transfers.extend(batch)

    await discover_erc20_transfers(
        client, limiter, asset, wallet, start_block=0, end_block=100, window=1000, sink=sink
    )
    amounts = sorted(t.amount for t in transfers)
    assert amounts == [500, 700]
    assert {t.tx_hash for t in transfers} == {"0xout", "0xin"}


def _encode_transfer_batch_data(position_ids: list[int], amounts: list[int]) -> str:
    ids_offset = 64
    values_offset = ids_offset + 32 * (1 + len(position_ids))
    words = [
        format(ids_offset, "064x"),
        format(values_offset, "064x"),
        format(len(position_ids), "064x"),
        *(format(position_id, "064x") for position_id in position_ids),
        format(len(amounts), "064x"),
        *(format(amount, "064x") for amount in amounts),
    ]
    return "0x" + "".join(words)


class FakeCtfClient:
    def __init__(
        self,
        single_out_log: dict[str, Any],
        single_in_log: dict[str, Any],
        batch_out_log: dict[str, Any],
        batch_in_log: dict[str, Any],
    ) -> None:
        self._single_out_log = single_out_log
        self._single_in_log = single_in_log
        self._batch_out_log = batch_out_log
        self._batch_in_log = batch_in_log

    async def call(
        self, _kind: str, _method: str, params: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        topics = params[0]["topics"]
        is_out_query = topics[2] is not None
        if topics[0] == TRANSFER_SINGLE_TOPIC:
            return [self._single_out_log] if is_out_query else [self._single_in_log]
        if topics[0] == TRANSFER_BATCH_TOPIC:
            return [self._batch_out_log] if is_out_query else [self._batch_in_log]
        raise AssertionError(f"unexpected topic0 {topics[0]}")


async def test_discover_ctf_transfers_merges_out_and_in_and_explodes_pairs() -> None:
    wallet = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
    counterparty = "0x9999999999999999999999999999999999999999"
    operator = "0x8888888888888888888888888888888888888888"
    single_out_log = {
        "topics": [
            TRANSFER_SINGLE_TOPIC,
            address_topic(operator),
            address_topic(wallet),
            address_topic(counterparty),
        ],
        "data": "0x" + format(9, "064x") + format(15, "064x"),
        "blockNumber": hex(9),
        "transactionHash": "0xsingle-out",
        "logIndex": "0x0",
    }
    single_in_log = {
        "topics": [
            TRANSFER_SINGLE_TOPIC,
            address_topic(operator),
            address_topic(counterparty),
            address_topic(wallet),
        ],
        "data": "0x" + format(42, "064x") + format(7, "064x"),
        "blockNumber": hex(11),
        "transactionHash": "0xsingle-in",
        "logIndex": "0x0",
    }
    batch_out_log = {
        "topics": [
            TRANSFER_BATCH_TOPIC,
            address_topic(operator),
            address_topic(wallet),
            address_topic(counterparty),
        ],
        "data": _encode_transfer_batch_data([1, 2], [100, 200]),
        "blockNumber": hex(22),
        "transactionHash": "0xbatch-out",
        "logIndex": "0x1",
    }
    batch_in_log = {
        "topics": [
            TRANSFER_BATCH_TOPIC,
            address_topic(operator),
            address_topic(counterparty),
            address_topic(wallet),
        ],
        "data": _encode_transfer_batch_data([3], [300]),
        "blockNumber": hex(33),
        "transactionHash": "0xbatch-in",
        "logIndex": "0x2",
    }
    client = FakeCtfClient(single_out_log, single_in_log, batch_out_log, batch_in_log)
    limiter = AdaptiveConcurrencyLimiter(2, 60, 100, 180)
    transfers: list[RawTransfer] = []

    async def sink(batch: list[RawTransfer]) -> None:
        transfers.extend(batch)

    await discover_ctf_transfers(
        client, limiter, "0xctf", wallet, start_block=0, end_block=100, window=1000, sink=sink
    )

    single_transfers = sorted(
        (t for t in transfers if t.source_event == "TransferSingle"),
        key=lambda t: t.tx_hash,
    )
    assert [t.tx_hash for t in single_transfers] == ["0xsingle-in", "0xsingle-out"]
    single_in = next(t for t in single_transfers if t.tx_hash == "0xsingle-in")
    assert (single_in.position_id, single_in.amount) == (42, 7)
    assert single_in.from_address == counterparty.lower()
    assert single_in.to_address == wallet.lower()
    single_out = next(t for t in single_transfers if t.tx_hash == "0xsingle-out")
    assert (single_out.position_id, single_out.amount) == (9, 15)
    assert single_out.from_address == wallet.lower()
    assert single_out.to_address == counterparty.lower()

    batch_transfers = sorted(
        (t for t in transfers if t.source_event == "TransferBatch"),
        key=lambda t: (t.tx_hash, t.position_id),
    )
    assert [(t.tx_hash, t.position_id, t.amount) for t in batch_transfers] == [
        ("0xbatch-in", 3, 300),
        ("0xbatch-out", 1, 100),
        ("0xbatch-out", 2, 200),
    ]
    batch_in = next(t for t in batch_transfers if t.tx_hash == "0xbatch-in")
    assert batch_in.from_address == counterparty.lower()
    assert batch_in.to_address == wallet.lower()
    for batch_out in (t for t in batch_transfers if t.tx_hash == "0xbatch-out"):
        assert batch_out.from_address == wallet.lower()
        assert batch_out.to_address == counterparty.lower()
        assert batch_out.block_number == 22
        assert batch_out.log_index == 1


async def test_discover_ctf_transfers_coalesces_duplicate_position_id_within_one_batch_log() -> (
    None
):
    wallet = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
    counterparty = "0x9999999999999999999999999999999999999999"
    operator = "0x8888888888888888888888888888888888888888"
    batch_in_log = {
        "topics": [
            TRANSFER_BATCH_TOPIC,
            address_topic(operator),
            address_topic(counterparty),
            address_topic(wallet),
        ],
        "data": _encode_transfer_batch_data([5, 5], [100, 200]),
        "blockNumber": hex(33),
        "transactionHash": "0xbatch-dup",
        "logIndex": "0x2",
    }

    class DuplicatePairBatchClient:
        async def call(
            self, _kind: str, _method: str, params: list[dict[str, Any]]
        ) -> list[dict[str, Any]]:
            topics = params[0]["topics"]
            if topics[0] == TRANSFER_BATCH_TOPIC and topics[3] is not None:
                return [batch_in_log]
            return []

    client = DuplicatePairBatchClient()
    limiter = AdaptiveConcurrencyLimiter(2, 60, 100, 180)
    transfers: list[RawTransfer] = []

    async def sink(batch: list[RawTransfer]) -> None:
        transfers.extend(batch)

    await discover_ctf_transfers(
        client, limiter, "0xctf", wallet, start_block=0, end_block=100, window=1000, sink=sink
    )

    batch_transfers = [t for t in transfers if t.source_event == "TransferBatch"]
    assert len(batch_transfers) == 1
    assert batch_transfers[0].position_id == 5
    assert batch_transfers[0].amount == 300


class ManyChunksFakeClient:
    def __init__(self, logs_per_chunk: int, counterparty: str) -> None:
        self._logs_per_chunk = logs_per_chunk
        self._counterparty_topic = address_topic(counterparty)

    async def call(
        self, _kind: str, _method: str, params: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        topics = params[0]["topics"]
        if topics[2] is None:
            return []
        from_block = int(params[0]["fromBlock"], 16)
        return [
            {
                "topics": [TRANSFER_TOPIC, self._counterparty_topic, topics[2]],
                "data": "0x" + format(1, "064x"),
                "blockNumber": hex(from_block),
                "transactionHash": f"0x{from_block}-{i}",
                "logIndex": hex(i),
            }
            for i in range(self._logs_per_chunk)
        ]


async def test_discover_erc20_transfers_streams_batches_instead_of_accumulating_everything() -> (
    None
):
    wallet = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
    counterparty = "0x9999999999999999999999999999999999999999"
    logs_per_chunk = 10
    chunk_count = 200
    client = ManyChunksFakeClient(logs_per_chunk, counterparty)
    limiter = AdaptiveConcurrencyLimiter(8, 8, 8, 180)
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    batch_sizes: list[int] = []
    total_transfers = 0

    async def sink(batch: list[RawTransfer]) -> None:
        nonlocal total_transfers
        batch_sizes.append(len(batch))
        total_transfers += len(batch)

    await discover_erc20_transfers(
        client,
        limiter,
        asset,
        wallet,
        start_block=0,
        end_block=chunk_count - 1,
        window=1,
        sink=sink,
    )

    expected_total = logs_per_chunk * chunk_count
    assert total_transfers == expected_total
    assert len(batch_sizes) > 1
    assert max(batch_sizes) <= logs_per_chunk
    assert max(batch_sizes) < expected_total

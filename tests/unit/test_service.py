from typing import Any

import pytest

from app.ledger.models import Asset, BalanceCheckResult, LedgerEntry
from app.ledger.service import CONFIRMATION_LAG_BLOCKS, LedgerReport, LedgerService
from app.rpc.codec import TRANSFER_SINGLE_TOPIC, address_topic


def test_ledger_report_all_matched_true_when_every_check_matches() -> None:
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    report = LedgerReport(
        balance_checks=[
            BalanceCheckResult(
                wallet_address="0xwallet",
                asset=asset,
                computed_balance=1,
                onchain_balance=1,
                checked_at_block=10,
                matched=True,
            )
        ]
    )
    assert report.all_matched is True


def test_ledger_report_all_matched_false_when_one_check_fails() -> None:
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    report = LedgerReport(
        balance_checks=[
            BalanceCheckResult(
                wallet_address="0xwallet",
                asset=asset,
                computed_balance=1,
                onchain_balance=1,
                checked_at_block=10,
                matched=True,
            ),
            BalanceCheckResult(
                wallet_address="0xwallet",
                asset=asset,
                computed_balance=2,
                onchain_balance=3,
                checked_at_block=10,
                matched=False,
            ),
        ]
    )
    assert report.all_matched is False


class FakeSettings:
    free_log_rpc_window_blocks = 1000
    goldsky_raw_log_rpc_concurrency_min = 2
    goldsky_raw_log_rpc_concurrency_start = 60
    goldsky_raw_log_rpc_concurrency_max = 100
    goldsky_raw_log_rpc_concurrency_grow_interval_sec = 180
    w3_free_receipt_rpc_batch_size = 500
    w3_block_timestamps_rpc_batch_size = 2000


class FakeClient:
    def __init__(self, wallet: str, counterparty: str) -> None:
        from app.rpc.codec import TRANSFER_TOPIC, address_topic

        self._wallet = wallet
        self._log = {
            "topics": [TRANSFER_TOPIC, address_topic(counterparty), address_topic(wallet)],
            "data": "0x" + format(1_000, "064x"),
            "blockNumber": hex(10),
            "transactionHash": "0xabc",
            "logIndex": "0x0",
        }

    async def call(self, _kind: str, method: str, params: list[Any]) -> Any:
        from app.rpc.codec import TRANSFER_TOPIC

        if method == "eth_blockNumber":
            return hex(10 + CONFIRMATION_LAG_BLOCKS)
        if method == "eth_getLogs":
            topics = params[0]["topics"]
            if topics[0] != TRANSFER_TOPIC:
                return []  # no CTF (TransferSingle/TransferBatch) activity in this test
            return [self._log] if topics[2] is not None else []
        if method == "eth_call":
            return "0x" + format(1_000, "064x")
        raise AssertionError(method)

    async def batch_call(self, kind: str, requests: list[Any]) -> list[Any]:
        if kind == "receipt":
            return [{"logs": []} for _ in requests]
        return [{"timestamp": hex(1_700_000_000)} for _ in requests]


class FakeRepository:
    def __init__(self) -> None:
        self.events: list[tuple[int, LedgerEntry]] = []
        self.checkpoints: dict[tuple[str, int], int] = {}
        self.checks: list[BalanceCheckResult] = []
        self._next_asset_id = 1
        self._asset_ids: dict[tuple[str, str, int | None], int] = {}

    async def ensure_wallet(self, _wallet_address: str) -> None:
        return None

    async def ensure_asset(self, asset: Asset) -> int:
        key = (asset.kind, asset.contract_address, asset.position_id)
        if key not in self._asset_ids:
            self._asset_ids[key] = self._next_asset_id
            self._next_asset_id += 1
        return self._asset_ids[key]

    async def get_checkpoint(self, wallet_address: str, asset_id: int) -> int | None:
        return self.checkpoints.get((wallet_address, asset_id))

    async def set_checkpoint(self, wallet_address: str, asset_id: int, block_number: int) -> None:
        self.checkpoints[(wallet_address, asset_id)] = block_number

    async def upsert_events(self, asset_id: int, entries: list[LedgerEntry]) -> None:
        self.events.extend((asset_id, entry) for entry in entries)

    async def sum_balance(self, wallet_address: str, asset_id: int) -> int:
        return sum(
            entry.delta
            for stored_asset_id, entry in self.events
            if entry.wallet_address == wallet_address and stored_asset_id == asset_id
        )

    async def save_balance_check(self, result: BalanceCheckResult, _asset_id: int) -> None:
        self.checks.append(result)

    async def list_erc1155_assets_for_wallet(self, wallet_address: str) -> list[Asset]:
        seen: dict[int | None, Asset] = {}
        for _asset_id, entry in self.events:
            asset = entry.asset
            if (
                entry.wallet_address == wallet_address
                and asset.kind == "erc1155"
                and asset.position_id is not None
            ):
                seen[asset.position_id] = asset
        return list(seen.values())


async def test_ledger_service_run_produces_matched_report_for_single_asset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.rpc import contracts

    wallet = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
    counterparty = "0x9999999999999999999999999999999999999999"
    monkeypatch.setattr(
        contracts,
        "COLLATERAL_ASSETS",
        (
            Asset(
                kind="erc20",
                contract_address="0xusdc",
                position_id=None,
                symbol="USDC",
                decimals=6,
            ),
        ),
    )
    monkeypatch.setattr(contracts, "COLLATERAL_DEPLOY_BLOCKS", {"0xusdc": 0})

    client = FakeClient(wallet, counterparty)
    repository = FakeRepository()
    service = LedgerService(client, repository, FakeSettings())
    report = await service.run(wallet)
    assert report.all_matched is True
    assert len(report.balance_checks) == 1


class FakeCtfClient:
    def __init__(self, wallet: str, single_incoming_logs: list[dict[str, Any]]) -> None:
        self._wallet = wallet
        self._single_incoming_logs = single_incoming_logs
        self.batch_calls: list[tuple[str, list[Any]]] = []

    async def call(self, _kind: str, method: str, params: list[Any]) -> Any:
        if method == "eth_blockNumber":
            return hex(10 + CONFIRMATION_LAG_BLOCKS)
        if method == "eth_getLogs":
            topics = params[0]["topics"]
            if topics[0] == TRANSFER_SINGLE_TOPIC and topics[3] is not None:
                return self._single_incoming_logs
            return []
        if method == "eth_call":
            return "0x" + format(0, "064x")
        raise AssertionError(method)

    async def batch_call(self, kind: str, requests: list[Any]) -> list[Any]:
        self.batch_calls.append((kind, requests))
        if kind == "receipt":
            return [{"logs": []} for _ in requests]
        return [{"timestamp": hex(1_700_000_000)} for _ in requests]


async def test_ledger_service_process_ctf_batches_receipts_and_timestamps_across_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.rpc import contracts

    wallet = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
    counterparty = "0x9999999999999999999999999999999999999999"
    monkeypatch.setattr(contracts, "COLLATERAL_ASSETS", ())
    monkeypatch.setattr(contracts, "CTF_DEPLOY_BLOCK", 0)

    single_in_position_1 = {
        "topics": [
            TRANSFER_SINGLE_TOPIC,
            address_topic(counterparty),
            address_topic(counterparty),
            address_topic(wallet),
        ],
        "data": "0x" + format(1, "064x") + format(500, "064x"),
        "blockNumber": hex(9),
        "transactionHash": "0xctf1",
        "logIndex": "0x0",
    }
    single_in_position_2 = {
        "topics": [
            TRANSFER_SINGLE_TOPIC,
            address_topic(counterparty),
            address_topic(counterparty),
            address_topic(wallet),
        ],
        "data": "0x" + format(2, "064x") + format(300, "064x"),
        "blockNumber": hex(11),
        "transactionHash": "0xctf2",
        "logIndex": "0x0",
    }

    client = FakeCtfClient(wallet, [single_in_position_1, single_in_position_2])
    repository = FakeRepository()
    service = LedgerService(client, repository, FakeSettings())
    report = await service.run(wallet)

    assert len(report.balance_checks) == 2
    assert len(repository.events) == 2

    receipt_calls = [requests for kind, requests in client.batch_calls if kind == "receipt"]
    timestamp_calls = [requests for kind, requests in client.batch_calls if kind != "receipt"]
    assert len(receipt_calls) == 1
    assert len(receipt_calls[0]) == 2
    assert len(timestamp_calls) == 1
    assert len(timestamp_calls[0]) == 2


class FakeCtfResumeClient:
    def __init__(self, single_incoming_log: dict[str, Any]) -> None:
        self._single_incoming_log = single_incoming_log
        self.latest_block = 10 + CONFIRMATION_LAG_BLOCKS
        self.discover_new_logs = True

    async def call(self, _kind: str, method: str, params: list[Any]) -> Any:
        if method == "eth_blockNumber":
            return hex(self.latest_block)
        if method == "eth_getLogs":
            topics = params[0]["topics"]
            if (
                self.discover_new_logs
                and topics[0] == TRANSFER_SINGLE_TOPIC
                and topics[3] is not None
            ):
                return [self._single_incoming_log]
            return []
        if method == "eth_call":
            return "0x" + format(0, "064x")
        raise AssertionError(method)

    async def batch_call(self, kind: str, requests: list[Any]) -> list[Any]:
        if kind == "receipt":
            return [{"logs": []} for _ in requests]
        return [{"timestamp": hex(1_700_000_000)} for _ in requests]


async def test_ledger_service_second_run_still_reconciles_ctf_positions_from_first_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.rpc import contracts

    wallet = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
    counterparty = "0x9999999999999999999999999999999999999999"
    monkeypatch.setattr(contracts, "COLLATERAL_ASSETS", ())
    monkeypatch.setattr(contracts, "CTF_DEPLOY_BLOCK", 0)

    single_incoming_log = {
        "topics": [
            TRANSFER_SINGLE_TOPIC,
            address_topic(counterparty),
            address_topic(counterparty),
            address_topic(wallet),
        ],
        "data": "0x" + format(7, "064x") + format(50, "064x"),
        "blockNumber": hex(9),
        "transactionHash": "0xctf-resume",
        "logIndex": "0x0",
    }
    client = FakeCtfResumeClient(single_incoming_log)
    repository = FakeRepository()
    service = LedgerService(client, repository, FakeSettings())

    first_report = await service.run(wallet)
    assert len(first_report.balance_checks) == 1

    client.latest_block = 20 + CONFIRMATION_LAG_BLOCKS
    client.discover_new_logs = False
    second_report = await service.run(wallet)

    assert len(second_report.balance_checks) == 1


async def test_ledger_service_run_scans_and_reconciles_at_confirmation_lagged_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.rpc import contracts

    wallet = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
    counterparty = "0x9999999999999999999999999999999999999999"
    monkeypatch.setattr(
        contracts,
        "COLLATERAL_ASSETS",
        (
            Asset(
                kind="erc20",
                contract_address="0xusdc",
                position_id=None,
                symbol="USDC",
                decimals=6,
            ),
        ),
    )
    monkeypatch.setattr(contracts, "COLLATERAL_DEPLOY_BLOCKS", {"0xusdc": 0})

    raw_latest_block = 10 + CONFIRMATION_LAG_BLOCKS
    client = FakeClient(wallet, counterparty)
    repository = FakeRepository()
    service = LedgerService(client, repository, FakeSettings())
    report = await service.run(wallet)

    expected_block = raw_latest_block - CONFIRMATION_LAG_BLOCKS
    assert report.balance_checks[0].checked_at_block == expected_block
    assert set(repository.checkpoints.values()) == {expected_block}

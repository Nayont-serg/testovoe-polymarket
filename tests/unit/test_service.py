from typing import Any

import pytest

from app.ledger.models import Asset, BalanceCheckResult, LedgerEntry
from app.ledger.service import LedgerReport, LedgerService


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
            return hex(10)
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
        self.events: list[LedgerEntry] = []
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

    async def upsert_events(self, _asset_id: int, entries: list[LedgerEntry]) -> None:
        self.events.extend(entries)

    async def sum_balance(self, wallet_address: str, _asset_id: int) -> int:
        return sum(e.delta for e in self.events if e.wallet_address == wallet_address)

    async def save_balance_check(self, result: BalanceCheckResult, _asset_id: int) -> None:
        self.checks.append(result)


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

from datetime import UTC, datetime
from typing import Any

from app.ledger.enrichment import build_ledger_entry, classify_event_type
from app.ledger.models import Asset, RawTransfer
from app.rpc.client import RpcKind
from app.rpc.codec import POSITION_SPLIT_TOPIC

WALLET = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
COUNTERPARTY = "0x9999999999999999999999999999999999999999"


def test_classify_event_type_detects_split_from_receipt_topics() -> None:
    assert classify_event_type(COUNTERPARTY, {POSITION_SPLIT_TOPIC}) == "SPLIT"


def test_classify_event_type_detects_trade_from_label_contract() -> None:
    exchange = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
    assert classify_event_type(exchange, set()) == "TRADE"


def test_classify_event_type_defaults_to_deposit_for_unknown_counterparty() -> None:
    assert classify_event_type(COUNTERPARTY, set()) == "DEPOSIT"


def test_build_ledger_entry_negates_delta_for_outgoing_transfer() -> None:
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    transfer = RawTransfer(
        contract_address="0xusdc",
        source_event="Transfer",
        from_address=WALLET,
        to_address=COUNTERPARTY,
        position_id=None,
        amount=1_000,
        block_number=10,
        tx_hash="0xabc",
        log_index=0,
    )
    receipt = {"logs": [{"topics": ["0xsomeothertopic"]}]}
    entry = build_ledger_entry(transfer, asset, WALLET, receipt, datetime(2024, 1, 1, tzinfo=UTC))
    assert entry.delta == -1_000
    assert entry.counterparty_address == COUNTERPARTY
    assert entry.event_type == "WITHDRAWAL"


def test_build_ledger_entry_keeps_positive_delta_for_incoming_transfer() -> None:
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    transfer = RawTransfer(
        contract_address="0xusdc",
        source_event="Transfer",
        from_address=COUNTERPARTY,
        to_address=WALLET,
        position_id=None,
        amount=1_000,
        block_number=10,
        tx_hash="0xabc",
        log_index=0,
    )
    receipt = {"logs": [{"topics": ["0xsomeothertopic"]}]}
    entry = build_ledger_entry(transfer, asset, WALLET, receipt, datetime(2024, 1, 1, tzinfo=UTC))
    assert entry.delta == 1_000
    assert entry.event_type == "DEPOSIT"


class FakeClient:
    def __init__(
        self,
        receipts_by_hash: dict[str, dict[str, Any]],
        blocks_by_number: dict[int, dict[str, Any]],
    ) -> None:
        self._receipts_by_hash = receipts_by_hash
        self._blocks_by_number = blocks_by_number

    async def batch_call(
        self, kind: RpcKind, requests: list[tuple[str, list[Any]]]
    ) -> list[dict[str, Any]]:
        if kind == "receipt":
            return [self._receipts_by_hash[params[0]] for _, params in requests]
        return [self._blocks_by_number[int(params[0], 16)] for _, params in requests]


async def test_fetch_receipts_returns_receipt_per_tx_hash() -> None:
    from app.ledger.enrichment import fetch_receipts

    client = FakeClient({"0xabc": {"logs": []}, "0xdef": {"logs": []}}, {})
    receipts = await fetch_receipts(client, ["0xabc", "0xdef"], batch_size=500)
    assert set(receipts) == {"0xabc", "0xdef"}


async def test_fetch_block_timestamps_returns_datetime_per_block() -> None:
    from app.ledger.enrichment import fetch_block_timestamps

    client = FakeClient({}, {10: {"timestamp": hex(1_700_000_000)}})
    timestamps = await fetch_block_timestamps(client, [10], batch_size=2000)
    assert timestamps[10] == datetime.fromtimestamp(1_700_000_000, tz=UTC)

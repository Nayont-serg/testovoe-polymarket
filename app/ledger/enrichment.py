from __future__ import annotations

from datetime import UTC, datetime

from app.ledger.models import Asset, LedgerEntry, RawTransfer
from app.rpc.client import JsonRpcClient
from app.rpc.codec import (
    PAYOUT_REDEMPTION_TOPIC,
    POSITION_SPLIT_TOPIC,
    POSITIONS_MERGE_TOPIC,
)
from app.rpc.contracts import LABEL_CONTRACTS


def classify_event_type(counterparty_address: str, receipt_topics: set[str]) -> str:
    if POSITION_SPLIT_TOPIC in receipt_topics:
        return "SPLIT"
    if POSITIONS_MERGE_TOPIC in receipt_topics:
        return "MERGE"
    if PAYOUT_REDEMPTION_TOPIC in receipt_topics:
        return "REDEEM"
    if counterparty_address in LABEL_CONTRACTS:
        return LABEL_CONTRACTS[counterparty_address]
    return "DEPOSIT"


async def fetch_receipts(
    client: JsonRpcClient, tx_hashes: list[str], batch_size: int
) -> dict[str, dict]:
    receipts: dict[str, dict] = {}
    for i in range(0, len(tx_hashes), batch_size):
        batch = tx_hashes[i : i + batch_size]
        results = await client.batch_call(
            "receipt", [("eth_getTransactionReceipt", [tx_hash]) for tx_hash in batch]
        )
        for tx_hash, receipt in zip(batch, results, strict=True):
            receipts[tx_hash] = receipt
    return receipts


async def fetch_block_timestamps(
    client: JsonRpcClient, block_numbers: list[int], batch_size: int
) -> dict[int, datetime]:
    timestamps: dict[int, datetime] = {}
    for i in range(0, len(block_numbers), batch_size):
        batch = block_numbers[i : i + batch_size]
        results = await client.batch_call(
            "call", [("eth_getBlockByNumber", [hex(block), False]) for block in batch]
        )
        for block, block_data in zip(batch, results, strict=True):
            timestamps[block] = datetime.fromtimestamp(int(block_data["timestamp"], 16), tz=UTC)
    return timestamps


def build_ledger_entry(
    transfer: RawTransfer,
    asset: Asset,
    wallet_address: str,
    receipt: dict,
    block_timestamp: datetime,
) -> LedgerEntry:
    is_outgoing = transfer.from_address == wallet_address
    delta = -transfer.amount if is_outgoing else transfer.amount
    counterparty = transfer.to_address if is_outgoing else transfer.from_address
    receipt_topics = {log["topics"][0] for log in receipt["logs"] if log["topics"]}
    event_type = classify_event_type(counterparty, receipt_topics)
    if event_type == "DEPOSIT" and is_outgoing:
        event_type = "WITHDRAWAL"
    return LedgerEntry(
        wallet_address=wallet_address,
        asset=asset,
        delta=delta,
        block_number=transfer.block_number,
        block_timestamp=block_timestamp,
        tx_hash=transfer.tx_hash,
        log_index=transfer.log_index,
        counterparty_address=counterparty,
        event_type=event_type,
        source_event=transfer.source_event,
    )

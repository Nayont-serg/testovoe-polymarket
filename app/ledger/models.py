from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

EventType = Literal["TRADE", "SPLIT", "MERGE", "REDEEM", "DEPOSIT", "WITHDRAWAL"]
SourceEvent = Literal["Transfer", "TransferSingle", "TransferBatch"]
AssetKind = Literal["erc20", "erc1155"]


@dataclass(frozen=True)
class Asset:
    kind: AssetKind
    contract_address: str
    position_id: int | None
    symbol: str | None
    decimals: int | None


@dataclass(frozen=True)
class RawTransfer:
    contract_address: str
    source_event: SourceEvent
    from_address: str
    to_address: str
    position_id: int | None
    amount: int
    block_number: int
    tx_hash: str
    log_index: int


@dataclass(frozen=True)
class LedgerEntry:
    wallet_address: str
    asset: Asset
    delta: int
    block_number: int
    block_timestamp: datetime
    tx_hash: str
    log_index: int
    counterparty_address: str
    event_type: EventType
    source_event: SourceEvent


@dataclass(frozen=True)
class BalanceCheckResult:
    wallet_address: str
    asset: Asset
    computed_balance: int
    onchain_balance: int
    checked_at_block: int
    matched: bool

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from app.config import Settings
from app.db.repository import LedgerRepository
from app.ledger.discovery import (
    AdaptiveConcurrencyLimiter,
    discover_ctf_transfers,
    discover_erc20_transfers,
)
from app.ledger.enrichment import build_ledger_entry, fetch_block_timestamps, fetch_receipts
from app.ledger.models import Asset, BalanceCheckResult, LedgerEntry, RawTransfer
from app.ledger.reconciliation import reconcile_asset
from app.rpc import contracts
from app.rpc.client import JsonRpcClient

CONFIRMATION_LAG_BLOCKS = 20


@dataclass(frozen=True)
class LedgerReport:
    balance_checks: list[BalanceCheckResult]

    @property
    def all_matched(self) -> bool:
        return all(check.matched for check in self.balance_checks)


class LedgerService:
    def __init__(
        self, client: JsonRpcClient, repository: LedgerRepository, settings: Settings
    ) -> None:
        self._client = client
        self._repository = repository
        self._settings = settings

    async def run(self, wallet_address: str) -> LedgerReport:
        wallet_address = wallet_address.lower()
        await self._repository.ensure_wallet(wallet_address)
        raw_latest_block = int(await self._client.call("call", "eth_blockNumber", []), 16)
        latest_block = raw_latest_block - CONFIRMATION_LAG_BLOCKS
        limiter = AdaptiveConcurrencyLimiter(
            self._settings.goldsky_raw_log_rpc_concurrency_min,
            self._settings.goldsky_raw_log_rpc_concurrency_start,
            self._settings.goldsky_raw_log_rpc_concurrency_max,
            self._settings.goldsky_raw_log_rpc_concurrency_grow_interval_sec,
        )
        self._client.on_rate_limited = lambda: limiter.degrade(
            self._client.rate_limit_cooldown_seconds
        )
        # Caps concurrent receipt/timestamp/DB work separately from limiter's raw-log-fetch
        # concurrency: a dense on-chain range can keep limiter.max_limit chunk fetches
        # concurrently alive, and each chunk's persist step can itself hold many full tx
        # receipts, so gating persist by the same (much larger) limiter let peak memory scale
        # with max_limit instead of with a small, fixed number of in-flight persists.
        persist_semaphore = asyncio.Semaphore(self._settings.persist_concurrency)

        checks: list[BalanceCheckResult] = []
        for asset in contracts.COLLATERAL_ASSETS:
            checks.append(
                await self._process_erc20_asset(
                    asset,
                    wallet_address,
                    limiter,
                    persist_semaphore,
                    latest_block,
                    default_start_block=contracts.COLLATERAL_DEPLOY_BLOCKS.get(
                        asset.contract_address, contracts.CTF_DEPLOY_BLOCK
                    ),
                )
            )

        for asset in await self._process_ctf(
            wallet_address, limiter, persist_semaphore, latest_block
        ):
            checks.append(await self._reconcile(asset, wallet_address, latest_block))

        return LedgerReport(balance_checks=checks)

    async def _process_erc20_asset(
        self,
        asset: Asset,
        wallet_address: str,
        limiter: AdaptiveConcurrencyLimiter,
        persist_semaphore: asyncio.Semaphore,
        latest_block: int,
        default_start_block: int,
    ) -> BalanceCheckResult:
        asset_id = await self._repository.ensure_asset(asset)
        checkpoint = await self._repository.get_checkpoint(wallet_address, asset_id)
        scan_start = checkpoint + 1 if checkpoint is not None else default_start_block

        async def persist_batch(batch: list[RawTransfer]) -> None:
            async with persist_semaphore:
                await self._persist_transfers(asset, asset_id, wallet_address, batch)

        await discover_erc20_transfers(
            self._client,
            limiter,
            asset,
            wallet_address,
            scan_start,
            latest_block,
            self._settings.free_log_rpc_window_blocks,
            persist_batch,
        )
        await self._repository.set_checkpoint(wallet_address, asset_id, latest_block)
        return await self._reconcile(asset, wallet_address, latest_block)

    async def _process_ctf(
        self,
        wallet_address: str,
        limiter: AdaptiveConcurrencyLimiter,
        persist_semaphore: asyncio.Semaphore,
        latest_block: int,
    ) -> list[Asset]:
        checkpoint_asset = Asset(
            kind="erc1155",
            contract_address=contracts.CTF_ADDRESS,
            position_id=None,
            symbol=None,
            decimals=None,
        )
        checkpoint_asset_id = await self._repository.ensure_asset(checkpoint_asset)
        checkpoint = await self._repository.get_checkpoint(wallet_address, checkpoint_asset_id)
        scan_start = checkpoint + 1 if checkpoint is not None else contracts.CTF_DEPLOY_BLOCK

        assets_by_position: dict[int, Asset] = {}
        asset_ids_by_position: dict[int, int] = {}

        async def persist_batch(batch: list[RawTransfer]) -> None:
            async with persist_semaphore:
                for transfer in batch:
                    if transfer.position_id not in asset_ids_by_position:
                        asset = Asset(
                            kind="erc1155",
                            contract_address=contracts.CTF_ADDRESS,
                            position_id=transfer.position_id,
                            symbol=None,
                            decimals=0,
                        )
                        # Concurrent batches may race here; ensure_asset's ON CONFLICT makes it
                        # benign.
                        asset_id = await self._repository.ensure_asset(asset)
                        assets_by_position[transfer.position_id] = asset
                        asset_ids_by_position[transfer.position_id] = asset_id

                tx_hashes = list({t.tx_hash for t in batch})
                receipts = await fetch_receipts(
                    self._client, tx_hashes, self._settings.w3_free_receipt_rpc_batch_size
                )
                block_numbers = list({t.block_number for t in batch})
                timestamps = await fetch_block_timestamps(
                    self._client, block_numbers, self._settings.w3_block_timestamps_rpc_batch_size
                )
                entries_by_asset_id: dict[int, list[LedgerEntry]] = {}
                for transfer in batch:
                    asset = assets_by_position[transfer.position_id]
                    asset_id = asset_ids_by_position[transfer.position_id]
                    entry = build_ledger_entry(
                        transfer,
                        asset,
                        wallet_address,
                        receipts[transfer.tx_hash],
                        timestamps[transfer.block_number],
                    )
                    entries_by_asset_id.setdefault(asset_id, []).append(entry)
                for asset_id, entries in entries_by_asset_id.items():
                    await self._repository.upsert_events(asset_id, entries)

        await discover_ctf_transfers(
            self._client,
            limiter,
            contracts.CTF_ADDRESS,
            wallet_address,
            scan_start,
            latest_block,
            self._settings.free_log_rpc_window_blocks,
            persist_batch,
        )

        await self._repository.set_checkpoint(wallet_address, checkpoint_asset_id, latest_block)
        return await self._repository.list_erc1155_assets_for_wallet(wallet_address)

    async def _persist_transfers(
        self,
        asset: Asset,
        asset_id: int,
        wallet_address: str,
        transfers: list[RawTransfer],
    ) -> None:
        if not transfers:
            return
        tx_hashes = list({t.tx_hash for t in transfers})
        receipts = await fetch_receipts(
            self._client, tx_hashes, self._settings.w3_free_receipt_rpc_batch_size
        )
        block_numbers = list({t.block_number for t in transfers})
        timestamps = await fetch_block_timestamps(
            self._client, block_numbers, self._settings.w3_block_timestamps_rpc_batch_size
        )
        entries = [
            build_ledger_entry(
                t, asset, wallet_address, receipts[t.tx_hash], timestamps[t.block_number]
            )
            for t in transfers
        ]
        await self._repository.upsert_events(asset_id, entries)

    async def _reconcile(
        self, asset: Asset, wallet_address: str, latest_block: int
    ) -> BalanceCheckResult:
        asset_id = await self._repository.ensure_asset(asset)
        computed = await self._repository.sum_balance(wallet_address, asset_id)
        result = await reconcile_asset(self._client, wallet_address, asset, computed, latest_block)
        await self._repository.save_balance_check(result, asset_id)
        return result

from __future__ import annotations

import asyncpg

from app.ledger.models import Asset, BalanceCheckResult, LedgerEntry


class LedgerRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def ensure_wallet(self, wallet_address: str) -> None:
        await self._pool.execute(
            "INSERT INTO wallets (address) VALUES ($1) ON CONFLICT DO NOTHING",
            wallet_address,
        )

    async def ensure_asset(self, asset: Asset) -> int:
        row = await self._pool.fetchrow(
            """
            INSERT INTO assets (kind, contract_address, position_id, symbol, decimals)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (kind, contract_address, position_id)
                DO UPDATE SET symbol = EXCLUDED.symbol
            RETURNING id
            """,
            asset.kind,
            asset.contract_address,
            asset.position_id,
            asset.symbol,
            asset.decimals,
        )
        return row["id"]

    async def upsert_events(self, asset_id: int, entries: list[LedgerEntry]) -> None:
        if not entries:
            return
        await self._pool.executemany(
            """
            INSERT INTO balance_events (
                wallet_address, asset_id, block_number, block_timestamp,
                tx_hash, log_index, delta, counterparty_address, event_type, source_event
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (tx_hash, log_index, asset_id, wallet_address) DO NOTHING
            """,
            [
                (
                    entry.wallet_address,
                    asset_id,
                    entry.block_number,
                    entry.block_timestamp,
                    entry.tx_hash,
                    entry.log_index,
                    entry.delta,
                    entry.counterparty_address,
                    entry.event_type,
                    entry.source_event,
                )
                for entry in entries
            ],
        )

    async def list_erc1155_assets_for_wallet(self, wallet_address: str) -> list[Asset]:
        rows = await self._pool.fetch(
            """
            SELECT DISTINCT a.kind, a.contract_address, a.position_id, a.symbol, a.decimals
            FROM assets a
            JOIN balance_events be ON be.asset_id = a.id
            WHERE be.wallet_address = $1 AND a.kind = 'erc1155' AND a.position_id IS NOT NULL
            """,
            wallet_address,
        )
        return [
            Asset(
                kind=row["kind"],
                contract_address=row["contract_address"],
                position_id=int(row["position_id"]),
                symbol=row["symbol"],
                decimals=row["decimals"],
            )
            for row in rows
        ]

    async def get_checkpoint(self, wallet_address: str, asset_id: int) -> int | None:
        row = await self._pool.fetchrow(
            "SELECT last_scanned_block FROM index_checkpoints "
            "WHERE wallet_address = $1 AND asset_id = $2",
            wallet_address,
            asset_id,
        )
        return row["last_scanned_block"] if row else None

    async def set_checkpoint(self, wallet_address: str, asset_id: int, block_number: int) -> None:
        await self._pool.execute(
            """
            INSERT INTO index_checkpoints (wallet_address, asset_id, last_scanned_block)
            VALUES ($1, $2, $3)
            ON CONFLICT (wallet_address, asset_id)
                DO UPDATE SET last_scanned_block = EXCLUDED.last_scanned_block
            """,
            wallet_address,
            asset_id,
            block_number,
        )

    async def sum_balance(self, wallet_address: str, asset_id: int) -> int:
        row = await self._pool.fetchrow(
            "SELECT COALESCE(SUM(delta), 0) AS total FROM balance_events "
            "WHERE wallet_address = $1 AND asset_id = $2",
            wallet_address,
            asset_id,
        )
        return int(row["total"])

    async def save_balance_check(self, result: BalanceCheckResult, asset_id: int) -> None:
        await self._pool.execute(
            """
            INSERT INTO balance_checks (
                wallet_address, asset_id, computed_balance, onchain_balance,
                checked_at_block, matched
            ) VALUES ($1, $2, $3, $4, $5, $6)
            """,
            result.wallet_address,
            asset_id,
            result.computed_balance,
            result.onchain_balance,
            result.checked_at_block,
            result.matched,
        )

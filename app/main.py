from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from app.config import Settings
from app.db.connection import create_pool
from app.db.repository import LedgerRepository
from app.ledger.service import LedgerService
from app.rpc.client import JsonRpcClient

logger = logging.getLogger("polymarket_wallet_history")


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore Polymarket wallet balance history from Polygon RPC"
    )
    parser.add_argument("--wallet-address", default=None)
    return parser.parse_args(argv)


async def run(wallet_address: str | None) -> bool:
    settings = Settings.from_env()
    address = (wallet_address or settings.wallet_address).lower()
    client = JsonRpcClient(
        urls_by_kind={
            "log": settings.log_rpc_urls,
            "receipt": settings.receipt_rpc_urls,
            "call": settings.call_rpc_urls,
        },
        timeout_seconds=settings.rpc_timeout_seconds,
        keepalive_timeout_seconds=settings.rpc_keepalive_timeout_seconds,
        rate_limit_cooldown_seconds=settings.receipt_rpc_rate_limit_cooldown_seconds,
    )
    pool = await create_pool(settings.database_url)
    try:
        repository = LedgerRepository(pool)
        service = LedgerService(client, repository, settings)
        report = await service.run(address)
        for check in report.balance_checks:
            label = check.asset.symbol or f"position:{check.asset.position_id}"
            status = "OK" if check.matched else "MISMATCH"
            logger.info(
                "[%s] %s: computed=%s onchain=%s",
                status,
                label,
                check.computed_balance,
                check.onchain_balance,
            )
        return report.all_matched
    finally:
        await pool.close()
        await client.aclose()


def main() -> None:
    configure_logging()
    args = parse_args(sys.argv[1:])
    matched = asyncio.run(run(args.wallet_address))
    sys.exit(0 if matched else 1)


if __name__ == "__main__":
    main()

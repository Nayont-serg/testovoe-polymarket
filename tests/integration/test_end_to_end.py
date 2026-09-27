import os

import asyncpg
import pytest

from app.config import Settings
from app.db.repository import LedgerRepository
from app.ledger.service import LedgerService
from app.rpc.client import JsonRpcClient

pytestmark = pytest.mark.integration

WALLET = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"


def _settings() -> Settings:
    env = dict(os.environ)
    env.setdefault(
        "DATABASE_URL",
        "postgresql://polymarket:polymarket@localhost:5433/polymarket_wallet_history",
    )
    env.setdefault(
        "LOG_RPC_URLS",
        "https://gateway.tenderly.co/public/polygon,https://polygon.gateway.tenderly.co",
    )
    env.setdefault(
        "RECEIPT_RPC_URLS",
        "https://polygon.gateway.tenderly.co,https://rpc.decentraland.org/polygon",
    )
    env.setdefault(
        "CALL_RPC_URLS",
        "https://gateway.tenderly.co/public/polygon,https://polygon.drpc.org",
    )
    return Settings.from_env(env)


async def test_wallet_balances_reconcile_against_onchain_balance_of(pool: asyncpg.Pool) -> None:
    settings = _settings()
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
    try:
        repository = LedgerRepository(pool)
        service = LedgerService(client, repository, settings)
        report = await service.run(WALLET)
        mismatched = [c for c in report.balance_checks if not c.matched]
        diffs = [
            (c.asset.symbol or c.asset.position_id, c.computed_balance, c.onchain_balance)
            for c in mismatched
        ]
        assert mismatched == [], f"balances did not reconcile for: {diffs}"
    finally:
        await client.aclose()

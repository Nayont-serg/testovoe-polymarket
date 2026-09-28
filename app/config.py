from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    wallet_address: str
    log_rpc_urls: tuple[str, ...]
    receipt_rpc_urls: tuple[str, ...]
    call_rpc_urls: tuple[str, ...]
    rpc_timeout_seconds: float
    rpc_keepalive_timeout_seconds: float
    receipt_rpc_rate_limit_cooldown_seconds: float
    free_log_rpc_window_blocks: int
    log_rpc_concurrency_min: int
    log_rpc_concurrency_start: int
    log_rpc_concurrency_max: int
    log_rpc_concurrency_grow_interval_sec: int
    receipt_rpc_batch_size: int
    block_timestamps_rpc_batch_size: int
    balance_check_batch_size: int
    persist_concurrency: int

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        source = env if env is not None else os.environ
        return cls(
            database_url=source["DATABASE_URL"],
            wallet_address=source.get(
                "WALLET_ADDRESS", "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
            ).lower(),
            log_rpc_urls=tuple(source["LOG_RPC_URLS"].split(",")),
            receipt_rpc_urls=tuple(source["RECEIPT_RPC_URLS"].split(",")),
            call_rpc_urls=tuple(source["CALL_RPC_URLS"].split(",")),
            rpc_timeout_seconds=float(source.get("RPC_TIMEOUT_SECONDS", "90")),
            rpc_keepalive_timeout_seconds=float(source.get("RPC_KEEPALIVE_TIMEOUT_SECONDS", "120")),
            receipt_rpc_rate_limit_cooldown_seconds=float(
                source.get("RECEIPT_RPC_RATE_LIMIT_COOLDOWN_SECONDS", "30")
            ),
            free_log_rpc_window_blocks=int(source.get("FREE_LOG_RPC_WINDOW_BLOCKS", "200000")),
            log_rpc_concurrency_min=int(source.get("LOG_RPC_CONCURRENCY_MIN", "2")),
            log_rpc_concurrency_start=int(source.get("LOG_RPC_CONCURRENCY_START", "8")),
            log_rpc_concurrency_max=int(source.get("LOG_RPC_CONCURRENCY_MAX", "16")),
            log_rpc_concurrency_grow_interval_sec=int(
                source.get("LOG_RPC_CONCURRENCY_GROW_INTERVAL_SEC", "180")
            ),
            receipt_rpc_batch_size=int(source.get("RECEIPT_RPC_BATCH_SIZE", "500")),
            block_timestamps_rpc_batch_size=int(
                source.get("BLOCK_TIMESTAMPS_RPC_BATCH_SIZE", "2000")
            ),
            balance_check_batch_size=int(source.get("BALANCE_CHECK_BATCH_SIZE", "500")),
            persist_concurrency=int(source.get("PERSIST_CONCURRENCY", "4")),
        )

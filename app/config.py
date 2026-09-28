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
    receipt_rpc_use_proxy: bool
    free_log_rpc_window_blocks: int
    goldsky_raw_log_rpc_concurrency_min: int
    goldsky_raw_log_rpc_concurrency_start: int
    goldsky_raw_log_rpc_concurrency_max: int
    goldsky_raw_log_rpc_concurrency_grow_interval_sec: int
    w3_wallet_logs_loading_batch_size: int
    w3_free_receipt_loading_batch_size: int
    w3_free_receipt_rpc_batch_size: int
    w3_block_timestamps_loading_batch_size: int
    w3_block_timestamps_rpc_batch_size: int
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
            receipt_rpc_use_proxy=source.get("RECEIPT_RPC_USE_PROXY", "false").lower() == "true",
            free_log_rpc_window_blocks=int(source.get("FREE_LOG_RPC_WINDOW_BLOCKS", "200000")),
            goldsky_raw_log_rpc_concurrency_min=int(
                source.get("GOLDSKY_RAW_LOG_RPC_CONCURRENCY_MIN", "2")
            ),
            goldsky_raw_log_rpc_concurrency_start=int(
                source.get("GOLDSKY_RAW_LOG_RPC_CONCURRENCY_START", "60")
            ),
            goldsky_raw_log_rpc_concurrency_max=int(
                source.get("GOLDSKY_RAW_LOG_RPC_CONCURRENCY_MAX", "100")
            ),
            goldsky_raw_log_rpc_concurrency_grow_interval_sec=int(
                source.get("GOLDSKY_RAW_LOG_RPC_CONCURRENCY_GROW_INTERVAL_SEC", "180")
            ),
            w3_wallet_logs_loading_batch_size=int(
                source.get("W3_WALLET_LOGS_LOADING_BATCH_SIZE", "6")
            ),
            w3_free_receipt_loading_batch_size=int(
                source.get("W3_FREE_RECEIPT_LOADING_BATCH_SIZE", "1400")
            ),
            w3_free_receipt_rpc_batch_size=int(source.get("W3_FREE_RECEIPT_RPC_BATCH_SIZE", "500")),
            w3_block_timestamps_loading_batch_size=int(
                source.get("W3_BLOCK_TIMESTAMPS_LOADING_BATCH_SIZE", "2000")
            ),
            w3_block_timestamps_rpc_batch_size=int(
                source.get("W3_BLOCK_TIMESTAMPS_RPC_BATCH_SIZE", "2000")
            ),
            persist_concurrency=int(source.get("PERSIST_CONCURRENCY", "4")),
        )

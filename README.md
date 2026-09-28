# Polymarket Wallet History

Reconstructs the balance history of a Polymarket wallet on Polygon directly from RPC logs
(no Polymarket API, no third-party indexer) and checks the computed balance of every asset
against `balanceOf` on the latest confirmed block.

## Setup

    cp .env.example .env
    uv sync
    make docker-up
    make migrate

Migrations are not versioned. To reset the schema:

    docker compose down -v && make docker-up && make migrate

## Run

    make run                                                           # WALLET_ADDRESS from .env
    uv run --env-file .env python -m app.main --wallet-address 0x...   # any other wallet

Prints one `[OK]` or `[MISMATCH]` line per asset. Exit code is `0` if all balances match,
`1` otherwise. Progress is stored in Postgres as per-asset checkpoints, so a rerun continues
from the last scanned block.

## Tests

    make test              # unit tests, no network or DB
    make test-integration  # needs `make docker-up` and network access

Integration tests use a separate `polymarket_wallet_history_test` database (override with
`TEST_DATABASE_URL`), created automatically.

## Scale

The default wallet is a Polymarket proxy with 500k+ USDC.e Transfer events. Free Polygon RPC
endpoints cap `eth_getLogs` at 20k results per request, so a full scan needs many narrow
requests and takes a long time. Ranges over the cap are bisected, RPC failures are retried
with backoff, and each batch is written to Postgres as soon as it is fetched, so memory stays
flat regardless of history size.

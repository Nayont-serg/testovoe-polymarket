# Polymarket Wallet History

Reconstructs the on-chain balance history of a Polymarket wallet on Polygon directly
from RPC logs (no Polymarket API, no third-party indexer) and proves the computed
balance of every asset matches `balanceOf` on the latest block.

## Setup

    cp .env.example .env
    uv sync
    make docker-up
    make migrate

`make migrate` (`app/db/migrate.py`) just runs every `*.sql` file in
`app/db/migrations` in sorted order with no tracking of what's already been
applied, and `001_init.sql` uses `CREATE TABLE IF NOT EXISTS`. If you already have
a `polymarket_wallet_history` database created before this project's schema was
corrected, `make migrate` will silently no-op against the old schema instead of
fixing it — drop and recreate the database instead:

    docker compose down -v && make docker-up && make migrate

## Run

    make run                                                     # uses WALLET_ADDRESS from .env
    uv run --env-file .env python -m app.main --wallet-address 0x...   # or any other wallet

Exit code is `0` when every asset balance matched on-chain, `1` otherwise — check
stdout for a per-asset `[OK]`/`[MISMATCH]` line either way. Progress persists in
Postgres via per-asset checkpoints, so re-running `make run` resumes from where it
left off rather than rescanning from genesis.

## Tests

    make test              # unit tests, no network/DB required
    make test-integration  # requires `make docker-up` and live network access

`make test-integration` includes a real end-to-end run against the wallet's actual
on-chain history — do not run it while `make run` is scanning against the same
database, since the repository tests truncate the ledger tables as part of cleanup.

## A note on scale

The default wallet (`0x46b353667fd7d846af3bbeda6584b0e5b883d3de`) is a Polymarket
proxy contract, not a quiet retail wallet — real-world verification found it has
500,000+ Transfer events on USDC.e alone. Free public Polygon RPC endpoints cap
`eth_getLogs` at 20,000 raw results per contract per block range (before applying
the wallet-specific topic filter), so a full historical scan of a high-volume wallet
against free RPC genuinely requires tens of thousands of narrow, bisected requests
and can take a long time. This is an inherent cost of the "no indexer, direct RPC
only" architecture for a wallet at this activity level, not a bug — the pipeline
itself handles it correctly: chunk bisection on the provider's cap, a bounded
worker pool (memory does not scale with total history), retry-with-backoff on
transient RPC failures, and streaming persistence (each discovered batch is
enriched and written to Postgres immediately, so memory is bounded by one batch's
size, not by total wallet activity).

## Design

See `docs/superpowers/specs/2026-09-25-polymarket-wallet-history-design.md` for the
full architecture rationale (contract registry, RPC client config, DB schema).

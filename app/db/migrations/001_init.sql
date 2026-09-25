CREATE TABLE IF NOT EXISTS wallets (
    address         TEXT PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS assets (
    id              SERIAL PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('erc20', 'erc1155')),
    contract_address TEXT NOT NULL,
    position_id     NUMERIC(78, 0),
    symbol          TEXT,
    decimals        SMALLINT,
    UNIQUE (kind, contract_address, position_id)
);

CREATE TABLE IF NOT EXISTS balance_events (
    id                  BIGSERIAL PRIMARY KEY,
    wallet_address      TEXT NOT NULL REFERENCES wallets(address),
    asset_id            INT NOT NULL REFERENCES assets(id),
    block_number        BIGINT NOT NULL,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    tx_hash             TEXT NOT NULL,
    log_index           INT NOT NULL,
    delta               NUMERIC(78, 0) NOT NULL,
    counterparty_address TEXT NOT NULL,
    event_type          TEXT NOT NULL CHECK (event_type IN ('TRADE','SPLIT','MERGE','REDEEM','DEPOSIT','WITHDRAWAL')),
    source_event        TEXT NOT NULL CHECK (source_event IN ('Transfer','TransferSingle','TransferBatch')),
    UNIQUE (tx_hash, log_index, asset_id, wallet_address)
);
CREATE INDEX IF NOT EXISTS balance_events_wallet_asset_block_idx
    ON balance_events (wallet_address, asset_id, block_number);

CREATE TABLE IF NOT EXISTS index_checkpoints (
    wallet_address      TEXT NOT NULL REFERENCES wallets(address),
    asset_id            INT NOT NULL REFERENCES assets(id),
    last_scanned_block  BIGINT NOT NULL,
    PRIMARY KEY (wallet_address, asset_id)
);

CREATE TABLE IF NOT EXISTS balance_checks (
    id                  BIGSERIAL PRIMARY KEY,
    wallet_address      TEXT NOT NULL REFERENCES wallets(address),
    asset_id            INT NOT NULL REFERENCES assets(id),
    computed_balance    NUMERIC(78, 0) NOT NULL,
    onchain_balance     NUMERIC(78, 0) NOT NULL,
    checked_at_block    BIGINT NOT NULL,
    matched             BOOLEAN NOT NULL,
    checked_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

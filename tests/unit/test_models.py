from app.ledger.models import Asset, RawTransfer


def test_asset_erc20_has_no_position_id() -> None:
    asset = Asset(
        kind="erc20",
        contract_address="0xusdc",
        position_id=None,
        symbol="USDC",
        decimals=6,
    )
    assert asset.position_id is None


def test_raw_transfer_holds_decoded_log_fields() -> None:
    transfer = RawTransfer(
        contract_address="0xctf",
        source_event="TransferSingle",
        from_address="0xfrom",
        to_address="0xto",
        position_id=123,
        amount=1_000_000,
        block_number=100,
        tx_hash="0xhash",
        log_index=2,
    )
    assert transfer.amount == 1_000_000
    assert transfer.position_id == 123

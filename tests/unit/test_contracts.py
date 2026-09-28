from app.rpc.contracts import (
    COLLATERAL_ASSETS,
    COLLATERAL_DEPLOY_BLOCKS,
    CTF_ADDRESS,
    LABEL_CONTRACTS,
)


def test_collateral_assets_cover_all_three_collateral_eras() -> None:
    symbols = {asset.symbol for asset in COLLATERAL_ASSETS}
    assert symbols == {"USDC.e", "USDC", "pUSD"}
    assert all(asset.kind == "erc20" for asset in COLLATERAL_ASSETS)


def test_collateral_deploy_blocks_cover_every_collateral_asset() -> None:
    assert {asset.contract_address for asset in COLLATERAL_ASSETS} == set(COLLATERAL_DEPLOY_BLOCKS)


def test_ctf_address_is_lowercase_checksum_free() -> None:
    assert CTF_ADDRESS.lower() == CTF_ADDRESS


def test_label_contracts_are_lowercase_keys() -> None:
    assert all(address == address.lower() for address in LABEL_CONTRACTS)

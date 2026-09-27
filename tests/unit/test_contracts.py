from typing import Any

from app.rpc.client import RpcKind
from app.rpc.contracts import (
    COLLATERAL_ASSETS,
    CTF_ADDRESS,
    LABEL_CONTRACTS,
    resolve_deploy_block,
)


def test_collateral_assets_cover_all_three_collateral_eras() -> None:
    symbols = {asset.symbol for asset in COLLATERAL_ASSETS}
    assert symbols == {"USDC.e", "USDC", "pUSD"}
    assert all(asset.kind == "erc20" for asset in COLLATERAL_ASSETS)


def test_ctf_address_is_lowercase_checksum_free() -> None:
    assert CTF_ADDRESS.lower() == CTF_ADDRESS


def test_label_contracts_are_lowercase_keys() -> None:
    assert all(address == address.lower() for address in LABEL_CONTRACTS)


class FakeClient:
    def __init__(self, deploy_block: int) -> None:
        self._deploy_block = deploy_block

    async def call(self, _kind: RpcKind, _method: str, params: list[Any]) -> str:
        block = int(params[1], 16)
        return "0xdeadbeef" if block >= self._deploy_block else "0x"


async def test_resolve_deploy_block_finds_first_block_with_code() -> None:
    client = FakeClient(deploy_block=45_319_261)
    result = await resolve_deploy_block(client, "0xtoken", latest_block=90_000_000)
    assert result == 45_319_261


async def test_resolve_deploy_block_handles_deploy_at_block_zero() -> None:
    client = FakeClient(deploy_block=0)
    result = await resolve_deploy_block(client, "0xtoken", latest_block=1_000)
    assert result == 0

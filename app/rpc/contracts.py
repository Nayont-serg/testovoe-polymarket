from __future__ import annotations

from typing import TYPE_CHECKING

from app.ledger.models import Asset

if TYPE_CHECKING:
    from app.rpc.client import JsonRpcClient

CTF_ADDRESS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
CTF_DEPLOY_BLOCK = 4_023_686

COLLATERAL_ASSETS: tuple[Asset, ...] = (
    Asset(
        kind="erc20",
        contract_address="0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
        position_id=None,
        symbol="USDC.e",
        decimals=6,
    ),
    Asset(
        kind="erc20",
        contract_address="0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
        position_id=None,
        symbol="USDC",
        decimals=6,
    ),
    Asset(
        kind="erc20",
        contract_address="0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb",
        position_id=None,
        symbol="pUSD",
        decimals=6,
    ),
)

COLLATERAL_DEPLOY_BLOCKS: dict[str, int] = {
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": 5_013_591,
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359": 45_319_261,
    "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb": 84_902_320,
}

LABEL_CONTRACTS: dict[str, str] = {
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e": "TRADE",
    "0xe111180000d2663c0091e4f400237545b87b996b": "TRADE",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a": "TRADE",
    "0xe2222d279d744050d28e00520010520000310f59": "TRADE",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296": "TRADE",
}


async def resolve_deploy_block(client: JsonRpcClient, address: str, latest_block: int) -> int:
    async def has_code(block: int) -> bool:
        code = await client.call("call", "eth_getCode", [address, hex(block)])
        return code not in ("0x", "0x0", None)

    low, high = 0, latest_block
    while low < high:
        mid = (low + high) // 2
        if await has_code(mid):
            high = mid
        else:
            low = mid + 1
    return low

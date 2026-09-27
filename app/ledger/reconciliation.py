from __future__ import annotations

from app.ledger.models import Asset, BalanceCheckResult
from app.rpc.client import JsonRpcClient
from app.rpc.codec import (
    decode_uint256,
    encode_balance_of_erc20_call,
    encode_balance_of_erc1155_call,
)


async def fetch_onchain_balance(
    client: JsonRpcClient, asset: Asset, wallet_address: str, block_number: int
) -> int:
    if asset.kind == "erc20":
        data = encode_balance_of_erc20_call(wallet_address)
    else:
        assert asset.position_id is not None
        data = encode_balance_of_erc1155_call(wallet_address, asset.position_id)
    result = await client.call(
        "call",
        "eth_call",
        [{"to": asset.contract_address, "data": data}, hex(block_number)],
    )
    return decode_uint256(result)


async def reconcile_asset(
    client: JsonRpcClient,
    wallet_address: str,
    asset: Asset,
    computed_balance: int,
    block_number: int,
) -> BalanceCheckResult:
    onchain_balance = await fetch_onchain_balance(client, asset, wallet_address, block_number)
    return BalanceCheckResult(
        wallet_address=wallet_address,
        asset=asset,
        computed_balance=computed_balance,
        onchain_balance=onchain_balance,
        checked_at_block=block_number,
        matched=computed_balance == onchain_balance,
    )

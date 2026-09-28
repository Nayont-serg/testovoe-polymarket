from __future__ import annotations

from typing import Any

from app.ledger.models import Asset, BalanceCheckResult
from app.rpc.client import JsonRpcClient
from app.rpc.codec import (
    decode_uint256,
    encode_balance_of_erc20_call,
    encode_balance_of_erc1155_call,
)


def _balance_of_params(asset: Asset, wallet_address: str, block_number: int) -> list[Any]:
    if asset.kind == "erc20":
        data = encode_balance_of_erc20_call(wallet_address)
    else:
        assert asset.position_id is not None
        data = encode_balance_of_erc1155_call(wallet_address, asset.position_id)
    return [{"to": asset.contract_address, "data": data}, hex(block_number)]


def _check_result(
    wallet_address: str, asset: Asset, computed_balance: int, onchain_balance: int, block: int
) -> BalanceCheckResult:
    return BalanceCheckResult(
        wallet_address=wallet_address,
        asset=asset,
        computed_balance=computed_balance,
        onchain_balance=onchain_balance,
        checked_at_block=block,
        matched=computed_balance == onchain_balance,
    )


async def fetch_onchain_balance(
    client: JsonRpcClient, asset: Asset, wallet_address: str, block_number: int
) -> int:
    result = await client.call(
        "call", "eth_call", _balance_of_params(asset, wallet_address, block_number)
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
    return _check_result(wallet_address, asset, computed_balance, onchain_balance, block_number)


async def reconcile_assets(
    client: JsonRpcClient,
    wallet_address: str,
    computed_by_asset: list[tuple[Asset, int]],
    block_number: int,
    batch_size: int,
) -> list[BalanceCheckResult]:
    results: list[BalanceCheckResult] = []
    for i in range(0, len(computed_by_asset), batch_size):
        batch = computed_by_asset[i : i + batch_size]
        onchain_balances = await client.batch_call(
            "call",
            [
                ("eth_call", _balance_of_params(asset, wallet_address, block_number))
                for asset, _ in batch
            ],
        )
        results.extend(
            _check_result(wallet_address, asset, computed, decode_uint256(onchain), block_number)
            for (asset, computed), onchain in zip(batch, onchain_balances, strict=True)
        )
    return results

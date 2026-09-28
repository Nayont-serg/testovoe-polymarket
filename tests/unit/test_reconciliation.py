from typing import Any

from app.ledger.models import Asset
from app.ledger.reconciliation import fetch_onchain_balance, reconcile_asset, reconcile_assets
from app.rpc.client import RpcKind

WALLET = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"


class FakeClient:
    def __init__(self, balance: int) -> None:
        self._balance = balance
        self.last_call: tuple[RpcKind, str, list[Any]] | None = None

    async def call(self, kind: RpcKind, method: str, params: list[Any]) -> str:
        self.last_call = (kind, method, params)
        return "0x" + format(self._balance, "064x")


async def test_fetch_onchain_balance_calls_erc20_balance_of() -> None:
    client = FakeClient(balance=1_000)
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    balance = await fetch_onchain_balance(client, asset, WALLET, block_number=100)
    assert balance == 1_000
    kind, method, params = client.last_call
    assert kind == "call"
    assert method == "eth_call"
    assert params[0]["to"] == "0xusdc"
    assert params[0]["data"].startswith("0x70a08231")
    assert params[1] == hex(100)


async def test_fetch_onchain_balance_calls_erc1155_balance_of_with_position_id() -> None:
    client = FakeClient(balance=7)
    asset = Asset(kind="erc1155", contract_address="0xctf", position_id=42, symbol=None, decimals=0)
    balance = await fetch_onchain_balance(client, asset, WALLET, block_number=100)
    assert balance == 7
    _, _, params = client.last_call
    assert params[0]["data"].startswith("0x00fdd58e")


async def test_reconcile_asset_matched_true_when_equal() -> None:
    client = FakeClient(balance=500)
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    result = await reconcile_asset(client, WALLET, asset, computed_balance=500, block_number=100)
    assert result.matched is True
    assert result.onchain_balance == 500


async def test_reconcile_asset_matched_false_when_different() -> None:
    client = FakeClient(balance=500)
    asset = Asset(
        kind="erc20", contract_address="0xusdc", position_id=None, symbol="USDC", decimals=6
    )
    result = await reconcile_asset(client, WALLET, asset, computed_balance=400, block_number=100)
    assert result.matched is False


class FakeBatchClient:
    def __init__(self, balance_by_position: dict[int, int]) -> None:
        self._balance_by_position = balance_by_position
        self.batch_sizes: list[int] = []

    async def batch_call(self, kind: RpcKind, requests: list[tuple[str, list[Any]]]) -> list[str]:
        assert kind == "call"
        self.batch_sizes.append(len(requests))
        balances = []
        for method, params in requests:
            assert method == "eth_call"
            position_id = int(params[0]["data"][-64:], 16)
            balances.append("0x" + format(self._balance_by_position[position_id], "064x"))
        return balances


async def test_reconcile_assets_splits_positions_into_rpc_batches() -> None:
    client = FakeBatchClient({position_id: position_id * 10 for position_id in range(1, 6)})
    computed_by_asset = [
        (
            Asset(
                kind="erc1155",
                contract_address="0xctf",
                position_id=position_id,
                symbol=None,
                decimals=0,
            ),
            position_id * 10,
        )
        for position_id in range(1, 6)
    ]
    computed_by_asset[4] = (computed_by_asset[4][0], 0)

    results = await reconcile_assets(
        client, WALLET, computed_by_asset, block_number=100, batch_size=2
    )

    assert client.batch_sizes == [2, 2, 1]
    assert [result.onchain_balance for result in results] == [10, 20, 30, 40, 50]
    assert [result.matched for result in results] == [True, True, True, True, False]
    assert {result.checked_at_block for result in results} == {100}

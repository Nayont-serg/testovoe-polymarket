import httpx
import pytest
import respx

from app.rpc.client import JsonRpcClient, RpcAllEndpointsExhaustedError


def make_client(urls: list[str], cooldown: float = 30.0) -> JsonRpcClient:
    return JsonRpcClient(
        urls_by_kind={"call": tuple(urls)},
        timeout_seconds=5,
        keepalive_timeout_seconds=5,
        rate_limit_cooldown_seconds=cooldown,
    )


@respx.mock
async def test_call_returns_result_from_working_url() -> None:
    respx.post("https://a.example").mock(
        return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x1"})
    )
    client = make_client(["https://a.example"])
    result = await client.call("call", "eth_blockNumber", [])
    assert result == "0x1"
    await client.aclose()


@respx.mock
async def test_call_falls_back_to_next_url_on_failure() -> None:
    respx.post("https://a.example").mock(return_value=httpx.Response(500))
    respx.post("https://b.example").mock(
        return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x2"})
    )
    client = make_client(["https://a.example", "https://b.example"])
    result = await client.call("call", "eth_blockNumber", [])
    assert result == "0x2"
    await client.aclose()


@respx.mock
async def test_call_raises_when_all_urls_fail() -> None:
    respx.post("https://a.example").mock(return_value=httpx.Response(500))
    client = make_client(["https://a.example"])
    with pytest.raises(RpcAllEndpointsExhaustedError):
        await client.call("call", "eth_blockNumber", [])
    await client.aclose()


@respx.mock
async def test_rate_limited_url_enters_cooldown_and_is_skipped_on_next_call() -> None:
    route_a = respx.post("https://a.example").mock(return_value=httpx.Response(429))
    respx.post("https://b.example").mock(
        return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x2"})
    )
    client = make_client(["https://a.example", "https://b.example"], cooldown=30.0)
    await client.call("call", "eth_blockNumber", [])
    await client.call("call", "eth_blockNumber", [])
    assert route_a.call_count == 1
    await client.aclose()


@respx.mock
async def test_batch_call_returns_results_in_request_order() -> None:
    respx.post("https://a.example").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"jsonrpc": "2.0", "id": 1, "result": "0x2"},
                {"jsonrpc": "2.0", "id": 0, "result": "0x1"},
            ],
        )
    )
    client = make_client(["https://a.example"])
    results = await client.batch_call(
        "call", [("eth_getBlockByNumber", ["0x1"]), ("eth_getBlockByNumber", ["0x2"])]
    )
    assert results == ["0x1", "0x2"]
    await client.aclose()


@respx.mock
async def test_batch_call_falls_back_to_next_url_when_item_has_error() -> None:
    respx.post("https://a.example").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"jsonrpc": "2.0", "id": 0, "result": "0x1"},
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32000, "message": "boom"},
                },
            ],
        )
    )
    respx.post("https://b.example").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"jsonrpc": "2.0", "id": 0, "result": "0x1"},
                {"jsonrpc": "2.0", "id": 1, "result": "0x2"},
            ],
        )
    )
    client = make_client(["https://a.example", "https://b.example"])
    results = await client.batch_call(
        "call", [("eth_getBlockByNumber", ["0x1"]), ("eth_getBlockByNumber", ["0x2"])]
    )
    assert results == ["0x1", "0x2"]
    await client.aclose()


@respx.mock
async def test_batch_call_raises_when_response_missing_requested_id() -> None:
    respx.post("https://a.example").mock(
        return_value=httpx.Response(
            200,
            json=[{"jsonrpc": "2.0", "id": 0, "result": "0x1"}],
        )
    )
    client = make_client(["https://a.example"])
    with pytest.raises(RpcAllEndpointsExhaustedError):
        await client.batch_call(
            "call",
            [("eth_getBlockByNumber", ["0x1"]), ("eth_getBlockByNumber", ["0x2"])],
        )
    await client.aclose()

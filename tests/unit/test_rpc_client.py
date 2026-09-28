import httpx
import pytest
import respx

from app.rpc.client import JsonRpcClient, LogQueryTooLargeError, RpcAllEndpointsExhaustedError


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
async def test_rate_limited_response_invokes_on_rate_limited_callback() -> None:
    respx.post("https://a.example").mock(return_value=httpx.Response(429))
    rate_limited_calls = 0

    def record_rate_limit() -> None:
        nonlocal rate_limited_calls
        rate_limited_calls += 1

    client = JsonRpcClient(
        urls_by_kind={"call": ("https://a.example",)},
        timeout_seconds=5,
        keepalive_timeout_seconds=5,
        rate_limit_cooldown_seconds=30.0,
        on_rate_limited=record_rate_limit,
    )
    with pytest.raises(RpcAllEndpointsExhaustedError):
        await client.call("call", "eth_blockNumber", [])
    assert rate_limited_calls == 1
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
async def test_call_raises_log_query_too_large_without_trying_second_url() -> None:
    route_a = respx.post("https://a.example").mock(
        return_value=httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32602,
                    "message": "Query returned more than 20000 results. Try with this "
                    "block range [0x0, 0x1]",
                },
            },
        )
    )
    route_b = respx.post("https://b.example").mock(
        return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": []})
    )
    client = JsonRpcClient(
        urls_by_kind={"log": ("https://a.example", "https://b.example")},
        timeout_seconds=5,
        keepalive_timeout_seconds=5,
        rate_limit_cooldown_seconds=30.0,
    )
    with pytest.raises(LogQueryTooLargeError):
        await client.call("log", "eth_getLogs", [])
    assert route_a.call_count == 1
    assert route_b.call_count == 0
    await client.aclose()


@respx.mock
async def test_call_raises_log_query_too_large_when_detail_is_in_data_field() -> None:
    route_a = respx.post("https://a.example").mock(
        return_value=httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32602,
                    "message": "invalid params",
                    "data": "Query returned more than 20000 results. Try with this "
                    "block range [0x0, 0x1].",
                },
            },
        )
    )
    route_b = respx.post("https://b.example").mock(
        return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": []})
    )
    client = JsonRpcClient(
        urls_by_kind={"log": ("https://a.example", "https://b.example")},
        timeout_seconds=5,
        keepalive_timeout_seconds=5,
        rate_limit_cooldown_seconds=30.0,
    )
    with pytest.raises(LogQueryTooLargeError):
        await client.call("log", "eth_getLogs", [])
    assert route_a.call_count == 1
    assert route_b.call_count == 0
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

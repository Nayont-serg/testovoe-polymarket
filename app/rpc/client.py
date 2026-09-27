from __future__ import annotations

import time
from typing import Any, Literal

import httpx

RpcKind = Literal["log", "receipt", "call"]


class RpcAllEndpointsExhaustedError(Exception):
    def __init__(self, kind: RpcKind, method: str) -> None:
        super().__init__(f"all {kind} RPC endpoints failed for {method}")


class LogQueryTooLargeError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


def _is_log_query_too_large(error: dict[str, object]) -> bool:
    haystack = f"{error.get('message', '')} {error.get('data', '')}".lower()
    return "query returned more than" in haystack and "results" in haystack


class JsonRpcClient:
    def __init__(
        self,
        urls_by_kind: dict[RpcKind, tuple[str, ...]],
        timeout_seconds: float,
        keepalive_timeout_seconds: float,
        rate_limit_cooldown_seconds: float,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._urls_by_kind = urls_by_kind
        self._cooldown_seconds = rate_limit_cooldown_seconds
        self._cooldown_until: dict[str, float] = {}
        self._http = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(keepalive_expiry=keepalive_timeout_seconds),
        )

    def _available_urls(self, kind: RpcKind) -> list[str]:
        now = time.monotonic()
        urls = [
            url for url in self._urls_by_kind[kind] if self._cooldown_until.get(url, 0.0) <= now
        ]
        return urls or list(self._urls_by_kind[kind])

    async def _post(self, url: str, payload: object) -> Any:
        response = await self._http.post(url, json=payload)
        if response.status_code == 429:
            self._cooldown_until[url] = time.monotonic() + self._cooldown_seconds
            raise httpx.HTTPStatusError("rate limited", request=response.request, response=response)
        response.raise_for_status()
        return response.json()

    async def call(self, kind: RpcKind, method: str, params: list[Any]) -> Any:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        last_error: Exception | None = None
        for url in self._available_urls(kind):
            try:
                body = await self._post(url, payload)
            except httpx.HTTPError as exc:
                last_error = exc
                continue
            if "error" in body:
                if _is_log_query_too_large(body["error"]):
                    raise LogQueryTooLargeError(str(body["error"]))
                last_error = RuntimeError(body["error"])
                continue
            return body["result"]
        raise RpcAllEndpointsExhaustedError(kind, method) from last_error

    async def batch_call(self, kind: RpcKind, requests: list[tuple[str, list[Any]]]) -> list[Any]:
        payload = [
            {"jsonrpc": "2.0", "id": index, "method": method, "params": params}
            for index, (method, params) in enumerate(requests)
        ]
        last_error: Exception | None = None
        for url in self._available_urls(kind):
            try:
                body = await self._post(url, payload)
            except httpx.HTTPError as exc:
                last_error = exc
                continue
            errors = [response_item["error"] for response_item in body if "error" in response_item]
            if errors:
                last_error = RuntimeError(errors[0])
                continue
            by_id = {response_item["id"]: response_item.get("result") for response_item in body}
            missing_ids = [index for index in range(len(requests)) if index not in by_id]
            if missing_ids:
                last_error = RuntimeError(f"batch response missing ids {missing_ids}")
                continue
            return [by_id[index] for index in range(len(requests))]
        raise RpcAllEndpointsExhaustedError(kind, requests[0][0]) from last_error

    async def aclose(self) -> None:
        await self._http.aclose()

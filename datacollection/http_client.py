from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class ProviderError(RuntimeError):
    """Raised when a provider returns an error payload or an HTTP failure."""


@dataclass
class HttpClient:
    headers: dict[str, str] | None = None
    min_interval: float = 0.0
    timeout: float = 60.0
    _last_request_at: float = field(default=0.0, init=False)
    _session: requests.Session = field(default_factory=requests.Session, init=False)

    def __post_init__(self) -> None:
        retry = Retry(
            total=5,
            backoff_factor=0.75,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        if self.headers:
            self._session.headers.update(self.headers)

    def get(self, url: str, **params: Any) -> requests.Response:
        wait = self.min_interval - (time.monotonic() - self._last_request_at)
        if wait > 0:
            time.sleep(wait)

        response = self._session.get(url, params=params or None, timeout=self.timeout)
        self._last_request_at = time.monotonic()
        response.raise_for_status()
        return response

    def get_json(self, url: str, **params: Any) -> dict[str, Any]:
        response = self.get(url, **params)
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise ProviderError(f"Expected JSON from {response.url}") from exc

        if isinstance(payload, dict):
            for key in ("Error Message", "Information", "Note"):
                if key in payload:
                    raise ProviderError(f"{key}: {payload[key]}")
        return payload

    def get_text(self, url: str, **params: Any) -> str:
        return self.get(url, **params).text

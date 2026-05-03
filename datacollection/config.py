from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependency is listed, fallback keeps imports usable.
    load_dotenv = None


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def load_environment() -> None:
    if load_dotenv is not None:
        load_dotenv(PACKAGE_ROOT / ".env")


load_environment()


def data_root() -> Path:
    configured = os.getenv("DATA_COLLECTION_ROOT", "./data")
    root = Path(configured)
    if not root.is_absolute():
        root = PACKAGE_ROOT / root
    root.mkdir(parents=True, exist_ok=True)
    return root


def sec_user_agent() -> str:
    return os.getenv(
        "SEC_USER_AGENT",
        "SenQuantDataCollection/0.1 configure-SEC_USER_AGENT@example.com",
    )


def alpha_vantage_api_key() -> str:
    return os.getenv("ALPHA_VANTAGE_API_KEY", "")

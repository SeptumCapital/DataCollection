from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .config import data_root, sec_user_agent
from .http_client import HttpClient
from .storage import write_dataframe, write_json


SEC_COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

COMMON_US_GAAP_TAGS = {
    "Revenues": "revenue",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "revenue",
    "SalesRevenueNet": "revenue",
    "CostOfRevenue": "cost_of_revenue",
    "CostOfGoodsAndServicesSold": "cost_of_revenue",
    "GrossProfit": "gross_profit",
    "OperatingIncomeLoss": "operating_income",
    "NetIncomeLoss": "net_income",
    "EarningsPerShareBasic": "eps_basic",
    "EarningsPerShareDiluted": "eps_diluted",
    "Assets": "assets",
    "AssetsCurrent": "current_assets",
    "Liabilities": "liabilities",
    "LiabilitiesCurrent": "current_liabilities",
    "StockholdersEquity": "stockholders_equity",
    "CashAndCashEquivalentsAtCarryingValue": "cash_and_equivalents",
    "NetCashProvidedByUsedInOperatingActivities": "operating_cash_flow",
    "PaymentsToAcquirePropertyPlantAndEquipment": "capex",
    "CommonStockSharesOutstanding": "shares_outstanding",
}


def fetch_company_facts(cik: str) -> dict[str, Any]:
    padded = str(cik).replace(".0", "").zfill(10)
    client = HttpClient(
        headers={
            "User-Agent": sec_user_agent(),
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov",
        },
        min_interval=0.11,
    )
    return client.get_json(SEC_COMPANY_FACTS_URL.format(cik=padded))


def company_facts_to_frame(payload: dict[str, Any], tags: dict[str, str] | None = None) -> pd.DataFrame:
    tags = tags or COMMON_US_GAAP_TAGS
    facts = payload.get("facts", {}).get("us-gaap", {})
    rows: list[dict[str, Any]] = []

    for tag, canonical_name in tags.items():
        tag_payload = facts.get(tag)
        if not tag_payload:
            continue
        for unit, entries in tag_payload.get("units", {}).items():
            for entry in entries:
                rows.append(
                    {
                        "cik": str(payload.get("cik", "")).zfill(10),
                        "entity_name": payload.get("entityName"),
                        "tag": tag,
                        "metric": canonical_name,
                        "label": tag_payload.get("label"),
                        "description": tag_payload.get("description"),
                        "unit": unit,
                        "value": entry.get("val"),
                        "start": entry.get("start"),
                        "end": entry.get("end"),
                        "fy": entry.get("fy"),
                        "fp": entry.get("fp"),
                        "form": entry.get("form"),
                        "filed": entry.get("filed"),
                        "accession": entry.get("accn"),
                        "frame": entry.get("frame"),
                    }
                )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(["metric", "end", "filed", "form"], na_position="last").reset_index(drop=True)


def save_company_facts(
    symbol: str,
    cik: str,
    output_dir: Path | None = None,
    save_raw: bool = True,
) -> tuple[Path | None, Path | None]:
    if output_dir is None:
        output_dir = data_root() / "fundamentals" / "sec_companyfacts"

    payload = fetch_company_facts(cik)
    frame = company_facts_to_frame(payload)

    safe_symbol = symbol.replace(".", "-")
    raw_path = None
    if save_raw:
        raw_path = write_json(output_dir / "raw" / f"{safe_symbol}.json", payload)

    facts_path = None
    if not frame.empty:
        frame.insert(0, "symbol", symbol)
        facts_path = write_dataframe(output_dir / "long" / f"{safe_symbol}.csv", frame)
    return facts_path, raw_path

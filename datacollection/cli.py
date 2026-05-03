from __future__ import annotations

import argparse
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from . import alpha_vantage
from .config import alpha_vantage_api_key, data_root
from .sec_fundamentals import save_company_facts
from .storage import ensure_dir, write_dataframe
from .technicals import compute_from_price_dir
from .universe import build_sp500_universe, save_sp500_universe
from .yahoo_enrichment import save_yahoo_enrichment_batch
from .yahoo_prices import fetch_yahoo_daily_prices


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_list(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def universe_path() -> Path:
    return data_root() / "universe" / "sp500_constituents.csv"


def load_or_create_universe(path: Path | None = None) -> pd.DataFrame:
    path = path or universe_path()
    if path.exists():
        return pd.read_csv(path, dtype={"cik": str})
    save_sp500_universe(path)
    return pd.read_csv(path, dtype={"cik": str})


def selected_universe(args: argparse.Namespace) -> pd.DataFrame:
    frame = load_or_create_universe(Path(args.universe) if args.universe else None)
    symbols = parse_list(args.symbols)
    if symbols:
        frame = frame[frame["symbol"].isin(symbols)]
    if args.limit:
        frame = frame.head(args.limit)
    return frame.reset_index(drop=True)


def record_errors(errors: list[dict[str, str]], name: str) -> None:
    if errors:
        write_dataframe(data_root() / "errors" / f"{name}.csv", pd.DataFrame(errors))


def cmd_universe(args: argparse.Namespace) -> None:
    output = Path(args.output) if args.output else universe_path()
    frame = build_sp500_universe(include_sec_exchange=not args.no_sec_exchange)
    write_dataframe(output, frame)
    print(f"Wrote {len(frame)} constituents to {output}")


def cmd_prices(args: argparse.Namespace) -> None:
    frame = selected_universe(args)
    output_dir = ensure_dir(data_root() / "prices" / "yahoo_daily")
    action_dir = ensure_dir(output_dir / "corporate_actions")
    errors: list[dict[str, str]] = []

    for row in tqdm(frame.to_dict("records"), desc="Yahoo daily prices"):
        symbol = row["symbol"]
        path = output_dir / f"{symbol.replace('.', '-')}.csv"
        if path.exists() and not args.overwrite:
            continue
        try:
            prices, actions = fetch_yahoo_daily_prices(
                symbol,
                start=parse_date(args.start),
                end=parse_date(args.end),
            )
            write_dataframe(path, prices)
            if not actions.empty:
                write_dataframe(action_dir / f"{symbol.replace('.', '-')}.csv", actions)
        except Exception as exc:  # noqa: BLE001 - continue batch collection and log failures.
            errors.append({"symbol": symbol, "stage": "prices", "error": str(exc)})
    record_errors(errors, "yahoo_prices")


def cmd_sec_fundamentals(args: argparse.Namespace) -> None:
    frame = selected_universe(args)
    errors: list[dict[str, str]] = []

    for row in tqdm(frame.to_dict("records"), desc="SEC company facts"):
        symbol = row["symbol"]
        cik = str(row.get("cik", "")).replace(".0", "").zfill(10)
        path = data_root() / "fundamentals" / "sec_companyfacts" / "long" / f"{symbol.replace('.', '-')}.csv"
        if path.exists() and not args.overwrite:
            continue
        try:
            save_company_facts(symbol=symbol, cik=cik, save_raw=not args.no_raw)
        except Exception as exc:  # noqa: BLE001
            errors.append({"symbol": symbol, "stage": "sec_fundamentals", "error": str(exc)})
    record_errors(errors, "sec_fundamentals")


def cmd_alpha_fundamentals(args: argparse.Namespace) -> None:
    frame = selected_universe(args)
    functions = tuple(parse_list(args.functions) or list(alpha_vantage.FUNDAMENTAL_FUNCTIONS))
    errors: list[dict[str, str]] = []

    for row in tqdm(frame.to_dict("records"), desc="Alpha Vantage fundamentals"):
        symbol = row.get("alpha_vantage_symbol") or row["symbol"]
        try:
            alpha_vantage.save_fundamentals(symbol, functions=functions, sleep_seconds=args.sleep)
            time.sleep(args.sleep)
        except Exception as exc:  # noqa: BLE001
            errors.append({"symbol": symbol, "stage": "alpha_fundamentals", "error": str(exc)})
    record_errors(errors, "alpha_fundamentals")


def cmd_alpha_technicals(args: argparse.Namespace) -> None:
    frame = selected_universe(args)
    functions = tuple(parse_list(args.functions) or list(alpha_vantage.TECHNICAL_FUNCTIONS))
    errors: list[dict[str, str]] = []

    for row in tqdm(frame.to_dict("records"), desc="Alpha Vantage technicals"):
        symbol = row.get("alpha_vantage_symbol") or row["symbol"]
        try:
            alpha_vantage.save_technical_indicators(symbol, functions=functions, sleep_seconds=args.sleep)
            time.sleep(args.sleep)
        except Exception as exc:  # noqa: BLE001
            errors.append({"symbol": symbol, "stage": "alpha_technicals", "error": str(exc)})
    record_errors(errors, "alpha_technicals")


def cmd_alpha_sentiment(args: argparse.Namespace) -> None:
    frame = selected_universe(args)
    errors: list[dict[str, str]] = []

    for row in tqdm(frame.to_dict("records"), desc="Alpha Vantage sentiment"):
        symbol = row.get("alpha_vantage_symbol") or row["symbol"]
        path = data_root() / "alpha_vantage" / "sentiment" / f"{symbol.replace('.', '-')}.json"
        if path.exists() and not args.overwrite:
            continue
        try:
            alpha_vantage.save_news_sentiment(
                symbol,
                time_from=args.time_from,
                time_to=args.time_to,
                limit=args.limit_per_symbol,
                sort=args.sort,
                sleep_seconds=args.sleep,
            )
            time.sleep(args.sleep)
        except Exception as exc:  # noqa: BLE001
            errors.append({"symbol": symbol, "stage": "alpha_sentiment", "error": str(exc)})
    record_errors(errors, "alpha_sentiment")


def cmd_local_technicals(args: argparse.Namespace) -> None:
    price_dir = Path(args.price_dir) if args.price_dir else data_root() / "prices" / "yahoo_daily"
    output_dir = Path(args.output_dir) if args.output_dir else data_root() / "technicals" / "from_yahoo_daily"
    written = compute_from_price_dir(price_dir=price_dir, output_dir=output_dir)
    print(f"Wrote {len(written)} technical feature files to {output_dir}")


def cmd_yahoo_enrichment(args: argparse.Namespace) -> None:
    frame = selected_universe(args)
    summaries = save_yahoo_enrichment_batch(
        frame["symbol"].astype(str).tolist(),
        sleep_seconds=args.sleep,
        overwrite=args.overwrite,
    )
    print(f"Wrote Yahoo enrichment for {len(summaries)} symbols")


def cmd_all(args: argparse.Namespace) -> None:
    cmd_universe(argparse.Namespace(output=None, no_sec_exchange=False))
    shared = argparse.Namespace(
        universe=None,
        symbols=args.symbols,
        limit=args.limit,
        start=args.start,
        end=args.end,
        overwrite=args.overwrite,
    )
    cmd_prices(shared)
    cmd_sec_fundamentals(
        argparse.Namespace(
            universe=None,
            symbols=args.symbols,
            limit=args.limit,
            no_raw=False,
            overwrite=args.overwrite,
        )
    )
    cmd_local_technicals(argparse.Namespace(price_dir=None, output_dir=None))
    if args.include_yahoo_enrichment:
        cmd_yahoo_enrichment(
            argparse.Namespace(
                universe=None,
                symbols=args.symbols,
                limit=args.limit,
                sleep=args.yahoo_sleep,
                overwrite=args.overwrite,
            )
        )

    if args.include_alpha:
        if not alpha_vantage_api_key():
            raise SystemExit("Set ALPHA_VANTAGE_API_KEY before running --include-alpha.")
        alpha_base = argparse.Namespace(
            universe=None,
            symbols=args.symbols,
            limit=args.limit,
            sleep=args.sleep,
            overwrite=args.overwrite,
        )
        cmd_alpha_fundamentals(argparse.Namespace(**vars(alpha_base), functions=None))
        cmd_alpha_technicals(argparse.Namespace(**vars(alpha_base), functions=None))
        cmd_alpha_sentiment(
            argparse.Namespace(
                **vars(alpha_base),
                time_from=args.sentiment_time_from,
                time_to=args.sentiment_time_to,
                limit_per_symbol=args.sentiment_limit,
                sort="EARLIEST",
            )
        )


def add_common_selection(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--universe", help="Path to a constituents CSV. Defaults to data/universe/sp500_constituents.csv.")
    parser.add_argument("--symbols", help="Comma-separated ticker subset, for testing or resuming.")
    parser.add_argument("--limit", type=int, help="Limit to the first N selected rows.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect archived market data for all S&P 500 stocks.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    universe = subparsers.add_parser("universe", help="Download the current S&P 500 constituents list.")
    universe.add_argument("--output")
    universe.add_argument("--no-sec-exchange", action="store_true")
    universe.set_defaults(func=cmd_universe)

    prices = subparsers.add_parser("prices", help="Download Yahoo chart API daily OHLCV archives.")
    add_common_selection(prices)
    prices.add_argument("--start", default="1990-01-01")
    prices.add_argument("--end", default=date.today().isoformat())
    prices.add_argument("--overwrite", action="store_true")
    prices.set_defaults(func=cmd_prices)

    sec = subparsers.add_parser("sec-fundamentals", help="Download SEC XBRL companyfacts fundamentals.")
    add_common_selection(sec)
    sec.add_argument("--no-raw", action="store_true", help="Skip raw JSON saves.")
    sec.add_argument("--overwrite", action="store_true")
    sec.set_defaults(func=cmd_sec_fundamentals)

    avf = subparsers.add_parser("alpha-fundamentals", help="Download Alpha Vantage company fundamentals.")
    add_common_selection(avf)
    avf.add_argument("--functions", help="Comma-separated Alpha Vantage fundamental functions.")
    avf.add_argument("--sleep", type=float, default=12.5, help="Seconds between API calls.")
    avf.set_defaults(func=cmd_alpha_fundamentals)

    avt = subparsers.add_parser("alpha-technicals", help="Download Alpha Vantage technical indicators.")
    add_common_selection(avt)
    avt.add_argument("--functions", help="Comma-separated indicators, e.g. SMA,RSI,MACD.")
    avt.add_argument("--sleep", type=float, default=12.5, help="Seconds between API calls.")
    avt.set_defaults(func=cmd_alpha_technicals)

    avs = subparsers.add_parser("alpha-sentiment", help="Download Alpha Vantage market news and sentiment.")
    add_common_selection(avs)
    avs.add_argument("--time-from", help="YYYYMMDDTHHMM, e.g. 20200101T0000.")
    avs.add_argument("--time-to", help="YYYYMMDDTHHMM.")
    avs.add_argument("--limit-per-symbol", type=int, default=1000)
    avs.add_argument("--sort", default="EARLIEST", choices=("LATEST", "EARLIEST", "RELEVANCE"))
    avs.add_argument("--sleep", type=float, default=12.5, help="Seconds between API calls.")
    avs.add_argument("--overwrite", action="store_true")
    avs.set_defaults(func=cmd_alpha_sentiment)

    local = subparsers.add_parser("local-technicals", help="Compute technical features from archived daily prices.")
    local.add_argument("--price-dir")
    local.add_argument("--output-dir")
    local.set_defaults(func=cmd_local_technicals)

    enrichment = subparsers.add_parser("yahoo-enrichment", help="Download Yahoo ownership, insider, analyst, and estimate tables.")
    add_common_selection(enrichment)
    enrichment.add_argument("--sleep", type=float, default=0.25, help="Seconds between symbols.")
    enrichment.add_argument("--overwrite", action="store_true")
    enrichment.set_defaults(func=cmd_yahoo_enrichment)

    all_cmd = subparsers.add_parser("all", help="Run universe, prices, SEC fundamentals, and local technicals.")
    all_cmd.add_argument("--symbols", help="Comma-separated ticker subset, for testing or resuming.")
    all_cmd.add_argument("--limit", type=int, help="Limit to the first N selected rows.")
    all_cmd.add_argument("--start", default="1990-01-01")
    all_cmd.add_argument("--end", default=date.today().isoformat())
    all_cmd.add_argument("--overwrite", action="store_true")
    all_cmd.add_argument("--include-alpha", action="store_true", help="Also run Alpha Vantage fundamentals, technicals, and sentiment.")
    all_cmd.add_argument("--include-yahoo-enrichment", action="store_true", help="Also collect Yahoo ownership, insider, analyst, and estimate tables.")
    all_cmd.add_argument("--yahoo-sleep", type=float, default=0.25, help="Seconds between Yahoo enrichment symbols.")
    all_cmd.add_argument("--sleep", type=float, default=12.5, help="Seconds between Alpha Vantage API calls.")
    all_cmd.add_argument("--sentiment-time-from")
    all_cmd.add_argument("--sentiment-time-to")
    all_cmd.add_argument("--sentiment-limit", type=int, default=1000)
    all_cmd.set_defaults(func=cmd_all)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

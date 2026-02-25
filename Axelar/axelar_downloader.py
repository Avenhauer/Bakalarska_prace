#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AxelarScan GMP downloader: search, single tx, and batch mode (150-per-CSV chunks).
- Respects API offset cap (150) by shifting a time window backwards in batch mode.
- Exports JSON and/or flattened CSV suitable for Excel/Pandas.
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import requests  # type: ignore[import-not-found]
    HTTPError = requests.HTTPError
    RequestException = requests.RequestException
except ModuleNotFoundError:
    requests = None
    from urllib import error as urllib_error
    from urllib import parse as urllib_parse
    from urllib import request as urllib_request

    HTTPError = urllib_error.HTTPError
    RequestException = urllib_error.URLError

# ---------------------------------------------------------------------------

BASE_URL = "https://api.axelarscan.io/gmp"
DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 25
MAX_FROM_OFFSET = 150  # API cap for 'from' (offset)

class AxelarscanError(RuntimeError):
    """Raised when the Axelarscan API reports an error condition."""

# ---------------------------------------------------------------------------

def call_axelarscan(params: Dict[str, Any], *, retries: int = 3, backoff: float = 1.5) -> Dict[str, Any]:
    """
    Call the Axelarscan GMP endpoint and return the parsed JSON payload.

    Parameters
    ----------
    params: dict
        Query parameters to send to the API. The `method` key will be injected automatically.
    retries: int
        Number of retry attempts on transient HTTP errors.
    backoff: float
        Multiplier (in seconds) between retry attempts.
    """
    params = {"method": "searchGMP", **params}
    attempt = 0
    while True:
        attempt += 1
        try:
            if requests is not None:
                response = requests.get(BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                payload: Dict[str, Any] = response.json()
            else:
                url = f"{BASE_URL}?{urllib_parse.urlencode(params)}"
                with urllib_request.urlopen(url, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8"))
        except HTTPError:
            if attempt > retries:
                raise
            sleep_for = backoff ** (attempt - 1)
            time.sleep(sleep_for)
            continue
        except RequestException:
            if attempt > retries:
                raise
            sleep_for = backoff ** (attempt - 1)
            time.sleep(sleep_for)
            continue
        if payload.get("error"):
            raise AxelarscanError(json.dumps(payload, ensure_ascii=False))
        return payload

def iterate_transactions(
    *,
    filters: Optional[Dict[str, Any]] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    limit: Optional[int] = None,
) -> Iterable[Dict[str, Any]]:
    """Yield GMP transactions using the `searchGMP` method with automatic pagination."""
    if page_size <= 0:
        raise ValueError("page_size must be positive")
    if page_size > MAX_PAGE_SIZE:
        raise ValueError(f"page_size cannot exceed {MAX_PAGE_SIZE} per API limits")

    filters = {**(filters or {})}
    offset = int(filters.pop("from", 0))
    fetched = 0
    effective_limit = min(limit, MAX_FROM_OFFSET) if limit is not None else MAX_FROM_OFFSET
    warn_on_truncation = limit is None or (limit is not None and limit > MAX_FROM_OFFSET)

    while True:
        if offset >= MAX_FROM_OFFSET:
            if warn_on_truncation:
                print(
                    f"Reached API offset cap of {MAX_FROM_OFFSET}. Narrow the window with --from-time/--to-time to retrieve more records.",
                    file=sys.stderr,
                )
            break

        params = {"from": offset, "size": page_size, **filters}
        payload = call_axelarscan(params)
        data: List[Dict[str, Any]] = payload.get("data", [])
        total = payload.get("total")

        if not data:
            break

        for tx in data:
            yield tx
            fetched += 1
            if effective_limit is not None and fetched >= effective_limit:
                if warn_on_truncation:
                    print(
                        f"Stopped after {fetched} records due to the API offset cap ({MAX_FROM_OFFSET}). Narrow the window with --from-time/--to-time for additional results.",
                        file=sys.stderr,
                    )
                return

        offset += len(data)
        if total is not None and offset >= int(total):
            break

        # Backend enforces a modest rate-limit; sleep a bit to be friendly.
        time.sleep(0.25)

def fetch_transaction(tx_hash: str) -> Optional[Dict[str, Any]]:
    """Retrieve a single transaction (if present) by its message/tx hash."""
    payload = call_axelarscan({"txHash": tx_hash, "size": 1})
    records = payload.get("data", [])
    if records:
        return records[0]
    return None

# ---------------------------------------------------------------------------

def write_json(data: List[Dict[str, Any]], path: Path) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))

def write_csv(data: List[Dict[str, Any]], path: Path) -> None:
    if not data:
        path.write_text("")
        return

    # Flatten a few frequently used top-level fields. Nested data will be stored as JSON strings.
    fieldnames = [
        "message_id",
        "status",
        "simplified_status",
        "symbol",
        "amount",
        "value",
        "source_chain",
        "destination_chain",
        "call.chain",
        "call.transactionHash",
        "created_at.ms",
    ]

    # Build rows.
    rows: List[Dict[str, Any]] = []
    for tx in data:
        row: Dict[str, Any] = {}
        row["message_id"] = tx.get("message_id")
        row["status"] = tx.get("status")
        row["simplified_status"] = tx.get("simplified_status")
        row["symbol"] = tx.get("symbol")
        row["amount"] = tx.get("amount")
        row["value"] = tx.get("value")
        row["source_chain"] = tx.get("source_chain") or tx.get("call", {}).get("chain")
        row["destination_chain"] = tx.get("destination_chain") or tx.get("call", {}).get("destination_chain")
        row["call.chain"] = tx.get("call", {}).get("chain")
        row["call.transactionHash"] = tx.get("call", {}).get("transactionHash")
        row["created_at.ms"] = tx.get("created_at", {}).get("ms")

        # Add any remaining keys (flattened JSON strings so nothing is dropped).
        for key, value in tx.items():
            if key in row:
                continue
            row[key] = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value

        rows.append(row)

    # Ensure all columns are represented.
    all_columns = list(dict.fromkeys(fieldnames + [key for row in rows for key in row.keys()]))
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=all_columns)
        writer.writeheader()
        writer.writerows(rows)

# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------

def parse_filters(args: argparse.Namespace) -> Dict[str, Any]:
    filters: Dict[str, Any] = {}
    if args.event:
        filters["event"] = args.event
    if args.source_chain:
        filters["sourceChain"] = args.source_chain
    if args.destination_chain:
        filters["destinationChain"] = args.destination_chain
    if args.status:
        filters["status"] = args.status
    if args.sender:
        filters["senderAddress"] = args.sender
    if args.contract:
        filters["contractAddress"] = args.contract
    if args.from_time:
        filters["fromTime"] = args.from_time
    if args.to_time:
        filters["toTime"] = args.to_time
    if args.sort:
        filters["sort"] = args.sort
    return filters

def add_search_arguments(target: argparse._ActionsContainer) -> None:
    target.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Items per request (max: 25, default: 25)")
    target.add_argument("--limit", type=int, help="Stop after N records (omit for full dataset).")
    target.add_argument("--event", help="Filter by event type, e.g. ContractCallWithToken")
    target.add_argument("--source-chain", help="Filter by source chain name (case sensitive)")
    target.add_argument("--destination-chain", help="Filter by destination chain name")
    target.add_argument("--status", help="Filter by status (executed, failed, etc.)")
    target.add_argument("--sender", help="Filter by sender address")
    target.add_argument("--contract", help="Filter by contract address")
    target.add_argument("--from-time", type=int, help="Unix timestamp (seconds) for start of window")
    target.add_argument("--to-time", type=int, help="Unix timestamp (seconds) for end of window")
    target.add_argument("--sort", help="Sort expression supported by the API (e.g. '-created_at')")
    target.add_argument("--out-json", type=Path, help="Write the complete response list to JSON")
    target.add_argument("--out-csv", type=Path, help="Write a flattened CSV alongside the JSON output")

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download Axelar GMP transactions via the public API.")

    parser.set_defaults(
        command="search",
        page_size=DEFAULT_PAGE_SIZE,
        limit=None,
        event=None,
        source_chain=None,
        destination_chain=None,
        status=None,
        sender=None,
        contract=None,
        from_time=None,
        to_time=None,
        sort=None,
        out_json=None,
        out_csv=None,
    )

    add_search_arguments(parser)
    subparsers = parser.add_subparsers(dest="command", required=False)

    # `search` command (default)
    search_parser = subparsers.add_parser("search", help="Iterate through transactions with optional filters.")
    search_parser.set_defaults(command="search")
    add_search_arguments(search_parser)

    # `tx` command to fetch a single transaction
    tx_parser = subparsers.add_parser("tx", help="Fetch the most recent record for a transaction hash / message id.")
    tx_parser.add_argument("tx_hash", help="Transaction hash or message id, e.g. 0x...-16")
    tx_parser.add_argument("--out-json", type=Path, help="Write the response to JSON")

    # `batch` command – download in 150-row CSV chunks into a directory
    batch_parser = subparsers.add_parser("batch", help="Download transactions in 150-row CSV batches into a directory.")
    batch_parser.set_defaults(command="batch")
    add_search_arguments(batch_parser)
    batch_parser.add_argument("--out-dir", type=Path, required=True, help="Output directory for CSV batches.")
    batch_parser.add_argument("--target-total", type=int, default=5000, help="Total rows to collect (default: 5000).")
    batch_parser.add_argument("--batch-size", type=int, default=MAX_FROM_OFFSET,
                              help=f"Rows per CSV (max {MAX_FROM_OFFSET}, default: {MAX_FROM_OFFSET}).")
    batch_parser.add_argument("--start-to-time", type=int, help="Unix seconds to start from (default: now).")
    batch_parser.add_argument("--hard-from-time", type=int, help="Stop when the window goes older than this (Unix seconds).")

    return parser

# ---------------------------------------------------------------------------
# Batch helpers
# ---------------------------------------------------------------------------

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def min_created_at_seconds(batch: List[Dict[str, Any]]) -> Optional[int]:
    """
    Najdi nejmenší created_at (ms) v dávce a vrať v sekundách.
    Pokud není k dispozici, vrať None.
    """
    ms_vals: List[int] = []
    for tx in batch:
        ms = None
        if isinstance(tx.get("created_at"), dict):
            ms = tx["created_at"].get("ms")
        if ms is None and "created_at.ms" in tx:
            try:
                ms = int(tx["created_at.ms"])
            except Exception:
                pass
        if isinstance(ms, (int, float)):
            ms_vals.append(int(ms))
    if not ms_vals:
        return None
    return max(0, min(ms_vals) // 1000)

def write_csv_batch(data: List[Dict[str, Any]], out_path: Path) -> None:
    write_csv(data, out_path)

def download_in_batches(
    out_dir: Path,
    *,
    base_filters: Optional[Dict[str, Any]] = None,
    batch_size: int = MAX_FROM_OFFSET,   # 150
    target_total: int = 5000,
    start_to_time: Optional[int] = None, # unix seconds; default "now"
    hard_from_time: Optional[int] = None # stop when older than this
) -> int:
    """
    Download transactions in batches so that each CSV has <= 150 rows (API cap).
    Walk backwards in time by setting `toTime` to (oldest_in_batch - 1s) each loop.
    Returns total downloaded rows across all batches.
    """
    ensure_dir(out_dir)
    filters = dict(base_filters or {})
    # enforce sorting by newest first; API expects '-created_at' for descending
    filters["sort"] = filters.get("sort") or "-created_at"

    # starting point (toTime)
    if start_to_time is None:
        start_to_time = int(time.time())
    current_to = start_to_time

    total = 0
    part = 0
    page_size = min(DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE)  # 25
    per_batch_limit = min(max(1, batch_size), MAX_FROM_OFFSET)

    while total < target_total:
        window_filters = dict(filters)
        window_filters["toTime"] = current_to
        if hard_from_time is not None:
            window_filters["fromTime"] = hard_from_time

        batch = list(
            iterate_transactions(
                filters=window_filters,
                page_size=page_size,
                limit=per_batch_limit,
            )
        )
        if not batch:
            # no data in this window -> stop
            break

        part += 1
        out_csv = out_dir / f"part_{part:04d}.csv"
        write_csv_batch(batch, out_csv)
        print(f"[batch {part:04d}] wrote {len(batch)} rows -> {out_csv}", file=sys.stderr)

        total += len(batch)
        if total >= target_total:
            break

        # compute next window's toTime as (oldest_in_batch - 1s)
        oldest_s = min_created_at_seconds(batch)
        if oldest_s is None:
            # cannot reliably continue without timestamps
            break
        next_to = oldest_s - 1

        if hard_from_time is not None and next_to < hard_from_time:
            break

        current_to = next_to

        time.sleep(0.25)  # be nice to the backend

    return total

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_search(args: argparse.Namespace) -> int:
    filters = parse_filters(args)
    data = list(
        iterate_transactions(
            filters=filters,
            page_size=args.page_size,
            limit=args.limit,
        )
    )

    if args.out_json:
        write_json(data, args.out_json)
        print(f"Wrote {len(data)} records to {args.out_json}")

    if args.out_csv:
        write_csv(data, args.out_csv)
        print(f"Wrote flattened CSV to {args.out_csv}")

    if not args.out_json and not args.out_csv:
        json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")

    return 0

def cmd_tx(args: argparse.Namespace) -> int:
    record = fetch_transaction(args.tx_hash)
    if record is None:
        print("No transaction found", file=sys.stderr)
        return 1

    if args.out_json:
        write_json([record], args.out_json)
        print(f"Wrote transaction to {args.out_json}")
    else:
        json.dump(record, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")

    return 0

def cmd_batch(args: argparse.Namespace) -> int:
    # take only filter args that make sense; sort enforced within download_in_batches
    filters = parse_filters(args)
    total = download_in_batches(
        args.out_dir,
        base_filters=filters,
        batch_size=args.batch_size,
        target_total=args.target_total,
        start_to_time=args.start_to_time,
        hard_from_time=args.hard_from_time,
    )
    print(f"Downloaded total {total} rows into directory {args.out_dir}", file=sys.stderr)
    return 0

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    command = args.command or "search"

    try:
        if command == "search":
            return cmd_search(args)
        if command == "tx":
            return cmd_tx(args)
        if command == "batch":
            return cmd_batch(args)
    except AxelarscanError as exc:
        print(f"Axelarscan API reported an error: {exc}", file=sys.stderr)
        return 1
    except RequestException as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        return 1

    parser.print_help()
    return 1

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

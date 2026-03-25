import argparse
import base64
import csv
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import requests


# -----------------------------
# Helpers
# -----------------------------
def http_get_json(url: str, params: Optional[dict] = None, timeout: int = 30, retries: int = 5) -> dict:
    last_err = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep_s = min(2 ** i, 10) + 0.1
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {retries} retries: {url} params={params} err={last_err}")


def parse_rfc3339(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    # Cosmos usually: 2026-03-04T12:59:58.123456Z
    try:
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def b64_to_hex(b64_str: str) -> str:
    try:
        return base64.b64decode(b64_str).hex()
    except Exception:
        return ""


def events_to_map(events: List[dict]) -> Dict[str, Dict[str, List[str]]]:
    """
    Convert CometBFT events:
      [{"type": "...", "attributes":[{"key":"...","value":"...","index":true}, ...]}, ...]
    to:
      {type: {key: [values...]}}
    Keys/values from tx_search are base64 encoded in some endpoints? In cosmos.directory RPC they are plain strings.
    We'll treat them as plain.
    """
    out: Dict[str, Dict[str, List[str]]] = {}
    for ev in events or []:
        et = ev.get("type")
        if not et:
            continue
        out.setdefault(et, {})
        for a in ev.get("attributes", []) or []:
            k = a.get("key")
            v = a.get("value")
            if k is None or v is None:
                continue
            out[et].setdefault(k, []).append(v)
    return out


def first_attr(evmap: Dict[str, Dict[str, List[str]]], etype: str, key: str) -> Optional[str]:
    vals = evmap.get(etype, {}).get(key)
    return vals[0] if vals else None


# -----------------------------
# Chain registry lookup
# -----------------------------
GITHUB_API_IBC_DIR = "https://api.github.com/repos/cosmos/chain-registry/contents/_IBC"
GITHUB_REF = "master"


def load_ibc_registry() -> Tuple[List[dict], Dict[Tuple[str, str, str], Tuple[str, str, str]]]:
    """
    Returns:
      - raw index of _IBC files
      - mapping (chain_name, channel_id, port_id) -> (counterparty_chain_name, counterparty_channel_id, counterparty_port_id)
    """
    idx = http_get_json(GITHUB_API_IBC_DIR, params={"ref": GITHUB_REF}, retries=5)
    files = [x for x in idx if x.get("type") == "file" and (x.get("name") or "").endswith(".json")]

    m: Dict[Tuple[str, str, str], Tuple[str, str, str]] = {}
    last_ok = 0

    for f in files:
        raw_url = f.get("download_url")
        if not raw_url:
            continue

        try:
            data = http_get_json(raw_url, retries=3)
        except Exception:
            continue

        c1 = data.get("chain_1", {}) or {}
        c2 = data.get("chain_2", {}) or {}
        chain1 = c1.get("chain_name")
        chain2 = c2.get("chain_name")

        for ch in data.get("channels", []) or []:
            a = ch.get("chain_1", {}) or {}
            b = ch.get("chain_2", {}) or {}

            chan1 = a.get("channel_id")
            port1 = a.get("port_id")
            chan2 = b.get("channel_id")
            port2 = b.get("port_id")

            if chain1 and chain2 and chan1 and port1 and chan2 and port2:
                m[(chain1, chan1, port1)] = (chain2, chan2, port2)
                m[(chain2, chan2, port2)] = (chain1, chan1, port1)

        last_ok += 1

    print(f"Loaded _IBC registry files: {last_ok}/{len(files)} | links: {len(m):,}")
    return idx, m


# -----------------------------
# RPC / REST endpoints via cosmos.directory
# -----------------------------
def rpc_tx_search(chain_name: str, query: str, page: int = 1, per_page: int = 100, order_by: str = "desc") -> dict:
    # order_by must be quoted for comet rpc: "asc"/"desc"
    url = f"https://rpc.cosmos.directory/{chain_name}/tx_search"
    params = {
        "query": f"\"{query}\"",
        "prove": "false",
        "page": str(page),
        "per_page": str(per_page),
        "order_by": f"\"{order_by}\"",
    }
    return http_get_json(url, params=params)


def rest_get_tx(chain_name: str, txhash: str) -> Optional[dict]:
    # Cosmos SDK REST GetTx
    url = f"https://rest.cosmos.directory/{chain_name}/cosmos/tx/v1beta1/txs/{txhash}"
    try:
        return http_get_json(url)
    except Exception:
        return None


# -----------------------------
# IBC extraction logic
# -----------------------------
def extract_send_packet_fields(tx: dict) -> Dict[str, Any]:
    """
    tx is one element from tx_search result["txs"].
    """
    txhash = tx.get("hash")
    height = int(tx.get("height")) if tx.get("height") else None
    evs = tx.get("tx_result", {}).get("events", [])
    evmap = events_to_map(evs)

    # Primary IBC send_packet event attributes
    src_port = first_attr(evmap, "send_packet", "packet_src_port")
    src_channel = first_attr(evmap, "send_packet", "packet_src_channel")
    dst_port = first_attr(evmap, "send_packet", "packet_dst_port")
    dst_channel = first_attr(evmap, "send_packet", "packet_dst_channel")
    seq = first_attr(evmap, "send_packet", "packet_sequence")
    timeout_height = first_attr(evmap, "send_packet", "packet_timeout_height")
    timeout_ts = first_attr(evmap, "send_packet", "packet_timeout_timestamp")
    conn_id = first_attr(evmap, "send_packet", "connection_id")
    packet_data_hex = first_attr(evmap, "send_packet", "packet_data_hex")

    # Sometimes there's ibc_transfer event with denom/amount/sender/receiver
    denom = first_attr(evmap, "ibc_transfer", "denom")
    amount = first_attr(evmap, "ibc_transfer", "amount")
    sender = first_attr(evmap, "ibc_transfer", "sender")
    receiver = first_attr(evmap, "ibc_transfer", "receiver")

    return {
        "send_txhash": txhash,
        "send_height": height,
        "packet_sequence": seq,
        "src_port": src_port,
        "src_channel": src_channel,
        "dst_port": dst_port,
        "dst_channel": dst_channel,
        "connection_id": conn_id,
        "timeout_height": timeout_height,
        "timeout_timestamp": timeout_ts,
        "packet_data_hex": packet_data_hex,
        "denom": denom,
        "amount": amount,
        "sender": sender,
        "receiver": receiver,
    }


def enrich_with_rest_tx(chain_name: str, txhash: str) -> Dict[str, Any]:
    """
    Adds timestamp, gas, fee.
    """
    out: Dict[str, Any] = {}
    data = rest_get_tx(chain_name, txhash)
    if not data:
        return out

    txr = data.get("tx_response") or {}
    out["tx_timestamp"] = txr.get("timestamp")
    out["gas_wanted"] = txr.get("gas_wanted")
    out["gas_used"] = txr.get("gas_used")
    out["tx_code"] = txr.get("code")

    # fee in auth_info
    fee_amount = None
    fee_denom = None
    try:
        fee = (data.get("tx") or {}).get("auth_info", {}).get("fee", {})
        amts = fee.get("amount") or []
        if amts:
            fee_amount = amts[0].get("amount")
            fee_denom = amts[0].get("denom")
    except Exception:
        pass

    out["fee_amount"] = fee_amount
    out["fee_denom"] = fee_denom
    return out


def find_matching_on_chain(
    chain_name: str,
    event_type: str,
    seq: str,
    channel_key: str,
    channel_val: str,
    port_key: str,
    port_val: str,
    order_by: str = "asc",
) -> Optional[Dict[str, Any]]:
    """
    Find earliest/latest matching tx for recv/ack by searching on event attributes.
    Returns {txhash,height,timestamp,gas_used,fee...} if found.
    """
    if not seq or not channel_val:
        return None

    q = f"{event_type}.packet_sequence='{seq}' AND {event_type}.{channel_key}='{channel_val}' AND {event_type}.{port_key}='{port_val}'"
    try:
        res = rpc_tx_search(chain_name, q, page=1, per_page=1, order_by=order_by)
    except Exception as e:
        return {
            "error": str(e),
            "chain": chain_name,
            "query": q,
        }
    txs = (res.get("result") or {}).get("txs") or []
    if not txs:
        return None

    tx0 = txs[0]
    txhash = tx0.get("hash")
    height = tx0.get("height")
    enriched = enrich_with_rest_tx(chain_name, txhash) if txhash else {}
    return {
        "txhash": txhash,
        "height": height,
        **{f"{event_type}_query": q},
        **enriched,
    }


def main():
    ap = argparse.ArgumentParser(description="Download IBC send/recv/ack sample into CSV (cosmos.directory RPC/REST + chain-registry mapping).")
    ap.add_argument("--src_chain", required=True, help="Source chain name in chain-registry (e.g., cosmoshub, osmosis, juno)")
    ap.add_argument("--limit", type=int, default=2500, help="How many send_packet txs to collect")
    ap.add_argument("--per_page", type=int, default=100, help="tx_search per_page (max usually 100)")
    ap.add_argument("--output", default="ibc_sample.csv", help="Output CSV path")
    ap.add_argument("--sleep", type=float, default=0.2, help="Sleep between requests (seconds)")
    ap.add_argument("--no_ack", action="store_true", help="Skip ack lookup (faster)")
    args = ap.parse_args()

    print("Loading chain-registry _IBC/*.json ...")
    _, chan_map = load_ibc_registry()
    print(f"Registry links loaded: {len(chan_map):,}")

    src_chain = args.src_chain
    limit = args.limit
    per_page = args.per_page

    # 1) Collect send_packet txs
    print(f"Collecting up to {limit} send_packet txs from {src_chain} ...")
    send_rows: List[Dict[str, Any]] = []
    page = 1

    # We filter to transfer port to focus on ICS-20; you can remove if needed.
    base_query = "send_packet.packet_src_port='transfer'"

    while len(send_rows) < limit:
        res = rpc_tx_search(src_chain, base_query, page=page, per_page=per_page, order_by="desc")
        txs = (res.get("result") or {}).get("txs") or []
        if not txs:
            break

        for tx in txs:
            row = extract_send_packet_fields(tx)
            # basic guard
            if not row.get("packet_sequence") or not row.get("src_channel"):
                continue
            send_rows.append(row)
            if len(send_rows) >= limit:
                break

        print(f"page={page} fetched {len(txs)} txs -> total send_rows={len(send_rows)}")
        page += 1
        time.sleep(args.sleep)

        if len(txs) < per_page:
            break

    # 2) Enrich + map destination + find recv/ack
    print("Enriching rows with REST GetTx (timestamp/gas/fee) and mapping destination chain/channel ...")
    out_rows: List[Dict[str, Any]] = []

    for i, row in enumerate(send_rows, start=1):
        src_channel = row.get("src_channel")
        src_port = row.get("src_port") or "transfer"
        dst = chan_map.get((src_chain, src_channel, src_port))
        if dst:
            dst_chain, counterparty_channel, counterparty_port = dst
        else:
            dst_chain, counterparty_channel, counterparty_port = (None, None, None)

        row["dst_chain_guess"] = dst_chain
        row["dst_counterparty_channel"] = counterparty_channel
        row["dst_counterparty_port"] = counterparty_port

        # enrich send tx
        send_txhash = row.get("send_txhash")
        if send_txhash:
            send_extra = enrich_with_rest_tx(src_chain, send_txhash)
            row["send_timestamp"] = send_extra.get("tx_timestamp")
            row["send_gas_used"] = send_extra.get("gas_used")
            row["send_gas_wanted"] = send_extra.get("gas_wanted")
            row["send_fee_amount"] = send_extra.get("fee_amount")
            row["send_fee_denom"] = send_extra.get("fee_denom")
            row["send_code"] = send_extra.get("tx_code")

        # find recv on destination chain (if mapped)
        recv_info = None
        if dst_chain and counterparty_channel and row.get("packet_sequence"):
            # On dest chain, recv_packet typically indexes packet_dst_channel = dest-side channel id (counterparty_channel)
            recv_info = find_matching_on_chain(
                chain_name=dst_chain,
                event_type="recv_packet",
                seq=row["packet_sequence"],
                channel_key="packet_dst_channel",
                channel_val=counterparty_channel,
                port_key="packet_dst_port",
                port_val=row.get("dst_counterparty_port") or row.get("dst_port") or "transfer",
                order_by="asc",
            )
            # fallback attempt: some chains index differently; try src_channel key too
            if not recv_info:
                recv_info = find_matching_on_chain(
                    chain_name=dst_chain,
                    event_type="recv_packet",
                    seq=row["packet_sequence"],
                    channel_key="packet_src_channel",
                    channel_val=counterparty_channel,
                    port_key="packet_src_port",
                    port_val=row.get("dst_counterparty_port") or row.get("dst_port") or "transfer",
                    order_by="asc",
                )

        if isinstance(recv_info, dict) and recv_info.get("error"):
            row["recv_lookup_error"] = recv_info.get("error")
            row["recv_lookup_chain"] = recv_info.get("chain")
            row["recv_lookup_query"] = recv_info.get("query")
            recv_info = None

        if recv_info:
            row["recv_txhash"] = recv_info.get("txhash")
            row["recv_height"] = recv_info.get("height")
            row["recv_timestamp"] = recv_info.get("tx_timestamp")
            row["recv_gas_used"] = recv_info.get("gas_used")
            row["recv_fee_amount"] = recv_info.get("fee_amount")
            row["recv_fee_denom"] = recv_info.get("fee_denom")
            row["recv_code"] = recv_info.get("tx_code")

        # find ack on source chain (optional)
        if not args.no_ack and row.get("packet_sequence") and row.get("src_channel"):
            ack_info = None

            # Variant A: write_acknowledgement (common)
            ack_info = find_matching_on_chain(
                chain_name=src_chain,
                event_type="write_acknowledgement",
                seq=row["packet_sequence"],
                channel_key="packet_src_channel",
                channel_val=row["src_channel"],
                port_key="packet_src_port",
                port_val=row.get("src_port") or "transfer",
                order_by="asc",
            )

            # Variant B: acknowledge_packet (some chains)
            if not ack_info:
                ack_info = find_matching_on_chain(
                    chain_name=src_chain,
                    event_type="acknowledge_packet",
                    seq=row["packet_sequence"],
                    channel_key="packet_src_channel",
                    channel_val=row["src_channel"],
                    port_key="packet_src_port",
                    port_val=row.get("src_port") or "transfer",
                    order_by="asc",
                )

            if isinstance(ack_info, dict) and ack_info.get("error"):
                row["ack_lookup_error"] = ack_info.get("error")
                row["ack_lookup_chain"] = ack_info.get("chain")
                row["ack_lookup_query"] = ack_info.get("query")
                ack_info = None

            if ack_info:
                row["ack_txhash"] = ack_info.get("txhash")
                row["ack_height"] = ack_info.get("height")
                row["ack_timestamp"] = ack_info.get("tx_timestamp")
                row["ack_gas_used"] = ack_info.get("gas_used")
                row["ack_fee_amount"] = ack_info.get("fee_amount")
                row["ack_fee_denom"] = ack_info.get("fee_denom")
                row["ack_code"] = ack_info.get("tx_code")

        # compute latencies if timestamps exist
        t_send = parse_rfc3339(row.get("send_timestamp") or "")
        t_recv = parse_rfc3339(row.get("recv_timestamp") or "")
        t_ack = parse_rfc3339(row.get("ack_timestamp") or "")

        row["latency_send_to_recv_s"] = (t_recv - t_send).total_seconds() if (t_send and t_recv) else ""
        row["latency_send_to_ack_s"] = (t_ack - t_send).total_seconds() if (t_send and t_ack) else ""
        row["latency_recv_to_ack_s"] = (t_ack - t_recv).total_seconds() if (t_recv and t_ack) else ""

        # rough success flags (for reliability metric)
        row["has_recv"] = 1 if row.get("recv_txhash") else 0
        row["has_ack"] = 1 if row.get("ack_txhash") else 0

        out_rows.append(row)

        if i % 50 == 0:
            print(f"processed {i}/{len(send_rows)}")
        time.sleep(args.sleep)

    # 3) Write CSV
    # Define columns (stable order)
    cols = [
        "send_txhash", "send_height", "send_timestamp", "send_code",
        "packet_sequence", "src_port", "src_channel", "dst_port", "dst_channel", "connection_id",
        "timeout_height", "timeout_timestamp",
        "dst_chain_guess", "dst_counterparty_channel", "dst_counterparty_port",
        "denom", "amount", "sender", "receiver",
        "packet_data_hex",
        "send_gas_wanted", "send_gas_used", "send_fee_amount", "send_fee_denom",
        "recv_txhash", "recv_height", "recv_timestamp", "recv_code",
        "recv_gas_used", "recv_fee_amount", "recv_fee_denom",
        "recv_lookup_error", "recv_lookup_chain", "recv_lookup_query",
        "ack_txhash", "ack_height", "ack_timestamp", "ack_code",
        "ack_gas_used", "ack_fee_amount", "ack_fee_denom",
        "ack_lookup_error", "ack_lookup_chain", "ack_lookup_query",
        "latency_send_to_recv_s", "latency_send_to_ack_s", "latency_recv_to_ack_s",
        "has_recv", "has_ack",
    ]

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"\nDone. Wrote {len(out_rows)} rows to {args.output}")
    # quick reliability summary
    if out_rows:
        recv_rate = sum(r["has_recv"] for r in out_rows) / len(out_rows)
        ack_rate = sum(r["has_ack"] for r in out_rows) / len(out_rows)
        print(f"Recv rate: {recv_rate:.3f} | Ack rate: {ack_rate:.3f}")


if __name__ == "__main__":
    main()

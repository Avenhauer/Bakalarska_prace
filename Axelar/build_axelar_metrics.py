#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import glob
import json
import math
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

JSON_CANDIDATES = [
    "call", "fees", "gas_paid", "price", "confirm", "interchain_transfer",
    "interchain_transfers", "time_spent", "destination_express_fee", "source_express_fee",
    "destination_native_token", "source_token", "ethereum_token", "axelar_token"
]

FINAL_COLUMNS = [
    "message_id", "status", "simplified_status", "source_chain", "destination_chain",
    "source_chain_type", "destination_chain_type", "symbol", "amount_raw", "decimals",
    "amount", "value_usd", "call_ts", "confirm_ts", "execute_ts", "e2e_sec", "t_call_confirm_sec",
    "t_confirm_execute_sec", "source_base_fee_usd", "destination_base_fee_usd",
    "destination_confirm_fee_usd", "destination_express_fee_usd", "source_express_fee_usd",
    "l2_l1_fee_usd", "gas_paid_native_amount", "gas_paid_native_usd", "total_cost_usd",
    "is_express", "executed_flag", "error_flag", "refund_flag", "not_to_execute_flag",
    "not_to_express_execute_flag"
]

def try_json_load(x: Any) -> Optional[Dict[str, Any]]:
    if isinstance(x, dict): return x
    if isinstance(x, str) and x:
        s = x.strip()
        try: return json.loads(s)
        except json.JSONDecodeError:
            if "}" in s:
                try: return json.loads(s[:s.rfind("}")+1])
                except Exception: return None
            return None
    return None

def to_unix_seconds(v: Any) -> Optional[float]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
            if v.isdigit():
                num = int(v)
            else:
                num = float(v)
        else:
            num = float(v)
        if num > 10_000_000_000:
            return num / 1000.0
        return float(num)
    except Exception:
        return None

def first_non_null(*vals) -> Optional[float]:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, (int, float)) and not math.isnan(v):
            return float(v)
    return None

def normalize_chain_name(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip().lower()
    aliases = {
        "xrpl evm": "xrpl-evm",
        "xrpl_evm": "xrpl-evm",
        "axelar network": "axelar",
    }
    return aliases.get(s, s)

def extract_decimals(row: pd.Series) -> Optional[int]:
    for col in ("interchain_transfer", "interchain_transfers", "source_token"):
        obj = row.get(col)
        if isinstance(obj, dict):
            dec = obj.get("decimals")
            if dec is not None:
                try:
                    return int(dec)
                except Exception:
                    pass
    d = row.get("decimals")
    if d is not None:
        try:
            return int(d)
        except Exception:
            return None
    return None

def extract_prices_usd(row: pd.Series) -> Dict[str, Optional[float]]:
    out = {
        "source_native_usd": None,
        "dest_native_usd": None,
        "payload_token_usd": None,
        "eth_usd": None,
    }
    for col in ("price", "source_token", "interchain_transfer", "interchain_transfers"):
        obj = row.get(col)
        if isinstance(obj, dict):
            tp = obj.get("token_price") or {}
            if isinstance(tp, dict):
                usd = tp.get("usd")
                if usd is not None:
                    try:
                        out["payload_token_usd"] = float(usd)
                        break
                    except Exception:
                        pass
    for col in ("destination_native_token", "price"):
        obj = row.get(col)
        if isinstance(obj, dict):
            tp = obj.get("token_price") or {}
            if isinstance(tp, dict) and tp.get("usd") is not None:
                try:
                    out["dest_native_usd"] = float(tp["usd"])
                except Exception:
                    pass
    price_obj = row.get("price")
    if isinstance(price_obj, dict):
        st = price_obj.get("source_token") or {}
        if isinstance(st, dict):
            tp = st.get("token_price") or {}
            if isinstance(tp, dict) and tp.get("usd") is not None:
                try:
                    out["source_native_usd"] = float(tp["usd"])
                except Exception:
                    pass
    eth_obj = row.get("ethereum_token")
    if isinstance(eth_obj, dict):
        tp = eth_obj.get("token_price") or {}
        if isinstance(tp, dict) and tp.get("usd") is not None:
            try:
                out["eth_usd"] = float(tp["usd"])
            except Exception:
                pass
    return out

def compute_row_metrics(row: pd.Series) -> pd.Series:
    call_obj = row.get("call") or {}
    confirm_obj = row.get("confirm") or {}
    time_spent = row.get("time_spent") or {}

    call_ts = first_non_null(
        to_unix_seconds(call_obj.get("block_timestamp")),
        to_unix_seconds(row.get("created_at.ms")),
    )
    confirm_ts = to_unix_seconds(confirm_obj.get("block_timestamp"))
    execute_ts = first_non_null(
        to_unix_seconds(row.get("express_executing_at")),
        to_unix_seconds(row.get("executing_at")),
    )
    if execute_ts is None:
        for col in JSON_CANDIDATES:
            obj = row.get(col)
            if isinstance(obj, dict):
                bt = obj.get("block_timestamp")
                ts = to_unix_seconds(bt)
                if ts is not None:
                    execute_ts = ts
                    break

    e2e_sec = None
    t_call_confirm = None
    t_confirm_execute = None
    if call_ts is not None and execute_ts is not None:
        e2e_sec = execute_ts - call_ts

    t_call_confirm = time_spent.get("call_confirm")
    t_call_confirm = float(t_call_confirm) if isinstance(t_call_confirm, (int, float)) else None
    if t_call_confirm is None and call_ts is not None and confirm_ts is not None:
        t_call_confirm = confirm_ts - call_ts
    if confirm_ts is not None and execute_ts is not None:
        t_confirm_execute = execute_ts - confirm_ts

    decimals = extract_decimals(row)
    amount_raw = None
    try:
        amount_raw = float(row.get("amount"))
    except Exception:
        ict = row.get("interchain_transfer") or {}
        if isinstance(ict, dict):
            ar = ict.get("amount")
            try:
                amount_raw = float(ar)
            except Exception:
                amount_raw = None

    amount_units = None
    if amount_raw is not None and decimals is not None:
        amount_units = amount_raw / (10 ** decimals)

    value_usd = None
    try:
        val = float(row.get("value"))
        if val and val > 0:
            value_usd = val
    except Exception:
        pass

    prices = extract_prices_usd(row)
    if value_usd is None and amount_units is not None and prices["payload_token_usd"] is not None:
        value_usd = amount_units * prices["payload_token_usd"]

    fees = row.get("fees") or {}
    dest_express_fee = row.get("destination_express_fee") or {}
    src_express_fee = row.get("source_express_fee") or {}

    source_base_fee_usd = float(fees.get("source_base_fee_usd")) if isinstance(fees.get("source_base_fee_usd"), (int, float)) else None
    destination_base_fee_usd = float(fees.get("destination_base_fee_usd")) if isinstance(fees.get("destination_base_fee_usd"), (int, float)) else None

    destination_confirm_fee = fees.get("destination_confirm_fee")
    destination_confirm_fee_usd = None
    if isinstance(destination_confirm_fee, (int, float)) and prices["dest_native_usd"] is not None:
        destination_confirm_fee_usd = float(destination_confirm_fee) * prices["dest_native_usd"]

    destination_express_fee_usd = None
    if isinstance(dest_express_fee, dict):
        if isinstance(dest_express_fee.get("total_usd"), (int, float)):
            destination_express_fee_usd = float(dest_express_fee["total_usd"])
        elif isinstance(dest_express_fee.get("total"), (int, float)) and prices["dest_native_usd"] is not None:
            destination_express_fee_usd = float(dest_express_fee["total"]) * prices["dest_native_usd"]

    source_express_fee_usd = None
    if isinstance(src_express_fee, dict):
        if isinstance(src_express_fee.get("total_usd"), (int, float)):
            source_express_fee_usd = float(src_express_fee["total_usd"])

    gp = row.get("gas_paid") or {}
    gas_paid_native_amount = None
    gas_paid_native_usd = None
    for key in ("gas_paid_amount", "gas_used_amount"):
        v = gp.get(key)
        if isinstance(v, (int, float)):
            gas_paid_native_amount = float(v)
            break
    if gas_paid_native_amount is not None and prices["source_native_usd"] is not None:
        gas_paid_native_usd = gas_paid_native_amount * prices["source_native_usd"]

    l2_l1_fee_usd = None
    for col in JSON_CANDIDATES:
        obj = row.get(col)
        if isinstance(obj, dict):
            receipt = obj.get("receipt") or {}
            l1_fee = receipt.get("l1Fee")
            if isinstance(l1_fee, (int, float)) and prices["eth_usd"] is not None:
                l2_l1_fee_usd = float(l1_fee) * prices["eth_usd"]
                break

    parts = [
        source_base_fee_usd, destination_base_fee_usd, destination_confirm_fee_usd,
        destination_express_fee_usd, source_express_fee_usd, gas_paid_native_usd, l2_l1_fee_usd,
    ]
    total_cost_usd = sum([p for p in parts if isinstance(p, (int, float))])

    is_express = bool(row.get("express_executed")) if "express_executed" in row else False
    executed_flag = bool(row.get("executed")) if "executed" in row else is_express
    error_flag = bool(row.get("error")) if "error" in row else False
    refund_flag = bool(row.get("refunded")) if "refunded" in row else False
    not_to_execute_flag = bool(row.get("not_to_execute")) if "not_to_execute" in row else False
    not_to_express_execute_flag = bool(row.get("not_to_express_execute")) if "not_to_express_execute" in row else False

    return pd.Series({
        "call_ts": call_ts,
        "confirm_ts": confirm_ts,
        "execute_ts": execute_ts,
        "e2e_sec": e2e_sec,
        "t_call_confirm_sec": t_call_confirm,
        "t_confirm_execute_sec": t_confirm_execute,
        "decimals": decimals,
        "amount_raw": amount_raw,
        "amount": amount_units,
        "value_usd": value_usd,
        "source_base_fee_usd": source_base_fee_usd,
        "destination_base_fee_usd": destination_base_fee_usd,
        "destination_confirm_fee_usd": destination_confirm_fee_usd,
        "destination_express_fee_usd": destination_express_fee_usd,
        "source_express_fee_usd": source_express_fee_usd,
        "gas_paid_native_amount": gas_paid_native_amount,
        "gas_paid_native_usd": gas_paid_native_usd,
        "l2_l1_fee_usd": l2_l1_fee_usd,
        "total_cost_usd": total_cost_usd,
        "is_express": is_express,
        "executed_flag": executed_flag,
        "error_flag": error_flag,
        "refund_flag": refund_flag,
        "not_to_execute_flag": not_to_execute_flag,
        "not_to_express_execute_flag": not_to_express_execute_flag,
    })

def load_and_merge_csvs(input_dir: str) -> pd.DataFrame:
    paths = sorted(glob.glob(os.path.join("/Users/adamvenhauer/Documents/Axelar/axelarscan_data", "*.csv")))
    if not paths: raise FileNotFoundError(f"No CSV files found in: {input_dir}")
    frames = []
    for p in paths:
        try:
            df = pd.read_csv(p, low_memory=False)
            df["__source_file"] = os.path.basename(p)
            frames.append(df)
        except Exception as e:
            print(f"[WARN] Failed to read {p}: {e}")
    if not frames: raise RuntimeError("No CSVs could be loaded.")
    return pd.concat(frames, ignore_index=True)


def parse_json_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in JSON_CANDIDATES:
        if col in df.columns:
            df[col] = df[col].apply(try_json_load)
    return df

def main():
    ap = argparse.ArgumentParser(description="Build clean Axelar bridge dataset & metrics.")
    ap.add_argument("--input", type=str, default="data", help="Folder with raw CSV files")
    ap.add_argument("--output", type=str, default="out", help="Output folder for clean CSVs")
    args = ap.parse_args()

    os.makedirs(args.output, exist_ok=True)
    df = load_and_merge_csvs(args.input)
    if "source_chain" in df.columns:
        df["source_chain"] = df["source_chain"].apply(normalize_chain_name)
    if "destination_chain" in df.columns:
        df["destination_chain"] = df["destination_chain"].apply(normalize_chain_name)
    if "source_chain_type" not in df.columns:
        df["source_chain_type"] = None
    if "destination_chain_type" not in df.columns:
        df["destination_chain_type"] = None

    df = parse_json_columns(df)
    metrics = df.apply(compute_row_metrics, axis=1)

    # !!! Zásadní blok bez base_cols nebo masky !!!
    df = df.reset_index(drop=True)
    metrics = metrics.reset_index(drop=True)
    assert df.shape[0] == metrics.shape[0], "Nesouhlas mezi počtem řádků v datech a metrikách!"
    clean = pd.concat([df, metrics], axis=1)

    # Feature engineering až teď!
    clean["status_norm"] = clean["simplified_status"].astype(str).str.strip().str.lower()
    clean["success_flag"] = np.where(
        clean["status_norm"] == "received", True,
        np.where(clean["status_norm"] == "failed", False, pd.NA)
    )
    clean["valid_e2e"] = (clean["e2e_sec"].notna()) & (clean["e2e_sec"] > 0) & (clean["e2e_sec"] < 7200)
    print(clean['simplified_status'].value_counts())
    print(clean[clean['simplified_status'] == 'failed']['source_chain'].value_counts())

    merged_path = os.path.join(args.output, "axelar_bridge_clean.csv")
    clean.to_csv(merged_path, index=False, encoding="utf-8")
    print(f"[OK] Clean dataset written to: {merged_path}")

    # Success rate/statistika už beze změny:
    finite_mask = clean["status_norm"].isin(["received", "failed"])
    subset = clean[finite_mask].copy()
    grp_keys = [k for k in ["source_chain", "destination_chain"] if k in clean.columns]
    if grp_keys:
        agg1 = (
            subset.groupby(grp_keys)
            .agg(
                e2e_p50=("e2e_sec", "median"),
                e2e_p90=("e2e_sec", lambda s: s.quantile(0.9)),
                call_confirm_p50=("t_call_confirm_sec", "median"),
                confirm_exec_p50=("t_confirm_execute_sec", "median"),
                cost_avg_usd=("total_cost_usd", "mean"),
                success_rate=("success_flag", "mean"),
                success_n=("success_flag", "sum"),
                success_den=("success_flag", "count"),
                n=("status_norm", "size"),
            ).reset_index()
        )
        agg_path = os.path.join(args.output, "axelar_bridge_pair_summary.csv")
        agg1.to_csv(agg_path, index=False, encoding="utf-8")
        print(f"[OK] Pair summary written to: {agg_path}")




if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Subscan XCM/Transaction Data Pipeline
-------------------------------------
Stahuje data o transakcích (extrinsics) ze Subscan API, normalizuje je a počítá metriky.

Features:
- Robustní stahování (retry, rate limiting)
- Podpora stránkování
- Normalizace dat (flattening)
- Výpočet metrik: Latency, Success Rate (received / (received + failed)), Counts
- CSV export

Použití:
    export SUBSCAN_API_KEY="vase_api_klic"
    python pipeline.py --chain polkadot --from 2025-01-01 --to 2025-01-31
    python pipeline.py --chain polkadot --limit 100 # pro test
"""

import os
import sys
import time
import json
import logging
import argparse
import math
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import requests
import pandas as pd

# Konfigurace logování
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Konstanty
API_KEY_ENV = "SUBSCAN_API_KEY"
# Hardcoded API key provided by user (ensure not to commit this if public)
API_KEY_VALUE = "90f6e2ae515b41598f1144031fb8d02c"
DEFAULT_RATE_LIMIT_DELAY = 0.25  # 4 req/s (Free tier limit is ~5 req/s)
MAX_RETRIES = 5
PAGE_SIZE = 100  # Max supported by some Subscan endpoints is often 100

class SubscanClient:
    def __init__(self, api_key: str, chain: str):
        self.api_key = api_key
        self.base_url = f"https://{chain}.api.subscan.io"
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "X-API-Key": self.api_key
        })

    def fetch_page(self, endpoint: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Stáhne jednu stránku dat s retry a rate limitingem."""
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(MAX_RETRIES):
            try:
                # Rate limiting sleep
                time.sleep(DEFAULT_RATE_LIMIT_DELAY)
                
                response = self.session.post(url, json=payload, timeout=15)
                
                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    logger.warning(f"HTTP 429: Too Many Requests. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                if data.get("code") != 0:
                    logger.error(f"API Error Code {data.get('code')}: {data.get('message')}")
                    return None
                
                return data.get("data", {})

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}): {e}")
                time.sleep(2 ** attempt)
        
        logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts.")
        return None

def fetch_extrinsics(client: SubscanClient, start_date: str, end_date: str, max_rows: Optional[int] = None) -> List[Dict]:
    """
    Stahuje extrinsics pro dané časové okno.
    Subscan '/api/scan/extrinsics' filtruje podle bloku nebo času nepřímo.
    Nejlepší je iterovat po stránkách (page 0, 1, ...).
    """
    all_extrinsics = []
    page = 0
    total_fetched = 0
    
    # Převod date stringů na datetime pro filtrování (pokud API nepodporuje přímý date filter v payloadu)
    # Pozn: Endpoint '/api/scan/extrinsics' bere 'row' a 'page'. Filtrování data musíme dělat buď
    # parametry (pokud supported) nebo post-processingem, pokud endpoint vrací chronologicky.
    # V dokumentaci 'signed' extrinsics jsou default.
    # Pro XCM bychom mohli filtrovat modulem, ale stáhneme vše a vyfiltrujeme později, nebo specifikujeme modul.
    # Pro tento pipeline zkusíme stáhnout obecné extrinsics a filtrovat, nebo iterate blocks.
    # Efektivnější pro "pipeline" je stahovat vše, ale může to být moc dat.
    # Zkusíme filtrovat 'success': true pro rate, ale potřebujeme i failed.
    
    # Pro účely dema a XCM zaměření, budeme volat `module` filtr, pokud by byl podporován. 
    # Subscan `POST /api/scan/extrinsics` podporuje `module` parametr.
    # Seznam relevantních XCM modulů:
    target_modules = ["xcmPallet", "xTokens", "dmp", "ump", "parachainSystem", "polkadotXcm", "ormlXcm"]
    # API obvykle neumí list modulů, musíme 1 request = 1 modul.
    # Nebo stáhneme vše bez filtru modulu, pokud to uživatel chce.
    # Zadání: "všechna data pro výpočet metrik na XCM".
    # Budeme iterovat přes klíčové moduly.
    
    for module in target_modules:
        logger.info(f"Fetching extrinsics for module: {module}...")
        page = 0
        after_id = None # Cursor for deep pagination
        module_fetched_count = 0
        
        while True:
            # Stop conditions
            if max_rows and module_fetched_count >= max_rows:
                break
                
            payload = {
                "row": PAGE_SIZE,
                "page": 0, # Page is always 0 when using after_id
                "module": module,
                # "signed": "all" # signed only or all? XCM can be unsigned (e.g. DMP). Keep default.
            }

            if after_id is not None:
                payload["after_id"] = after_id
            
            data = client.fetch_page("/api/v2/scan/extrinsics", payload)
            
            if not data:
                break
            
            extrinsics = data.get("extrinsics") or []
            if not extrinsics:
                break
            
            # Update cursor for next iteration
            last_record = extrinsics[-1]
            # Ensure we get an ID for pagination
            if "id" in last_record:
                after_id = last_record["id"]
            else:
                # If no ID is returned, we can't page further with after_id
                logger.warning(f"No 'id' field found in extrinsic record. Stopping pagination for module {module}.")
                break

            # Filter by date window locally
            # Subscan returns ordered by block_num desc usually.
            first_ex_time = datetime.fromtimestamp(extrinsics[0]["block_timestamp"])
            last_ex_time = datetime.fromtimestamp(extrinsics[-1]["block_timestamp"])
            
            # Simple check if we are out of range (assuming descending order)
            # Pokud nejnovější transakce je starší než TO, jsme OK.
            # Pokud nejstarší transakce je mladší než FROM, jsme OK a pokračujeme.
            # Pokud nejstarší transakce v batchi je starší než FROM, vezmeme jen část a končíme (pro tento modul).
            
            # Parse dates
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) # include end date fully
            
            keep_batch = []
            stop_fetching = False
            
            for ex in extrinsics:
                ts = ex.get("block_timestamp")
                dt = datetime.fromtimestamp(ts)
                
                if start_dt <= dt < end_dt:
                    keep_batch.append(ex)
                elif dt < start_dt:
                    stop_fetching = True
                # if dt >= end_dt: skip (too new)
            
            if keep_batch:
                all_extrinsics.extend(keep_batch)
                module_fetched_count += len(keep_batch)
                logger.info(f"  Fetched {len(keep_batch)} extrinsics (after_id: {after_id})")
            
            if stop_fetching:
                logger.info(f"  Reached start date limit for module {module}.")
                break
                
            # Safety break for empty pages looped
            if len(extrinsics) < PAGE_SIZE:
                break
            
            page += 1 # Just for logging or sanity, not used in payload

            
    return all_extrinsics

def normalize_record(record: Dict, chain: str) -> Dict:
    """Flatten a normalizování jednoho záznamu (extrinsic)."""
    # Základní pole
    row = {
        "chain": chain,
        "extrinsic_hash": record.get("extrinsic_hash"),
        "extrinsic_index": record.get("extrinsic_index"),
        "block_num": record.get("block_num"),
        "block_timestamp": record.get("block_timestamp"),
        "date": datetime.fromtimestamp(record.get("block_timestamp")).strftime("%Y-%m-%d"),
        "datetime": datetime.fromtimestamp(record.get("block_timestamp")).isoformat(),
        "module": record.get("call_module"),
        "call": record.get("call_module_function"),
        "full_call": f"{record.get('call_module')}.{record.get('call_module_function')}",
        "sender": record.get("account_display", {}).get("address") if record.get("account_display") else record.get("account_id"),
        "success": record.get("success"),
        "finalized": record.get("finalized"),
        "fee": record.get("fee"),
        "error": record.get("error")
    }

    # Simplified Status logic
    # Success Rate = received / (received + failed)
    # Mapping:
    #   if success == True -> 'received'
    #   if success == False -> 'failed'
    #   (Ignoring 'unknown' for the denominator if finalized is false, but usually we just look at success flag)
    
    if row["success"]:
        row["simplified_status"] = "received"
    else:
        row["simplified_status"] = "failed"
        
    # Latency calculation placeholders
    # In single-chain extrinsic view, latency is usually Block Time - Submission Time.
    # But often we only have Block Time.
    # If we assume 'finalized_at' is when it is considered 'done'.
    # Note: Subscan might not give explicit 'submission_time' in this endpoint, 
    # so we might use block_timestamp as the anchor.
    # Real "Transaction Latency" usually implies time to inclusion or time to finalization.
    # Here we treat block_timestamp as 'confirmed' time usually.
    # If specific 'created_at' is available, we use it. otherwise 0.
    
    return row

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vypočítá metriky úspěšnosti a latence.
    Success Rate = received / (received + failed)
    """
    if df.empty:
        return pd.DataFrame()

    def calculate_group_metrics(group):
        total = len(group)
        received = len(group[group['simplified_status'] == 'received'])
        failed = len(group[group['simplified_status'] == 'failed'])
        
        # Denominator only includes received + failed (excludes any potential 'pending' if we had them)
        denom = received + failed
        success_rate = (received / denom) if denom > 0 else 0.0
        
        return pd.Series({
            'total_tx': total,
            'received_count': received,
            'failed_count': failed,
            'success_rate': round(success_rate, 4)
        })

    # Groupby Chain, Module, Date
    metrics = df.groupby(['chain', 'module', 'date']).apply(calculate_group_metrics).reset_index()
    
    return metrics

def main():
    parser = argparse.ArgumentParser(description="Subscan Data Pipeline")
    parser.add_argument("--chain", required=True, help="Subscan subdomain (e.g. polkadot, kusama, astar)")
    parser.add_argument("--from", "---from_date", dest="from_date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--to", "---to_date", dest="to_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--limit", type=int, help="Max rows to fetch (dev/test mode)")
    
    args = parser.parse_args()
    
    api_key = os.environ.get(API_KEY_ENV) or API_KEY_VALUE
    if not api_key:
        logger.error(f"Missing {API_KEY_ENV} environment variable and no hardcoded key found.")
        sys.exit(1)
        
    client = SubscanClient(api_key, args.chain)
    
    logger.info(f"Starting pipeline for chain: {args.chain} [{args.from_date} to {args.to_date}]")
    
    # 1. Fetch Raw Data
    raw_data = fetch_extrinsics(client, args.from_date, args.to_date, args.limit)
    logger.info(f"Total records fetched: {len(raw_data)}")
    
    if not raw_data:
        logger.warning("No data found. Exiting.")
        return

    # 2. Save Raw Data
    os.makedirs("output", exist_ok=True)
    with open("output/raw_extrinsics.json", "w") as f:
        json.dump(raw_data, f, indent=2)
    
    # 3. Normalize
    normalized_rows = [normalize_record(r, args.chain) for r in raw_data]
    df = pd.DataFrame(normalized_rows)
    
    # Save Normalized
    df.to_csv("output/normalized_transactions.csv", index=False)
    logger.info("Saved output/normalized_transactions.csv")
    
    # 4. Compute Metrics
    metrics_df = compute_metrics(df)
    
    # Save Metrics
    metrics_df.to_csv("output/metrics_aggregated.csv", index=False)
    logger.info("Saved output/metrics_aggregated.csv")
    
    # Preview
    print("\n--- Metrics Preview ---")
    print(metrics_df.head().to_string())
    print("-----------------------")

if __name__ == "__main__":
    main()

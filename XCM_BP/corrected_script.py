
import os
import re
import time
import requests
import pandas as pd
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import sys

# =========================================
# Config
# =========================================
API_KEY = os.getenv("SUBSCAN_API_KEY", "90f6e2ae515b41598f1144031fb8d02c").strip()
SRC_HOST  = "https://polkadot.api.subscan.io"

# Destination Config
DESTINATIONS = {
    1000: {"name": "assethub", "host": "https://assethub-polkadot.api.subscan.io"},
    2034: {"name": "hydradx", "host": "https://hydradx.api.subscan.io"},
    2004: {"name": "moonbeam", "host": "https://moonbeam.api.subscan.io"},
    2000: {"name": "acala", "host": "https://acala.api.subscan.io"}
}

# Target count per chain? Or total? 
# Let's say we want to find *any* XCM to these chains until we hit a total target or scan limit.
TOTAL_TARGET_COUNT = 12000 # Increased to ensure >2000 matched pairs

# Scan Logic
START_BLOCK = 22000000
SCAN_CHUNK_SIZE = 50000 
MAX_CHUNKS = 500 # Increased limit 

SRC_MODULE_EVENTS = [{"module": "xcmPallet", "event_id": "Sent"}]

# Common dest events (adjust if chains differ, but these are standard XCM/DMP)
DEST_MODULE_EVENTS = [
    {"module": "messageQueue", "event_id": "Processed"}, 
    {"module": "xcmpQueue", "event_id": "Success"},
    {"module": "parachainSystem", "event_id": "DownwardMessagesProcessed"},
    {"module": "dmpQueue", "event_id": "ExecutedDownward"} # Moonbeam/Acala might use this?
]

ROW_PER_PAGE = 100
MAX_PAGES_PER_CHUNK = 100
OUTPUT_FILE = "corrected_xcm_pairs_multichain.csv"

# =========================================
# Utils (Same as before)
# =========================================
@dataclass
class SubscanClient:
    host: str
    api_key: str
    timeout_s: int = 45 

    def _headers(self) -> Dict[str, str]:
        return {"Content-Type": "application/json", "X-API-Key": self.api_key}

    def post(self, path: str, payload: Dict[str, Any], max_retries: int = 5) -> Dict[str, Any]:
        url = self.host.rstrip("/") + path
        for attempt in range(max_retries):
            try:
                resp = requests.post(url, json=payload, headers=self._headers(), timeout=self.timeout_s)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and data.get("code") == 0: return data
                elif resp.status_code == 429:
                    time.sleep(1 + attempt)
                    continue
            except: pass
            time.sleep(0.5)
        return {}

def fetch_events(client: SubscanClient, block_range: str, module_events: List[Dict[str, str]]) -> pd.DataFrame:
    all_rows = []
    page = 0
    while page < MAX_PAGES_PER_CHUNK:
        payload = {"block_range": block_range, "module_event": module_events, "row": ROW_PER_PAGE, "page": page}
        data = client.post("/api/v2/scan/events", payload)
        events = data.get("data", {}).get("events", [])
        if not events: break
        all_rows.extend(events)
        page += 1
        if len(events) < ROW_PER_PAGE: break
    return pd.DataFrame(all_rows)

def fetch_event_params_batched(client: SubscanClient, event_indexes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    out = {}
    batch_size = 50 
    for i in range(0, len(event_indexes), batch_size):
        batch = event_indexes[i:i+batch_size]
        data = client.post("/api/scan/event/params", {"event_index": batch})
        items = data.get("data", []) or []
        for item in items: out[item.get("event_index")] = item.get("params", [])
    return out

HEX_HASH_RE = re.compile(r"^0x[a-fA-F0-9]{64}$")
def extract_hex_hashes(params: List[Dict[str, Any]]) -> List[str]:
    hashes = []
    for p in params or []:
        val = p.get("value")
        if isinstance(val, str) and HEX_HASH_RE.match(val): hashes.append(val)
    return list(set(hashes))

def check_destination(params: List[Dict[str, Any]]) -> int:
    # Returns ParaID if found, else None
    for p in params or []:
        if p.get("name") == "destination":
            val = p.get("value")
            try:
                if isinstance(val, dict):
                     interior = val.get("interior", {})
                     x1 = interior.get("X1")
                     if isinstance(x1, list):
                         for item in x1: return item.get("Parachain")
                     elif isinstance(x1, dict):
                         return x1.get("Parachain")
            except: pass
    return None

# =========================================
# Main
# =========================================
src = SubscanClient(SRC_HOST, API_KEY)
all_target_events = []
all_src_params = {}

print(f"=== STAGE 1: Fast Collection (Multi-Chain) ===")
print(f"Goal: Find {TOTAL_TARGET_COUNT} aggregated events...")

src_block_min = START_BLOCK
src_block_max = START_BLOCK

# 1. Scan Loop
for i in range(MAX_CHUNKS):
    b_max = START_BLOCK - (i * SCAN_CHUNK_SIZE)
    b_min = b_max - SCAN_CHUNK_SIZE
    rng = f"{b_min}-{b_max}"
    
    hits_count = sum(len(df) for df in all_target_events)
    print(f"[{i+1}/{MAX_CHUNKS}] Scanning {rng}... (Total Hits: {hits_count})")
    
    chunk_events = fetch_events(src, rng, SRC_MODULE_EVENTS)
    if chunk_events.empty: continue
        
    chunk_params = fetch_event_params_batched(src, chunk_events["event_index"].astype(str).tolist())
    all_src_params.update(chunk_params)
    
    def get_dest_para_id(row):
        pid = check_destination(chunk_params.get(row["event_index"], []))
        return pid if pid in DESTINATIONS else None
    
    chunk_events["dest_para_id"] = chunk_events.apply(get_dest_para_id, axis=1)
    hits = chunk_events[chunk_events["dest_para_id"].notna()]
    
    if not hits.empty:
        all_target_events.append(hits)
        print(f"  Found {len(hits)} target events.")
        # Update min/max observed
        # Need extracted block num again
        hits["block_num_extracted"] = hits["event_index"].apply(lambda x: int(x.split("-")[0]))
        src_block_min = min(src_block_min, hits["block_num_extracted"].min())
        # src_block_max is roughly START_BLOCK
    
    if hits_count + len(hits) >= TOTAL_TARGET_COUNT:
        break

if not all_target_events:
    print("No events found.")
    exit(0)

target_events = pd.concat(all_target_events, ignore_index=True)
print(f"Total Source Events: {len(target_events)}")

# --- 2. Process per Destination ---
final_pairs = []

# Anchor Point (Aug 8, 2024 - Polkadot 22,000,000)
SRC_ANCHOR_BLOCK = 22000000

# Calibrated Anchors
DEST_ANCHORS = {
    1000: 6860658, # Asset Hub
    2034: 5710453, # HydraDX
    2004: 6931208, # Moonbeam
    2000: 6731118  # Acala
}

# Assumed Block Time Ratios (Source 6s / Dest 12s = 0.5)
# If a chain is 6s, ratio is 1.0. Most parachains are 12s.
DEST_RATIOS = {
    1000: 0.5,
    2034: 0.5,
    2004: 0.5,
    2000: 0.5 
}

for pid, conf in DESTINATIONS.items():
    chain_name = conf["name"]
    host = conf["host"]
    print(f"Processing chain: {chain_name} ({pid})...")
    
    subset = target_events[target_events["dest_para_id"] == pid].copy()
    if subset.empty:
        print("  No events.")
        continue
        
    print(f"  Events: {len(subset)}")
    dst_client = SubscanClient(host, API_KEY)
    
    # Use extracted block number
    # If missing, extract again (safety)
    if "block_num_extracted" not in subset.columns:
         subset["block_num_extracted"] = subset["event_index"].apply(lambda x: int(x.split("-")[0]))

    min_src = subset["block_num_extracted"].min()
    max_src = subset["block_num_extracted"].max()
    
    anchor = DEST_ANCHORS.get(pid, 0)
    ratio = DEST_RATIOS.get(pid, 0.5)
    
    # Formula: Dest = Anchor + (Src - Src_Anchor) * Ratio
    est_min = int(anchor + (min_src - SRC_ANCHOR_BLOCK) * ratio) - 2000 # Buffer
    est_max = int(anchor + (max_src - SRC_ANCHOR_BLOCK) * ratio) + 2000 # Buffer
    
    print(f"  Est Range: {est_min}-{est_max}")
    
    # Fetch events
    dst_events = fetch_events(dst_client, f"{est_min}-{est_max}", DEST_MODULE_EVENTS)
    print(f"  Fetched {len(dst_events)} dest events.")
    
    if not dst_events.empty:
        dst_params = fetch_event_params_batched(dst_client, dst_events["event_index"].astype(str).tolist())
        
        subset["corr_id"] = subset.apply(lambda r: extract_hex_hashes(all_src_params.get(r["event_index"], [])), axis=1)
        dst_events["corr_id"] = dst_events.apply(lambda r: extract_hex_hashes(dst_params.get(r["event_index"], [])), axis=1)
        
        s_flat = subset.explode("corr_id").dropna(subset=["corr_id"])
        d_flat = dst_events.explode("corr_id").dropna(subset=["corr_id"])
        
        j = s_flat.merge(d_flat, on="corr_id", how="inner", suffixes=("_src", "_dst"))
        print(f"  Matched: {len(j)}")
        if not j.empty:
            j["dest_chain"] = chain_name
            final_pairs.append(j)

# Combine
if final_pairs:
    full_df = pd.concat(final_pairs, ignore_index=True)
    full_df["source_chain"] = "polkadot"
    full_df["latency_s"] = full_df["block_timestamp_dst"] - full_df["block_timestamp_src"]
    full_df["cost"] = "Pending..."
    
    def get_status(row):
        ev = row.get("event_id_dst")
        if ev in ["Success", "Processed", "DownwardMessagesProcessed", "ExecutedDownward"]: return "Success"
        return "Unknown"
    full_df["status"] = full_df.apply(get_status, axis=1)

    full_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {OUTPUT_FILE} with {len(full_df)} pairs.")
    
    # Fee enrichment (Stage 2) can follow...
else:
    print("No pairs matched across any chain.")


import os
import time
import requests
import pandas as pd
from dataclasses import dataclass
from typing import Any, Dict

# =========================================
# Config
# =========================================
API_KEY = os.getenv("SUBSCAN_API_KEY", "90f6e2ae515b41598f1144031fb8d02c").strip()
SRC_HOST  = "https://polkadot.api.subscan.io"
INPUT_FILE = "corrected_xcm_pairs_multichain.csv"
OUTPUT_FILE = INPUT_FILE # Overwrite

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

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} rows.")
    
    if "cost" not in df.columns:
        df["cost"] = "Pending..."
        
    # Filter for pending
    # Check if extrinsic_index_src exists
    col_name = "extrinsic_index_src"
    if col_name not in df.columns:
        # Fallback
        if "extrinsic_index" in df.columns: col_name = "extrinsic_index"
        else:
            print("Error: extrinsic_index column not found.")
            return

    pending_mask = df["cost"] == "Pending..."
    if not pending_mask.any():
        print("No pending costs found.")
        return

    unique_ext = df.loc[pending_mask, col_name].unique().tolist()
    total = len(unique_ext)
    print(f"Fetching fees for {total} unique extrinsics...")
    
    client = SubscanClient(SRC_HOST, API_KEY)
    fees_map = {}
    save_interval = 20
    
    for i, idx in enumerate(unique_ext):
        # Fetch
        data = client.post("/api/scan/extrinsic", {"extrinsic_index": idx})
        vals = data.get("data", {})
        fee = "0"
        if vals:
             fee = str(vals.get("fee", "0"))
        
        fees_map[idx] = fee
        
        # Update immediately in memory (optimized later for write)
        # Actually for safety, let's update a copy or just map at save
        
        if (i+1) % save_interval == 0 or (i+1) == total:
             print(f"  Processed {i+1}/{total}...")
             # Apply map to dataframe
             # We only want to update the ones we've fetched.
             # Logic: df['cost'] = df[col].map(fees_map).fillna(df['cost']) -- this might overwrite with NaN if not careful?
             # Better: Update using a loop or specific index? 
             # Iterate and update is slow for DF.
             # Vectorized:
             # Create a series from map
             mapped = df[col_name].map(fees_map)
             # Update where not NaN
             mask = mapped.notna()
             df.loc[mask, "cost"] = mapped[mask]
             
             df.to_csv(OUTPUT_FILE, index=False)
             
        time.sleep(0.5) # Rate limit
        
    print("Done filling costs.")

if __name__ == "__main__":
    main()

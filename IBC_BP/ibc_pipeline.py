import requests
import pandas as pd
import argparse
import logging
import time
import os
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_TIMEOUT = 10
MAX_RETRIES = 5
BACKOFF_FACTOR = 0.5
PAGE_LIMIT = 100  # Default page limit for Cosmos SDK pagination

def get_env_var(key: str, default: Optional[str] = None) -> str:
    """Gets an environment variable or returns a default value."""
    val = os.environ.get(key, default)
    if val is None:
        raise ValueError(f"Environment variable {key} is not set and no default provided.")
    return val

def parse_iso_date(date_str: str) -> datetime:
    """Parses ISO 8601 date string to datetime object."""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc)
    except ValueError:
        try:
            # Fallback for simple date YYYY-MM-DD
            dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            return dt
        except ValueError as e:
            raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD or ISO 8601.") from e

class CosmosClient:
    """Client for interacting with Cosmos SDK REST APIs."""
    
    def __init__(self, api_url: str, rate_limit_sleep: float = 0.1):
        self.api_url = api_url.rstrip('/')
        self.rate_limit_sleep = rate_limit_sleep
        self.session = requests.Session()

    def _request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Internal request method with retries and error handling."""
        url = f"{self.api_url}{endpoint}"
        for attempt in range(MAX_RETRIES):
            try:
                # Rate limiting sleep
                time.sleep(self.rate_limit_sleep)
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
                    'Referer': 'https://cosmos.network/'
                }
                response = self.session.request(method, url, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
                
                if response.status_code == 429: # Too Many Requests
                    wait = float(response.headers.get('Retry-After', (attempt + 1) * BACKOFF_FACTOR))
                    logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue
                
                if response.status_code >= 500:
                    wait = (attempt + 1) * BACKOFF_FACTOR
                    logger.warning(f"Server error {response.status_code}: {response.text} URL: {response.url}. Retrying in {wait}s...")
                    time.sleep(wait)
                    continue
                
                response.raise_for_status()
                return response.json()
            
            except requests.exceptions.RequestException as e:
                wait = (attempt + 1) * BACKOFF_FACTOR
                logger.warning(f"Request failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
        
        logger.error(f"Failed to fetching {url} after {MAX_RETRIES} attempts.")
        return {}

    def fetch_txs(self, query_params: Dict, max_txs: Optional[int] = None) -> List[Dict]:
        """Fetches transactions based on query parameters with pagination."""
        txs = []
        next_key = None
        total_fetched = 0
        
        # Cosmos SDK pagination uses 'pagination.key' which is base64 encoded
        # Initial request has no key
        
        while True:
            current_params = query_params.copy()
            if next_key:
                current_params['pagination.key'] = next_key
            
            # Request page
            data = self._request('GET', '/cosmos/tx/v1beta1/txs', params=current_params)
            
            if not data:
                break
            
            # Parse responses (handle both 'txs' and 'tx_responses' structure depending on version/endpoint)
            # Typically /cosmos/tx/v1beta1/txs returns 'tx_responses' which contains logs and 'txs' which contains decoded msgs
            page_txs = data.get('tx_responses', [])
            if not page_txs:
                # Fallback or empty
                break
                
            txs.extend(page_txs)
            total_fetched += len(page_txs)
            
            if max_txs and total_fetched >= max_txs:
                logger.info(f"Reached max_txs limit ({max_txs}).")
                txs = txs[:max_txs]
                break
            
            # Check pagination
            pagination = data.get('pagination', {})
            next_key = pagination.get('next_key')
            
            if not next_key:
                break
                
            logger.info(f"Fetched {total_fetched} txs so far... Next page exists.")
            
        return txs

    def fetch_ibc_transfers(self, min_height: Optional[int] = None, max_height: Optional[int] = None, max_txs: int = 1000) -> List[Dict]:
        """Specific wrapper to fetch IBC MsgTransfer transactions."""
        # Note: Event query syntax is 'message.action=...'
        # We can also filter by tx.height if needed headers allow it, but usually query params are:
        # events='message.action...'
        
        # Construct query
        events = ["message.action='/ibc.applications.transfer.v1.MsgTransfer'"]
        if min_height:
             events.append(f"tx.height>={min_height}")
        if max_height:
             events.append(f"tx.height<={max_height}")

        # The standard cosmos REST API takes 'events' as a list of strings
        # However, some nodes properly support 'events' param directly in GET, 
        # others might need the key to be repeated 'events=a&events=b'.
        # Python requests handles list in params as repeated keys usually.
        
        params = {
            'events': events,
            'pagination.limit': PAGE_LIMIT,
            'order_by': 'ORDER_BY_DESC' # valid values: ORDER_BY_UNSPECIFIED, ORDER_BY_ASC, ORDER_BY_DESC
        }
        
        return self.fetch_txs(params, max_txs=max_txs)


def parse_ibc_events(txs: List[Dict], chain_id: str) -> List[Dict]:
    """Parses raw transaction responses into standardized IBC event dictionaries."""
    parsed_events = []
    
    for tx in txs:
        if tx.get('code', 0) != 0:
            continue # Skip failed transactions at the chain level for now, or log as failed attempt
            
        tx_hash = tx.get('txhash')
        timestamp = tx.get('timestamp')
        height = tx.get('height')
        logs = tx.get('logs', [])
        
        if not logs:
            continue

        for log in logs:
            msg_index = log.get('msg_index', 0)
            events = log.get('events', [])
            
            for event in events:
                event_type = event.get('type')
                attributes = {attr['key']: attr['value'] for attr in event.get('attributes', [])}
                
                # We are interested in standard IBC lifecycle events
                # packet_data is usually hex encoded in newer versions, or JSON text.
                # Key attributes usually: packet_sequence, packet_src_port, packet_src_channel, packet_dst_port, packet_dst_channel
                
                if event_type in ['send_packet', 'recv_packet', 'acknowledge_packet', 'timeout_packet']:
                    
                    # Normalize simple attributes that define the packet identity
                    seq = attributes.get('packet_sequence')
                    src_port = attributes.get('packet_src_port')
                    src_channel = attributes.get('packet_src_channel')
                    dst_port = attributes.get('packet_dst_port')
                    dst_channel = attributes.get('packet_dst_channel')
                    
                    if not (seq and src_port and src_channel and dst_port and dst_channel):
                        # Might handle cases where keys are different (e.g. older SDK versions) but this is standard
                        continue

                    # Determine packet data if available (often in 'packet_data' attribute)
                    # For basic metrics we primarily need identity + timestamp + type
                    
                    parsed_event = {
                        'chain_id': chain_id,
                        'tx_hash': tx_hash,
                        'height': height,
                        'timestamp': timestamp,
                        'msg_index': msg_index,
                        'event_type': event_type,
                        'packet_sequence': int(seq),
                        'src_port': src_port,
                        'src_channel': src_channel,
                        'dst_port': dst_port,
                        'dst_channel': dst_channel,
                        'packet_data_hex': attributes.get('packet_data'), # Optional payload
                        'packet_timeout_timestamp': attributes.get('packet_timeout_timestamp'),
                        # For ack events, might have error info
                    }
                    parsed_events.append(parsed_event)

    return parsed_events

def match_ibc_packets(all_events: List[Dict]) -> pd.DataFrame:
    """Matches send, recv, ack, timeout events to reconstruct packet lifecycles."""
    # Group by packet identity: (src_port, src_channel, sequence) - this is globally unique FROM the source chain perspective
    # Note: A packet is uniquely identified by (src_port, src_channel, sequence).
    
    packets = {}
    
    for event in all_events:
        # Key ID
        pkt_id = (event['src_port'], event['src_channel'], event['packet_sequence'])
        
        if pkt_id not in packets:
            packets[pkt_id] = {
                'src_port': event['src_port'],
                'src_channel': event['src_channel'],
                'dst_port': event['dst_port'],
                'dst_channel': event['dst_channel'],
                'sequence': event['packet_sequence'],
                'chain_from': None, # To be filled from send_packet
                'chain_to': None,   # To be filled (inferred)
                't_send': None,
                't_recv': None,
                't_ack': None,
                't_timeout': None,
                'status': 'pending', 
                'error': None
            }
        
        p = packets[pkt_id]
        evt_type = event['event_type']
        ts = parse_iso_date(event['timestamp']) if event['timestamp'] else None

        if evt_type == 'send_packet':
            p['t_send'] = ts
            p['chain_from'] = event['chain_id'] # The chain where send_packet happened IS the source chain
            
        elif evt_type == 'recv_packet':
            p['t_recv'] = ts
            # If we saw recv_packet, the chain_id of this event IS the destination chain
            # Note: We might verify if it matches dst_channel logic if we had full topology, but for now trust the event
            p['chain_to'] = event['chain_id'] 
            if p['status'] == 'pending':
                p['status'] = 'received'

        elif evt_type == 'acknowledge_packet':
            p['t_ack'] = ts
            # Acknowledgement happens on the SOURCE chain (processed by the source chain)
            # So event['chain_id'] should match p['chain_from']
            # If we didn't see send_packet (e.g. out of time window), we might infer chain_from here
            if not p['chain_from']:
                p['chain_from'] = event['chain_id']
                
            # Check for error in ack? (Would need to parse packet_ack hex usually if not explicit in attributes)
            # Simple assumption: Ack exists = success lifecycle complete, unless ack payload indicates error.
            # parsing error from ack is complex without decoding the hex/json ack.
            
        elif evt_type == 'timeout_packet':
            p['t_timeout'] = ts
            p['status'] = 'timeout'
            if not p['chain_from']:
                p['chain_from'] = event['chain_id'] # Timeout happens on source

    # Convert to list
    packet_list = list(packets.values())
    
    # Filter incomplete? Maybe keep them to show 'pending'.
    # We want to keep them to calculate success rates correctly (missing recv = failed/pending)
    
    df = pd.DataFrame(packet_list)
    return df

def compute_metrics(packets_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Computes basic metrics and aggregations."""
    if packets_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Calculate Latencies (in seconds)
    # recv latency = t_recv - t_send
    # ack latency = t_ack - t_send
    
    # Ensure datetimes
    for col in ['t_send', 't_recv', 't_ack', 't_timeout']:
        packets_df[col] = pd.to_datetime(packets_df[col], utc=True)

    packets_df['latency_recv_s'] = (packets_df['t_recv'] - packets_df['t_send']).dt.total_seconds()
    packets_df['latency_ack_s'] = (packets_df['t_ack'] - packets_df['t_send']).dt.total_seconds()
    
    # Determine precise status for success rate
    # Success = has t_recv AND (t_ack is optional but good). 
    # Failure = timeout OR (long time pending? -> we treat pending as pending, not fail immediately unless very old)
    # User definition: success = recv/ack exists. failed = missing recv/ack in time window OR timeout.
    
    # We'll use a simple 'is_successful' flag
    # If timeout -> False
    # If recv -> True (basic success of transfer reaching dst)
    # If pending -> None/NaN (exclude from rate? or treat as fail?)
    # Let's treat 'timeout' as explicit fail. 
    
    packets_df['is_success'] = packets_df['status'].apply(
        lambda s: 1 if s == 'received' else (0 if s == 'timeout' else None)
    )
    
    # 1. Aggregation by (chain_from, chain_to, src_channel)
    # We need to drop NAs for aggregation if we want valid stats
    # But for counts we keep them.
    
    aggs = []
    
    # Group by Channel
    if 'chain_from' in packets_df.columns and 'chain_to' in packets_df.columns:
         # Fill N/As for grouping
        df_clean = packets_df.fillna({'chain_from': 'unknown', 'chain_to': 'unknown', 'src_channel': 'unknown'})
        
        grouped = df_clean.groupby(['chain_from', 'chain_to', 'src_channel'])
        
        agg_df = grouped.agg(
            total_count=('sequence', 'count'),
            success_count=('is_success', lambda x: (x==1).sum()),
            timeout_count=('status', lambda x: (x=='timeout').sum()),
            avg_latency_recv=('latency_recv_s', 'mean'),
            p95_latency_recv=('latency_recv_s', lambda x: x.quantile(0.95)),
            max_latency_recv=('latency_recv_s', 'max')
        ).reset_index()
        
        # Calculate success rate
        # We can define rate as success / total (including pending) or success / (success + fail)
        # Usually success / total is safer for specific windows, but let's do success / (success + timeout) to be precise on resolved packets
        # or just success / total
        
        agg_df['success_rate'] = agg_df['success_count'] / agg_df['total_count']
        
        return packets_df, agg_df
        
    return packets_df, pd.DataFrame()

def main():
    parser = argparse.ArgumentParser(description="IBC Data Pipeline")
    parser.add_argument("--chains", type=str, required=True, help="Comma-separated list of chain names (must match env var keys like COSMOSHUB_API)")
    parser.add_argument("--from", dest="start_date", type=str, required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="end_date", type=str, required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--max-txs", type=int, default=1000, help="Max txs to fetch per chain")
    parser.add_argument("--config", type=str, help="Path to JSON config file (optional)")
    
    args = parser.parse_args()
    
    # Load Config
    chain_names = [c.strip() for c in args.chains.split(',')]
    chain_apis = {}
    
    # Try loading from file first if provided
    config_data = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config_data = json.load(f)
            
    # Resolve APIs
    for chain in chain_names:
        # Check config file
        api = config_data.get(chain)
        # Check env var (upper case)
        if not api:
            env_key = f"{chain.upper()}_API"
            api = os.environ.get(env_key)
        
        if not api:
            logger.warning(f"No API URL found for chain '{chain}'. Skipping.")
            continue
            
        chain_apis[chain] = api
        
    if not chain_apis:
        logger.error("No valid chain APIs found. Exiting.")
        return

    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)
    
    logger.info(f"Starting IBC Pipeline from {start_date} to {end_date} for chains: {list(chain_apis.keys())}")
    
    all_events = []
    
    # FETCH
    for chain_id, api_url in chain_apis.items():
        logger.info(f"Fetching data for {chain_id} from {api_url}...")
        client = CosmosClient(api_url)
        
        # We fetch only MsgTransfer for now to catch sends
        # Ideally we also need to catch RecvPacket and AckPacket which are DIFFERENT message types
        # MsgRecvPacket: /ibc.core.channel.v1.MsgRecvPacket
        # MsgAcknowledgement: /ibc.core.channel.v1.MsgAcknowledgement
        # The prompt mentioned filtering message.action='/ibc.applications.transfer.v1.MsgTransfer'
        # BUT that only gets SEND events initiated by transfer.
        # To get the full lifecycle we need all relevant IBC messages or just search by events regardless of message type.
        # Searching by event type directly is safer: events='send_packet.packet_sequence EXISTS'?
        # Standard Cosmos API /txs is usually by message action.
        
        # Strategy: Fetch all relevant actions
        actions = [
            '/ibc.applications.transfer.v1.MsgTransfer', # Sends (ICS20)
            '/ibc.core.channel.v1.MsgRecvPacket',        # Recvs
            '/ibc.core.channel.v1.MsgAcknowledgement',   # Acks
            '/ibc.core.channel.v1.MsgTimeout'            # Timeouts
        ]
        
        chain_txs = []
        for action in actions:
            logger.info(f"  Fetching action: {action}")
            params = {
                'events': f"message.action='{action}'",
                'pagination.limit': PAGE_LIMIT
            }
            # Note: We are ignoring date filtering in the API call for simplicity (Cosmos API date filtering is tricky via events)
            # We will filter by date post-fetch or implementation of tx.height/block_date search if needed.
            # Here we rely on max_txs and post-filter.
            
            txs = client.fetch_txs(params, max_txs=args.max_txs)
            
            # Simple date filter
            valid_txs = []
            for tx in txs:
                ts_str = tx.get('timestamp')
                if ts_str:
                    ts = parse_iso_date(ts_str)
                    if start_date <= ts <= end_date:
                        valid_txs.append(tx)
            
            chain_txs.extend(valid_txs)
            logger.info(f"  Found {len(valid_txs)} valid txs for {action}")

        # Save Raw 
        if chain_txs:
            raw_filename = f"ibc_raw_txs_{chain_id}.json" # JSON better for raw structure
            with open(raw_filename, 'w') as f:
                json.dump(chain_txs, f, indent=2)
            logger.info(f"Saved raw txs to {raw_filename}")

        # PARSE
        events = parse_ibc_events(chain_txs, chain_id)
        all_events.extend(events)
        logger.info(f"Extracted {len(events)} IBC events from {chain_id}")

    # MATCH
    logger.info("Matching IBC packets...")
    packets_df = match_ibc_packets(all_events)
    logger.info(f"Matched {len(packets_df)} unique packets.")

    if packets_df.empty:
        logger.warning("No packets found. Exiting.")
        return

    # COMPUTE
    logger.info("Computing metrics...")
    packets_df, agg_df = compute_metrics(packets_df)
    
    # SAVE
    packets_df.to_csv("ibc_packets_normalized.csv", index=False)
    agg_df.to_csv("ibc_metrics_aggregated.csv", index=False)
    
    logger.info("Done! Saved 'ibc_packets_normalized.csv' and 'ibc_metrics_aggregated.csv'.")
    
    # Print sample
    print("\n--- Aggregated Metrics Sample ---")
    print(agg_df.head().to_string())

if __name__ == "__main__":
    main()

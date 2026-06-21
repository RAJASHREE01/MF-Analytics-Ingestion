import os
import time
import logging
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Fix: Force standard redirection processing by avoiding blank statuses in the retry list
session = requests.Session()
retries = Retry(
    total=3, 
    backoff_factor=1, 
    status_forcelist=[500, 502, 503, 504, 429],
    raise_on_status=False
)
session.mount("https://", HTTPAdapter(max_retries=retries))

INPUT_CSV = "schemes.csv"
MASTER_PARQUET = "schemes_master.parquet"
NAV_PARQUET = "nav_history.parquet"
CHECKPOINT_FILE = "pipeline_checkpoint.txt"
TIMEOUT_SECS = 15

def get_last_processed_index() -> int:
    """Reads the progress file to see where the script left off."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    return 0

def update_checkpoint(index: int):
    """Saves the current successful item index loop position to disk."""
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(index))

def save_to_parquet(df: pd.DataFrame, filepath: str):
    """Appends data cleanly into Parquet format using PyArrow tables."""
    if df.empty:
        return
    df = df.astype(str)
    table = pa.Table.from_pandas(df, preserve_index=False)
    
    with pq.ParquetWriter(filepath, table.schema, version="2.6") as writer:
        writer.write_table(table)

def process_pipeline():
    if not os.path.exists(INPUT_CSV):
        logging.error(f"Source file {INPUT_CSV} missing!")
        return
        
    df_codes = pd.read_csv(INPUT_CSV)
    scheme_codes = df_codes["schemeCode"].dropna().unique()
    total_schemes = len(scheme_codes)
    logging.info(f"Loaded {total_schemes} targets for ingestion.")
    
    # Checkpoint configuration resume checkpoint state
    start_index = get_last_processed_index()
    if start_index > 0:
        logging.info(f"Resuming pipeline from checkpoint tracking index position: {start_index}")

    for idx in range(start_index, total_schemes):
        code = scheme_codes[idx]
        clean_code = str(int(float(code)))
        
        # Explicit target endpoint structure mapping
        url = f"https://api.mfapi.in/mf/{clean_code}"
        
        try:
            logging.info(f"[{idx + 1}/{total_schemes}] Requesting Endpoint: {url}")
            
            # Explicitly allow standard browser redirects
            response = session.get(url, timeout=TIMEOUT_SECS, allow_redirects=True)
            response.raise_for_status()
            
            payload = response.json()
            meta_block = payload.get("meta")
            data_block = payload.get("data")
            
            if not meta_block or not data_block:
                logging.warning(f"Scheme {clean_code} returned empty arrays. Skipping.")
                update_checkpoint(idx + 1)
                continue

            # Save data blocks
            df_meta = pd.DataFrame([meta_block])
            save_to_parquet(df_meta, MASTER_PARQUET)

            df_nav = pd.DataFrame(data_block)
            df_nav["scheme_code"] = str(clean_code)
            save_to_parquet(df_nav, NAV_PARQUET)

            # Successfully processed—save progress checkpoint marker
            update_checkpoint(idx + 1)
            
            # Tiny cool-down buffer to safeguard your local IP profile from rate-limiting bans
            time.sleep(0.2)

        except requests.exceptions.RequestException as net_err:
            logging.error(f"Network error on scheme index {idx+1} (Code {clean_code}): {net_err}")
            # Do not advance checkpoint automatically on catastrophic core connection dropouts
            time.sleep(5) 
            continue
            
        except ValueError as json_err:
            logging.error(f"JSON validation error on scheme {clean_code}: {json_err}")
            update_checkpoint(idx + 1)
            continue

if __name__ == "__main__":
    process_pipeline()

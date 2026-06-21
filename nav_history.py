import os
import logging
import threading
import pandas as pd
import pyarrow as pa
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from threading import Lock
from urllib3.util import Retry
from deltalake import DeltaTable, write_deltalake

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

INPUT_CSV        = "schemes.csv"
MASTER_DELTA     = "schemes_master_delta"
NAV_DELTA        = "nav_history_delta"
CHECKPOINT_FILE  = "pipeline_checkpoint.txt"

TIMEOUT_SECS     = 15
MAX_WORKERS      = 20    # lower to 10 if mfapi.in returns 429s
WRITE_BATCH_SIZE = 200   # flush to Delta every N schemes

# ---------------------------------------------------------------------------
# Thread-local HTTP sessions (requests.Session is not thread-safe)
# ---------------------------------------------------------------------------

_local = threading.local()

def get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        s = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504, 429],
            raise_on_status=False,
        )
        s.mount("https://", HTTPAdapter(max_retries=retries))
        _local.session = s
    return _local.session

# ---------------------------------------------------------------------------
# Checkpoint  — tracks processed scheme codes, not fragile numeric indices
# ---------------------------------------------------------------------------

def load_checkpoint() -> set:
    """Returns set of scheme codes already successfully processed."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return {line.strip() for line in f if line.strip()}
    return set()

_checkpoint_lock = Lock()

def mark_done(code: str):
    """Appends a single scheme code to the checkpoint file."""
    with _checkpoint_lock:
        with open(CHECKPOINT_FILE, "a") as f:
            f.write(f"{code}\n")

# ---------------------------------------------------------------------------
# Delta Lake upsert  — no deletes, just check-and-add / check-and-update
# ---------------------------------------------------------------------------

_delta_lock = Lock()

def upsert_to_delta(df: pd.DataFrame, path: str, merge_keys: list[str]):
    """
    Upserts df into a Delta table at `path`.

    Behaviour per row:
        - Key already exists  → update all columns in place
        - Key is new          → insert the row
        - Old rows not in df  → untouched (no deletes, ever)

    Safe to call concurrently — protected by a process-level lock.
    """
    if df.empty:
        return

    table = pa.Table.from_pandas(df.astype(str), preserve_index=False)

    with _delta_lock:
        if not DeltaTable.is_deltatable(path):
            # First ever write — create the Delta table
            write_deltalake(path, table, mode="overwrite")
            logging.info(f"Delta table created at: {path}")
            return

        dt = DeltaTable(path)
        predicate = " AND ".join(
            f"target.{k} = source.{k}" for k in merge_keys
        )

        (
            dt.merge(
                source=table,
                predicate=predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()      # existing key  → update columns
            .when_not_matched_insert_all()  # new key       → insert row
            .execute()
        )

# ---------------------------------------------------------------------------
# Per-scheme fetch  — runs inside a worker thread
# ---------------------------------------------------------------------------

def fetch_scheme(code: str) -> dict | None:
    """
    Fetches metadata + NAV history for one scheme from mfapi.in.
    Returns a dict on success, None on any failure.
    """
    url = f"https://api.mfapi.in/mf/{code}"
    try:
        response = get_session().get(url, timeout=TIMEOUT_SECS, allow_redirects=True)
        response.raise_for_status()
        payload = response.json()

        meta = payload.get("meta")
        data = payload.get("data")

        if not meta or not data:
            logging.warning(f"[{code}] Empty payload — skipping.")
            return None

        return {"meta": meta, "nav": data, "code": code}

    except requests.exceptions.RequestException as e:
        logging.error(f"[{code}] Network error: {e}")
        return None
    except ValueError as e:
        logging.error(f"[{code}] JSON parse error: {e}")
        return None

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_pipeline():
    if not os.path.exists(INPUT_CSV):
        logging.error(f"Input file not found: {INPUT_CSV}")
        return

    # Load all scheme codes
    df_codes = pd.read_csv(INPUT_CSV)
    all_codes = [
        str(int(float(c)))
        for c in df_codes["schemeCode"].dropna().unique()
    ]

    # Skip codes already processed
    done      = load_checkpoint()
    pending   = [c for c in all_codes if c not in done]
    total     = len(all_codes)

    logging.info(
        f"Total schemes : {total} | "
        f"Already done  : {len(done)} | "
        f"To fetch      : {len(pending)}"
    )

    if not pending:
        logging.info("Nothing to do. Pipeline is up to date.")
        return

    meta_buffer: list[dict] = []
    nav_buffer:  list[dict] = []
    completed = 0
    errors    = 0

    def flush_buffers():
        nonlocal meta_buffer, nav_buffer
        if meta_buffer:
            upsert_to_delta(
                pd.DataFrame(meta_buffer),
                MASTER_DELTA,
                merge_keys=["scheme_code"],         # one row per scheme
            )
        if nav_buffer:
            upsert_to_delta(
                pd.DataFrame(nav_buffer),
                NAV_DELTA,
                merge_keys=["scheme_code", "date"], # one NAV per scheme per day
            )
        meta_buffer = []
        nav_buffer  = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_scheme, code): code for code in pending}

        for future in as_completed(futures):
            code   = futures[future]
            result = future.result()

            if result:
                # --- Accumulate meta ---
                meta_row = dict(result["meta"])
                # Ensure scheme_code is always present as the merge key
                meta_row.setdefault("scheme_code", result["code"])
                meta_buffer.append(meta_row)

                # --- Accumulate NAV rows ---
                for row in result["nav"]:
                    nav_buffer.append({
                        "scheme_code": result["code"],
                        "date":        row.get("date"),
                        "nav":         row.get("nav"),
                    })

                mark_done(code)
            else:
                errors += 1

            completed += 1

            # --- Periodic flush ---
            if completed % WRITE_BATCH_SIZE == 0:
                logging.info(
                    f"Progress: {completed}/{len(pending)} | "
                    f"Errors so far: {errors} — flushing batch..."
                )
                flush_buffers()

    # --- Final flush for the tail batch ---
    flush_buffers()

    logging.info(
        f"Pipeline complete. "
        f"Processed: {completed - errors} | "
        f"Failed:    {errors}"
    )

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    process_pipeline()
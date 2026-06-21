import os
import logging
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import threading
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MAX_WORKERS  = 20
TIMEOUT_SECS = 15

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database="MF_ANALYTICS",
        schema="RAW",
        role=os.getenv("SF_ROLE")
    )

# ---------------------------------------------------------------------------
# Thread-local sessions
# ---------------------------------------------------------------------------

_local = threading.local()

def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500,502,503,504,429])
        s.mount("https://", HTTPAdapter(max_retries=retries))
        _local.session = s
    return _local.session

# ---------------------------------------------------------------------------
# Step 1 — fetch scheme codes + max nav_date already in Snowflake
# ---------------------------------------------------------------------------

def get_existing_state(conn):
    """
    Returns a dict: { scheme_code: max_nav_date_already_loaded }
    This is our state — tells us what to skip per scheme.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT n.SCHEME_CODE, MAX(n.NAV_DATE) as max_date
        FROM RAW.NAV_HISTORY n
        GROUP BY n.SCHEME_CODE
    """)
    rows = cursor.fetchall()
    cursor.close()
    # { '100027': datetime.date(2024, 6, 5), ... }
    return {str(row[0]): row[1] for row in rows}

def get_all_scheme_codes(conn):
    """All scheme codes from SCHEMES_MASTER — source of truth."""
    cursor = conn.cursor()
    cursor.execute("SELECT SCHEME_CODE FROM RAW.SCHEMES_MASTER")
    codes = [str(row[0]) for row in cursor.fetchall()]
    cursor.close()
    return codes

# ---------------------------------------------------------------------------
# Step 2 — fetch latest NAV for one scheme
# ---------------------------------------------------------------------------

def fetch_latest_nav(scheme_code, max_loaded_date):
    """
    Hits mfapi.in for one scheme.
    Returns only NAV rows newer than max_loaded_date.
    """
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    try:
        resp = get_session().get(url, timeout=TIMEOUT_SECS)
        resp.raise_for_status()
        payload = resp.json()

        meta = payload.get("meta", {})
        data = payload.get("data", [])

        if not data:
            return [], None

        new_rows = []
        for row in data:
            nav_date = pd.to_datetime(row["date"], format="%d-%m-%Y").date()
            # Only keep rows newer than what's already in Snowflake
            if max_loaded_date is None or nav_date > max_loaded_date:
                new_rows.append({
                    "SCHEME_CODE": scheme_code,
                    "NAV_DATE":    nav_date,
                    "NAV":         float(row["nav"]) if row["nav"] else None
                })

        return new_rows, meta

    except Exception as e:
        logging.error(f"[{scheme_code}] Failed: {e}")
        return [], None

# ---------------------------------------------------------------------------
# Step 3 — detect and insert new schemes
# ---------------------------------------------------------------------------

def insert_new_schemes(conn, new_schemes: list[dict]):
    if not new_schemes:
        return
    df = pd.DataFrame(new_schemes)
    df.columns = [c.upper() for c in df.columns]
    write_pandas(conn, df, "SCHEMES_MASTER", auto_create_table=False, overwrite=False)
    logging.info(f"Inserted {len(df)} new schemes into SCHEMES_MASTER")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    conn = get_connection()
    try:
        logging.info("Fetching existing state from Snowflake...")
        existing_state = get_existing_state(conn)      # { code: max_date }
        all_codes      = get_all_scheme_codes(conn)    # [code, ...]

        logging.info(f"Total schemes: {len(all_codes)}")

        all_new_nav     = []
        new_schemes     = []

        # Run concurrently
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(
                    fetch_latest_nav,
                    code,
                    existing_state.get(code)  # None if scheme never loaded
                ): code
                for code in all_codes
            }

            for future in as_completed(futures):
                code = futures[future]
                new_rows, meta = future.result()

                if new_rows:
                    all_new_nav.extend(new_rows)

                # New scheme not in SCHEMES_MASTER yet
                if meta and code not in existing_state:
                    new_schemes.append(meta)

        logging.info(f"New NAV rows to insert: {len(all_new_nav)}")

        # Insert new schemes first (FK integrity)
        insert_new_schemes(conn, new_schemes)

        # Bulk insert new NAV rows
        if all_new_nav:
            df_nav = pd.DataFrame(all_new_nav)
            df_nav["NAV"] = pd.to_numeric(df_nav["NAV"], errors="coerce")
            df_nav = df_nav.dropna(subset=["NAV", "NAV_DATE"])
            df_nav = df_nav[df_nav["NAV"] > 0]
            write_pandas(conn, df_nav, "NAV_HISTORY",
                        auto_create_table=False, overwrite=False)
            logging.info(f"Inserted {len(df_nav)} new NAV rows into Snowflake")
        else:
            logging.info("No new NAV data today.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run()
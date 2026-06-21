import os
import platform
import pandas as pd
from deltalake import DeltaTable
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from dotenv import load_dotenv

platform.libc_ver = lambda *args, **kwargs: ("", "")
load_dotenv()

DELTA_PATHS = {
    "SCHEMES_MASTER": r"D:\Automation\MF\schemes_master_delta",
    "NAV_HISTORY":    r"D:\Automation\MF\nav_history_delta",
}

CHUNK_SIZE = 500_000  # rows per batch

def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database="MF_ANALYTICS",
        schema="RAW",
        role=os.getenv("SF_ROLE"),
        client_telemetry_enabled=False
    )

def load_in_chunks(conn, df, table_name):
    total = len(df)
    loaded = 0
    for i in range(0, total, CHUNK_SIZE):
        chunk = df.iloc[i : i + CHUNK_SIZE]
        success, _, rows, _ = write_pandas(
            conn, chunk, table_name,
            auto_create_table=False,
            overwrite=False
        )
        loaded += rows
        print(f"  [{table_name}] {loaded}/{total} rows loaded...")
    print(f"  [{table_name}] Done. Total: {loaded} rows.")

def load_schemes_master(conn):
    print("Loading SCHEMES_MASTER...")
    df = DeltaTable(DELTA_PATHS["SCHEMES_MASTER"]).to_pandas()
    df.columns = [c.upper() for c in df.columns]
    df["SCHEME_CODE"] = df["SCHEME_CODE"].astype(str)
    load_in_chunks(conn, df, "SCHEMES_MASTER")

def load_nav_history(conn):
    print("Loading NAV_HISTORY...")
    df = DeltaTable(DELTA_PATHS["NAV_HISTORY"]).to_pandas()
    df.columns = [c.upper() for c in df.columns]
    df["NAV_DATE"] = pd.to_datetime(df["DATE"], format="%d-%m-%Y").dt.date
    df = df.drop(columns=["DATE"])
    df["NAV"] = pd.to_numeric(df["NAV"], errors="coerce")
    df = df.dropna(subset=["NAV", "NAV_DATE"])
    df["SCHEME_CODE"] = df["SCHEME_CODE"].astype(str)
    load_in_chunks(conn, df, "NAV_HISTORY")

if __name__ == "__main__":
    conn = get_connection()
    try:
        load_schemes_master(conn)
        load_nav_history(conn)
        print("\nAll done.")
    except Exception as e:
        print(f"Failed: {e}")
    finally:
        conn.close()
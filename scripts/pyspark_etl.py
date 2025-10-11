# scripts/pyspark_etl.py
# ==========================================
# PySpark ETL — формирует витрину клиентов (mart_customer_agg),
# сохраняет CSV и загружает в MinIO.
# Источник: ClickHouse (внутри docker-сети)
# ==========================================

import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
from clickhouse_driver import Client
from minio import Minio

# ---- Настройки ----
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "piccha")

MINIO_HOST = os.getenv("MINIO_HOST", "minio")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_URL = f"{MINIO_HOST}:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "analytics")

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    stream=sys.stdout
)
log = logging.getLogger("pyspark_etl")

def read_table_to_df(ch: Client, db: str, tbl: str) -> pd.DataFrame:
    log.info(" → Loading table: %s", tbl)
    q = f"SELECT * FROM {db}.{tbl}"
    result = ch.execute(q, with_column_types=True)
    if not result or not result[0]:
        # возвращаем пустой DataFrame (не падаем, обработаем дальше)
        log.warning("Table %s.%s is empty or query returned no rows.", db, tbl)
        return pd.DataFrame()
    rows, cols = result
    colnames = [c[0] for c in cols]
    return pd.DataFrame(rows, columns=colnames)


def prepare_customer_agg(purchases_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Считает агрегации уровня клиента:
    purchase_count, total_spent, avg_basket, first_purchase, last_purchase,
    distinct_stores, cash_count, card_count, delivery_count, new_customer, is_loyalty_member, registration_date
    """
    if purchases_df.empty:
        log.warning("Purchases table is empty -> returning empty agg DataFrame")
        return pd.DataFrame(columns=[
            "customer_id","purchase_count","total_spent","avg_basket","first_purchase","last_purchase",
            "distinct_stores","cash_count","card_count","delivery_count","new_customer","is_loyalty_member",
            "registration_date"
        ])

    df = purchases_df.copy()

    # Normalize column names commonly used in DDL (just in case)
    # Expect columns: purchase_id, customer_id, store_id, total_amount, payment_method, is_delivery, purchase_datetime
    # Convert types
    if "purchase_datetime" in df.columns:
        df["purchase_datetime"] = pd.to_datetime(df["purchase_datetime"], errors="coerce")
    else:
        df["purchase_datetime"] = pd.NaT

    # Ensure numeric
    if "total_amount" in df.columns:
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0.0)
    else:
        df["total_amount"] = 0.0

    # Make sure flags are 0/1 ints
    if "is_delivery" in df.columns:
        df["is_delivery"] = df["is_delivery"].astype(int).fillna(0).astype(int)
    else:
        df["is_delivery"] = 0

    # Normalize payment_method lower
    if "payment_method" in df.columns:
        df["payment_method"] = df["payment_method"].astype(str).str.lower().fillna("")
    else:
        df["payment_method"] = ""

    # Group by customer
    agg = df.groupby("customer_id").agg(
        purchase_count = ("purchase_id", "count"),
        total_spent = ("total_amount", "sum"),
        avg_basket = ("total_amount", "mean"),
        first_purchase = ("purchase_datetime", "min"),
        last_purchase = ("purchase_datetime", "max"),
        distinct_stores = ("store_id", lambda s: int(pd.Series(s).nunique())),
        cash_count = ("payment_method", lambda p: int((p == "cash").sum())),
        card_count = ("payment_method", lambda p: int((p == "card").sum())),
        delivery_count = ("is_delivery", "sum")
    ).reset_index()

    # Ensure proper dtypes
    agg["purchase_count"] = agg["purchase_count"].astype(int)
    agg["distinct_stores"] = agg["distinct_stores"].astype(int)
    agg["cash_count"] = agg["cash_count"].astype(int)
    agg["card_count"] = agg["card_count"].astype(int)
    agg["delivery_count"] = agg["delivery_count"].astype(int)
    agg["avg_basket"] = agg["avg_basket"].fillna(0.0)

    # Join with customers to get registration_date and is_loyalty_member
    cust = customers_df.copy()
    if "registration_date" in cust.columns:
        cust["registration_date"] = pd.to_datetime(cust["registration_date"], errors="coerce")
    else:
        cust["registration_date"] = pd.NaT

    if "is_loyalty_member" in cust.columns:
        cust["is_loyalty_member"] = pd.to_numeric(cust["is_loyalty_member"], errors="coerce").fillna(0).astype(int)
    else:
        cust["is_loyalty_member"] = 0

    cust_sub = cust[["customer_id", "registration_date", "is_loyalty_member"]].drop_duplicates(subset=["customer_id"])

    merged = agg.merge(cust_sub, how="left", on="customer_id")

    # Compute new_customer: registration_date within last 30 days -> 1, else 0
    now = pd.Timestamp.now()
    merged["new_customer"] = merged["registration_date"].apply(lambda d: 1 if pd.notna(d) and (now - d).days < 30 else 0)
    merged["is_loyalty_member"] = merged["is_loyalty_member"].fillna(0).astype(int)
    merged["registration_date"] = merged["registration_date"].where(pd.notna(merged["registration_date"]), None)

    # Reorder columns to match mart table
    cols = [
        "customer_id","purchase_count","total_spent","avg_basket","first_purchase","last_purchase",
        "distinct_stores","cash_count","card_count","delivery_count","new_customer","is_loyalty_member","registration_date"
    ]
    for c in cols:
        if c not in merged.columns:
            merged[c] = None

    merged = merged[cols]
    return merged


def insert_into_clickhouse(ch: Client, df: pd.DataFrame):
    """
    TRUNCATE piccha.mart_customer_agg и вставить строки из df.
    Ожидаем столбцы:
    customer_id, purchase_count, total_spent, avg_basket, first_purchase, last_purchase,
    distinct_stores, cash_count, card_count, delivery_count, new_customer, is_loyalty_member, registration_date
    """
    log.info("Truncating piccha.mart_customer_agg")
    ch.execute("TRUNCATE TABLE IF EXISTS piccha.mart_customer_agg")

    if df.empty:
        log.info("No aggregated rows to insert (empty DataFrame).")
        return

    # Prepare rows as tuples in order expected by table (including ins_ts last)
    rows = []
    now = datetime.now()
    for _, r in df.iterrows():
        tup = (
            str(r["customer_id"]) if pd.notna(r["customer_id"]) else "",
            int(r["purchase_count"]) if pd.notna(r["purchase_count"]) else 0,
            float(r["total_spent"]) if pd.notna(r["total_spent"]) else 0.0,
            float(r["avg_basket"]) if pd.notna(r["avg_basket"]) else 0.0,
            r["first_purchase"].to_pydatetime() if pd.notna(r["first_purchase"]) else None,
            r["last_purchase"].to_pydatetime() if pd.notna(r["last_purchase"]) else None,
            int(r["distinct_stores"]) if pd.notna(r["distinct_stores"]) else 0,
            int(r["cash_count"]) if pd.notna(r["cash_count"]) else 0,
            int(r["card_count"]) if pd.notna(r["card_count"]) else 0,
            int(r["delivery_count"]) if pd.notna(r["delivery_count"]) else 0,
            int(r["new_customer"]) if pd.notna(r["new_customer"]) else 0,
            int(r["is_loyalty_member"]) if pd.notna(r["is_loyalty_member"]) else 0,
            r["registration_date"].to_pydatetime() if pd.notna(r["registration_date"]) else None,
            now
        )
        rows.append(tup)

    # Build insert query with explicit columns (ins_ts last)
    insert_query = """
    INSERT INTO piccha.mart_customer_agg
    (customer_id, purchase_count, total_spent, avg_basket, first_purchase, last_purchase,
     distinct_stores, cash_count, card_count, delivery_count, new_customer, is_loyalty_member,
     registration_date, ins_ts)
    VALUES
    """
    log.info("Inserting %d rows into ClickHouse mart_customer_agg...", len(rows))
    ch.execute(insert_query, rows)
    log.info("Insert finished.")


def upload_csv_to_minio(local_path: str, object_name: str):
    client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    if not client.bucket_exists(BUCKET_NAME):
        log.info("Creating bucket %s", BUCKET_NAME)
        client.make_bucket(BUCKET_NAME)
    log.info("Uploading %s -> %s/%s", local_path, BUCKET_NAME, object_name)
    client.fput_object(BUCKET_NAME, object_name, local_path)
    log.info("Upload done.")


def main():
    log.info("Start ETL: %s", datetime.now().isoformat())
    # Connect to ClickHouse native (TCP) for reads/writes
    ch = Client(host=CLICKHOUSE_HOST)  # default port 9000

    # Read tables
    customers_df = read_table_to_df(ch, CLICKHOUSE_DB, "clean_customers") \
        if "clean_customers" in [r[0] for r in ch.execute(f"SHOW TABLES FROM {CLICKHOUSE_DB}", with_column_types=True)[0]] else read_table_to_df(ch, CLICKHOUSE_DB, "customers")
    purchases_df = read_table_to_df(ch, CLICKHOUSE_DB, "clean_purchases") \
        if "clean_purchases" in [r[0] for r in ch.execute(f"SHOW TABLES FROM {CLICKHOUSE_DB}", with_column_types=True)[0]] else read_table_to_df(ch, CLICKHOUSE_DB, "purchases")
    products_df = read_table_to_df(ch, CLICKHOUSE_DB, "clean_products") \
        if "clean_products" in [r[0] for r in ch.execute(f"SHOW TABLES FROM {CLICKHOUSE_DB}", with_column_types=True)[0]] else read_table_to_df(ch, CLICKHOUSE_DB, "products")
    stores_df = read_table_to_df(ch, CLICKHOUSE_DB, "clean_stores") \
        if "clean_stores" in [r[0] for r in ch.execute(f"SHOW TABLES FROM {CLICKHOUSE_DB}", with_column_types=True)[0]] else read_table_to_df(ch, CLICKHOUSE_DB, "stores")

    # Compute customer-level aggregates (pandas)
    agg_df = prepare_customer_agg(purchases_df, customers_df)

    # Insert into ClickHouse mart table
    insert_into_clickhouse(ch, agg_df)

    # Save CSV locally and upload to MinIO
    os.makedirs("/tmp/outputs", exist_ok=True)
    today = datetime.now().strftime("%Y_%m_%d")
    csv_path = f"/tmp/outputs/mart_customer_agg_{today}.csv"
    agg_df.to_csv(csv_path, index=False)
    log.info("Wrote CSV: %s", csv_path)

    # upload
    try:
        upload_csv_to_minio(csv_path, os.path.basename(csv_path))
    except Exception as e:
        log.exception("MinIO upload failed: %s", e)

    log.info("ETL finished successfully.")


if __name__ == "__main__":
    main()

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

try:
    import mysql.connector
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError as e:
    print(f"ERROR: Missing package — {e}")
    print("Ensure mysql-connector-python and psycopg2-binary are installed.")
    sys.exit(1)

# --- Config ---
START_DATE  = "2025-01-01"   # only migrate payment_date >= this date
BATCH_SIZE  = 10_000         # rows per batch
SOURCE_TABLE = "agent_transactions"
TARGET_TABLE = "agent_transactions"

mysql_cfg = {
    "host":            os.getenv("MYSQL_HOST"),
    "port":            int(os.getenv("MYSQL_PORT", 3306)),
    "user":            os.getenv("MYSQL_USER"),
    "password":        os.getenv("MYSQL_PASSWORD"),
    "database":        os.getenv("MYSQL_DATABASE"),
    "connect_timeout": 10,
}

pg_cfg = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD"),
    "dbname":   os.getenv("PG_DATABASE"),
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS agent_transactions (
    idagent_transactions    BIGINT PRIMARY KEY,
    agency                  VARCHAR(45),
    state_id                VARCHAR(255),
    agent_user              VARCHAR(50),
    agent_code              VARCHAR(255),
    trans_date              TIMESTAMP,
    trans_ref               VARCHAR(200),
    payment_ref             VARCHAR(255),
    rev_head                VARCHAR(255),
    rev_code                VARCHAR(255),
    refcode                 VARCHAR(255),
    reference               VARCHAR(255),
    initialize_url          VARCHAR(255),
    payment_token           VARCHAR(50),
    amount                  NUMERIC(19,2),
    payment_period          VARCHAR(255),
    next_date               VARCHAR(255),
    no_of_days              VARCHAR(255),
    trans_channel           VARCHAR(120),
    status                  VARCHAR(255),
    trans_type              VARCHAR(45),
    lga                     VARCHAR(50),
    createtime              VARCHAR(255),
    vehicle_type            VARCHAR(255),
    vehicle_tonnage         VARCHAR(255),
    vehicle_content         VARCHAR(255),
    take_off_point          VARCHAR(255),
    drop_off_destination    VARCHAR(255),
    taxpayer_type           VARCHAR(100),
    taxpayer_name           VARCHAR(255),
    taxpayer_email          VARCHAR(255),
    taxpayer_phone          VARCHAR(255),
    market                  VARCHAR(255),
    zone_line               VARCHAR(255),
    shop_number             VARCHAR(255),
    revenue_item            VARCHAR(255),
    payment_method          VARCHAR(50),
    plate_number            VARCHAR(255),
    enumeration_id          VARCHAR(255),
    comment                 VARCHAR(255),
    taxoffice               VARCHAR(255),
    notice_number           VARCHAR(40),
    notice_number_fiscal_year VARCHAR(5),
    payment_date            TIMESTAMP,
    valid_date              TIMESTAMP,
    terminalid              VARCHAR(50),
    terminal_serial_number  VARCHAR(60),
    updated_at              TIMESTAMP
);
"""

# Column order must match CREATE TABLE and the SELECT below
COLUMNS = [
    "idagent_transactions", "agency", "state_id", "agent_user", "agent_code",
    "trans_date", "trans_ref", "payment_ref", "rev_head", "rev_code",
    "refcode", "reference", "initialize_url", "paymentToken", "amount",
    "payment_period", "next_date", "no_of_days", "trans_channel", "status",
    "trans_type", "lga", "createtime", "vehicle_type", "vehicle_tonnage",
    "vehicle_content", "take_off_point", "drop_off_destination", "taxpayer_type",
    "taxpayer_name", "taxpayer_email", "taxpayer_phone", "Market", "zoneLine",
    "shopNumber", "revenue_item", "payment_method", "plate_number",
    "enumeration_id", "comment", "taxoffice", "notice_number",
    "notice_number_fiscal_year", "payment_date", "valid_date",
    "terminalid", "terminalSerialNumber", "updated_at",
]

SELECT_SQL = f"""
    SELECT {', '.join(f'`{c}`' for c in COLUMNS)}
    FROM `{SOURCE_TABLE}`
    WHERE payment_date >= %s
    ORDER BY payment_date ASC
"""

INSERT_SQL = f"""
    INSERT INTO {TARGET_TABLE} VALUES %s
    ON CONFLICT (idagent_transactions) DO UPDATE SET
        status        = EXCLUDED.status,
        amount        = EXCLUDED.amount,
        payment_date  = EXCLUDED.payment_date,
        updated_at    = EXCLUDED.updated_at
"""


def main():
    print(f"\n{'='*60}")
    print(f"ETL: MariaDB -> PostgreSQL")
    print(f"Filter: payment_date >= {START_DATE}")
    print(f"Batch size: {BATCH_SIZE:,}")
    print(f"{'='*60}\n")

    # --- Connect to source ---
    print("Connecting to MariaDB...")
    src = mysql.connector.connect(**mysql_cfg)
    src_cursor = src.cursor()

    # Get total count for progress tracking
    src_cursor.execute(
        f"SELECT COUNT(*) FROM `{SOURCE_TABLE}` WHERE payment_date >= %s",
        (START_DATE,)
    )
    total = src_cursor.fetchone()[0]
    print(f"Rows to migrate: {total:,}")

    # --- Connect to target ---
    print("Connecting to PostgreSQL...")
    tgt = psycopg2.connect(**pg_cfg)
    tgt_cursor = tgt.cursor()

    # Create table if it doesn't exist
    tgt_cursor.execute(CREATE_TABLE_SQL)
    tgt.commit()
    print(f"Table '{TARGET_TABLE}' ready in PostgreSQL.\n")

    # --- Stream and load in batches ---
    src_cursor.execute(SELECT_SQL, (START_DATE,))

    loaded     = 0
    batch_num  = 0
    start_time = datetime.now()

    while True:
        rows = src_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        batch_num += 1
        execute_values(tgt_cursor, INSERT_SQL, rows, page_size=BATCH_SIZE)
        tgt.commit()

        loaded += len(rows)
        elapsed = (datetime.now() - start_time).seconds or 1
        rate    = loaded / elapsed
        pct     = (loaded / total * 100) if total else 0
        print(f"  Batch {batch_num:>4} | {loaded:>10,} / {total:,} rows ({pct:.1f}%) | {rate:,.0f} rows/sec")

    src_cursor.close()
    src.close()
    tgt_cursor.close()
    tgt.close()

    elapsed_total = datetime.now() - start_time
    print(f"\nDone. {loaded:,} rows loaded in {elapsed_total}.")


if __name__ == "__main__":
    main()

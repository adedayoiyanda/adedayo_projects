import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- Logging setup ---
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"incremental_load_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

try:
    import mysql.connector
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError as e:
    log.error(f"Missing package: {e}")
    sys.exit(1)

# --- Config ---
BATCH_SIZE     = 10_000
FALLBACK_START = datetime(2025, 1, 1)

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

# -----------------------------------------------------------------------
# DDL
# -----------------------------------------------------------------------
CREATE_WATERMARK_TABLE = """
CREATE TABLE IF NOT EXISTS etl_watermark (
    table_name      VARCHAR(100) PRIMARY KEY,
    last_loaded_at  TIMESTAMP NOT NULL,
    last_run_at     TIMESTAMP NOT NULL,
    rows_loaded     BIGINT DEFAULT 0
);
"""

CREATE_AGENT_TRANSACTIONS = """
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

CREATE_REV_SUB_NEWCODE = """
CREATE TABLE IF NOT EXISTS rev_sub_newcode (
    id              INT PRIMARY KEY,
    mda             VARCHAR(60),
    rev_head        VARCHAR(60),
    rev_code        VARCHAR(255),
    item            VARCHAR(200),
    receipt         VARCHAR(60),
    idrev_sub       INT,
    revenue_item    TEXT,
    enter_by        VARCHAR(200),
    created_at      TIMESTAMP NOT NULL
);
"""

CREATE_BUS_TICKETING_TRANSACTION = """
CREATE TABLE IF NOT EXISTS bus_ticketing_transaction (
    id              INT PRIMARY KEY,
    event           VARCHAR(100),
    event_name      VARCHAR(200),
    trans_id        VARCHAR(50),
    amount          NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    card_serial     VARCHAR(50),
    phone           VARCHAR(15),
    trans_date      TIMESTAMP,
    checkout        TIMESTAMP,
    bus_id          VARCHAR(50),
    route_name      VARCHAR(100),
    entry_point     VARCHAR(200),
    exit_point      VARCHAR(200),
    i_type          VARCHAR(100),
    issuer_id       VARCHAR(50),
    issuer_name     VARCHAR(100),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP
);
"""

CREATE_CONCESSION_PERMIT = """
CREATE TABLE IF NOT EXISTS concession_permit (
    id              INT PRIMARY KEY,
    merchant_id     VARCHAR(255),
    category        VARCHAR(255),
    product_tag     VARCHAR(255),
    product_code    VARCHAR(255),
    product_name    VARCHAR(255),
    amount          NUMERIC(10,2),
    p_amount        NUMERIC(10,2),
    plan_code       VARCHAR(255),
    status          VARCHAR(255)
);
"""

CREATE_ENUMERATION = """
CREATE TABLE IF NOT EXISTS enumeration (
    id                      INT PRIMARY KEY,
    site_id                 VARCHAR(255),
    enumeration_id          VARCHAR(255),
    park                    VARCHAR(255),
    taxpayer_id             VARCHAR(255),
    taxpayer_name           VARCHAR(255),
    revenue_year            VARCHAR(255),
    location                VARCHAR(255),
    revenue_item            VARCHAR(255),
    category                VARCHAR(255),
    union_name              VARCHAR(255),
    plate_number            VARCHAR(255),
    market                  VARCHAR(255),
    market_id               VARCHAR(30),
    zone_line               VARCHAR(255),
    shop_number             VARCHAR(255),
    have_abssin             VARCHAR(255),
    taxpayer_phone          VARCHAR(255),
    shop_occupants          VARCHAR(255),
    income_category         VARCHAR(255),
    monthly_income          VARCHAR(255),
    income_amount           VARCHAR(255),
    shop_category           VARCHAR(255),
    occupant_income_amount  VARCHAR(255),
    payment_method          VARCHAR(255),
    enumeration_plan        VARCHAR(255),
    enumeration_fee         VARCHAR(255),
    status                  VARCHAR(255),
    enumeration_status      VARCHAR(255),
    asset_code              VARCHAR(15),
    enumeration_type        VARCHAR(60),
    notice_generated        INT,
    created_by              VARCHAR(255),
    create_time             TIMESTAMP NOT NULL
);
"""

CREATE_MERCHANT_USER = """
CREATE TABLE IF NOT EXISTS merchant_user (
    user_id         INT PRIMARY KEY,
    abssin          VARCHAR(50),
    user_cat        VARCHAR(45),
    first_name      VARCHAR(45),
    last_name       VARCHAR(255),
    company         VARCHAR(255),
    merchant_id     VARCHAR(45),
    agent_code      VARCHAR(255),
    collection_type VARCHAR(50),
    phone_no        VARCHAR(45),
    email           VARCHAR(45),
    status          VARCHAR(45),
    create_by       VARCHAR(45),
    lga             VARCHAR(255),
    lga_zone        VARCHAR(255),
    bank            VARCHAR(255),
    bank_account    VARCHAR(255),
    balance         VARCHAR(255),
    create_time     TIMESTAMP,
    createdate      TIMESTAMP,
    update_by       VARCHAR(45),
    update_time     TIMESTAMP
);
"""

# -----------------------------------------------------------------------
# concession_permit — full refresh (small reference table)
# -----------------------------------------------------------------------
CP_COLUMNS = ["id", "merchant_id", "category", "productTag", "productCode",
              "productName", "amount", "pAmount", "planCode", "status"]

CP_SELECT = f"SELECT {', '.join(f'`{c}`' for c in CP_COLUMNS)} FROM `concession_permit`"

CP_INSERT = """
    INSERT INTO concession_permit VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        merchant_id  = EXCLUDED.merchant_id,
        category     = EXCLUDED.category,
        product_tag  = EXCLUDED.product_tag,
        product_code = EXCLUDED.product_code,
        product_name = EXCLUDED.product_name,
        amount       = EXCLUDED.amount,
        p_amount     = EXCLUDED.p_amount,
        plan_code    = EXCLUDED.plan_code,
        status       = EXCLUDED.status
"""

# -----------------------------------------------------------------------
# enumeration — incremental load by CreateTime (3-day lookback)
# -----------------------------------------------------------------------
ENUM_COLUMNS = [
    "id", "SiteID", "EnumerationID", "Park", "TaxpayerID", "TaxpayerName",
    "RevenueYear", "Location", "RevenueItem", "Category", "UnionName",
    "PlateNumber", "Market", "Market_Id", "zoneLine", "shopNumber",
    "HaveABSSIN", "TaxpayerPhone", "shopOccupants", "IncomeCategory",
    "MonthlyIncome", "IncomeAmount", "shopCategory", "occupantIncomeAmount",
    "PaymentMethod", "EnumerationPlan", "EnumerationFee", "Status",
    "EnumerationStatus", "assetCode", "enumerationType", "NoticeGenerated",
    "CreatedBy", "CreateTime",
]
ENUM_CREATE_TIME_IDX = ENUM_COLUMNS.index("CreateTime")

ENUM_SELECT = f"""
    SELECT {', '.join(f'`{c}`' for c in ENUM_COLUMNS)}
    FROM `enumeration`
    WHERE CreateTime > %s
    ORDER BY CreateTime ASC
"""

ENUM_INSERT = """
    INSERT INTO enumeration VALUES %s
    ON CONFLICT (id) DO NOTHING
"""

# -----------------------------------------------------------------------
# merchant_user — incremental SCD (create_time OR update_time)
# -----------------------------------------------------------------------
MU_COLUMNS = [
    "user_id", "abssin", "user_cat", "first_name", "last_name", "company",
    "merchant_id", "agent_code", "collection_type", "phone_no", "email",
    "status", "create_by", "lga", "lga_zone", "bank", "bank_account",
    "balance", "create_time", "createdate", "update_by", "update_time",
]
MU_CREATE_TIME_IDX = MU_COLUMNS.index("create_time")
MU_UPDATE_TIME_IDX = MU_COLUMNS.index("update_time")

MU_SELECT = f"""
    SELECT {', '.join(f'`{c}`' for c in MU_COLUMNS)}
    FROM `merchant_user`
    WHERE create_time > %s OR (update_time IS NOT NULL AND update_time > %s)
    ORDER BY GREATEST(create_time, COALESCE(update_time, create_time)) ASC
"""

MU_INSERT = """
    INSERT INTO merchant_user VALUES %s
    ON CONFLICT (user_id) DO UPDATE SET
        abssin          = EXCLUDED.abssin,
        user_cat        = EXCLUDED.user_cat,
        first_name      = EXCLUDED.first_name,
        last_name       = EXCLUDED.last_name,
        company         = EXCLUDED.company,
        merchant_id     = EXCLUDED.merchant_id,
        agent_code      = EXCLUDED.agent_code,
        collection_type = EXCLUDED.collection_type,
        phone_no        = EXCLUDED.phone_no,
        email           = EXCLUDED.email,
        status          = EXCLUDED.status,
        create_by       = EXCLUDED.create_by,
        lga             = EXCLUDED.lga,
        lga_zone        = EXCLUDED.lga_zone,
        bank            = EXCLUDED.bank,
        bank_account    = EXCLUDED.bank_account,
        balance         = EXCLUDED.balance,
        create_time     = EXCLUDED.create_time,
        createdate      = EXCLUDED.createdate,
        update_by       = EXCLUDED.update_by,
        update_time     = EXCLUDED.update_time
"""

# -----------------------------------------------------------------------
# agent_transactions — incremental load by trans_date (3-day lookback)
# -----------------------------------------------------------------------
AT_COLUMNS = [
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
AT_TRANS_DATE_IDX = AT_COLUMNS.index("trans_date")

AT_SELECT = f"""
    SELECT {', '.join(f'`{c}`' for c in AT_COLUMNS)}
    FROM `agent_transactions`
    WHERE trans_date > %s
    ORDER BY trans_date ASC
"""

AT_INSERT = """
    INSERT INTO agent_transactions VALUES %s
    ON CONFLICT (idagent_transactions) DO UPDATE SET
        status       = EXCLUDED.status,
        amount       = EXCLUDED.amount,
        payment_date = EXCLUDED.payment_date,
        updated_at   = EXCLUDED.updated_at
"""

SELECT_WATERMARK = "SELECT last_loaded_at FROM etl_watermark WHERE table_name = %s"
UPSERT_WATERMARK = """
    INSERT INTO etl_watermark (table_name, last_loaded_at, last_run_at, rows_loaded)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (table_name) DO UPDATE SET
        last_loaded_at = EXCLUDED.last_loaded_at,
        last_run_at    = EXCLUDED.last_run_at,
        rows_loaded    = EXCLUDED.rows_loaded
"""

# -----------------------------------------------------------------------
# bus_ticketing_transaction — incremental load by transDate (3-day lookback)
# -----------------------------------------------------------------------
BT_COLUMNS = [
    "id", "event", "eventName", "transID", "amount", "cardSerial", "phone",
    "transDate", "checkout", "busID", "routeName", "entryPoint", "exitPoint",
    "iType", "issuerID", "issuerName", "createdAt", "updatedAt",
]
BT_TRANS_DATE_IDX = BT_COLUMNS.index("transDate")

BT_SELECT = f"""
    SELECT {', '.join(f'`{c}`' for c in BT_COLUMNS)}
    FROM `bus_ticketing_transaction`
    WHERE transDate > %s
    ORDER BY transDate ASC
"""

BT_INSERT = """
    INSERT INTO bus_ticketing_transaction VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        amount     = EXCLUDED.amount,
        checkout   = EXCLUDED.checkout,
        updated_at = EXCLUDED.updated_at
"""

# -----------------------------------------------------------------------
# rev_sub_newcode — full refresh (no updated_at, small table)
# -----------------------------------------------------------------------
RSN_COLUMNS = ["id", "mda", "rev_head", "rev_code", "item", "receipt",
               "idrev_sub", "revenue_item", "enter_by", "created_at"]

RSN_SELECT = f"SELECT {', '.join(f'`{c}`' for c in RSN_COLUMNS)} FROM `rev_sub_newcode`"

RSN_INSERT = """
    INSERT INTO rev_sub_newcode VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        mda          = EXCLUDED.mda,
        rev_head     = EXCLUDED.rev_head,
        rev_code     = EXCLUDED.rev_code,
        item         = EXCLUDED.item,
        receipt      = EXCLUDED.receipt,
        idrev_sub    = EXCLUDED.idrev_sub,
        revenue_item = EXCLUDED.revenue_item,
        enter_by     = EXCLUDED.enter_by,
        created_at   = EXCLUDED.created_at
"""


def load_agent_transactions(src_cursor, tgt, tgt_cursor, run_start):
    log.info("-" * 40)
    log.info("Table: agent_transactions (incremental)")

    tgt_cursor.execute(SELECT_WATERMARK, ("agent_transactions",))
    row = tgt_cursor.fetchone()
    watermark = row[0] if row else FALLBACK_START
    lookback = watermark - timedelta(days=3)
    log.info(f"Watermark: {watermark}  |  Lookback: {lookback}")

    src_cursor.execute(
        "SELECT COUNT(*) FROM `agent_transactions` WHERE trans_date > %s",
        (lookback,)
    )
    total = src_cursor.fetchone()[0]
    log.info(f"New rows: {total:,}")

    if total == 0:
        log.info("Nothing to load.")
        tgt_cursor.execute(UPSERT_WATERMARK, ("agent_transactions", watermark, run_start, 0))
        tgt.commit()
        return

    src_cursor.execute(AT_SELECT, (lookback,))
    loaded, batch_num, new_watermark = 0, 0, watermark

    while True:
        rows = src_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        batch_num += 1
        execute_values(tgt_cursor, AT_INSERT, rows, page_size=BATCH_SIZE)
        tgt.commit()
        loaded += len(rows)
        batch_max = max((r[AT_TRANS_DATE_IDX] for r in rows if r[AT_TRANS_DATE_IDX]), default=None)
        if batch_max and batch_max > new_watermark:
            new_watermark = batch_max
        pct = loaded / total * 100
        log.info(f"  Batch {batch_num:>4} | {loaded:>8,} / {total:,} ({pct:.1f}%)")

    tgt_cursor.execute(UPSERT_WATERMARK, ("agent_transactions", new_watermark, run_start, loaded))
    tgt.commit()
    log.info(f"agent_transactions: {loaded:,} rows loaded. New watermark: {new_watermark}")


def load_rev_sub_newcode(src_cursor, tgt, tgt_cursor, run_start):
    log.info("-" * 40)
    log.info("Table: rev_sub_newcode (full refresh)")

    src_cursor.execute("SELECT COUNT(*) FROM `rev_sub_newcode`")
    total = src_cursor.fetchone()[0]
    log.info(f"Source rows: {total:,}")

    src_cursor.execute(RSN_SELECT)
    rows = src_cursor.fetchall()

    # Truncate and reload for a clean full refresh
    tgt_cursor.execute("TRUNCATE TABLE rev_sub_newcode")
    execute_values(tgt_cursor, RSN_INSERT, rows, page_size=BATCH_SIZE)
    tgt.commit()

    tgt_cursor.execute(UPSERT_WATERMARK, ("rev_sub_newcode", datetime.now(), run_start, total))
    tgt.commit()
    log.info(f"rev_sub_newcode: {total:,} rows reloaded.")


def load_bus_ticketing_transaction(src_cursor, tgt, tgt_cursor, run_start):
    log.info("-" * 40)
    log.info("Table: bus_ticketing_transaction (incremental)")

    tgt_cursor.execute(SELECT_WATERMARK, ("bus_ticketing_transaction",))
    row = tgt_cursor.fetchone()
    watermark = row[0] if row else FALLBACK_START
    lookback = watermark - timedelta(days=3)
    log.info(f"Watermark: {watermark}  |  Lookback: {lookback}")

    src_cursor.execute(
        "SELECT COUNT(*) FROM `bus_ticketing_transaction` WHERE transDate > %s",
        (lookback,)
    )
    total = src_cursor.fetchone()[0]
    log.info(f"New rows: {total:,}")

    if total == 0:
        log.info("Nothing to load.")
        tgt_cursor.execute(UPSERT_WATERMARK, ("bus_ticketing_transaction", watermark, run_start, 0))
        tgt.commit()
        return

    src_cursor.execute(BT_SELECT, (lookback,))
    loaded, batch_num, new_watermark = 0, 0, watermark

    while True:
        rows = src_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        batch_num += 1
        execute_values(tgt_cursor, BT_INSERT, rows, page_size=BATCH_SIZE)
        tgt.commit()
        loaded += len(rows)
        batch_max = max((r[BT_TRANS_DATE_IDX] for r in rows if r[BT_TRANS_DATE_IDX]), default=None)
        if batch_max and batch_max > new_watermark:
            new_watermark = batch_max
        pct = loaded / total * 100
        log.info(f"  Batch {batch_num:>4} | {loaded:>8,} / {total:,} ({pct:.1f}%)")

    tgt_cursor.execute(UPSERT_WATERMARK, ("bus_ticketing_transaction", new_watermark, run_start, loaded))
    tgt.commit()
    log.info(f"bus_ticketing_transaction: {loaded:,} rows loaded. New watermark: {new_watermark}")


def load_concession_permit(src_cursor, tgt, tgt_cursor, run_start):
    log.info("-" * 40)
    log.info("Table: concession_permit (full refresh)")

    src_cursor.execute("SELECT COUNT(*) FROM `concession_permit`")
    total = src_cursor.fetchone()[0]
    log.info(f"Source rows: {total:,}")

    src_cursor.execute(CP_SELECT)
    rows = src_cursor.fetchall()

    tgt_cursor.execute("TRUNCATE TABLE concession_permit")
    execute_values(tgt_cursor, CP_INSERT, rows, page_size=BATCH_SIZE)
    tgt.commit()

    tgt_cursor.execute(UPSERT_WATERMARK, ("concession_permit", datetime.now(), run_start, total))
    tgt.commit()
    log.info(f"concession_permit: {total:,} rows reloaded.")


def load_enumeration(src_cursor, tgt, tgt_cursor, run_start):
    log.info("-" * 40)
    log.info("Table: enumeration (incremental)")

    tgt_cursor.execute(SELECT_WATERMARK, ("enumeration",))
    row = tgt_cursor.fetchone()
    watermark = row[0] if row else FALLBACK_START
    lookback = watermark - timedelta(days=3)
    log.info(f"Watermark: {watermark}  |  Lookback: {lookback}")

    src_cursor.execute(
        "SELECT COUNT(*) FROM `enumeration` WHERE CreateTime > %s",
        (lookback,)
    )
    total = src_cursor.fetchone()[0]
    log.info(f"New rows: {total:,}")

    if total == 0:
        log.info("Nothing to load.")
        tgt_cursor.execute(UPSERT_WATERMARK, ("enumeration", watermark, run_start, 0))
        tgt.commit()
        return

    src_cursor.execute(ENUM_SELECT, (lookback,))
    loaded, batch_num, new_watermark = 0, 0, watermark

    while True:
        rows = src_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        batch_num += 1
        execute_values(tgt_cursor, ENUM_INSERT, rows, page_size=BATCH_SIZE)
        tgt.commit()
        loaded += len(rows)
        batch_max = max((r[ENUM_CREATE_TIME_IDX] for r in rows if r[ENUM_CREATE_TIME_IDX]), default=None)
        if batch_max and batch_max > new_watermark:
            new_watermark = batch_max
        pct = loaded / total * 100
        log.info(f"  Batch {batch_num:>4} | {loaded:>8,} / {total:,} ({pct:.1f}%)")

    tgt_cursor.execute(UPSERT_WATERMARK, ("enumeration", new_watermark, run_start, loaded))
    tgt.commit()
    log.info(f"enumeration: {loaded:,} rows loaded. New watermark: {new_watermark}")


def load_merchant_user(src_cursor, tgt, tgt_cursor, run_start):
    log.info("-" * 40)
    log.info("Table: merchant_user (incremental SCD)")

    tgt_cursor.execute(SELECT_WATERMARK, ("merchant_user",))
    row = tgt_cursor.fetchone()
    watermark = row[0] if row else FALLBACK_START
    lookback = watermark - timedelta(days=3)
    log.info(f"Watermark: {watermark}  |  Lookback: {lookback}")

    src_cursor.execute("""
        SELECT COUNT(*) FROM `merchant_user`
        WHERE create_time > %s OR (update_time IS NOT NULL AND update_time > %s)
    """, (lookback, lookback))
    total = src_cursor.fetchone()[0]
    log.info(f"New/updated rows: {total:,}")

    if total == 0:
        log.info("Nothing to load.")
        tgt_cursor.execute(UPSERT_WATERMARK, ("merchant_user", watermark, run_start, 0))
        tgt.commit()
        return

    src_cursor.execute(MU_SELECT, (lookback, lookback))
    loaded, batch_num, new_watermark = 0, 0, watermark

    while True:
        rows = src_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        batch_num += 1
        execute_values(tgt_cursor, MU_INSERT, rows, page_size=BATCH_SIZE)
        tgt.commit()
        loaded += len(rows)
        for r in rows:
            ct = r[MU_CREATE_TIME_IDX]
            ut = r[MU_UPDATE_TIME_IDX]
            effective = max((t for t in [ct, ut] if t is not None), default=None)
            if effective and effective > new_watermark:
                new_watermark = effective
        pct = loaded / total * 100
        log.info(f"  Batch {batch_num:>4} | {loaded:>8,} / {total:,} ({pct:.1f}%)")

    tgt_cursor.execute(UPSERT_WATERMARK, ("merchant_user", new_watermark, run_start, loaded))
    tgt.commit()
    log.info(f"merchant_user: {loaded:,} rows loaded. New watermark: {new_watermark}")


def main():
    run_start = datetime.now()
    log.info("=" * 60)
    log.info("ETL started")

    try:
        log.info("Connecting to MariaDB...")
        src = mysql.connector.connect(**mysql_cfg)
        src_cursor = src.cursor()
    except mysql.connector.Error as e:
        log.error(f"MariaDB connection failed: {e}")
        sys.exit(1)

    try:
        log.info("Connecting to PostgreSQL...")
        tgt = psycopg2.connect(**pg_cfg)
        tgt_cursor = tgt.cursor()
    except psycopg2.Error as e:
        log.error(f"PostgreSQL connection failed: {e}")
        src.close()
        sys.exit(1)

    try:
        # Ensure tables exist
        tgt_cursor.execute(CREATE_WATERMARK_TABLE)
        tgt_cursor.execute(CREATE_AGENT_TRANSACTIONS)
        tgt_cursor.execute(CREATE_REV_SUB_NEWCODE)
        tgt_cursor.execute(CREATE_BUS_TICKETING_TRANSACTION)
        tgt_cursor.execute(CREATE_CONCESSION_PERMIT)
        tgt_cursor.execute(CREATE_ENUMERATION)
        tgt_cursor.execute(CREATE_MERCHANT_USER)
        tgt.commit()

        # Migrate rev_head from INT to VARCHAR if not already done
        tgt_cursor.execute("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'rev_sub_newcode'
                      AND column_name = 'rev_head'
                      AND data_type = 'integer'
                ) THEN
                    DROP VIEW IF EXISTS public_staging.stg_rev_sub_newcode;
                    ALTER TABLE rev_sub_newcode
                        ALTER COLUMN rev_head TYPE VARCHAR(60) USING rev_head::VARCHAR;
                END IF;
            END $$;
        """)
        tgt.commit()

        load_agent_transactions(src_cursor, tgt, tgt_cursor, run_start)
        load_rev_sub_newcode(src_cursor, tgt, tgt_cursor, run_start)
        load_bus_ticketing_transaction(src_cursor, tgt, tgt_cursor, run_start)
        load_concession_permit(src_cursor, tgt, tgt_cursor, run_start)
        load_enumeration(src_cursor, tgt, tgt_cursor, run_start)
        load_merchant_user(src_cursor, tgt, tgt_cursor, run_start)

        elapsed = datetime.now() - run_start
        log.info("=" * 60)
        log.info(f"ETL completed in {elapsed}")

    except Exception as e:
        log.error(f"ETL failed: {e}", exc_info=True)
        tgt.rollback()
        sys.exit(1)

    finally:
        src_cursor.close()
        src.close()
        tgt_cursor.close()
        tgt.close()


if __name__ == "__main__":
    main()

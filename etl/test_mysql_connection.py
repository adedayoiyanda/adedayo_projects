import os
import sys
from dotenv import load_dotenv

load_dotenv()

REQUIRED_VARS = ["MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"]
missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
if missing:
    print(f"ERROR: Missing required .env variables: {', '.join(missing)}")
    sys.exit(1)

try:
    import mysql.connector
except ImportError:
    print("ERROR: mysql-connector-python is not installed.")
    print("Run: pip install mysql-connector-python python-dotenv")
    sys.exit(1)

SCHEMA = os.getenv("MYSQL_DATABASE")
TABLE  = "agent_transactions"

config = {
    "host":            os.getenv("MYSQL_HOST"),
    "port":            int(os.getenv("MYSQL_PORT", 3306)),
    "user":            os.getenv("MYSQL_USER"),
    "password":        os.getenv("MYSQL_PASSWORD"),
    "database":        SCHEMA,
    "connect_timeout": 10,
}

print(f"\nConnecting to {config['host']}:{config['port']}  db={SCHEMA}  user={config['user']} ...")

try:
    conn   = mysql.connector.connect(**config)
    cursor = conn.cursor(dictionary=True)

    # --- 1. Server version ---
    cursor.execute("SELECT VERSION() AS version")
    print(f"Connected. MySQL version: {cursor.fetchone()['version']}")

    # --- 2. Confirm table exists ---
    cursor.execute(
        "SELECT COUNT(*) AS cnt FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (SCHEMA, TABLE)
    )
    if cursor.fetchone()["cnt"] == 0:
        print(f"\nERROR: Table `{SCHEMA}`.`{TABLE}` not found.")
        print("Check that your user has access and the table name is correct.")
        sys.exit(1)

    print(f"\nTable `{SCHEMA}`.`{TABLE}` found.")

    # --- 3. Row count ---
    cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{TABLE}`")
    total_rows = cursor.fetchone()["cnt"]
    print(f"Total rows: {total_rows:,}")

    # --- 4. Column access check ---
    cursor.execute(f"SHOW COLUMNS FROM `{TABLE}`")
    columns = cursor.fetchall()
    col_names = [c["Field"] for c in columns]
    print(f"Columns accessible: {len(col_names)}")

    # Spot-check the key columns from the known schema
    expected_cols = [
        "idagent_transactions", "agency", "state_id", "agent_user", "agent_code",
        "trans_date", "trans_ref", "payment_ref", "rev_head", "rev_code",
        "amount", "status", "trans_type", "lga", "trans_channel",
        "taxpayer_name", "taxpayer_email", "taxpayer_phone", "plate_number",
        "payment_date", "updated_at",
    ]
    missing_cols = [c for c in expected_cols if c not in col_names]
    if missing_cols:
        print(f"WARNING: Expected columns not visible: {missing_cols}")
    else:
        print(f"All key columns are accessible.")

    # --- 5. Date range ---
    cursor.execute(f"SELECT MIN(trans_date) AS earliest, MAX(trans_date) AS latest FROM `{TABLE}`")
    dates = cursor.fetchone()
    print(f"\nDate range (trans_date):  {dates['earliest']}  to  {dates['latest']}")

    cursor.execute(f"SELECT MIN(payment_date) AS earliest, MAX(payment_date) AS latest FROM `{TABLE}`")
    dates2 = cursor.fetchone()
    print(f"Date range (payment_date): {dates2['earliest']}  to  {dates2['latest']}")

    # --- 6. Status breakdown ---
    cursor.execute(
        f"SELECT status, COUNT(*) AS cnt FROM `{TABLE}` GROUP BY status ORDER BY cnt DESC LIMIT 10"
    )
    print("\nStatus breakdown (top 10):")
    for row in cursor.fetchall():
        print(f"  {str(row['status']).ljust(30)}  {row['cnt']:>10,}")

    # --- 7. Amount summary ---
    cursor.execute(
        f"SELECT MIN(amount) AS min_amt, MAX(amount) AS max_amt, "
        f"ROUND(AVG(amount),2) AS avg_amt, ROUND(SUM(amount),2) AS total_amt "
        f"FROM `{TABLE}` WHERE amount IS NOT NULL"
    )
    amt = cursor.fetchone()
    print(f"\nAmount summary:")
    print(f"  Min:   {amt['min_amt']:>15,.2f}")
    print(f"  Max:   {amt['max_amt']:>15,.2f}")
    print(f"  Avg:   {amt['avg_amt']:>15,.2f}")
    print(f"  Total: {amt['total_amt']:>15,.2f}")

    # --- 8. Sample rows ---
    sample_cols = (
        "idagent_transactions, agency, state_id, agent_user, trans_date, "
        "amount, status, trans_type, lga, taxpayer_name, payment_date"
    )
    cursor.execute(f"SELECT {sample_cols} FROM `{TABLE}` ORDER BY trans_date DESC LIMIT 5")
    rows = cursor.fetchall()
    print(f"\nLatest 5 rows ({sample_cols.replace(chr(10), '')}):")
    for row in rows:
        print(f"  {dict(row)}")

    cursor.close()
    conn.close()
    print("\nExtraction test passed. Connection closed.")

except mysql.connector.Error as e:
    print(f"\nMySQL error [{e.errno}]: {e.msg}")
    hints = {
        1045: "Access denied — check MYSQL_USER / MYSQL_PASSWORD in .env",
        2003: "Can't reach host — check MYSQL_HOST / MYSQL_PORT in .env",
        1049: "Unknown database — check MYSQL_DATABASE in .env",
        1142: "Permission denied on table — your user may lack SELECT privilege",
    }
    if e.errno in hints:
        print(f"Hint: {hints[e.errno]}")
    sys.exit(1)

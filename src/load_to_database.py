"""
load_to_database.py
load_to_database.py
Session 6 – Load validated data into SQLite database
"""

import os
import sqlite3
import logging
import pandas as pd

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
data_path = os.path.join("data", "cleaned_timetable.csv")
db_path = os.path.join("database", "transport_timetable.db")

# Step 1: Check for dataset
if not os.path.exists(data_path):
    raise FileNotFoundError(f"{data_path} not found. Run clean_data.py first.")

# Step 2: Load validated data
logging.info("📦 Loading cleaned timetable data...")
df = pd.read_csv(data_path)
logging.info(f"Dataset shape: {df.shape}")
logging.info(f"Columns: {list(df.columns)}")

# Step 3: Create 'database' folder if not exists
os.makedirs("database", exist_ok=True)

# Step 4: Connect to SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
logging.info(f"Connected to database: {db_path}")

# Step 5: Create table
create_table_query = """
CREATE TABLE IF NOT EXISTS timetable (
    route_id TEXT,
    stop_name TEXT,
    arrival_time TEXT,
    departure_time TEXT
);
"""
cursor.execute(create_table_query)
logging.info("🧱 Table 'timetable' verified or created.")

# Step 6: Load data into table
df.to_sql("timetable", conn, if_exists="replace", index=False)
logging.info("✅ Data successfully loaded into 'timetable' table.")

# Step 7: Test fetch (optional)
logging.info("🔍 Verifying first 5 rows from the database:")
preview = pd.read_sql_query("SELECT * FROM timetable LIMIT 5;", conn)
logging.info("\n" + preview.to_string(index=False))

# Step 8: Close connection
conn.close()
logging.info("🔒 Database connection closed.")

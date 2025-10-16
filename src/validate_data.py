"""
validate_data.py
Session 5 – Data Validation for Public Transportation Timetable Normalizer
"""

import os
import logging
import pandas as pd
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
cleaned_data_path = os.path.join("data", "cleaned_timetable.csv")

if not os.path.exists(cleaned_data_path):
    raise FileNotFoundError(f"{cleaned_data_path} not found. Run clean_data.py first.")

# Load data
logging.info("Loading cleaned timetable data...")
df = pd.read_csv(cleaned_data_path)
logging.info(f"Dataset shape: {df.shape}")
logging.info(f"Columns present: {list(df.columns)}")

# Normalize column names to lowercase for consistency
df.columns = df.columns.str.lower()

# Expected minimal columns
required_cols = ["route_id", "stop_name", "arrival_time", "departure_time"]

missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    raise ValueError(f"Missing expected columns: {missing_cols}")

# --- Helper: validate time format ---
def is_valid_time(t):
    try:
        datetime.strptime(t.strip(), "%H:%M")
        return True
    except Exception:
        return False

# --- Validation 1: Missing values ---
missing_values = df.isnull().sum()
if missing_values.any():
    logging.warning("⚠️ Missing values found:")
    logging.warning(missing_values[missing_values > 0])
else:
    logging.info("✅ No missing values found.")

# --- Validation 2: Invalid time format ---
invalid_times = df[
    ~df["arrival_time"].apply(is_valid_time) |
    ~df["departure_time"].apply(is_valid_time)
]
if not invalid_times.empty:
    logging.warning(f"⚠️ Found {len(invalid_times)} invalid time entries.")
    invalid_times.to_csv("data/invalid_time_entries.csv", index=False)
else:
    logging.info("✅ All times are in valid HH:MM format.")

# --- Validation 3: Arrival after departure ---
def time_to_minutes(t):
    h, m = map(int, t.split(":"))
    return h * 60 + m

df["dep_minutes"] = df["departure_time"].apply(time_to_minutes)
df["arr_minutes"] = df["arrival_time"].apply(time_to_minutes)

invalid_order = df[df["arr_minutes"] < df["dep_minutes"]]
if not invalid_order.empty:
    logging.warning(f"⚠️ {len(invalid_order)} entries where arrival is before departure.")
    invalid_order.to_csv("data/invalid_order_entries.csv", index=False)
else:
    logging.info("✅ All trips have arrival after departure.")

# --- Optional: Duration check ---
if "traveltimeminutes" in df.columns:
    invalid_duration = df[
        abs((df["arr_minutes"] - df["dep_minutes"]) - df["traveltimeminutes"]) > 5
    ]
    if not invalid_duration.empty:
        logging.warning(f"⚠️ {len(invalid_duration)} inconsistent travel durations.")
        invalid_duration.to_csv("data/invalid_duration_entries.csv", index=False)
    else:
        logging.info("✅ All travel durations are consistent.")

# Drop helper columns
df.drop(columns=["dep_minutes", "arr_minutes"], inplace=True)

logging.info("🎯 Data validation completed successfully!")

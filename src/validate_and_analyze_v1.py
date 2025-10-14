import pandas as pd
from pathlib import Path
import logging

# --- Setup ---
DATA_DIR = Path("data/processed")
OUT_DIR = Path("data/validated")
OUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --- 1. Load normalized data ---
file_path = DATA_DIR / "normalized_timetable.csv"
df = pd.read_csv(file_path)
logging.info(f"Loaded normalized timetable: {df.shape[0]} rows")

# --- 2. Validate data ---
# Missing values
missing_counts = df.isna().sum()
logging.info(f"Missing values per column:\n{missing_counts}")

# Invalid times: arrival > departure
def time_to_minutes(t):
    if pd.isna(t):
        return None
    h, m = map(int, str(t).split(":"))
    return h * 60 + m

df["arrival_mins"] = df["arrival_time"].apply(time_to_minutes)
df["departure_mins"] = df["departure_time"].apply(time_to_minutes)
invalid_time_rows = df[df["arrival_mins"] > df["departure_mins"]]
logging.info(f"Invalid time rows: {len(invalid_time_rows)}")

# Duplicates
duplicates = df.duplicated(subset=["route_id", "stop_name", "arrival_time", "departure_time"])
logging.info(f"Duplicate rows: {duplicates.sum()}")
df = df[~duplicates]

# --- 3. Clean data ---
clean_df = df.dropna(subset=["route_id", "stop_name"])
clean_df = clean_df.drop(columns=["arrival_mins", "departure_mins"])
clean_df.to_csv(OUT_DIR / "clean_timetable.csv", index=False)
logging.info(f"Clean timetable saved: {OUT_DIR / 'clean_timetable.csv'}")

# --- 4. Analytics ---
summary = {
    "total_rows": len(clean_df),
    "unique_routes": clean_df["route_id"].nunique(),
    "unique_stops": clean_df["stop_name"].nunique(),
    "missing_arrival_times": clean_df["arrival_time"].isna().sum(),
    "missing_departure_times": clean_df["departure_time"].isna().sum(),
    "invalid_time_pairs": len(invalid_time_rows)
}

pd.DataFrame([summary]).to_csv(OUT_DIR / "analytics_report.csv", index=False)
logging.info(f"Analytics report created: {OUT_DIR / 'analytics_report.csv'}")

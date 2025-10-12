import pandas as pd
from pathlib import Path
import logging

RAW = Path("data/raw")
OUT = Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

standard_cols = ["route_id", "stop_name", "arrival_time", "departure_time"]

dfs = []

for file_path in RAW.glob("*.csv"):
    df = pd.read_csv(file_path)
    column_map = {}
    for col in df.columns:
        if col.lower() in ["route_id", "bus_number", "bus_num", "route"]:
            column_map[col] = "route_id"
        elif col.lower() in ["stop_name", "station", "stop"]:
            column_map[col] = "stop_name"
        elif col.lower() in ["arrival", "arrives_at", "arrival_time", "arr_time"]:
            column_map[col] = "arrival_time"
        elif col.lower() in ["departure", "leaves_at", "departure_time", "dep_time"]:
            column_map[col] = "departure_time"
    df = df.rename(columns=column_map)
    for c in standard_cols:
        if c not in df.columns:
            df[c] = pd.NA
    dfs.append(df[standard_cols])

if dfs:
    normalized_df = pd.concat(dfs, ignore_index=True)
    for col in ["arrival_time", "departure_time"]:
        normalized_df[col] = pd.to_datetime(normalized_df[col], errors='coerce').dt.strftime("%H:%M")
    normalized_df.to_csv(OUT / "normalized_timetable.csv", index=False)
    logging.info(f"Normalized timetable created at: {OUT / 'normalized_timetable.csv'}")
else:
    logging.warning("No CSV files found in data/raw/")

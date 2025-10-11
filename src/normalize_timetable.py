import pandas as pd
from pathlib import Path

RAW = Path("data/raw")
OUT = Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)

# Define how to rename columns for each city
mappings = [
    {"path": RAW / "bangalore_buses.csv",
     "cols": {"route_id": "route_id", "stop_name": "stop_name", "arrival": "arrival_time", "departure": "departure_time"}},
    {"path": RAW / "mumbai_buses.csv",
     "cols": {"bus_number": "route_id", "station": "stop_name", "arrives_at": "arrival_time", "leaves_at": "departure_time"}},
]

dfs = []

for m in mappings:
    if m["path"].exists():
        df = pd.read_csv(m["path"])
        df = df.rename(columns=m["cols"])
        for c in ["route_id", "stop_name", "arrival_time", "departure_time"]:
            if c not in df.columns:
                df[c] = pd.NA
        dfs.append(df[["route_id", "stop_name", "arrival_time", "departure_time"]])

if dfs:
    out = pd.concat(dfs, ignore_index=True)
    out.to_csv(OUT / "normalized_timetable.csv", index=False)
    print("✅ Normalized timetable created at:", OUT / "normalized_timetable.csv")
else:
    print("⚠️ No raw files found. Please add data in data/raw/")

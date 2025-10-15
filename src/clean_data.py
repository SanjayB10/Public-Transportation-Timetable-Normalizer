import pandas as pd
import os

# Define input and output paths
raw_data_path = "data/raw_timetable.csv"
cleaned_data_path = "data/cleaned_timetable.csv"

# Check if file exists
if not os.path.exists(raw_data_path):
    raise FileNotFoundError(f"{raw_data_path} not found. Please place your dataset in the 'data' folder.")

# Load dataset
df = pd.read_csv(raw_data_path)

print("Before Cleaning:")
print(df.info())
print("\nMissing values per column:\n", df.isnull().sum())

# ---------------------------
# Basic cleaning operations
# ---------------------------

# 1. Remove leading/trailing spaces in column names
df.columns = df.columns.str.strip()

# 2. Remove extra spaces in string columns
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# 3. Drop duplicates
df.drop_duplicates(inplace=True)

# 4. Fill missing values with placeholders or remove
df.fillna("Unknown", inplace=True)

# 5. Convert column names to lowercase with underscores
df.columns = df.columns.str.lower().str.replace(" ", "_")

# Save cleaned file
df.to_csv(cleaned_data_path, index=False)

print("\nAfter Cleaning:")
print(df.info())
print(f"\n✅ Cleaned data saved to {cleaned_data_path}")

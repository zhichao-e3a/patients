"""
BEFORE RUNNING THIS CODE:
1) Add historical patients datasets into the "historical_data.csv"
2) Some datasets (EDD, bmi, age, ga_entry_weeks, ga_exit_weeks) can be obtained from the SQL database
"""

from config.configs import DEFAULT_MONGO_CONFIG
from utils.consolidate import *

import pandas as pd
from pathlib import Path
from pymongo import MongoClient

DB_HOST = DEFAULT_MONGO_CONFIG["DB_HOST"]
DB_NAME = DEFAULT_MONGO_CONFIG["DB_NAME"]

client = MongoClient(DB_HOST)
db = client[DB_NAME]

out_collection  = db["patients_unified"]

ROOT    = Path(__file__).parent
PATH  = ROOT / "datasets" / "historical_data.csv"

df = pd.read_csv(PATH, dtype=str)
df = df.loc[:, ~df.columns.str.contains(r"^Unnamed")]

# Strip hidden whitespace from column names
df.columns = df.columns.str.strip()

# Replace NaN with None
df = df.replace({np.nan: None})

# Ensure ID
if "patient_id" not in df.columns:
    if "contact_number" in df.columns:
        df["patient_id"] = df["contact_number"]
    else:
        raise SystemExit("CSV must contain 'patient_id' or 'contact_number'")

# Add recruitment_type if missing
if "recruitment_type" not in df.columns:
    df["recruitment_type"] = "historical"

# Convert numeric fields
if "age" in df.columns:
    df["age"] = df["age"].apply(to_int_or_none)
if "bmi" in df.columns:
    df["bmi"] = df["bmi"].apply(to_float_or_none)
if "ga_entry_weeks" in df.columns:
    df["ga_entry_weeks"] = df["ga_entry_weeks"].apply(to_float_or_none)
if "ga_exit_weeks" in df.columns:
    df["ga_exit_weeks"] = df["ga_exit_weeks"].apply(to_float_or_none)
if "pih" in df.columns:
    df["pih"] = df["pih"].apply(to_int_or_none)
if "gdm" in df.columns:
    df["gdm"] = df["gdm"].apply(to_int_or_none)

# ---- Normalize date-only fields to 'YYYY-MM-DD' ----
# Add any other date-only cols you want here.
date_only_cols = ["date_joined", "first_encounter_date", "last_encounter_date", "estimated_delivery_date"]
for col in date_only_cols:
    if col in df.columns:
        df[col] = df[col].apply(to_ymd_or_none)

# If first_encounter_date missing, copy from date_joined (optional)
if "first_encounter_date" in df.columns and "date_joined" in df.columns:
    df["first_encounter_date"] = df["first_encounter_date"].where(
        df["first_encounter_date"].notna() & (df["first_encounter_date"].astype(str).str.strip() != ""),
        df["date_joined"]
    )

# ---- Normalize datetime fields to 'YYYY-MM-DD HH:MM' ----
datetime_cols = ["delivery_datetime", "onset_datetime"]
for col in datetime_cols:
    if col in df.columns:
        df[col] = df[col].apply(to_ymd_hm_or_none)

# Convert to dict
records = df.to_dict(orient="records")

try:
    out_collection.create_index("patient_id", unique=True)
except Exception as e:
    print(e)
    pass

upserts = 0
for rec in records:
    pid = rec.get("patient_id")
    if not pid:
        continue
    res = out_collection.update_one({"patient_id": pid}, {"$set": rec}, upsert=True)
    if res.upserted_id is not None or res.modified_count > 0:
        upserts += 1

print(f"Upserted {upserts} doc(s) into 'patients_unified'")
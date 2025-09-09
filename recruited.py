from config.configs import DEFAULT_MONGO_CONFIG
from utils.consolidate import *

import pandas as pd
from pymongo import MongoClient

DB_HOST = DEFAULT_MONGO_CONFIG["DB_HOST"]
DB_NAME = DEFAULT_MONGO_CONFIG["DB_NAME"]

client = MongoClient(DB_HOST)
db = client[DB_NAME]

pre_collection  = db["patient_presurvey"]
post_collection = db["patient_postsurvey"]
out_collection  = db["patients_unified"]

# Read required fields
pre_docs = list(pre_collection.find({}, {
    "_id": 0,
    "contact_number": 1,
    "joined_date": 1,
    "estimated_delivery_date": 1,  # 'YYYY-MM-DD'
    "num_children": 1,
    "current_height": 1,
    "weight_before_pregnancy": 1,
    "diagnosed_conditions": 1,
    "age": 1,
    "last_menstrual_date": 1,  # 'YYYY-MM-DD'
    "current_gestational_age": 1,
}))

post_docs = list(post_collection.find({}, {
    "_id": 0,
    "contact_number": 1,
    "actual_delivery_date": 1,  # 'YYYY-MM-DD'
    "delivery_timing": 1,
    "delivery_method": 1,
    "gestational_age": 1,
    "water_break_datetime": 1,
}))

print(f"pre:  {len(pre_docs)} docs from {DB_NAME}.patient_presurvey")
print(f"post: {len(post_docs)} docs from {DB_NAME}.patient_postsurvey")

# DataFrames
pre = pd.DataFrame(pre_docs)
post = pd.DataFrame(post_docs)

if pre.empty or "contact_number" not in pre.columns:
    raise SystemExit("No presurvey docs or missing 'contact_number' in presurvey.")

# Normalize join key & dedup
pre["contact_number"] = pre["contact_number"].astype(str).str.strip()
if not post.empty:
    post["contact_number"] = post["contact_number"].astype(str).str.strip()

pre = pre.drop_duplicates("contact_number", keep="last")
post = post.drop_duplicates("contact_number", keep="last") if not post.empty else post

# ---- derive from pre ----
pres = pre.copy()
pres["patient_id"] = pres["contact_number"]
pres["date_joined"] = pres["joined_date"].apply(parse_date_ymd)
pres["estimated_delivery_date"] = pres["estimated_delivery_date"].apply(parse_date_ymd)
pres["parity"] = pres["num_children"].apply(parity_from_num_children)
pres["bmi"] = pres.apply(
    lambda r: bmi_choose_weight_kg(r.get("current_height"), r.get("weight_before_pregnancy")), axis=1
)
pres["gdm"] = pres["diagnosed_conditions"].apply(lambda s: flag_contains_1_0(s, "妊娠糖尿病"))
pres["pih"] = pres["diagnosed_conditions"].apply(lambda s: flag_contains_1_0(s, "妊娠高血压"))
pres["age"] = pres["age"]
pres["last_menstrual_period"] = pres["last_menstrual_date"].apply(parse_date_ymd)
pres["ga_entry_weeks"] = (
    pres["current_gestational_age"].apply(ga_simple_to_float)
    if "current_gestational_age" in pres.columns else np.nan
)

# ---- derive from post ----
posts = post.copy()
if not posts.empty:
    def delivery_type_map(s: Any) -> Any:
        s = "" if s is None else str(s)
        if "顺产" in s:
            return "natural"
        if "剖腹产（剖宫产）" in s:
            return "c-section"
        if "紧急剖腹产" in s:
            return "emergency c-section"
        return np.nan

    posts["delivery_type"] = posts["delivery_method"].apply(delivery_type_map)
    posts["delivery_datetime"] = posts.apply(
        lambda r: append_two(r.get("actual_delivery_date"), r.get("delivery_timing")), axis=1
    )
    posts["ga_exit_weeks"] = posts["gestational_age"].apply(ga_simple_to_float)

    posts["onset_datetime"] = posts.apply(compute_onset_from_posts_row, axis=1)

# ---- join pres + posts ----
join_cols = ["contact_number", "delivery_datetime", "ga_exit_weeks", "delivery_type", "onset_datetime"]
if not posts.empty and all(c in posts.columns for c in join_cols):
    merged = pres.merge(posts[join_cols], on="contact_number", how="left")
else:
    merged = pres.merge(pd.DataFrame(columns=join_cols), on="contact_number", how="left")

# ---- final schema ----
out = pd.DataFrame({
    "patient_id": merged["patient_id"],
    "date_joined": merged["date_joined"],
    "estimated_delivery_date": merged["estimated_delivery_date"],
    "parity": merged["parity"],
    "bmi": merged["bmi"],
    "gdm": merged["gdm"],  # 0 or 1
    "pih": merged["pih"],  # 0 or 1
    "age": merged["age"],
    "last_menstrual_period": merged["last_menstrual_period"],
    "onset_datetime": merged["onset_datetime"],  # 'YYYY-MM-DD HH:MM' or ''
    "delivery_datetime": merged["delivery_datetime"],  # 'YYYY-MM-DD HH:MM' or ''
    "ga_entry_weeks": merged["ga_entry_weeks"],
    "ga_exit_weeks": merged["ga_exit_weeks"],
    "recruitment_type": "recruited",  # cause this code is for recruited patients only
    "delivery_type": merged["delivery_type"],
})

# Ensure date columns render as 'YYYY-MM-DD'
for col in ["estimated_delivery_date", "last_menstrual_period", "date_joined"]:
    if pd.api.types.is_datetime64_any_dtype(out[col]):
        out[col] = out[col].dt.strftime("%Y-%m-%d")
    out[col] = out[col].fillna("")

records = out.replace({np.nan: None}).to_dict(orient="records")

# Optional: ensure a unique index on patient_id (ignore error if it already exists)
try:
    out_collection.create_index("patient_id", unique=True)
except Exception as e:
    print(e)
    pass

upserts = 0
for rec in records:
    print(rec)
    pid = rec.get("patient_id")
    if not pid:
        continue
    res = out_collection.update_one({"patient_id": pid}, {"$set": rec}, upsert=True)
    if res.upserted_id is not None or res.modified_count > 0:
        upserts += 1

print(f"Upserted {upserts} docs into MongoDB collection 'patients_unified'")
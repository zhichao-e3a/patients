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
pre_docs = list(
    pre_collection.find(
        {},
        {
            "_id"                   : 0,
            "date_joined"           : 1,
            "name"                  : 1,
            "mobile"                : 1,
            "age"                   : 1,
            "ga_entry"              : 1,
            "curr_height"           : 1,
            "pre_weight"            : 1,
            "last_menstrual"        : 1,
            "edd"                   : 1,
            "n_pregnancy"           : 1,
            "n_children"            : 1,
            "last_delivery"         : 1,
            "had_preterm"           : 1,
            "had_surgery"           : 1,
            "diagnosed_conditions"  : 1,
        }
    )
)

post_docs = list(
    post_collection.find(
        {},
        {
            "_id"                   : 0,
            "mobile"                : 1,
            "delivery_type"         : 1,
            "add"                   : 1,
            "delivery_time"         : 1,
            "ga_exit"               : 1,
            "water_break_datetime"  : 1
        }
    )
)

print(f"Retrieved {len(pre_docs)} docs from patient_presurvey")
print(f"Retrieved {len(post_docs)} docs from patient_postsurvey")

pre = pd.DataFrame(pre_docs) ; post = pd.DataFrame(post_docs)

# pre     = pre.drop_duplicates("contact_number", keep="last")
# post    = post.drop_duplicates("contact_number", keep="last") if not post.empty else post

# Construct new dataframe for pre-survey fields
new_pre = pd.DataFrame()

new_pre["date_joined"]      = pre["date_joined"]
new_pre["name"]             = pre["name"]
new_pre["mobile"]           = pre["mobile"]
new_pre["age"]              = pre["age"]
new_pre["ga_entry"]         = pre["ga_entry"]
new_pre["last_menstrual"]   = pre["last_menstrual"]
new_pre["edd"]              = pre["edd"]
new_pre["n_pregnancy"]      = pre["n_pregnancy"]
new_pre["n_children"]       = pre["n_children"]
new_pre["last_delivery"]    = pre["last_delivery"]
new_pre["had_preterm"]      = pre["had_preterm"]
new_pre["had_surgery"]      = pre["had_surgery"]
new_pre["bmi"]              = pre.apply(
    lambda x: bmi_choose_weight_kg(x.get("curr_height"), x.get("pre_weight")),
    axis=1
)
new_pre["gdm"]              = pre["diagnosed_conditions"].apply(lambda s: flag_contains_1_0(s, "妊娠糖尿病"))
new_pre["pih"]              = pre["diagnosed_conditions"].apply(lambda s: flag_contains_1_0(s, "妊娠高血压"))

# Construct new dataframe for post-survey fields
new_post = pd.DataFrame()

new_post["mobile"]          = post["mobile"]
new_post["delivery_type"]   = post["delivery_type"].apply(delivery_type_map)
new_post["ga_exit"]         = post["ga_exit"]
new_post["onset"]           = post.apply(compute_onset_from_posts_row, axis=1)
new_post["add"]             = post.apply(
    lambda r: f"{r.get("add")} {r.get("delivery_time")}", axis=1
)

merged          = new_pre.merge(new_post, on="mobile", how="left")
merged["type"]  = "recruited"

upserts = 0
for rec in merged.replace({np.nan: None}).to_dict("records"):

    pid = rec.get("mobile")

    if not pid:
        print("No Mobile:", rec)
        continue

    res = out_collection.update_one(
        {"mobile": pid},
        {"$set": rec},
        upsert=True
    )

    if res.upserted_id is not None or res.modified_count > 0:
        upserts += 1

print(f"Upserted {upserts} docs into MongoDB collection 'patients_unified'")
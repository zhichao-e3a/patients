from database.MongoDBConnector import MongoDBConnector
from utils.consolidate import *

import asyncio
import pandas as pd

mongo = MongoDBConnector(mode='remote')

pre_docs = asyncio.run(
    mongo.get_all_documents(
        "patient_presurvey",
        projection = {
            "_id"                   : 0,
            "date_joined"           : 1,
            "name"                  : 1,
            "mobile"                : 1,
            "age"                   : 1,
            "ga_entry"              : 1,
            "curr_height"           : 1,
            "pre_weight"            : 1,
            "edd"                   : 1,
            "n_pregnancy"           : 1,
            "had_preterm"           : 1,
            "had_surgery"           : 1,
            "diagnosed_conditions"  : 1,
        }
    )
)

post_docs = asyncio.run(
    mongo.get_all_documents(
        "patient_postsurvey",
        projection = {
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
out_collection  = mongo.get_all_documents("patients_unified")

print(f"{len(pre_docs)} pre-survey records retrieved")
print(f"{len(post_docs)} post-survey records retrieved")

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
new_pre["bmi"]              = pre.apply(
    lambda x: bmi_choose_weight_kg(x.get("curr_height"), x.get("pre_weight")),
    axis=1
)
new_pre["edd"]              = pre["edd"]
new_pre["had_pregnancy"]    = pre["n_pregnancy"].apply(lambda s: 1 if (s != "0" and s != "1") else 0)
new_pre["had_preterm"]      = pre["had_preterm"].apply(lambda s: 1 if (s == "Yes") else 0)
new_pre["had_surgery"]      = pre["had_surgery"].apply(lambda s: 1 if (s == "Yes") else 0)
new_pre["gdm"]              = pre["diagnosed_conditions"].apply(lambda s: flag_contains_1_0(s, "妊娠糖尿病"))
new_pre["pih"]              = pre["diagnosed_conditions"].apply(lambda s: flag_contains_1_0(s, "妊娠高血压"))

# Construct new dataframe for post-survey fields
new_post = pd.DataFrame()

new_post["mobile"]          = post["mobile"]
new_post["ga_exit"]         = post["ga_exit"]
new_post["delivery_type"]   = post["delivery_type"].apply(delivery_type_map)
new_post["add"]             = post.apply(
    lambda r: f"{r.get("add")} {r.get("delivery_time")}", axis=1
)
new_post["onset"]           = post.apply(compute_onset_from_posts_row, axis=1)

merged          = new_pre.merge(new_post, on="mobile", how="left")
merged["type"]  = "recruited"

new_records = merged.replace({np.nan: None}).to_dict("records")

asyncio.run(
    mongo.upsert_documents(
        new_records,
        coll_name = "patients_unified",
        id_fields = ["mobile"]
    )
)

print(f"{len(new_records)} consolidated patients upserted")
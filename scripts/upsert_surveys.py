# from config.configs import REMOTE_MONGO_CONFIG
from database.MongoDBConnector import MongoDBConnector
from utils.surveys import *

import argparse
import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
# from pymongo import MongoClient

# DB_HOST         = REMOTE_MONGO_CONFIG["DB_HOST"]
# DB_NAME         = REMOTE_MONGO_CONFIG["DB_NAME"]
# client          = MongoClient(DB_HOST)
# db              = client[DB_NAME]
# pre_collection  = db["patient_presurvey"]
# post_collection = db["patient_postsurvey"]

async def upsert():

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, required=True, default="local")
    parser.add_argument('--date', type=str, required=False, default=datetime.today().strftime("%y%m%d"))
    args = parser.parse_args()
    mode = args.mode ; date = args.date

    print(f"---UPSERT SURVEYS ({mode}, {date})---")

    mongo = MongoDBConnector(mode=args.mode)

    ROOT    = Path(__file__).parent.parent
    PRE_IN  = ROOT / "datasets" / f"{date}_pre_survey.csv"
    POST_IN = ROOT / "datasets" / f"{date}_post_survey.csv"
    df_pre  = pd.read_csv(PRE_IN, encoding='utf-8')
    df_post = pd.read_csv(POST_IN, encoding='utf-8')

    ######################################## PRE-SURVEY ########################################

    mapped_data_pre = []

    for _, row in df_pre.iterrows():

        ga_str, ga_days = parse_ga_str(safe_get_value(row, COL["ga_now"]))

        first_pregnancy_str     = safe_get_value(row, COL["first_preg"])
        first_pregnancy_cleaned = strip_choice(first_pregnancy_str)
        first_pregnancy         = first_pregnancy_map.get(first_pregnancy_cleaned)

        num_pregnancy_str       = safe_get_value(row, COL["num_preg"])
        num_pregnancy_cleaned   = strip_choice(num_pregnancy_str)
        num_pregnancy           = num_pregnancy_map.get(num_pregnancy_cleaned, "1")

        first_delivery_str      = safe_get_value(row, COL["first_deliv"])
        first_delivery_cleaned  = strip_choice(first_delivery_str)
        first_delivery          = first_delivery_map.get(first_delivery_cleaned, "Yes")

        num_children_str        = safe_get_value(row, COL["num_children"])
        num_children_cleaned    = strip_choice(num_children_str)
        num_children            = num_children_map.get(num_children_cleaned, "0")

        prev_preterm_str        = safe_get_value(row, COL["prev_preterm"])
        prev_preterm_cleaned    = strip_choice(prev_preterm_str)
        prev_preterm            = previous_preterm_map.get(prev_preterm_cleaned, "NA")

        smoking_history_str     = safe_get_value(row, COL["smoke_hist"])
        smoking_history_cleaned = strip_choice(smoking_history_str)
        smoking_history         = smoking_history_map.get(smoking_history_cleaned)

        still_smoking_str       = safe_get_value(row, COL["still_smoke"])
        still_smoking_cleaned   = strip_choice(still_smoking_str)
        still_smoking           = still_smoking_map.get(still_smoking_cleaned, "NA")

        quit_smoking_str        = safe_get_value(row, COL["quit_smoke_preg"])
        quit_smoking_cleaned    = strip_choice(quit_smoking_str)
        quit_smoking            = quit_smoking_map.get(quit_smoking_cleaned, "NA")

        alcohol_history_str     = safe_get_value(row, COL["alcohol_hist"])
        alcohol_history_cleaned = strip_choice(alcohol_history_str)
        alcohol_history         = alcohol_consumption_history_map.get(alcohol_history_cleaned)

        still_drinking_str      = safe_get_value(row, COL["still_drink"])
        still_drinking_cleaned  = strip_choice(still_drinking_str)
        still_drinking          = still_drinking_map.get(still_drinking_cleaned, "NA")

        quit_drinking_str       = safe_get_value(row, COL["quit_drink_preg"])
        quit_drinking_cleaned   = strip_choice(quit_drinking_str)
        quit_drinking           = quit_drinking_map.get(quit_drinking_cleaned, "NA")

        drug_history_str        = safe_get_value(row, COL["drug_hist"])
        drug_history_cleaned    = strip_choice(drug_history_str)
        drug_history            = drug_history_map.get(drug_history_cleaned)

        surgery_history_str     = safe_get_value(row, COL["surgery_history"])
        surgery_history_cleaned = strip_choice(surgery_history_str)

        # Build record

        # Handle '其他' for symptoms
        record = {
            "date_joined"           : date_only(safe_get_value(row, COL["joined_date"])),           # Non-empty
            "name"                  : safe_get_value(row, COL["name"]),                             # Non-empty
            "mobile"                : safe_get_value(row, COL["contact"]),                          # Non-empty
            "age"                   : safe_get_value(row, COL["age"]),                              # Non-empty
            "ga_entry_str"          : ga_str,
            "ga_entry"              : ga_days,                                                      # Non-empty
            "curr_height"           : safe_get_value(row, COL["height_cm"]),                        # Non-empty
            "curr_weight"           : safe_get_value(row, COL["weight_jin"]),
            "pre_weight"            : safe_get_value(row, COL["pre_weight_jin"]),                   # Non-empty
            "last_menstrual"        : safe_get_value(row, COL["last_menstrual"]),                   # Non-empty
            "edd"                   : safe_get_value(row, COL["edd"], default=""),                  # Empty (defaults to "")
            "had_pregnancy"         : "Yes" if first_pregnancy == "No" else "No",
            "had_delivery"          : "Yes" if first_delivery == "No" else "No",
            "n_pregnancy"           : num_pregnancy,                                                # Empty (defaults to "1")
            "n_children"            : num_children,                                                 # Empty (defaults to "0")
            "last_delivery"         : safe_get_value(row, COL["last_delivery_date"], default="NA"),      # Empty (defaults to "NA")
            "had_preterm"           : prev_preterm,                                                 # Empty (defaults to "NA")
            "had_surgery"           : "Yes" if surgery_history_cleaned != "没有" else "No",          # Non-empty
            "pregnancy_symptoms"    : join_values(
                [
                    strip_choice(row[col]) for col in row.index\
                    if\
                    col.startswith(COL["symptoms_prefix"]) \
                    and not col.startswith(COL["symptoms_other"])\
                    and safe_get_value(row, col)\
                    and not is_other_placeholder(safe_get_value(row, col))
                ]
            ),
            "diagnosed_conditions"  : join_values(  # Non-empty
                [
                    strip_choice(row[col]) for col in row.index\
                    if\
                    col.startswith(COL["diagnosed_prefix"])\
                    and not col.startswith(COL["diagnosed_other"])\
                    and safe_get_value(row, col)\
                    and not is_other_placeholder(safe_get_value(row, col))
                ]
            ),
            "smoking_history"       : smoking_history,
            "still_smoking"         : still_smoking,
            "quit_smoking"          : quit_smoking,
            "alcohol_history"       : alcohol_history,
            "still_drinking"        : still_drinking,
            "quit_drinking"         : quit_drinking,
            "drug_history"          : drug_history
        }

        if\
        COL["symptoms_other"] in row\
        and safe_get_value(row, COL["symptoms_other"]):

            curr = record["pregnancy_symptoms"]

            record["pregnancy_symptoms"] = f"{curr}, {safe_get_value(row, COL['symptoms_other'])}"\
            if curr else safe_get_value(row, COL['symptoms_other'])

        # Handle '其他' for diagnosed conditions
        if\
        COL["diagnosed_other"] in row\
        and safe_get_value(row, COL["diagnosed_other"]):

            curr = record["diagnosed_conditions"]

            record["diagnosed_conditions"] = f"{curr}, {safe_get_value(row, COL['diagnosed_other'])}"\
            if curr else safe_get_value(row, COL['diagnosed_other'])

        mapped_data_pre.append(record)

    pre_contact_set = set() ; pre_unique_records = []
    for record in mapped_data_pre:

        contact_number = record.get("mobile")

        if not contact_number:
            continue

        if contact_number in pre_contact_set:
            print(f"Pre-survey: Repeated survey response for {contact_number}")
            continue

        pre_contact_set.add(contact_number)
        pre_unique_records.append(record)

        # res = pre_collection.update_one(
        #     {"mobile": contact_number},
        #     {"$set": record},
        #     upsert=True
        # )
        #
        # if res.upserted_id is not None or res.modified_count > 0:
        #     pre_added += 1

    await mongo.upsert_documents(
        pre_unique_records,
        coll_name="patient_presurvey",
        id_fields=["mobile"]
    )

    ######################################## POST-SURVEY ########################################

    cols = list(df_post.columns)

    name_col                = find_col_name(cols, [r"姓名"])
    phone_col               = find_col_name(cols, [r"手机.?号码|联系方式|联系电话"])
    hospitalised_col        = find_col_name(cols, [r"什么时候.*住院|住院.*准备分娩"])
    delivery_method_col     = find_col_name(cols, [r"分娩方式"])
    water_break_col         = find_col_name(cols, [r"羊水.*(什么.*时候|日期|时间).*破"])
    duration_contr_col      = find_col_name(cols, [r"从开始.*宫缩.*到.*出生.*持续.*多长.*时间"])
    duration_room_col       = find_col_name(cols, [r"从进入.*产房.*到.*出生.*多长.*时间"])
    interval_informed_col   = find_col_name(cols, [r"从.*被告知.*进入.*产程.*到.*转入.*产房.*间隔.*多久"])
    csect_entry_col         = find_col_name(cols, [r"剖(腹|宫)产.*几点.*进入.*产房"])
    csect_reason_col        = find_col_name(cols, [r"剖(腹|宫)产.*因为什么原因|剖.*产.*原因"])
    actual_date_col         = find_col_name(cols, [r"实际.*分娩.*日期"])
    delivery_time_col       = find_col_name(cols, [r"实际.*分娩.*时间.*几点|实际.*分娩.*时间"])
    ga_col                  = find_col_name(cols, [r"分娩时.*孕周.*第几周.*几天|孕周.*例如.*38.*周|孕周"])
    usage_reason_col        = find_col_name(cols, [r"使用.*主要.*目的|使用.*目的"])
    awareness_col           = find_col_name(cols, [r"使用.*期间.*(感受到|察觉).*(宫缩|收缩).*逐渐加剧"])
    influence_col           = find_col_name(cols, [r"(读数|数据).*(是否|有无).*(去医院|生产).*产生.*影响"])
    usefulness_col          = find_col_name(cols, [r"使用.*是否.*有助.*(监测|了解).*宝宝.*健康"])
    problems_col            = find_col_name(cols, [r"使用.*是否.*遇到过问题"])
    problems_desc_col       = find_col_name(cols, [r"简要描述.*遇到.*问题|问题.*描述"])
    recommend_col           = find_col_name(cols, [r"会.*推荐|是否.*推荐"])
    why_recommend_col       = find_col_name(cols, [r"为什么.*会.*推荐"])
    why_not_recommend_col   = find_col_name(cols, [r"为什么.*不会.*推荐"])
    ultrasound_col          = find_col_name(cols, [r"(医院|同时).*(胎儿|胎).*监护|CTG"])
    informed_doc_col        = find_col_name(cols, [r"是否.*告诉.*医生.*使用.*萌动|是否.*告知.*医生"])
    reaction_col            = find_col_name(cols, [r"(他们|医生).*(什么).*反应|医生.*反应"])
    improvement_col         = find_col_name(cols, [r"认为.*可以.*改进|改进.*地方|改进.*建议"])

    mapped_data_post = []

    for _, row in df_post.iterrows():

        ga_str, ga_days = parse_ga_str(safe_get_value(row, ga_col))

        advantages      = collect_mrq_by_keywords(row, cols, group_keywords=["使用", "优点"])
        disadvantages   = collect_mrq_by_keywords(row, cols, group_keywords=["使用", "不足"])
        usage_reason    = safe_get_value(row, usage_reason_col)

        # Map values
        contraction_awareness   = map_choice(safe_get_value(row, awareness_col), contraction_awareness_map) if awareness_col else None
        modoo_influence         = map_choice(safe_get_value(row, influence_col), modoo_influence_map) if influence_col else None
        modoo_usefulness        = map_choice(safe_get_value(row, usefulness_col), modoo_usefulness_map) if usefulness_col else None
        problems_faced          = map_choice(safe_get_value(row, problems_col), problems_faced_map) if problems_col else None
        recommendation          = map_choice(safe_get_value(row, recommend_col), recommendation_map) if recommend_col else None
        ultrasound              = map_choice(safe_get_value(row, ultrasound_col), did_ultrasound_map) if ultrasound_col else None
        informed_doctor         = map_choice(safe_get_value(row, informed_doc_col), informed_doctor_map) if informed_doc_col else None

        record = {
            "name"                          : safe_get_value(row, name_col),
            "mobile"                        : safe_get_value(row, phone_col),                           # Non-empty
            "hospitalised_date"             : safe_get_value(row, hospitalised_col),
            "delivery_type"                 : strip_choice(safe_get_value(row, delivery_method_col)),   # Non-empty
            "water_break_datetime"          : safe_get_value(row, water_break_col, default=""),         # Empty (defaults to "")
            "contraction_duration"          : safe_get_value(row, duration_contr_col),
            "birthroom_duration"            : safe_get_value(row, duration_room_col),
            "birthroom_interval_duration"   : safe_get_value(row, interval_informed_col),
            "csect_entry_time"              : safe_get_value(row, csect_entry_col),
            "csect_reason"                  : safe_get_value(row, csect_reason_col),
            "add"                           : safe_get_value(row, actual_date_col),                     # Non-empty
            "delivery_time"                 : safe_get_value(row, delivery_time_col),                   # Non-empty
            "ga_exit_str"                   : ga_str,
            "ga_exit"                       : ga_days,                                                  # Non-empty
            "modoo_usage_reason"            : usage_reason,
            "modoo_influence"               : modoo_influence,
            "modoo_usefulness"              : modoo_usefulness,
            "modoo_advantages"              : advantages,
            "modoo_disadvantages"           : disadvantages,
            "increased_awareness"           : contraction_awareness,
            "had_problems"                  : problems_faced,
            "problems_faced"                : safe_get_value(row, problems_desc_col),
            "will_recommend"                : recommendation,
            "reasons_for_recommend"         : safe_get_value(row, why_recommend_col),
            "reasons_for_not_recommend"     : safe_get_value(row, why_not_recommend_col),
            "had_ultrasound"                : ultrasound,
            "informed_doctor"               : informed_doctor,
            "doctor_reaction"               : safe_get_value(row, reaction_col),
            "improvement"                   : safe_get_value(row, improvement_col),
        }

        mapped_data_post.append(record)

    post_contact_set = set() ; post_unique_records = []
    for record in mapped_data_post:

        contact_number = record.get("mobile")

        if not contact_number:
            continue

        if contact_number in post_contact_set:
            print(f"Post-survey: Repeated survey response for {contact_number}")
            continue

        post_contact_set.add(contact_number)
        post_unique_records.append(record)

        # res = post_collection.update_one(
        #     {"mobile": contact_number},
        #     {"$set": record},
        #     upsert=True
        # )
        #
        # if res.upserted_id is not None or res.modified_count > 0:
        #     post_added += 1

    await mongo.upsert_documents(
        post_unique_records,
        coll_name="patient_postsurvey",
        id_fields=["mobile"]
    )

    print(f"Upserted {len(pre_unique_records)} records into MongoDB collection 'patient_presurvey'")
    print(f"Upserted {len(post_unique_records)} records into MongoDB collection 'patient_postsurvey'")

if __name__ == "__main__":
    asyncio.run(upsert())
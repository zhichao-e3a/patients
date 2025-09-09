from config.configs import DEFAULT_MONGO_CONFIG
from utils.surveys import *

import pandas as pd
from pathlib import Path
from pymongo import MongoClient
from tqdm.auto import tqdm

DB_HOST = DEFAULT_MONGO_CONFIG["DB_HOST"]
DB_NAME = DEFAULT_MONGO_CONFIG["DB_NAME"]

client = MongoClient(DB_HOST)
db = client[DB_NAME]

pre_collection  = db["patient_presurvey"]
post_collection = db["patient_postsurvey"]

ROOT    = Path(__file__).parent
PRE_IN  = ROOT / "datasets" / "pre_survey.csv"
POST_IN = ROOT / "datasets" / "post_survey.csv"
df_pre  = pd.read_csv(PRE_IN, encoding='utf-8')
df_post = pd.read_csv(POST_IN, encoding='utf-8')

######################################## PRE-SURVEY ########################################

mapped_data_pre = []

for _, row in tqdm(df_pre.iterrows()):

    ga_str, ga_total_days = parse_ga_str(safe_get_value(row, COL["ga_now"]))

    first_pregnancy_str = safe_get_value(row, COL["first_preg"])
    first_pregnancy_cleaned = strip_choice(first_pregnancy_str)
    first_pregnancy = first_pregnancy_map.get(first_pregnancy_cleaned, 0)

    num_pregnancy_str = safe_get_value(row, COL["num_preg"])
    num_pregnancy_cleaned = strip_choice(num_pregnancy_str)
    num_pregnancy = num_pregnancy_map.get(num_pregnancy_cleaned, 1)

    first_delivery_str = safe_get_value(row, COL["first_deliv"])
    first_delivery_cleaned = strip_choice(first_delivery_str)
    first_delivery = first_delivery_map.get(first_delivery_cleaned, "Yes")

    num_children_str = safe_get_value(row, COL["num_children"])
    num_children_cleaned = strip_choice(num_children_str)
    num_children = num_children_map.get(num_children_cleaned, 0)

    prev_preterm_str = safe_get_value(row, COL["prev_preterm"])
    prev_preterm_cleaned = strip_choice(prev_preterm_str)
    prev_preterm = previous_preterm_map.get(prev_preterm_cleaned, "No")

    smoking_history_str = safe_get_value(row, COL["smoke_hist"])
    smoking_history_cleaned = strip_choice(smoking_history_str)
    smoking_history = smoking_history_map.get(smoking_history_cleaned)

    still_smoking_str = safe_get_value(row, COL["still_smoke"])
    still_smoking_cleaned = strip_choice(still_smoking_str)
    still_smoking = still_smoking_map.get(still_smoking_cleaned)

    quit_smoking_str = safe_get_value(row, COL["quit_smoke_preg"])
    quit_smoking_cleaned = strip_choice(quit_smoking_str)
    quit_smoking = quit_smoking_map.get(quit_smoking_cleaned)

    alcohol_history_str = safe_get_value(row, COL["alcohol_hist"])
    alcohol_history_cleaned = strip_choice(alcohol_history_str)
    alcohol_history = alcohol_consumption_history_map.get(alcohol_history_cleaned)

    still_drinking_str = safe_get_value(row, COL["still_drink"])
    still_drinking_cleaned = strip_choice(still_drinking_str)
    still_drinking = still_drinking_map.get(still_drinking_cleaned)

    quit_drinking_str = safe_get_value(row, COL["quit_drink_preg"])
    quit_drinking_cleaned = strip_choice(quit_drinking_str)
    quit_drinking = quit_drinking_map.get(quit_drinking_cleaned)

    drug_history_str = safe_get_value(row, COL["drug_hist"])
    drug_history_cleaned = strip_choice(drug_history_str)
    drug_history = drug_history_map.get(drug_history_cleaned)

    # Build record
    record = {
        "joined_date": date_only_from_dmy(safe_get_value(row, COL["joined_date"])),
        "name": safe_get_value(row, COL["name"]),
        "contact_number": safe_get_value(row, COL["contact"]),
        "age": int(row[COL["age"]]) if pd.notna(row.get(COL["age"])) else "",
        "current_gestational_age": ga_str,
        "current_gestational_age_total_days": ga_total_days,  # convenient numeric
        "current_height": int(row[COL["height_cm"]]) if pd.notna(row.get(COL["height_cm"])) else "",
        "current_weight": int(row[COL["weight_jin"]]) if pd.notna(row.get(COL["weight_jin"])) else "",
        "weight_before_pregnancy": int(row[COL["pre_weight_jin"]]) if pd.notna(row.get(COL["pre_weight_jin"])) else "",
        "last_menstrual_date": safe_get_value(row, COL["lmp"]),
        "estimated_delivery_date": safe_get_value(row, COL["edd"]),
        "first_pregnancy": first_pregnancy,
        "num_pregnancy": num_pregnancy,
        "first_delivery": first_delivery,
        "num_children": num_children,
        "last_delivery_date": safe_get_value(row, COL["last_delivery_date"]),
        "previous_preterm": prev_preterm,
        "surgery_history": safe_get_value(row, COL["surgery_history"]),

        # MRQ fields (shifted prefixes)
        "pregnancy_symptoms": clean_and_join([
            row[col]
            for col in row.index
            if col.startswith(COL["symptoms_prefix"])
               and pd.notna(row[col]) and str(row[col]).strip()
               and not col.startswith(COL["symptoms_other"])
        ]),
        "diagnosed_conditions": clean_and_join([
            row[col]
            for col in row.index
            if col.startswith(COL["diagnosed_prefix"])
               and pd.notna(row[col]) and str(row[col]).strip()
               and not col.startswith(COL["diagnosed_other"])
        ]),

        "smoking_history": smoking_history,
        "currently_still_smoking": still_smoking,
        "quit_smoking_due_to_pregnancy": quit_smoking,
        "alcohol_consumption_history": alcohol_history,
        "currently_still_drinking": still_drinking,
        "quit_drinking_due_to_pregnancy": quit_drinking,
        "drug_abuse_history": drug_history
    }

    # Handle '其他' for symptoms
    if COL["symptoms_other"] in row and pd.notna(row[COL["symptoms_other"]]) and str(
            row[COL["symptoms_other"]]).strip():
        record["pregnancy_symptoms"] = (
            record["pregnancy_symptoms"] + ", " + str(row[COL["symptoms_other"]]).strip()
            if record["pregnancy_symptoms"] else str(row[COL["symptoms_other"]]).strip()
        )

    # Handle '其他' for diagnosed conditions
    if COL["diagnosed_other"] in row and pd.notna(row[COL["diagnosed_other"]]) and str(
            row[COL["diagnosed_other"]]).strip():
        record["diagnosed_conditions"] = (
            record["diagnosed_conditions"] + ", " + str(row[COL["diagnosed_other"]]).strip()
            if record["diagnosed_conditions"] else str(row[COL["diagnosed_other"]]).strip()
        )

    # Final tidy (in case any mapped fields still have prefixes)
    for key in [
        "first_pregnancy", "first_delivery", "num_children",
        "previous_preterm", "smoking_history", "currently_still_smoking",
        "quit_smoking_due_to_pregnancy", "alcohol_consumption_history",
        "currently_still_drinking", "quit_drinking_due_to_pregnancy",
        "drug_abuse_history"
    ]:
        record[key] = strip_choice(record[key])

    mapped_data_pre.append(record)

for record in mapped_data_pre:
    contact_number = record.get("contact_number")
    if not contact_number:
        continue  # skip records without key

    # Push the entire record (including empty fields) into MongoDB
    pre_collection.update_one(
        {"contact_number": contact_number},  # match key
        {"$set": record},  # set everything in record
        upsert=True
    )

print(f"Upserted {len(mapped_data_pre)} records into MongoDB collection 'patient_presurvey'")

######################################## POST-SURVEY ########################################

cols = list(df_post.columns)

name_col = find_first_col(cols, [r"姓名"])
phone_col = find_first_col(cols, [r"手机.?号码|联系方式|联系电话"])

hospitalised_col = find_first_col(cols, [r"什么时候.*住院|住院.*准备分娩"])
delivery_method_col = find_first_col(cols, [r"分娩方式"])
water_break_col = find_first_col(cols, [r"羊水.*(什么.*时候|日期|时间).*破"])

duration_contr_col = find_first_col(cols, [r"从开始.*宫缩.*到.*出生.*持续.*多长.*时间"])
duration_room_col = find_first_col(cols, [r"从进入.*产房.*到.*出生.*多长.*时间"])
interval_informed_col = find_first_col(cols, [r"从.*被告知.*进入.*产程.*到.*转入.*产房.*间隔.*多久"])
csect_entry_col = find_first_col(cols, [r"剖(腹|宫)产.*几点.*进入.*产房"])
csect_reason_col = find_first_col(cols, [r"剖(腹|宫)产.*因为什么原因|剖.*产.*原因"])

actual_date_col = find_first_col(cols, [r"实际.*分娩.*日期"])
delivery_time_col = find_first_col(cols, [r"实际.*分娩.*时间.*几点|实际.*分娩.*时间"])
ga_col = find_first_col(cols, [r"分娩时.*孕周.*第几周.*几天|孕周.*例如.*38.*周|孕周"])

awareness_col = find_first_col(cols, [r"使用.*期间.*(感受到|察觉).*(宫缩|收缩).*逐渐加剧"])
influence_col = find_first_col(cols, [r"(读数|数据).*(是否|有无).*产生.*影响.*(去医院|生产)"])
usefulness_col = find_first_col(cols, [r"使用.*是否.*有助.*(监测|了解).*宝宝.*健康"])

problems_col = find_first_col(cols, [r"使用.*是否.*遇到过问题"])
problems_desc_col = find_first_col(cols, [r"简要描述.*遇到.*问题|问题.*描述"])
recommend_col = find_first_col(cols, [r"会.*推荐|是否.*推荐"])
why_recommend_col = find_first_col(cols, [r"为什么.*会.*推荐"])
why_not_recommend_col = find_first_col(cols, [r"为什么.*不会.*推荐"])
ultrasound_col = find_first_col(cols, [r"(医院|同时).*(胎儿|胎).*监护|CTG"])
informed_doc_col = find_first_col(cols, [r"是否.*告诉.*医生.*使用.*萌动|是否.*告知.*医生"])
reaction_col = find_first_col(cols, [r"(他们|医生).*(什么).*反应|医生.*反应"])
improvement_col = find_first_col(cols, [r"认为.*可以.*改进|改进.*地方|改进.*建议"])

mapped_data_post = []

for _, row in tqdm(df_post.iterrows()):
    advantages = collect_mrq_by_keywords(row, cols, group_keywords=["使用", "优点"])
    disadvantages = collect_mrq_by_keywords(row, cols, group_keywords=["使用", "不足"])

    usage_reason_col = find_first_col(cols, [r"使用.*主要.*目的|使用.*目的"])
    usage_reason = safe_get(row, usage_reason_col) if usage_reason_col else collect_mrq_by_keywords(row, cols,
                                                                                                    group_keywords=[
                                                                                                        "使用", "目的"])

    # Map values
    contraction_awareness = map_choice(safe_get(row, awareness_col),
                                       contraction_awareness_map) if awareness_col else None
    modoo_influence = map_choice(safe_get(row, influence_col), modoo_influence_map) if influence_col else None
    modoo_usefulness = map_choice(safe_get(row, usefulness_col), modoo_usefulness_map) if usefulness_col else None
    problems_faced = map_choice(safe_get(row, problems_col), problems_faced_map) if problems_col else None
    recommendation = map_choice(safe_get(row, recommend_col), recommendation_map) if recommend_col else None
    ultrasound = map_choice(safe_get(row, ultrasound_col), did_ultrasound_map) if ultrasound_col else None
    informed_doctor = map_choice(safe_get(row, informed_doc_col), informed_doctor_map) if informed_doc_col else None

    record = {
        "name": safe_get(row, name_col),
        "contact_number": safe_get(row, phone_col),
        "hospitalised_date": safe_get(row, hospitalised_col),
        "delivery_method": strip_choice(safe_get(row, delivery_method_col)),
        "water_break_datetime": safe_get(row, water_break_col),
        "duration_contr_to_birth": safe_get(row, duration_contr_col),
        "duration_room_to_birth": safe_get(row, duration_room_col),
        "interval_informed_to_room": safe_get(row, interval_informed_col),
        "csect_entry_time": safe_get(row, csect_entry_col),
        "c_sect_reason": safe_get(row, csect_reason_col),
        "actual_delivery_date": safe_get(row, actual_date_col),
        "delivery_timing": safe_get(row, delivery_time_col),
        "gestational_age": safe_get(row, ga_col),
        "modoo_usage_reason": usage_reason,
        "increased_contraction_awareness": contraction_awareness,
        "modoo_influence": modoo_influence,
        "modoo_usefulness_baby_monitoring": modoo_usefulness,
        "modoo_advantages": advantages,
        "modoo_disadvantages": disadvantages,
        "are_there_any_problems_faced": problems_faced,
        "problems_faced": safe_get(row, problems_desc_col),
        "recommendation": recommendation,
        "reasons_for_recommending": safe_get(row, why_recommend_col),
        "reasons_for_not_recommending": safe_get(row, why_not_recommend_col),
        "did_ultrasound": ultrasound,
        "informed_doctor": informed_doctor,
        "doctor_reaction": safe_get(row, reaction_col),
        "improvement": safe_get(row, improvement_col),
    }

    mapped_data_post.append(record)

for record in mapped_data_post:
    contact_number = record.get("contact_number")
    if not contact_number:
        continue  # skip records without key

    post_collection.update_one(
        {"contact_number": contact_number},  # match key
        {"$set": record},  # set everything in record
        upsert=True
    )

print(f"Upserted {len(mapped_data_post)} records into MongoDB collection 'patient_postsurvey'")
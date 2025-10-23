from database.queries import RECRUITED_QUERY
from database.SQLDBConnector import SQLDBConnector
from database.MongoDBConnector import MongoDBConnector
from utils.consolidate import *

import argparse
import asyncio
import numpy as np
import pandas as pd

print("---RECRUITED---")

parser = argparse.ArgumentParser()
parser.add_argument("--mode", required=True, choices=['local', 'remote'])
mode = parser.parse_args().mode

sql   = SQLDBConnector()
mongo = MongoDBConnector(mode=mode)

pre_docs = asyncio.run(
    mongo.get_all_documents(
        "patient_presurvey",
        projection = {
            "_id"                   : 0,
            "name"                  : 1,
            "mobile"                : 1,
            "age"                   : 1,
            "curr_height"           : 1,
            "pre_weight"            : 1,
            "edd"                   : 1,
            "had_pregnancy"         : 1,
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
            "water_break_datetime"  : 1
        }
    )
)

pre = pd.DataFrame(pre_docs) ; post = pd.DataFrame(post_docs)

mobile_query_str    = ",".join([f"'{i['mobile']}'" for i in pre_docs])
measurements_df     = sql.query_to_dataframe(query=RECRUITED_QUERY.format(mobile_query_str=mobile_query_str))
measurements_df     = measurements_df.sort_values(["mobile", "m_time"])
grouped_df          = measurements_df.groupby("mobile")

"""
Not all patients in pre-survey will be present in database
Patients that will not be queried:
- Did not register on Modoo (no patient data -> no measurement data)
- Did not use Modoo products (no measurement data)
"""
queried_mobile_set = set([mobile for mobile, _ in grouped_df])

print(f"[Mongo] {len(pre_docs)} pre-survey records")

print(f"[Mongo] {len(post_docs)} post-survey records")

for d in pre_docs:

    if d['mobile']  not in queried_mobile_set:
        print(f"[MySQL] {d['mobile']}: Not registered on Modoo / No measurement data")

print(f'[MySQL] {len(queried_mobile_set)} patients from MySQL')

merged = pre.merge(post, on="mobile", how="left")
merged.replace({np.nan: None}, inplace=True)

new_records = []
for _, patient in merged.iterrows():

    mobile = patient["mobile"]
    if mobile not in queried_mobile_set:
        continue

    patient_measurements_df = grouped_df.get_group(patient["mobile"])
    earliest_iter = patient_measurements_df.iterrows() ; ga_entry_iter = patient_measurements_df.iterrows()

    # Get ADD (could be None)
    add = f"{patient["add"]} {patient["delivery_time"]}" if patient["add"] else None

    # Get the earliest measurement (cannot be None)
    earliest_idx, earliest_m = next(earliest_iter)
    earliest = earliest_m['m_time']

    # Get the ga_entry for earliest measurement (cannot be None)
    ga_entry_idx, ga_entry_m = next(ga_entry_iter)
    basic_info_str  = ga_entry_m['basic_info']
    conclusion_str  = ga_entry_m['conclusion'] if pd.notna(ga_entry_m['conclusion']) else None
    ga_entry_temp   = extract_gest_age(conclusion_str, basic_info_str)

    # Get ga_entry (cannot be None)
    ga_entry_mismatch = False
    while ga_entry_temp is None:

        ga_entry_mismatch = True

        ga_entry_idx, ga_entry_m = next(ga_entry_iter)

        basic_info_str  = ga_entry_m['basic_info']
        conclusion_str  = ga_entry_m['conclusion'] if pd.notna(ga_entry_m['conclusion']) else None
        ga_entry_temp   = extract_gest_age(conclusion_str, basic_info_str)

    if ga_entry_mismatch:
        ga_entry = ga_entry_temp - (ga_entry_m['m_time']-earliest).days
    else:
        ga_entry = ga_entry_temp

    # Calculate ga_exit_add, ga_exit_last if ADD present ; Recalculate the earliest if needed
    ga_exit_add = None ; ga_exit_last = None
    if add is not None:

        # Get Delivery Exit Time and Last Exit Time (None if ADD is None)
        exit_time_add   = datetime.strptime(add, "%Y-%m-%d %H:%M") if add else None
        exit_time_last  = patient_measurements_df['m_time'].iloc[-1] if add else None

        # If the measurement date is too early (indicates previous pregnancy, and the earliest measurement is wrong)
        if (exit_time_add-earliest).days > 280:

            print(f"[Retry] {mobile}: Recalculate earliest measurement")

            # Recalculate the earliest measurement if the initial one was wrong
            while (exit_time_add-earliest).days > 280:
                earliest_idx, earliest_m = next(earliest_iter)
                earliest = earliest_m['m_time']

            # Iterate until ga_entry and earliest meet
            while ga_entry_idx < earliest_idx:
                ga_entry_idx, ga_entry_m = next(ga_entry_iter)
            while earliest_idx < ga_entry_idx:
                earliest_idx, earliest_m = next(earliest_iter)

            basic_info_str  = ga_entry_m['basic_info']
            conclusion_str  = ga_entry_m['conclusion'] if pd.notna(ga_entry_m['conclusion']) else None
            ga_entry_temp   = extract_gest_age(conclusion_str, basic_info_str)

            ga_entry_mismatch = False
            while ga_entry_temp is None:

                ga_entry_mismatch = True

                ga_entry_idx, ga_entry_m = next(ga_entry_iter)

                basic_info_str  = ga_entry_m['basic_info']
                conclusion_str  = ga_entry_m['conclusion'] if pd.notna(ga_entry_m['conclusion']) else None
                ga_entry_temp   = extract_gest_age(conclusion_str, basic_info_str)

            if ga_entry_mismatch:
                ga_entry = ga_entry_temp - (ga_entry_m['m_time'] - earliest).days
            else:
                ga_entry = ga_entry_temp

        # Get ga_exit_add, ga_exit_last
        ga_exit_add  = ga_entry + (exit_time_add-earliest).days
        ga_exit_last = ga_entry + (exit_time_last-earliest).days

    record = {
        'type'          : 'rec',
        'date_joined'   : earliest.strftime("%Y-%m-%d"),
        'name'          : patient['name'] if pd.notna(patient['name']) else None,
        'mobile'        : patient['mobile'],
        'age'           : int(patient['age']) if pd.notna(patient['age']) else None,
        'ga_entry'      : ga_entry,
        'ga_exit_add'   : ga_exit_add,
        'ga_exit_last'  : ga_exit_last,# if ga_exit_last <= ga_exit_add else ga_exit_add,
        'bmi'           : bmi_choose_weight_kg(patient['curr_height'], patient['pre_weight']),
        'edd'           : patient['edd'] if patient['edd'] else None,
        'had_pregnancy' : 1 if patient['had_pregnancy'] == 'Yes' else 0,
        'had_preterm'   : 1 if patient['had_preterm'] == 'Yes' else 0,
        'had_surgery'   : 1 if patient['had_surgery'] == 'Yes' else 0,
        'gdm'           : flag_contains_1_0(patient['diagnosed_conditions'], "妊娠糖尿病"),
        'pih'           : flag_contains_1_0(patient['diagnosed_conditions'], "妊娠高血压"),
        'delivery_type' : delivery_type_map(patient['delivery_type']),
        'add'           : add,
        'onset'         : compute_onset(patient) if add else None
    }

    new_records.append(record)

asyncio.run(
    mongo.upsert_documents(
        new_records,
        coll_name = "patients_unified",
        id_fields = ["mobile"]
    )
)

print(f"Recruited: {len(new_records)} consolidated patients upserted")
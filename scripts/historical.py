from database.queries import HISTORICAL_QUERY
from database.SQLDBConnector import SQLDBConnector
from database.MongoDBConnector import MongoDBConnector
from utils.consolidate import *

import asyncio
import pandas as pd
from pathlib import Path

ROOT    = Path(__file__).parent.parent

sql     = SQLDBConnector()
mongo   = MongoDBConnector(mode="remote")

hist_df             = pd.read_excel(ROOT / "datasets" / "historical_metadata.xlsx")
hist_df['mobile']   = hist_df['mobile'].astype(str)

mobile_query_str = ",".join([f"'{i}'" for i in hist_df["mobile"].tolist()])

hist_sql = sql.query_to_dataframe(query=HISTORICAL_QUERY.format(mobile_query_str=mobile_query_str))
print(f"{len(hist_sql)} patient records fetched")

hist_pivot = hist_sql.pivot(
    index=[i for i in hist_sql.columns if i not in ['record_type', 'record_answer']],
    columns='record_type',
    values='record_answer'
).reset_index()

merged = hist_df.merge(hist_pivot, on='mobile', how='left')

new_records = []
for _, row in merged.iterrows():

    basic_info_str  = row['basic_info']
    conclusion_str  = row['conclusion'] if pd.notna(row['conclusion']) else None
    ga_entry        = extract_gest_age(conclusion_str, basic_info_str)
    entry_time      = row['earliest']
    exit_time       = row['add']
    ga_exit         = int(ga_entry + (exit_time - entry_time).days) if pd.notna(exit_time) else None

    # 0='0 pregnancies', 1='1 pregnancies', 2='2 pregnancies', 3='>2 pregnancies'
    # Count current pregnancy as well so treat 0 and 1 as same
    preg_count  = row[1.0]
    # 0='有', 1='无', 2='未知'
    had_misc    = row[2.0]
    gdm         = row[4.0]
    pih         = row[5.0]
    had_preterm = row[8.0]
    had_surgery = row[13.0]

    bmi = bmi_choose_weight_kg(
        height_cm = row['height'],
        weight_val = row['old_weight']
    )

    record = {
        'type'          : 'historical',
        'date_joined'   : row['reg_time'].to_pydatetime().strftime("%Y-%m-%d"),
        'name'          : row['name'] if pd.notna(row['name']) else None,
        'mobile'        : row['mobile'],
        'age'           : str(int(row['age'])) if pd.notna(row['age']) else None,
        'ga_entry'      : ga_entry,
        'ga_exit'       : ga_exit,
        'bmi'           : bmi if pd.notna(bmi) else None,
        'edd'           : row['edd'].strftime("%Y-%m-%d") if pd.notna(row['edd']) else None,
        'had_pregnancy' : 1 if (preg_count > 1) else 0,
        'had_preterm'   : 1 if had_preterm == 0 else 0,
        'had_surgery'   : 1 if had_surgery == 0 else 0,
        'gdm'           : 1 if gdm == 0 else 0,
        'pih'           : 1 if pih == 0 else 0,
        'delivery_type' : row['delivery_type'],
        'add'           : row['add'].to_pydatetime().strftime("%Y-%m-%d %H:$M"),
        'onset'         : row['onset'].to_pydatetime().strftime("%Y-%m-%d %H:$M") if pd.notna(row['onset']) else None
    }

    new_records.append(record)

asyncio.run(
    mongo.upsert_documents(new_records, coll_name='patients_unified', id_fields=['mobile'])
)
print(f"{len(new_records)} consolidated patients upserted")
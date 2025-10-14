import re
import pandas as pd

def find_col_name(cols, patterns):
    """
    Return the first column whose header matches ANY regex in `patterns`.
    """
    for pat in patterns:

        rx = re.compile(pat)

        for c in cols:

            if rx.search(c):
                return c

    return None

def safe_get_value(row, col, default=""):
    """
    Ensure that all values read are strings
    Ensure that null/none fields are still returned as empty strings
    """

    value = row.get(col, default)

    # Handle null values
    if pd.isna(value):
        return default

    return str(value).strip()

def strip_choice(val):
    """
    For MCQ/MRQ answers like 'A.是' or 'B.头晕', drop the letter+dot prefix
    and return only the text after the dot. Otherwise, return the trimmed value
    """

    # If empty string
    if not val:
        return ""

    # If '.' not in val, nothing to split
    elif "." not in val:
        return val

    else:
        _, rest = val.split(".", 1)
        return rest.strip()

def is_other_placeholder(text: str) -> bool:
    """
    True for values like: '其他', '其它', '其他____{...}', '其它________', '其他__________'
    i.e. Returns True for the 'Other' label without real user-provided content
    """

    # If empty string, it is a placeholder for 'Other'
    if not text:
        return True

    # Drop leading choice code like 'A.' if present
    t = strip_choice(text)
    if t in OTHER_ALIASES:
        return True

    # "其他____{fillblank-xxxx}" or "其它________"
    if re.match(rf"^({'|'.join(OTHER_ALIASES)})\s*(_+|\{{.*\}})\s*$", t):
        return True

    # "其他" followed only by punctuation/spaces
    if re.match(rf"^({'|'.join(OTHER_ALIASES)})[\s\W]*$", t):
        return True

    return False

def join_values(values):
    """
    Joins a list of strings
    """
    return ", ".join(values)

def collect_mrq_by_keywords(
        row, cols,
        group_keywords
):
    """
    Generic multi-response collector
    - Picks columns whose header contains ALL keywords in `group_keywords`
    - Keeps ticked values (ignores '其他' placeholders)
    - Appends free-text from '[选项填空]' or raw '{fillblank-...}' columns when present
    """

    def header_in_group(h):
        return all(kw in h for kw in group_keywords)

    values          = []
    free_other      = ""

    # The one explicit free-text column ends with "[选项填空]"
    for c in cols:
        if header_in_group(c) and c.endswith("[选项填空]"):
            free_other = safe_get_value(row, c)

    # Normal MRQ ticked options
    for c in cols:

        if header_in_group(c) and not c.endswith("[选项填空]"):

            v = strip_choice(safe_get_value(row, c))

            if v and not is_other_placeholder(v):
                values.append(v)

    joined  = join_values(values)

    if free_other:
        joined = f"{joined}, {free_other}" if joined else free_other

    return normalize_commas(joined)

def parse_ga_str(ga_str):
    """
    Ensure that ga_str has format '{week}.{day}'
    For example, '38.4' means 38 weeks, 4 days
    Returns (original_str, total_days) or ("", None) if invalid
    """

    # Handle empty strings
    if not ga_str:
        return "", None

    if "." in ga_str:
        week_str, day_str = ga_str.split(".", 1)
        week = int(week_str) ; day = int(day_str)
    else:
        week = int(ga_str) ; day = 0

    day = max(0, min(6, day))

    return ga_str, week*7+day

def date_only(x: str) -> str:
    """Input like '31/8/2025 21:16' -> '2025-08-31' (or '' if invalid)."""
    ts = pd.to_datetime(x, errors="coerce", dayfirst=True)
    return ts.strftime("%Y-%m-%d") if pd.notna(ts) else ""

def map_choice(value, mapping):

    if not value:
        return value

    return mapping.get(strip_choice(value), strip_choice(value))

def normalize_commas(s: str) -> str:
    if not s:
        return s
    s = re.sub(r"(,\s*)+", ", ", s)
    return s.strip(", ").strip()

num_children_map = {
    "一": "1",
    "两": "2",
    "三": "3",
    "四": "4",
    "五": "5",
    "多过六个": ">=6"
}

num_pregnancy_map = {
    "二": "2",
    "三": "3",
    "四": "4",
    "五": "5",
    "多过六次": ">=6"
}

first_pregnancy_map = {
    "是": "Yes",
    "不是": "No"
}

first_delivery_map = {
    "是": "Yes",
    "不是": "No"
}

previous_preterm_map = {
    "有": "Yes",
    "没有": "No"
}

smoking_history_map = {
    "有": "Yes",
    "没有": "No"
}

still_smoking_map = {
    "还在抽烟": "Yes",
    "戒烟了": "No"
}

quit_smoking_map = {
    "是": "Yes",
    "不是": "No"
}

alcohol_consumption_history_map = {
    "有": "Yes",
    "没有": "No"
}

still_drinking_map = {
    "还在饮酒": "Yes",
    "戒酒了": "No"
}

quit_drinking_map = {
    "是": "Yes",
    "不是": "No"
}

drug_history_map = {
    "有": "Yes",
    "没有": "No"
}

surgery_history_map = {
    "有": "Yes",
    "没有": "No"
}

COL = {
    "joined_date": "开始答题时间",
    "name": "1.名字",
    "contact": "2.电话号码",
    "age": "3.您的年龄是多少？",
    "ga_now": "4.目前的孕周（例如 38.4 代表38周4天）",
    "height_cm": "5.目前的身高（厘米）",
    "weight_jin": "6.目前的体重（斤）",
    "pre_weight_jin": "7.怀孕前的体重（斤）",
    "last_menstrual": "8.您的最后一次月经大概是什么时候？",
    "edd": "10.预产期是？",
    "first_preg": "11.这是您第一次怀孕吗？",
    "num_preg": "12.  这是您第几次怀孕？（包括现在）",
    "first_deliv": "13.这是您第一次分娩吗？",
    "num_children": "14.您已经有几个孩子了？",
    "last_delivery_date": "15.最近一次分娩大概是什么时候？",
    "prev_preterm": "16.上一次怀孕有出现早产吗？",
    "surgery_history": "17.您是否做过剖腹产或子宫相关手术？[选项填空]",

    # MRQ groups
    "symptoms_prefix": "18.您现在有没有以下不适？",
    "symptoms_other": "18.您现在有没有以下不适？:其他____{fillblank-6b00}[选项填空]",

    "diagnosed_prefix": "19.此次怀孕期间，医生是否诊断以下疾病？",
    "diagnosed_other": "19.此次怀孕期间，医生是否诊断以下疾病？ :其他____{fillblank-69e2}[选项填空]",

    # Lifestyle
    "smoke_hist": "20.您有抽烟习惯吗？",
    "still_smoke": "21.目前还在抽烟吗？",
    "quit_smoke_preg": "22.是怀孕后才戒的吗？",
    "alcohol_hist": "23.您有饮酒习惯吗？",
    "still_drink": "24.目前还在饮酒吗？",
    "quit_drink_preg": "25.是怀孕后才戒的吗？",
    "drug_hist": "26.您是否有药物或毒品滥用史？",
}

OTHER_ALIASES = ("其他", "其它")

yn_map = {"是": "Yes", "否": "No", "有": "Yes", "没有": "No"}
contraction_awareness_map = {"能感受/察觉": "Yes", "不能感受/察觉": "No"}
modoo_influence_map       = yn_map
modoo_usefulness_map      = yn_map
problems_faced_map        = yn_map
recommendation_map        = {"会": "Yes", "不会": "No"}
did_ultrasound_map        = {"有": "Yes", "没有": "No", "否": "No"}
informed_doctor_map       = yn_map
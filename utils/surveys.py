import re
import pandas as pd

def strip_choice(val):
    """
    For MCQ/MRQ answers like 'A.是' or 'B.头晕', drop the letter+dot prefix
    and return only the text after the dot. Otherwise, return the trimmed value.
    """
    if isinstance(val, str) and "." in val:
        code, rest = val.split(".", 1)
        return rest.strip()
    return val.strip() if isinstance(val, str) else val

def clean_and_join(columns):
    """
    Joins a list of strings and removes leading/trailing commas and unwanted empty entries.
    """
    cleaned = [strip_choice(val) for val in columns if pd.notna(val) and str(val).strip()]
    return ", ".join(cleaned)

def parse_ga_str(ga_str):
    """
    Parse GA like '38.4' meaning 38周4天.
    Returns (original_str, total_days) or ("", None) if invalid.
    """
    if not isinstance(ga_str, str):
        return "", None
    s = ga_str.strip()
    if not s:
        return "", None
    try:
        s = s.replace("，", ".").replace(" ", ".")
        if "." in s:
            w_str, d_str = s.split(".", 1)
            w = int(w_str)
            d = int("".join(ch for ch in d_str if ch.isdigit()) or "0")
        else:
            w, d = int(s), 0
        d = max(0, min(6, d))  # clamp days to 0–6
        return s, w * 7 + d
    except Exception as e:
        print(e)
        return "", None

def date_only_from_dmy(x: str) -> str:
    """Input like '31/8/2025 21:16' -> '2025-08-31' (or '' if invalid)."""
    ts = pd.to_datetime(x, errors="coerce", dayfirst=True)
    return ts.strftime("%Y-%m-%d") if pd.notna(ts) else ""

def safe_get_value(row, col):
    value = row.get(col, "")
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip() if value else ""

def map_choice(value, mapping):
    if value is None:
        return None
    return mapping.get(strip_choice(value), strip_choice(value))

def safe_get(row, colname, default=""):
    """Get a cell as a trimmed string; NaN→''."""
    if not colname:
        return default
    if colname not in row.index:
        return default
    v = row[colname]
    if pd.isna(v):
        return default
    return str(v).strip()

def first_existing_name(row, candidates):
    """Return the first column name that exists in this row (for old/new numbering)."""
    for c in candidates:
        if c in row.index:
            return c
    return None

def is_other_placeholder(text: str) -> bool:
    """
    True for values like:
      '其他', '其它', '其他____{...}', '其它________', '其他__________'
    I.e., the 'Other' label without real user-provided content.
    """
    if not isinstance(text, str):
        return False
    t = text.strip()
    if not t:
        return True
    # drop leading choice code like 'A.' if present
    if "." in t:
        t = t.split(".", 1)[1].strip()

    if t in OTHER_ALIASES:
        return True

    # "其他____{fillblank-xxxx}" or "其它________"
    if re.match(rf"^({'|'.join(OTHER_ALIASES)})\s*(_+|\{{.*\}})\s*$", t):
        return True
    # "其他" followed only by punctuation/spaces
    if re.match(rf"^({'|'.join(OTHER_ALIASES)})[\s\W]*$", t):
        return True
    return False

def normalize_commas(s: str) -> str:
    if not s:
        return s
    s = re.sub(r"(,\s*)+", ", ", s)
    return s.strip(", ").strip()

def collect_mrq(row, prefixes, other_fill_suffix):
    """
    Legacy MRQ collector by exact prefix. (Kept for backward-compat.)
    """
    vals = []
    other_text_from_fill = ""
    other_text_from_raw = ""

    for pref in prefixes:
        for col in row.index:
            if not col.startswith(pref):
                continue

            if col.endswith("[选项填空]"):
                other_text_from_fill = safe_get(row, col, "").strip()
                continue

            v = strip_choice(safe_get(row, col, ""))
            if v and not is_other_placeholder(v):
                vals.append(v)

        raw_other_col = f"{pref}:其他____{other_fill_suffix}"
        if raw_other_col in row.index:
            raw_v = strip_choice(safe_get(row, raw_other_col, "")).strip()
            if raw_v and not is_other_placeholder(raw_v):
                other_text_from_raw = raw_v

    joined = clean_and_join(vals)

    extra = other_text_from_fill or other_text_from_raw
    if extra:
        joined = f"{joined}, {extra}" if joined else extra

    return normalize_commas(joined)

def find_first_col(cols, patterns):
    """
    Return the first column whose header matches ANY regex in `patterns`.
    """
    for pat in patterns:
        rx = re.compile(pat)
        for c in cols:
            if rx.search(c):
                return c
    return None

def collect_mrq_by_keywords(row, all_cols, group_keywords, other_fill_hint=r"\{fillblank-.*?\}"):
    """
    Generic multi-response collector.
    - Picks columns whose header contains ALL keywords in `group_keywords`.
    - Keeps ticked values (ignores '其他' placeholders).
    - Appends free-text from '[选项填空]' or raw '{fillblank-...}' columns when present.
    """

    def header_in_group(h):
        return all(kw in h for kw in group_keywords)

    values = []
    free_other = ""

    # explicit free-text columns often end with "[选项填空]"
    for c in all_cols:
        if header_in_group(c) and c.endswith("[选项填空]"):
            free_other = safe_get(row, c, "").strip()

    # normal MRQ ticked options
    for c in all_cols:
        if not header_in_group(c) or c.endswith("[选项填空]"):
            continue
        v = strip_choice(safe_get(row, c, ""))
        if v and not is_other_placeholder(v):
            values.append(v)

    raw_other_cols = [c for c in all_cols if
                      header_in_group(c) and ("其他" in c or "其它" in c) and re.search(other_fill_hint, c)]
    raw_other_text = ""
    for c in raw_other_cols:
        t = strip_choice(safe_get(row, c, ""))
        if t and not is_other_placeholder(t):
            raw_other_text = t
            break

    joined = clean_and_join(values)
    extra = free_other or raw_other_text
    if extra:
        joined = f"{joined}, {extra}" if joined else extra
    return normalize_commas(joined)

num_children_map = {
    "一": 1,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "多过六个": ">=6"
}

num_pregnancy_map = {
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
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

COL = {
    "joined_date": "开始答题时间",
    "name": "1.名字",
    "contact": "2.电话号码",
    "age": "3.您的年龄是多少？",
    "ga_now": "4.目前的孕周（例如 38.4 代表38周4天）",
    "height_cm": "5.目前的身高（厘米）",
    "weight_jin": "6.目前的体重（斤）",
    "pre_weight_jin": "7.怀孕前的体重（斤）",
    "lmp": "8.您的最后一次月经大概是什么时候？",
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
import json
import numpy as np
import pandas as pd
from typing import Any, Optional
from datetime import datetime

# ---------- Helper Functions ----------
def parse_date_ymd(x: Any) -> pd.Timestamp:
    """Strictly parse 'YYYY-MM-DD'."""
    return pd.to_datetime(x, format="%Y-%m-%d", errors="coerce")

def delivery_type_map(s: Any) -> Any:
    s = "" if s is None else str(s)
    if "顺产" in s:
        return "natural"
    if "剖腹产（剖宫产）" in s:
        return "c-section"
    if "紧急剖腹产" in s:
        return "emergency c-section"
    return None

def bmi_choose_weight_kg(height_cm: Any, weight_val: Any) -> Optional[float]:
    """
    Resolve 斤 vs kg:
      - If weight > 110 → treat as 斤 (kg = x * 0.5)
      - Else compute BMI for both kg and 斤 and pick the one within [15, 45].
        If both plausible or both implausible, default to kg when <= 110.
    """

    def _try_float(x: Any) -> Optional[float]:
        try:
            return float(str(x).strip())
        except Exception as e:
            print(e)
            return None

    h_cm = pd.to_numeric(height_cm, errors="coerce")
    w = _try_float(weight_val)
    if pd.isna(h_cm) or h_cm <= 0 or w is None:
        return None

    h_m = h_cm / 100.0
    kg_if_kg = w
    kg_if_jin = w * 0.5

    def _bmi(kg: Optional[float]) -> Optional[float]:
        return (kg / (h_m ** 2)) if (kg and h_m > 0) else None

    b1 = _bmi(kg_if_kg)
    b2 = _bmi(kg_if_jin)

    def plausible(b: Optional[float]) -> bool:
        return (b is not None) and (15.0 <= b <= 45.0)

    if w > 110:
        return round(b2, 1) if b2 is not None else None
    if plausible(b1) and not plausible(b2):
        return round(b1, 1)
    if plausible(b2) and not plausible(b1):
        return round(b2, 1)
    return round(b1, 1) if b1 is not None else None

def flag_contains_1_0(text: Any, needle: str) -> int:
    return 1 if (text is not None and needle in str(text)) else 0

def ga_simple_to_float(x: Any) -> float:
    try:
        s = str(x).strip()
        return float(s) if s != "" else np.nan
    except Exception as e:
        print(e)
        return np.nan

def compute_onset(row: pd.Series) -> str:
    """Onset = parsed water_break_datetime, else ''."""

    def parse_water_break_datetime(wb: Any) -> Optional[pd.Timestamp]:
        """Expect 'YYYY-MM-DD HH:MM' or '' from Mongo; return Timestamp or None."""
        s = "" if wb is None else str(wb).strip()
        if not s:
            return None
        t = pd.to_datetime(s, format="%Y-%m-%d %H:%M", errors="coerce")
        return t if pd.notna(t) else None

    ts = parse_water_break_datetime(row.get("water_break_datetime"))
    return ts.strftime("%Y-%m-%d %H:%M") if ts is not None else ""

######################################## HISTORICAL ########################################

def to_int_or_none(x):
    if x is None or str(x).strip() == "":
        return None
    try:
        return int(float(x))
    except Exception as e:
        print(e)
        return None


def to_float_or_none(x):
    if x is None or str(x).strip() == "":
        return None
    try:
        v = float(x)
        return None if np.isnan(v) else v
    except Exception as e:
        print(e)
        return None

def to_ymd_or_none(x):
    """
    Parse date-like values (CSV has DD/MM/YYYY or D/MM/YYYY) to 'YYYY-MM-DD'.
    Returns None if it can't be parsed.
    """
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date().isoformat()  # 'YYYY-MM-DD'


def to_ymd_hm_or_none(x):
    """
    Parse datetime-like values (e.g. '4/7/2023 20:00') to 'YYYY-MM-DD HH:MM'.
    Accepts DD/MM/YYYY[ HH:MM[:SS]] and ISO-like inputs. Returns None if unparsable.
    """
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d %H:%M")

def extract_gest_age(

        conclusion : str,
        basic_info : str

) -> Optional[int]:

    gest_age        = None
    basic_info_json = json.loads(basic_info)

    # Check if gest_age can be obtained from 'basic_info' field
    if basic_info_json["setPregTime"]:

        gest_string = basic_info_json["pregTime"]

        digits = [int(c) for c in gest_string if c.isdigit()]

        if len(digits) == 3:
            gest_age = digits[0] * 10 * 7 + digits[1] * 7 + digits[2]
        elif len(digits) == 2:
            gest_age = digits[0] * 10 * 7 + digits[1] * 7

    # If 'conclusion' field available and gest_age still not found
    if conclusion and not gest_age:

        gest_string = conclusion.split("。")[0]

        digits = [int(c) for c in gest_string if c.isdigit()]

        if len(digits) == 3:
            gest_age = digits[0] * 10 * 7 + digits[1] * 7 + digits[2]
        elif len(digits) == 2:
            gest_age = digits[0] * 10 * 7 + digits[1] * 7

    return gest_age
# Patients Data

Script for consolidating survey and patient data into MongoDB.   
The main entrypoint is [`main.py`](./main.py), which runs the full data ingestion sequence.

---

## 📁 Repository Structure

```bash
patients/
│
├─ config/
│  ├─ .env                 # Credentials for Navicat, MongoDB
│  ├─ configs.py           # Configurations for Navicat, MongoDB
│  └─ ef_aliyun_pem        # Navicat SSH Key
│
├─ database/
│  ├─ MongoDBConnector.py  # MongoDB connector class
│  ├─ SQLDBConnector.py    # MySQL connector class
│  └─ queries.py           # Parameterized SQL queries
│
├─ datasets/               # Pre and Post survey data
│
├─ scripts/
│  ├─ upsert_surveys.py    # Upserts latest survey responses in ./datasets
│  ├─ recruited.py         # Upserts recruited patients
│  └─ historical.py        # Upserts historical patients
│
├─ utils/                  # Utility modules
│
├─ main.py                 # Entrypoint orchestrating all scripts
│
├─ checK_patient.ipynb     # Notebook to check patient eligibity
│
└─ requirements.txt        # Dependencies
```

### Install dependencies

```bash
python -m venv venv
venv/Scripts/activate
pip install --upgrade pip
pip install -r requirements.txt
````

---

## ▶️ Running the Main Script

### Pre-requisites

* Create `./datasets` directory at the root level if it doesn't exist

* Recruited Patients

  * Ensure that pre and post survey CSV data files are present in `./datasets`, named `{yymmdd}_pre_survey.csv` and `{yymmdd}_post_survey.csv` respectively

  * Ensure consistent data format in pre and post survey files (e.g. change '38周2天' to '38.2')

* Historical Patients

  * Ensure that historical patient metadata file is present in `./datasets` named `historical_metadata.xlsx`

### Execution

```bash
python main.py --mode remote --date {yymmdd}

# --mode: remote | local
# --date: optional, defaults to today ('yymmdd' format)
```

`main.py` is the **entrypoint** and executes these scripts in sequence:

| Step | Script              | Description                                   |
| ---- | ------------------- |-----------------------------------------------|
| 1️⃣  | `upsert_surveys.py` | Upserts patient pre and post survey responses |
| 2️⃣  | `recruited.py`      | Upserts recruited patient information         |
| 3️⃣  | `historical.py`     | Upserts historical patient information        |

**NOTE: Pre and Post survey data for the given date must exist in `./datasets`**

---

## 🧩 Running Individual Scripts

You can also run scripts independently for testing or partial updates:

```bash
python -m scripts.upsert_surveys --date {yymmdd}
python -m scripts.recruited --mode remote
python -m scripts.historical --mode remote
```

---

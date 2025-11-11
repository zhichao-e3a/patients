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

```bash
python main.py --mode remote --date {yymmdd}
```

`--mode`: 'remote' or 'local' 

`--date`: Optional ; provide in 'yymmdd' format, defaults to today

### Execution

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

# TeleConnect-DataFlow

📡 **TeleConnect-DataFlow** is a real-time data engineering pipeline designed for customer churn prediction. It processes messy, multi-format data from various telecom systems, performs complex transformations, enforces schema validation, encrypts sensitive information, and uploads cleaned data to HDFS for downstream analysis and machine learning models.

---
## 🚀 Project Overview

TeleConnect, a telecom company, is facing high customer churn. This project helps by:

- Integrating and cleaning real-time data from 6 different sources (CSV, JSON, TXT).
- Transforming and enriching datasets with calculated insights.
- Encrypting sensitive message content using a Caesar cipher.
- Saving transformed data in Parquet format and uploading to HDFS.
- Supporting the Data Science team with ML-ready, structured data.

---

## 🛠️ Tech Stack

- **Language:** Python 3.9+
- **Data Handling:** Pandas, JSON, CSV, Regex
- **Filesystem:** OS, Glob, Pathlib
- **Logging & Monitoring:** Python logging module
- **Email Alerts:** smtplib, email.mime
- **Security:** Caesar Cipher + dictionary-based decryption
- **Output Format:** Parquet
- **Storage:** HDFS via subprocess
- **Design:** OOP, SOLID principles

---

## 📂 Project Structure (Suggested)

```plaintext
TeleConnect-DataFlow/
├── incoming_data/                # Raw data organized by date/hour
├── pipeline/
│   ├── __init__.py
│   ├── main.py                   # Main orchestrator
│   ├── extractor.py
│   ├── transformer/
│   │   ├── base.py
│   │   ├── customer.py
│   │   ├── billing.py
│   │   ├── ...
│   ├── loader.py
│   ├── logger.py
│   ├── schema_validator.py
│   ├── config/
│   │   ├── schemas.json
│   │   ├── paths.json
│   │   └── email_credentials.txt
├── logs/                         # Persistent log files
├── archive/                      # Archived processed files
├── utils
│   ├── datahandler.py
│   ├── __init__.py
│   ├── logger.py
│
├── README.md
├── .gitignore
```
--- 

## 🔄 Pipeline Features

- **Schema validation & rejection**
- **Logging every step with timestamps**
- **Data quality checks**
- **Real-time operation (no reprocessing)**
- **Caesar encryption with random key per file**
- **Secure email notifications for failures**
- **Upload to HDFS using subprocess**

---

## 🛡 Security & Privacy

- **Passwords stored in a secure .txt file**
- **No hardcoded credentials**
- **Sensitive data encrypted at rest (messages)**

---

## 📊 Suggested Analyses

The transformed data enables rich analysis for churn prediction and business insight. Some suggested directions include:

- **Churn Analysis**
  - Identify customer segments (age, city, tenure) with high churn.
  - Correlate churn with late payment behavior and service complaints.

- **Revenue Analysis**
  - Calculate ARPU (Average Revenue Per User).
  - Compare revenue between churners and loyal customers.

- **Behavior Trend Analysis**
  - Explore usage patterns (data, calls, messages) that indicate churn risk.
  - Track drop in engagement over time.

- **Geographic Insights**
  - Highlight cities/regions with higher churn or complaints.

---

## 👥 Authors & Contributions

This project was built collaboratively by a team of two as part of a university-level Data Engineering course.

Key contributions include:
- 🧠 Data modeling and transformation logic
- 🧱 Real-time ETL orchestration
- 🔒 Message encryption and schema validation
- 🔔 Failure recovery and logging system
- 📈 Insights for churn analysis

---

## 📫 Contact

Have questions, feedback, or collaboration ideas?  
Feel free to reach out via GitHub Issues or create a Pull Request!


_Enjoy exploring the data flow and unlocking insights for customer retention!_

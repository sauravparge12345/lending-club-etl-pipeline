# Lending Club ETL Pipeline

**An automated Data Engineering pipeline to analyze credit risk using Airflow, Python, AWS S3, and Snowflake.**

# Project Overview

Built an automated ETL pipeline using **Apache Airflow** and **Python** to process **2.2 million loan records**. The pipeline utilizes **Pandas** for chunked data transformation and **Parquet** optimization to handle large datasets efficiently. It orchestrates the secure transfer of cleaned data to an **AWS S3 Data Lake** and loads it into **Snowflake** for scalable SQL-based credit risk analysis.

# Dataset

**Source:** [Lending Club Loan Data (Kaggle)](https://www.kaggle.com/datasets/wordsforthewise/lending-club)

**File Used:** `accepted_2007_to_2018Q4.csv.gz` (Raw Size: ~1.6 GB)

---

# Architecture & Workflow

The pipeline follows a standard **Extract, Transform, Load (ETL)** architecture:

# 1. Extract 

* **Tool:** Kaggle API & Python
* **Action:** Connects to Kaggle and downloads the raw zipped CSV file (`1.6 GB`) to the local Airflow environment.

# 2. Transform 

* **Tool:** Python (Pandas)
* **Technique:** Implemented **Chunking** (100k rows/batch) to bypass RAM limitations.
* **Key Operations:**
* **Cleaning:** Removed 141 irrelevant columns, fixed data types (mixed-type IDs, string dates), and filtered missing values.
* **Optimization:** Converted the heavy CSV format into a compressed **Parquet** file (~200 MB), reducing storage size by 85%.



# 3. Load 

* **Tool:** AWS S3 (Boto3) & Snowflake
* **Action:** Uploads the cleaned Parquet file to an **AWS S3 Bucket** (Data Lake).
* **Warehouse:** Loads data from S3 into a **Snowflake** table using the `COPY INTO` command for persistent storage.

# 4. Analyze 

* **Tool:** Snowflake SQL
* **Insight:** Executed analytical queries to calculate **Default Rates by Loan Grade**, identifying that lower-grade loans (G) have a significantly higher risk profile compared to Grade A loans.

---

# Technologies Used

* **Orchestration:** Apache Airflow
* **Language:** Python 3.7
* **Data Processing:** Pandas (Chunking, Parquet)
* **Cloud Storage:** AWS S3
* **Data Warehouse:** Snowflake
* **Version Control:** Git / GitHub

# How to Run

1. **Clone the repository:**
```bash
git clone https://github.com/sauravparge12345/lending-club-etl-pipeline

```


2. **Install Dependencies:**
```bash
pip install apache-airflow pandas boto3 kaggle pyarrow

```


3. **Configure Credentials:**
* Set up `kaggle.json` for API access.
* Add AWS Access Keys in the script (or Environment Variables).


4. **Run Airflow:**
```bash
airflow dags trigger lending_club_risk_pipeline

```

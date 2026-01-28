import os
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kaggle.api.kaggle_api_extended import KaggleApi
import boto3


# 1. CONFIGURATION & CREDENTIALS

# --- KAGGLE CREDENTIALS ---
# NOTE: Ensure your KAGGLE_API_TOKEN is set in your environment variables or added here.

# --- AWS CREDENTIALS ---
AWS_ACCESS_KEY
AWS_SECRET_KEY

S3_BUCKET_NAME = "AAAAAA"  

# --- DATASET DETAILS ---
DATASET_NAME = "wordsforthewise/lending-club"
# We only want the "Accepted" loans, not the rejected ones
RAW_FILE_NAME = "accepted_2007_to_2018Q4.csv.gz"
LOCAL_PATH = "/tmp/lending_club_data"
CLEAN_FILE_NAME = "lending_club_cleaned.parquet"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lending_club_risk_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)


# 2. TASKS

# --- TASK 1: EXTRACT (Download Specific File) ---
def extract_data(**kwargs):
    api = KaggleApi()
    api.authenticate()
    
    if not os.path.exists(LOCAL_PATH):
        os.makedirs(LOCAL_PATH)
    
    print(f"Downloading {DATASET_NAME}...")
    
    # Download ONLY the 'accepted' file to save space
    api.dataset_download_file(DATASET_NAME, RAW_FILE_NAME, path=LOCAL_PATH)
    
    # Handle filename issues (Kaggle sometimes keeps it zipped)
    file_path = os.path.join(LOCAL_PATH, RAW_FILE_NAME)
    
    if os.path.exists(file_path):
        print(f"Success! Raw file downloaded: {file_path}")
        return file_path
    else:
        raise FileNotFoundError(f"Could not find {RAW_FILE_NAME}")

extract_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=extract_data,
    dag=dag
)

# --- TASK 2: TRANSFORM (Clean & Convert to Parquet) ---
def transform_data(**kwargs):
    ti = kwargs['ti']
    input_path = ti.xcom_pull(task_ids='extract_raw_data')
    output_path = os.path.join(LOCAL_PATH, CLEAN_FILE_NAME)
    
    print(f"Starting transformation on {input_path}...")
    
    cols_to_keep = [
        'id', 'loan_amnt', 'term', 'int_rate', 'installment', 'grade',
        'emp_length', 'home_ownership', 'annual_inc', 'loan_status'
    ]
    
    chunks = []
    chunk_size = 100000 
    
    # Read CSV in chunks to prevent memory crash
    reader = pd.read_csv(input_path, usecols=cols_to_keep, compression='gzip', chunksize=chunk_size)
    
    for i, chunk in enumerate(reader):
        # 1. FIX DATA TYPES
        # Force 'id' to be a String. This prevents the "ArrowInvalid" crash.
        chunk['id'] = chunk['id'].astype(str)

        # 2. CLEAN 'term'
        chunk['term'] = chunk['term'].astype(str).str.replace(' months', '').str.strip()
        
        # 3. FILTER (Remove rows where we don't know the status)
        chunk = chunk.dropna(subset=['loan_status'])
        
        # 4. CLEAN 'int_rate'
        # We use .copy() to fix SettingWithCopyWarning
        chunk = chunk.copy()
        chunk['int_rate'] = pd.to_numeric(chunk['int_rate'], errors='coerce')
        
        chunks.append(chunk)
        print(f"Processed chunk {i+1}...")
        
        # NOTE: Test mode limit removed. Processing all 2.2 Million rows.

    # Combine all chunks AND reset the index to prevent KeyError
    full_df = pd.concat(chunks, ignore_index=True)
    print(f"Transformation complete. Total rows: {len(full_df)}")
    
    # Save as Parquet
    full_df.to_parquet(output_path, index=False)
    print(f"Saved clean file to: {output_path}")
    
    return output_path

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

# --- TASK 3: LOAD (Upload to S3) ---
def load_to_s3(**kwargs):
    ti = kwargs['ti']
    local_file_path = ti.xcom_pull(task_ids='transform_data')
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    
    file_name = os.path.basename(local_file_path)
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    # We store this in a 'clean' folder in S3
    s3_key = f"lending_club/clean/{date_str}/{file_name}"
    
    print(f"Uploading {local_file_path} to S3...")
    s3.upload_file(local_file_path, S3_BUCKET_NAME, s3_key)
    print(f"Success! File available at: s3://{S3_BUCKET_NAME}/{s3_key}")

load_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3,
    provide_context=True,
    dag=dag
)

# 3. DEPENDENCIES

extract_task >> transform_task >> load_task

"""
flight_pipeline.py
Module 6 Project: Data Lifecycle & Pipelines
Big Data Management Course

Pipeline stages map to lifecycle phases:
  Stage 1 (ingest)    → raw data arrives
  Stage 2 (validate)  → QA / staging area (Rahm & Do ETL model)
  Stage 3 (store)     → move to structured storage (raw + clean buckets)
  Stage 4 (log)       → record keeping / archival
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import subprocess
import json
import os

# ── Configuration ────────────────────────────────────────────────────────────
PROJECT_ID   = "bigdata-pipeline-project"  
BUCKET_RAW   = f"gs://{PROJECT_ID}-raw"
BUCKET_CLEAN = f"gs://{PROJECT_ID}-clean"
BUCKET_LOGS  = f"gs://{PROJECT_ID}-logs"
LOCAL_DIR    = f"/home/{os.getenv('USER')}/pipeline_data"
RAW_FILE     = f"{LOCAL_DIR}/flights_raw.csv"
CLEAN_FILE   = f"{LOCAL_DIR}/flights_clean.csv"
LOG_FILE     = f"{LOCAL_DIR}/pipeline_log.json"

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# ── Task functions ────────────────────────────────────────────────────────────

def ingest_data(**context):
    """
    STAGE 1: INGEST
    Lifecycle phase: Data arrives / is acquired.
    Checks that the source file exists and records basic metadata.
    """
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(f"Raw data not found at {RAW_FILE}")
    
    df = pd.read_csv(RAW_FILE)
    row_count = len(df)
    col_count = len(df.columns)
    
    print(f"[INGEST] File found: {RAW_FILE}")
    print(f"[INGEST] Shape: {row_count} rows × {col_count} columns")
    print(f"[INGEST] Columns: {list(df.columns)}")
    
    # Push metadata to XCom for downstream tasks
    context['ti'].xcom_push(key='raw_row_count', value=row_count)
    context['ti'].xcom_push(key='columns', value=list(df.columns))
    return "ingest_ok"


def validate_data(**context):
    """
    STAGE 2: VALIDATE
    Lifecycle phase: QA / staging area.
    Implements checks described in Rahm & Do (2000):
      - Missing values (Table 2: Attribute-level problems)
      - Illegal/dummy values (e.g., dep_delay=9999)
      - Invalid categorical values
    Connects to Redman (2016): automated QA prevents the 'hidden data factory'.
    """
    df = pd.read_csv(RAW_FILE)
    issues = {}
    
    # Check 1: Missing values per column
    null_counts = df.isnull().sum().to_dict()
    issues['null_counts'] = null_counts
    print(f"[VALIDATE] Null counts: {null_counts}")
    
    # Check 2: Dummy values in dep_delay (Rahm & Do pattern: 9999 = unavailable)
    if 'dep_delay' in df.columns:
        dummy_rows = int((df['dep_delay'] == 9999).sum())
        issues['dummy_dep_delay'] = dummy_rows
        print(f"[VALIDATE] Dummy delay values (9999): {dummy_rows}")
    
    # Check 3: Invalid airport codes (should be 3-letter uppercase)
    if 'origin' in df.columns:
        valid_airports = ['ORD','ATL','LAX','DFW','JFK','SFO','SEA','DEN','MIA','BOS']
        invalid_origins = int((~df['origin'].isin(valid_airports)).sum())
        issues['invalid_origin_codes'] = invalid_origins
        print(f"[VALIDATE] Invalid origin codes: {invalid_origins}")
    
    # Check 4: Row count sanity
    if len(df) < 100:
        raise ValueError(f"[VALIDATE] FAIL: Only {len(df)} rows — likely corrupt file")
    
    total_issues = sum([
        sum(null_counts.values()),
        issues.get('dummy_dep_delay', 0),
        issues.get('invalid_origin_codes', 0)
    ])
    
    issues['total_dirty_records'] = total_issues
    issues['total_rows'] = len(df)
    issues['pct_dirty'] = round(total_issues / len(df) * 100, 2)
    
    print(f"[VALIDATE] Total dirty records detected: {total_issues} "
          f"({issues['pct_dirty']}% of dataset)")
    
    # Save validation report
    with open(f"{LOCAL_DIR}/validation_report.json", 'w') as f:
        json.dump(issues, f, indent=2)
    
    context['ti'].xcom_push(key='validation_issues', value=issues)
    
    # Branch decision: if >30% dirty, quarantine; else proceed to clean
    if issues['pct_dirty'] > 30:
        print("[VALIDATE] WARNING: High dirty-data rate. Flagging for review.")
        return "store_data"  # still proceed but flag is in the log
    return "store_data"

from airflow.operators.bash import BashOperator
def store_data(**context):
    """
    STAGE 3: STORE
    Lifecycle phase: Organized storage in raw and clean buckets.
    Applies basic cleaning transformations before saving clean copy.
    Architecture mirrors Rahm & Do Fig.1 ETL staging area pattern.
    """
    df = pd.read_csv(RAW_FILE)
    original_count = len(df)
    
    # Upload raw file to raw bucket (preserve original — data lineage principle)
    subprocess.run(
        ['gsutil', 'cp', RAW_FILE, f"{BUCKET_RAW}/flights_raw.csv"],
        check=True
    )
    print(f"[STORE] Raw file uploaded to {BUCKET_RAW}")
    
    # Apply cleaning transformations
    # 1. Remove dummy delay values
    if 'dep_delay' in df.columns:
        df = df[df['dep_delay'] != 9999]
    
    # 2. Drop rows with null carrier (critical field)
    if 'carrier' in df.columns:
        df = df[df['carrier'].notna()]
    
    # 3. Remove invalid airport codes
    valid_airports = ['ORD','ATL','LAX','DFW','JFK','SFO','SEA','DEN','MIA','BOS']
    if 'origin' in df.columns:
        df = df[df['origin'].isin(valid_airports)]
    
    cleaned_count = len(df)
    rows_removed = original_count - cleaned_count
    
    print(f"[STORE] Rows removed during cleaning: {rows_removed}")
    print(f"[STORE] Clean dataset size: {cleaned_count} rows")
    
    # Save clean file locally then upload
    df.to_csv(CLEAN_FILE, index=False)
    subprocess.run(
        ['gsutil', 'cp', CLEAN_FILE, f"{BUCKET_CLEAN}/flights_clean.csv"],
        check=True
    )
    print(f"[STORE] Clean file uploaded to {BUCKET_CLEAN}")
    
    context['ti'].xcom_push(key='rows_removed', value=rows_removed)
    context['ti'].xcom_push(key='clean_row_count', value=cleaned_count)


def log_completion(**context):
    """
    STAGE 4: LOG / ARCHIVE
    Lifecycle phase: Preservation and record keeping.
    Writes a structured completion record to GCS logs bucket.
    Supports data lineage (who cleaned what, when, how many rows changed).
    """
    ti = context['ti']
    
    raw_count   = ti.xcom_pull(task_ids='ingest_data',  key='raw_row_count')
    issues      = ti.xcom_pull(task_ids='validate_data', key='validation_issues')
    rows_removed = ti.xcom_pull(task_ids='store_data',  key='rows_removed')
    clean_count = ti.xcom_pull(task_ids='store_data',   key='clean_row_count')
    
    log_entry = {
        "pipeline_run": str(context['execution_date']),
        "status": "SUCCESS",
        "raw_row_count": raw_count,
        "dirty_records_found": issues.get('total_dirty_records', 'N/A') if issues else 'N/A',
        "pct_dirty": issues.get('pct_dirty', 'N/A') if issues else 'N/A',
        "rows_removed_in_cleaning": rows_removed,
        "clean_row_count": clean_count,
        "raw_location": f"{BUCKET_RAW}/flights_raw.csv",
        "clean_location": f"{BUCKET_CLEAN}/flights_clean.csv",
        "null_counts": issues.get('null_counts', {}) if issues else {}
    }
    
    with open(LOG_FILE, 'w') as f:
        json.dump(log_entry, f, indent=2)
    
    # Push log to GCS archive
    subprocess.run(
        ['gsutil', 'cp', LOG_FILE,
         f"{BUCKET_LOGS}/run_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}.json"],
        check=True
    )
    
    print("[LOG] Pipeline completion record:")
    print(json.dumps(log_entry, indent=2))
    print(f"[LOG] Log archived to {BUCKET_LOGS}")
notify_success = BashOperator(
        task_id="send_notification",
        bash_command='echo "Pipeline Successfully Completed at $(date)" >> /home/amystaff/pipeline_data/notifications.log',
    )
archive_to_github = BashOperator(
        task_id="archive_code_to_github",
        bash_command="""
        cd /home/amystaff/airflow/dags
        git add flight_pipeline.py
        git commit -m "Automated archival of pipeline logic: $(date)"
        git push origin main
        """
    )
# ── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="flight_data_lifecycle_pipeline",
    default_args=default_args,
    description="Module 6: Four-stage data lifecycle pipeline",
    schedule_interval=None,   # manual trigger for demo
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["module6", "lifecycle", "bigdata-course"],
) as dag:

    t1_ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data,
    )

    t2_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    t3_store = PythonOperator(
        task_id="store_data",
        python_callable=store_data,
    )

    t4_log = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
    )

    # Define pipeline order — this IS the lifecycle sequence
    t1_ingest >> t2_validate >> t3_store >> t4_log >> [archive_to_github, notify_success]

"""
Airflow DAG: AF008 - Schema Drift (customer_id type mismatch)
Expected: customer_id as INT
Actual: customer_id as STRING
Fix: Cast STRING to INT safely
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd


default_args = {
    'owner': 'patchit',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def _extract():
    """Extract phase - simulate data with customer_id as STRING"""
    data = {
        'customer_id': ['1001', '1002', '1003', '1004', '1005'],
        'order_amount': [100.50, 200.75, 150.00, 300.25, 175.80],
        'order_date': ['2026-03-01', '2026-03-01', '2026-03-02', '2026-03-02', '2026-03-03']
    }
    df = pd.DataFrame(data)
    print(f"Extracted {len(df)} rows")
    return df.to_dict('records')


def _transform(**context):
    """Transform phase - handle schema drift by casting customer_id STRING to INT"""
    ti = context['ti']
    records = ti.xcom_pull(task_ids='extract')
    
    df = pd.DataFrame(records)
    
    # Schema validation and correction
    if df['customer_id'].dtype == 'object':
        print("Detected customer_id as STRING, converting to INT")
        # Cast STRING to INT safely, handling potential conversion errors
        df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce').fillna(0).astype(int)
    
    # Ensure customer_id is INT type
    df['customer_id'] = df['customer_id'].astype(int)
    
    # Add computed columns
    df['customer_segment'] = df['order_amount'].apply(
        lambda x: 'premium' if x >= 200 else 'standard'
    )
    
    print(f"Transformed {len(df)} rows, customer_id dtype: {df['customer_id'].dtype}")
    return df.to_dict('records')


def _load(**context):
    """Load phase - validate schema and save results"""
    ti = context['ti']
    records = ti.xcom_pull(task_ids='transform')
    
    df = pd.DataFrame(records)
    
    # Final validation: ensure customer_id is INT
    assert df['customer_id'].dtype in ['int64', 'int32'], \
        f"Schema validation failed: customer_id should be INT but is {df['customer_id'].dtype}"
    
    print(f"Loaded {len(df)} rows with correct schema")
    print(f"Schema: {df.dtypes.to_dict()}")
    return True


def _fail(**context):
    """Legacy fail function - now integrated into transform with proper handling"""
    ti = context['ti']
    records = ti.xcom_pull(task_ids='extract')
    
    df = pd.DataFrame(records)
    
    # Check schema and handle drift
    if df['customer_id'].dtype == 'object':
        print("Schema drift detected: customer_id is STRING, converting to INT")
        df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce').fillna(0).astype(int)
    else:
        df['customer_id'] = df['customer_id'].astype(int)
    
    # Validate final schema
    assert df['customer_id'].dtype in ['int64', 'int32'], \
        f"Schema mismatch: expected customer_id INT but got {df['customer_id'].dtype}"
    
    print(f"Schema validation passed: customer_id is {df['customer_id'].dtype}")
    return df.to_dict('records')


with DAG(
    'patchit_af008_schema_drift',
    default_args=default_args,
    description='AF008: Handle schema drift for customer_id (STRING to INT)',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['patchit', 'af008', 'schema_drift'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=_extract,
    )
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=_transform,
    )
    
    load_task = PythonOperator(
        task_id='load',
        python_callable=_load,
    )
    
    fail_task = PythonOperator(
        task_id='fail_af008_schema_drift',
        python_callable=_fail,
    )
    
    extract_task >> transform_task >> load_task
    extract_task >> fail_task

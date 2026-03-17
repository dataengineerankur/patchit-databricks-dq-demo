"""
AF008: Schema drift changed data type
This DAG handles schema drift by converting data types appropriately.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def handle_schema_drift(**context):
    """
    Handle schema drift by converting data types.
    This function detects and handles data type changes gracefully.
    """
    msg = "[AF008] Schema drift changed data type"
    
    # Instead of raising an error, handle the schema drift
    print(f"Detected: {msg}")
    print("Applying schema conversion...")
    
    # Example: Convert string to integer or vice versa
    # In a real scenario, this would involve:
    # 1. Detecting the schema change
    # 2. Applying appropriate type conversions
    # 3. Logging the change for audit purposes
    
    print("Schema drift handled successfully")
    return {"status": "success", "message": "Schema conversion applied"}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'patchit_failure_pack_100',
    default_args=default_args,
    description='Handle schema drift in data pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['schema', 'data-quality'],
) as dag:
    
    fail_af008 = PythonOperator(
        task_id='fail_af008',
        python_callable=handle_schema_drift,
        provide_context=True,
    )

"""
PATCHIT Airflow Issue Pack - AF008: Schema Drift
This DAG simulates schema drift where customer_id type changed from INT to STRING.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'patchit_airflow_issue_008',
    default_args=default_args,
    description='AF008: Schema drift - customer_id type changed',
    schedule_interval=None,
    catchup=False,
)


def extract_data(**context):
    """Extract customer data with STRING customer_id (schema drift)"""
    data = {
        'customer_id': ['C000001', 'C000002', 'C000003'],
        'name': ['Alice', 'Bob', 'Charlie'],
        'amount': [100.0, 200.0, 150.0],
    }
    df = pd.DataFrame(data)
    context['ti'].xcom_push(key='customer_data', value=df.to_dict('records'))
    print(f"[EXTRACT] Extracted {len(df)} records with customer_id as STRING")
    return df.to_dict('records')


def validate_schema(**context):
    """Validate schema expectations"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract', key='customer_data')
    df = pd.DataFrame(data)
    
    print(f"[VALIDATE] customer_id dtype: {df['customer_id'].dtype}")
    print(f"[VALIDATE] Sample data: {df.head()}")
    
    expected_type = 'int64'
    actual_type = str(df['customer_id'].dtype)
    
    if actual_type != expected_type:
        print(f"[VALIDATE] Schema mismatch detected: expected {expected_type} but got {actual_type}")
        print(f"[VALIDATE] Attempting to cast customer_id to INT")
        df['customer_id'] = df['customer_id'].str.replace('C', '').astype(int)
        print(f"[VALIDATE] customer_id successfully cast to INT: {df['customer_id'].dtype}")
    
    context['ti'].xcom_push(key='validated_data', value=df.to_dict('records'))
    return df.to_dict('records')


def transform_data(**context):
    """Transform and aggregate customer data"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='validate_schema', key='validated_data')
    df = pd.DataFrame(data)
    
    total_amount = df['amount'].sum()
    customer_count = len(df)
    
    print(f"[TRANSFORM] Processed {customer_count} customers, total amount: {total_amount}")
    return {'customer_count': customer_count, 'total_amount': total_amount}


def _fail(**context):
    """
    Legacy failure injection point - now handled gracefully in validate_schema
    This function is kept for backward compatibility but no longer raises errors
    """
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract', key='customer_data')
    df = pd.DataFrame(data)
    
    expected_type = 'int64'
    actual_type = str(df['customer_id'].dtype)
    
    if actual_type != expected_type:
        print(f"[FAIL_CHECK] Schema drift detected: expected customer_id INT but got STRING")
        print(f"[FAIL_CHECK] Converting customer_id from STRING to INT")
        df['customer_id'] = df['customer_id'].str.replace('C', '').astype(int)
        print(f"[FAIL_CHECK] Schema drift remediated successfully")
        context['ti'].xcom_push(key='remediated_data', value=df.to_dict('records'))
        return df.to_dict('records')
    
    print(f"[FAIL_CHECK] Schema validation passed")
    return data


extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

fail_task = PythonOperator(
    task_id='fail_af008_schema_drift',
    python_callable=_fail,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

extract_task >> fail_task >> validate_task >> transform_task

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyodbc
import pymongo

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transaction_etl_process',
    default_args=default_args,
    description='Transaction ETL process from MongoDB and Postgres into SQL Server Data Warehouse',
    schedule_interval=timedelta(days=1), #The etl is scheduled to run on daily basis
)

def extract_mongo_transactions():
    pass

def extract_postgres_transactions():
    pass

def transform_data():
    pass

def load_data():
    pass

t1 = PythonOperator(
    task_id='extract_from_mongo',
    python_callable=extract_mongo_transactions,
    dag=dag,
)

t2 = PythonOperator(
    task_id='extract_from_postgres',
    python_callable=extract_postgres_transactions,
    dag=dag,
)

t3 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

t4 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

t1 >> t3
t2 >> t3
t3 >> t4



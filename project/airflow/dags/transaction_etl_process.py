from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyodbc
import pymongo
import psycopg2
from psycopg2 import sql

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
    # MongoDB connection details
    mongo_host = 'localhost'
    mongo_port = 27017
    mongo_db = 'financial_data'
    mongo_collection = 'transactions'

    # Connect to MongoDB
    client = pymongo.MongoClient(host=mongo_host, port=mongo_port)
    db = client[mongo_db]
    collection = db[mongo_collection]

    # Extract data from MongoDB
    data = list(collection.find())

    return data

def extract_postgres_transactions():
    # PostgreSQL connection details
    conn = psycopg2.connect(
        dbname="financial_data",
        user="postgres",
        password="postgres23!",
        host="localhost"
    )
    
    query = sql.SQL("""
        SELECT t.transaction_id, t.transaction_date, c.customer_id, 
               c.first_name, c.last_name, p.product_id, p.product_name, 
               l.location_id, l.location_name, l.country,
               t.quantity, t.total_amount
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        JOIN products p ON t.product_id = p.product_id
        JOIN locations l ON t.location_id = l.location_id
    """)
    
    df = pd.read_sql_query(query, conn)
    
    conn.close()
    return df

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



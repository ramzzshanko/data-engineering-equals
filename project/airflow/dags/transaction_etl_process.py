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
        password="postgres",
        host="172.31.176.1"
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

def transform_data(ti, **kwargs):
    # Pull data from XCom
    mongo_data = ti.xcom_pull(task_ids='extract_from_mongo')
    postgres_data = ti.xcom_pull(task_ids='extract_from_postgres')

    # Convert MongoDB data to DataFrame
    df_mongo = pd.DataFrame(mongo_data)

    # Rename columns in MongoDB DataFrame to match PostgreSQL
    df_mongo = df_mongo.rename(columns={
        '_id': 'transaction_id',
        'amount': 'total_amount'
    })

    # Ensure date columns are in datetime format
    df_mongo['transaction_date'] = pd.to_datetime(df_mongo['transaction_date'])
    postgres_data['transaction_date'] = pd.to_datetime(postgres_data['transaction_date'])

    # Combine data from both sources
    combined_df = pd.concat([df_mongo, postgres_data], ignore_index=True)

    # Transform data for dimension tables
    date_dim = combined_df[['transaction_date']].drop_duplicates()
    date_dim['date_id'] = range(1, len(date_dim) + 1)
    date_dim['day'] = date_dim['transaction_date'].dt.day
    date_dim['month'] = date_dim['transaction_date'].dt.month
    date_dim['year'] = date_dim['transaction_date'].dt.year
    date_dim['quarter'] = date_dim['transaction_date'].dt.quarter

    customer_dim = combined_df[['customer_id', 'first_name', 'last_name']].drop_duplicates()
    customer_dim['customer_name'] = customer_dim['first_name'] + ' ' + customer_dim['last_name']
    customer_dim['customer_email'] = customer_dim['customer_name'].apply(lambda x: x.lower().replace(' ', '.') + '@example.com')
    customer_dim['customer_type'] = 'Regular'  # You might want to create a logic for different customer types
    customer_dim = customer_dim[['customer_id', 'customer_name', 'customer_email', 'customer_type']]

    product_dim = combined_df[['product_id', 'product_name']].drop_duplicates()
    product_dim['product_category'] = 'Default'  # You might want to create a logic for different product categories
    product_dim['product_price'] = 0  # You might want to add actual product prices if available

    location_dim = combined_df[['location_id', 'location_name', 'country']].drop_duplicates()
    location_dim = location_dim.rename(columns={'location_name': 'city'})
    location_dim['state'] = 'N/A'  # You might want to add state information if available

    # Transform data for fact table
    fact_table = combined_df.merge(date_dim, left_on='transaction_date', right_on='transaction_date')
    fact_table = fact_table[['transaction_id', 'date_id', 'customer_id', 'product_id', 'location_id', 'total_amount']]
    fact_table = fact_table.rename(columns={'total_amount': 'transaction_amount'})
    fact_table['transaction_type'] = 'Purchase'  # You might want to create a logic for different transaction types

    # Push the transformed data to XCom
    ti.xcom_push(key='date_dim', value=date_dim.to_dict('records'))
    ti.xcom_push(key='customer_dim', value=customer_dim.to_dict('records'))
    ti.xcom_push(key='product_dim', value=product_dim.to_dict('records'))
    ti.xcom_push(key='location_dim', value=location_dim.to_dict('records'))
    ti.xcom_push(key='fact_table', value=fact_table.to_dict('records'))

def load_data(ti, **kwargs):
    # Pull transformed data from XCom
    date_dim = pd.DataFrame(ti.xcom_pull(key='date_dim', task_ids='transform_data'))
    customer_dim = pd.DataFrame(ti.xcom_pull(key='customer_dim', task_ids='transform_data'))
    product_dim = pd.DataFrame(ti.xcom_pull(key='product_dim', task_ids='transform_data'))
    location_dim = pd.DataFrame(ti.xcom_pull(key='location_dim', task_ids='transform_data'))
    fact_table = pd.DataFrame(ti.xcom_pull(key='fact_table', task_ids='transform_data'))

    # SQL Server connection details
    server = 'project-sqlserver-1'
    database = 'DataWarehouse'
    username = 'sa'
    password = 'Password2024!'
    driver = '{ODBC Driver 18 for SQL Server}'

    # Establish connection to SQL Server
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    cursor = conn.cursor()

    # Function to insert data into a table
    def insert_data(df, table_name):
        for _, row in df.iterrows():
            columns = ', '.join(row.index)
            placeholders = ', '.join(['?' for _ in row.index])
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))

    # Insert data into dimension tables
    insert_data(date_dim, 'Date')
    insert_data(customer_dim, 'Customer')
    insert_data(product_dim, 'Product')
    insert_data(location_dim, 'Location')

    # Insert data into fact table
    insert_data(fact_table, 'FinancialTransactions')

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print(f"Loaded {len(fact_table)} transactions into the data warehouse.")

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
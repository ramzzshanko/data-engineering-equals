from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.kafka.operators.kafka_produce import KafkaProduceOperator
from datetime import datetime, timedelta
import json
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'realtime_analytics',
    default_args=default_args,
    description='Real-time analytics pipeline',
    schedule_interval=timedelta(minutes=5),
)

def generate_transaction():
    transaction = {
        'transaction_id': random.randint(1000, 9999),
        'customer_id': random.randint(1, 1000),
        'product_id': random.randint(1, 100),
        'timestamp': datetime.now().isoformat(),
        'amount': round(random.uniform(10, 1000), 2),
        'transaction_type': random.choice(['purchase', 'refund', 'deposit', 'withdrawal'])
    }
    return json.dumps(transaction)

produce_to_kafka = KafkaProduceOperator(
    task_id='produce_to_kafka',
    topic='financial_transactions',
    producer_function=generate_transaction,
    kafka_config={'bootstrap.servers': 'kafka:9092'},
    dag=dag
)

def trigger_flink_job():
    # In a real scenario, you would use Flink's REST API to submit or control jobs
    print("Triggering Flink job for real-time processing")

trigger_flink = PythonOperator(
    task_id='trigger_flink',
    python_callable=trigger_flink_job,
    dag=dag
)

produce_to_kafka >> trigger_flink
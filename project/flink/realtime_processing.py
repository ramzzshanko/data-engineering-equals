from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json

def process_transactions():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # Define Kafka source
    t_env.execute_sql("""
        CREATE TABLE financial_transactions (
            transaction_id INT,
            customer_id INT,
            product_id INT,
            timestamp TIMESTAMP(3),
            amount DECIMAL(10, 2),
            transaction_type STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'financial_transactions',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-consumer-group',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # Process data
    result = t_env.sql_query("""
        SELECT
            TUMBLE_START(timestamp, INTERVAL '1' MINUTE) AS window_start,
            transaction_type,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount
        FROM financial_transactions
        GROUP BY TUMBLE(timestamp, INTERVAL '1' MINUTE), transaction_type
    """)

    # Define a UDF to write results to InfluxDB
    @udf(result_type=DataTypes.BOOLEAN())
    def write_to_influxdb(window_start, transaction_type, transaction_count, total_amount):
        client = InfluxDBClient(url="http://influxdb:8086", token="mytoken", org="myorg")
        write_api = client.write_api(write_options=SYNCHRONOUS)
        
        point = Point("financial_transactions") \
            .tag("transaction_type", transaction_type) \
            .field("transaction_count", transaction_count) \
            .field("total_amount", total_amount) \
            .time(window_start)
        
        write_api.write(bucket="mybucket", record=point)
        return True

    # Apply the UDF
    result.select(
        write_to_influxdb(
            result.window_start,
            result.transaction_type,
            result.transaction_count,
            result.total_amount
        )
    )

    # Execute
    env.execute("Real-time Financial Transaction Processing")

if __name__ == '__main__':
    process_transactions()
# Financial Data Pipeline and Analytics Project

## Project Overview

This project involves setting up a comprehensive data infrastructure for a financial analytics platform. The project includes the implementation of ETL pipelines, a data warehouse, real-time data processing, and data governance policies. The goal is to create an end-to-end data solution that integrates data from various sources, processes it, and provides meaningful insights in real-time, all while ensuring data quality and compliance with industry regulations.

### Objectives

1. **Data Integration and ETL**: Integrate data from MongoDB and MySQL into a data warehouse.
2. Data Warehouse: Design and implement a star or snowflake schema suitable for financial analytics.
3. Real-Time Analytics: Process and analyze real-time financial data using a streaming platform.
4. Data Quality and Governance: Implement data quality checks and ensure compliance with data governance policies.

## Project Structure

├── project/                           # Project Parent folder
│   ├── airflow/    # Apache Airflow parent folder
│   └──── dags/       # Airflow DAGs for ETL and real-time processing

│   └────── transaction_etl_process.py      # DAG for the ETL pipeline

│   └────── realtime_analytics_dag.py       # DAG for real-time processing
|    ├── flink/                            # Apache Flink Docker file and real time processing file
│   ├──── Dockerfile           # Dockerfile
│   ├──── realtime_processing.py          # Realtime processing script
|    ├── influxdb/                            # Influx database dockerfile
│   ├──── Dockerfile           # Dockerfilefor 
|    ├── kafka/                            # Apache kafka Dockerfile and config
│   ├──── Dockerfile           # Dockerfile

│   ├──── kafka_server_jaas.conf         # config file

|    |---- sql-server # Data Warehouse ddl scripts
│   └── data_governance_policy.md          # Data governance policies and implementation
├── README.md                       # Project README file


## Getting Started

### Prerequisites

1. Python 3.10+
2. Docker
3. Apache Airflow 2.x
4. MongoDB
5. Postgres
6. Apache Kafka (for real-time data processing)
7. Pandas, PyMongo, Pymssql (Python libraries)

## Setup Instructions

1. Clone the Repository: `git clone https://github.com/ramzzshanko/data-engineering-equals.git`
2. `cd data-engineering-equals` && `docker compose up --build`
3. Access Apache Airflow webserver at `http://localhost:8080`, username is `airflow` and password is `airflow`.

## Running the Project

### ETL Pipeline

Access the Airflow UI (http://localhost:8080) and trigger the transaction_etl_process DAG.


### Real-Time Data Processing

Simulate Real-Time Data:
Use Apache Kafka to simulate real-time financial transactions or market data.
Run the realtime_analytics_dag.py DAG in Airflow to start ingesting and processing this data.


## Data Warehouse Schema

### Schema Design

The data warehouse uses a Star Schema to organize financial data. The schema includes the following tables:

#### Fact Table:

#### Transactions: Stores financial transactions, including references to dimension tables.

Dimension Tables:
Time: Stores date and time-related information.
Customer: Stores customer information.
Product: Stores details about financial products.

## Data Quality and Governance

### Data Quality Checks

Completeness: Ensures all required fields are populated.
Accuracy: Validates that numerical data falls within expected ranges.
Consistency: Checks for consistency across different data sources.

### Data Governance Policies

Access Control: Implemented using role-based access control (RBAC) in the data warehouse.
Encryption: All data is encrypted at rest and in transit.
Compliance: Documentation and checks to ensure compliance with GDPR, CCPA, and other relevant regulations.

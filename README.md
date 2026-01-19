# DBahn Train Movement ETL Pipeline

This project contains ETL pipelines for Deutsche Bahn train movement data. 
It extracts, transforms, and loads train data into a database or processes it with Spark.


## Project Structure

- part1_postgres_etl: ETL pipeline using PostgreSQL (Task 1)
- part3_spark_etl: ETL pipeline using Apache Spark (Task 3)

## Prerequisites
- Docker >= 20.10 (tested with 29.1.4)
- Docker Compose plugin >= 2.29 (tested with v2.29.2)
- Python >= 3.13

##  part1 : Postgres ETL pipline
1. Navigate to the project directory:

bash

cd part1_postgres_etl

docker compose build --no-cache

docker compose up -d

docker exec -w /app/src dbahn-etl python main.py



## part3: Spark Etl Pipeline

## task 3.1
source ./venv/bin/activate
cd ./part3_spark_etl

python3 etl_main.py

## task 3.2

python3 etl_task_3.2.py
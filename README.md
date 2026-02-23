# Berlin Public Transport Data Analysis

A data engineering project analyzing 6 weeks of real-world public transport data from 133 Berlin stations using PostgreSQL and PySpark.

## Overview

This project processes data from Berlin's public transport system collected between September and October 2025. The dataset includes timetable information collected every hour and disruption data (delays, cancellations) collected every 15 minutes from 133 stations across Berlin.

I built two separate ETL pipelines to handle this data:

1. A PostgreSQL-based pipeline with a star schema design for relational analysis
2. A PySpark pipeline for large-scale distributed processing

## What I Built

### PostgreSQL Data Warehouse

I designed a star schema with one fact table for train movements and multiple dimension tables for stations, trains, and time information. The ETL pipeline parses thousands of XML and JSON files and loads them into PostgreSQL.

The pipeline handles:
- Station metadata from JSON files
- Timetable data collected hourly
- Disruption data collected every 15 minutes
- Proper timestamp parsing and timezone handling

I also wrote SQL queries to answer questions like:
- Finding the nearest station to given coordinates
- Calculating average delays per station
- Counting canceled trains during specific time periods

### PySpark Pipeline

For large-scale processing, I built a PySpark job that reads all the XML files, extracts relevant information, and stores everything in Parquet format with time-based partitioning.

Using this Parquet dataset, I implemented queries to:
- Calculate average daily delays per station over the entire collection period
- Find the average number of departures during peak hours (7-9 AM and 5-7 PM)

## Technologies Used

- PostgreSQL for relational data warehousing
- PySpark for distributed data processing
- Python for ETL pipeline development
- SQL for analytical queries
- Parquet for optimized data storage

## Dataset Details

- Time period: September 2 to October 15, 2025 (6 weeks)
- Number of stations: 133 across Berlin
- Timetable files: Collected hourly at HH:01
- Disruption files: Collected every 15 minutes
- Data sources: Deutsche Bahn API marketplace

## How to Run

### PostgreSQL Pipeline

Navigate to the postgres_etl folder:

```bash
cd postgres_etl/

pip install -r requirements.txt

psql -U postgres -f schema.sql

python etl_pipeline.py --data-path /path/to/data

psql -U postgres -d transport_db -f queries/query_2.1.sql
```

### PySpark Pipeline

Navigate to the spark_etl folder:

```bash
cd spark_etl/

pip install pyspark

python spark_etl.py

python average_delay_query.py

python peak_hours_query.py
```

## Example Queries

Finding the nearest station to specific coordinates:

```sql
SELECT station_name, 
       calculate_distance(52.5200, 13.4050, latitude, longitude) as distance
FROM dim_stations
ORDER BY distance LIMIT 1;
```

Calculating average delay for a specific station:

```sql
SELECT s.station_name, AVG(f.delay_minutes) as avg_delay
FROM fact_train_movements f
JOIN dim_stations s ON f.station_id = s.station_id
WHERE s.station_name = 'Berlin Hauptbahnhof'
GROUP BY s.station_name;
```

## Project Structure

The postgres_etl folder contains the star schema definition, Python ETL code, and SQL queries for analysis.

The spark_etl folder contains the PySpark script that generates the Parquet dataset and query scripts for analyzing delays and peak hour traffic.

## Key Insights

Through this analysis, I discovered patterns in Berlin's public transport system:
- Peak hours (7-9 AM and 5-7 PM) show significantly higher traffic
- Certain stations experience consistently higher delays
- The Parquet partitioning strategy reduced query execution time

## Technical Challenges

Some interesting challenges I solved:
- Parsing timestamps from folder names and XML attributes correctly
- Handling missing or inconsistent data in the XML files
- Designing a schema that efficiently handles both planned and actual train movements
- Processing thousands of XML files with PySpark

## Academic Context

This project was completed as part of the Data Integration and Analytics course, demonstrating skills in data warehouse design, ETL development, and large-scale data processing with PySpark.

## Author

Zahra Abdi

## Acknowledgments

Data provided by Deutsche Bahn (DB) API marketplace for educational purposes.
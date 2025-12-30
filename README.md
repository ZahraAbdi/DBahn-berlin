# DBahn Train Movement ETL Pipeline

Data ingestion pipeline for Deutsche Bahn train movement data.

## Project Structure
source venv/bin/activate

docker-compose build --no-cache

docker-compose up -d

docker exec -w /app/src dbahn-etl python main.py
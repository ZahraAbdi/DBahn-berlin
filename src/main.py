from config.db_config import connect_db
from etl.load_stations import load_stations
from etl.load_time import scan_folders_and_load_times
from etl.load_trains import load_all_trains
from etl.load_fact_train_movement import load_fact_train

def main():
    print("Starting ETL pipeline...")
    conn = connect_db()
    if not conn:
        print("Failed to connect to database")
        return

    print("\n=== 1. Load Stations ===")
    station_file = "../data/raw/DBahn-berlin/station_data.json"
    load_stations(conn, station_file)

    print("\n=== 2. Load Time Dimension ===")
    scan_folders_and_load_times(conn, "../data/raw/DBahn-berlin")

    # print("\n=== 3. Load Trains ===")
    # load_all_trains(conn, "../data/raw/DBahn-berlin")

    # print("\n=== 4. Load Fact Table ===")
    # load_fact_train(conn, "../data/raw/DBahn-berlin")

    conn.close()
    print("ETL pipeline completed!")

if __name__ == "__main__":
    main()
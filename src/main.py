from config.db_config import connect_db
from etl.load_stations import load_stations
from etl.load_time import scan_and_insert_time_dimension
from etl.load_trains import load_all_trains
from etl.load_fact_train import load_fact_train
import sys

import tarfile
import os


def unzip_all(parent_dir):
    for name in os.listdir(parent_dir):
        if not name.endswith(".tar.gz"):
            continue

        tar_path = os.path.join(parent_dir, name)
        target_dir = os.path.join(parent_dir, name[:-7])

        if os.path.exists(target_dir):
            print(f"Already unzipped: {name}")
            continue

        print(f"Unzipping: {name}")
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(target_dir)

def prepare_data(base_path):
    unzip_all(os.path.join(base_path, "timetables"))
    unzip_all(os.path.join(base_path, "timetable_changes"))

def main():
    print("Starting ETL pipeline...")
    conn = connect_db()
    if not conn:
        print("Failed to connect to database")
        return
    
    base_path = "/app/data/raw/DBahn-berlin"

    print("Unzipping raw data")

    prepare_data(base_path)

    
    print("\n=== 1. Load Stations ")
    # #station_file = "base_path/station_data.json"
    # #os.path.join(base_path, "station_data.json")
    load_stations(conn, os.path.join(base_path, "station_data.json"))

    print("\n=== 2. Load Time Dimension ===")
    # # # scan_folders_and_load_times(conn, base_path)
    scan_and_insert_time_dimension(conn, base_path)

    print("\n=== 3. Load Trains ===")
    load_all_trains(conn, base_path)

    print("\n=== 4. Load Fact Table ===")
    # #load_fact_train(conn, "../data/raw/DBahn-berlin")
    
    # run_etl(conn, base_path)
    #run_load_changes(conn, os.path.join(base_path, "timetable_changes"))
    load_fact_train(conn, base_path)

    # load_fact_train(conn, base_path)

    conn.close()
    print("ETL pipeline completed!")

if __name__ == "__main__":
    main()
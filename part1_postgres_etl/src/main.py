from config.db_config import connect_db
from etl.load_stations import load_stations
from etl.load_time import load_time
from etl.load_trains import load_trains
from etl.load_fact_train import load_fact_train
import sys

import tarfile
import os


def unzip_folders(parent_dir):
    for name in os.listdir(parent_dir):
        if not name.endswith(". tar.gz"):
            continue

        tar_path = os.path.join(parent_dir, name)
        target_dir = os.path.join(parent_dir, name[:-7])

        if os.path.exists(target_dir):
            print(f"unzipped before: {name}")
            continue

        print(f"unzipping floder:  {name}")
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(target_dir)

    

def main():
    print("starting ETL pipeline...")
    conn = connect_db()
 
    
    path = "/app/data/raw/DBahn-berlin"
    print("Unzipping raw data")
    unzip_folders(os.path.join(path, "timetables"))
    unzip_folders(os.path.join(path, "timetable_changes"))
      
    
    print("Stations loading:")
    # # #station_file = "base_path/station_data.json"
    # # #os.path.join(base_path, "station_data.json")
    load_stations(conn, os.path.join(path, "station_data.json"))

    print("Time Dimension loading:")
    # # # # scan_folders_and_load_times(conn, base_path)
    load_time(conn, path)

    print("\nTrains loading:")
    load_trains(conn, path)

    print("\n Fact Table loading: ")
    load_fact_train(conn, path)
    # #load_fact_train(conn, "../data/raw/DBahn-berlin")
    
    # run_etl(conn, base_path)
    #run_load_changes(conn, os.path.join(base_path, "timetable_changes"))
    

    # load_fact_train(conn, base_path)

    conn.close()
    print("ETL pipelined successfully:")

if __name__ == "__main__":
    main()
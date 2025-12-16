from config.db_config import connect_db
from etl.load_stations import load_stations
from etl.load_movements import load_all_xml_files
from etl.load_time import scan_folders_and_load_times
# from load_movements import load_all_xml_files
import sys
import os
sys.path. insert(0, os.path. dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    print("Starting ETL pipeline...")
    
    # connect to database
    conn = connect_db()
    if not conn:
        print("Failed to connect to database")
        return
    
    # step 1: load stations from JSON file
    print("\n=== Loading Stations ===")
    station_file = "../data/raw/DBahn-berlin/station_data.json"
    load_stations(conn, station_file)

    print("\n--- Step 2: Load Time Dimension ---")
    scan_folders_and_load_times(conn, '../data/raw/DBahn-berlin')


    # load trains from XML files
    print("\n=== Loading Trains from XML ===")
    load_all_xml_files(conn, '../data/raw/DBahn-berlin')
    
    # # step 2: load train movements from XML files
    # print("\n=== Loading Train Movements ===")
    # data_folder = "DBahn-berlin"
    # load_all_xml_files(conn, data_folder)
    
    # close connection
    conn.close()
    print("\nETL pipeline completed!")


if __name__ == '__main__':
    main()
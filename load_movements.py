import xml.etree.ElementTree as ET
import os
import gzip
from datetime import datetime
from load_trains import get_train_key

def parse_xml_file(conn, xml_file):
    # parse XML file and get train info
    print(f"Processing:  {os.path.basename(xml_file)}")
    
    try:
        # check if file is compressed
        if xml_file.endswith('.gz'):
            with gzip.open(xml_file, 'rt', encoding='utf-8') as f:
                tree = ET.parse(f)
        else:
            tree = ET. parse(xml_file)
        
        root = tree.getroot()
        cursor = conn.cursor()
        loaded = 0
        
        # loop through all movements
        for movement in root.findall('.//s'):
            train_elem = movement.find('tl')
            if train_elem is None:
                continue
            
            # get train attributes
            train_category = train_elem.get('c')
            train_number = train_elem. get('n')
            operator_code = train_elem.get('o')
            train_type = train_elem.get('t')
            train_class = train_elem.get('f')
            
            if not train_category or not train_number:
                continue
            
            # build unique train id
            if operator_code:
                train_id = f"{train_category}_{train_number}_{operator_code}"
            else:
                train_id = f"{train_category}_{train_number}_UNK"
            
            # prepare train data
            train_data = {
                'train_id':  train_id,
                'train_number': train_number,
                'operator_code': operator_code,
                'operator_name': None,
                'train_category': train_category,
                'train_category_desc': None,
                'train_class':  int(train_class) if train_class and train_class.isdigit() else None,
                'train_type': train_type,
                'train_type_desc': None
            }
            
            # insert or get existing train
            train_key = get_train_key(conn, train_data)
            loaded += 1
        
        conn.commit()
        cursor.close()
        print(f"  Loaded {loaded} trains from this file")
        
    except Exception as e:
        print(f"  Error processing file: {e}")
        conn.rollback()


def load_all_xml_files(conn, data_folder):
    # load all XML files from timetables folder
    timetables_folder = os.path.join(data_folder, 'timetables')
    
    if not os.path.exists(timetables_folder):
        print(f"Error: folder {timetables_folder} not found!")
        return
    
    # get all subfolders (each represents a time period like 250909_250916)
    period_folders = [f for f in os.listdir(timetables_folder) 
                      if os.path.isdir(os.path.join(timetables_folder, f))]
    
    print(f"Found {len(period_folders)} time period folders")
    
    total_files = 0
    for period_folder in sorted(period_folders):
        period_path = os.path.join(timetables_folder, period_folder)
        
        # get all date_hour subfolders inside (like 250909_00, 250909_01, etc.)
        date_hour_folders = [f for f in os. listdir(period_path) 
                             if os.path.isdir(os.path.join(period_path, f))]
        
        print(f"\nProcessing {period_folder}:  {len(date_hour_folders)} date-hour folders")
        
        for date_hour_folder in sorted(date_hour_folders):
            date_hour_path = os.path.join(period_path, date_hour_folder)
            
            # get all xml files in this date_hour folder
            xml_files = [f for f in os.listdir(date_hour_path) 
                         if f.endswith('.xml') or f.endswith('.xml. gz')]
            
            print(f"  {date_hour_folder}: {len(xml_files)} files")
            
            for xml_file in xml_files:
                file_path = os.path.join(date_hour_path, xml_file)
                parse_xml_file(conn, file_path)
                total_files += 1
    
    print(f"\nDone!  Processed {total_files} files total")
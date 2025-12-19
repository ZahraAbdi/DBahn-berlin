import os
import xml.etree.ElementTree as ET
import json
from etl.load_trains import get_train_key
from etl.load_time import get_or_create_time_key
import unicodedata
import re

from datetime import datetime


def parse_db_time(ts):
    if ts is None or not ts.isdigit():
        return None
    ts = str(ts)
    # 10 digits: YYMMDDHHMM
    if len(ts) == 10:
        year = int(ts[0:2])
        year += 2000 if year < 60 else 1900  # or use fixed 2000s if your data is only recent!
        month = int(ts[2:4])
        day = int(ts[4:6])
        hour = int(ts[6:8])
        minute = int(ts[8:10])
        try:
            return datetime(year, month, day, hour, minute)
        except Exception:
            return None
    # 12 digits: YYYYMMDDHHMM
    elif len(ts) == 12:
        year = int(ts[0:4])
        month = int(ts[4:6])
        day = int(ts[6:8])
        hour = int(ts[8:10])
        minute = int(ts[10:12])
        try:
            return datetime(year, month, day, hour, minute)
        except Exception:
            return None
    return None

def normalize_station_name(name):
    name = name.strip().lower()
    # Manual German character replacements (before anything else)
    name = name.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    name = name.replace("ß", "ss")
    # Hyphens/underscores to space
    name = name.replace("-", " ").replace("_", " ")
    # Remove unicode accents (just in case)
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    # Collapse multiple spaces
    name = " ".join(name.split())
    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)
    return name

def build_station_key_map(conn):
    cur = conn.cursor()
    cur.execute("SELECT station_key, station_name FROM dim_station")
    name_map = {}
    for row in cur.fetchall():
        normalized = normalize_station_name(row[1])
        name_map[normalized] = row[0]
    cur.close()
    return name_map

def extract_station_name_from_filename(filename):
    # Remove _timetable.xml or .xml from the filename
    name = os.path.basename(filename)
    if name.endswith('_timetable.xml'):
        name = name[:-len('_timetable.xml')]
    elif name.endswith('.xml'):
        name = name[:-len('.xml')]
    return name.strip()

def get_station_key_any(name_map, xml_station, file_station):
    norm_xml = normalize_station_name(xml_station)
    norm_file = normalize_station_name(file_station)
    # Try both possibilities
    print(f"norm_xml :{norm_xml}")
    print(f"norm_file :{norm_file}")
    return name_map.get(norm_xml) or name_map.get(norm_file)



def get_station_name_from_root(root):
    station_name = root.get('station')
    if station_name:
        return station_name
    else:
        print("No 'station' attribute found in timetable root XML!")
        return None


def parse_movement_event(movement_elem):
    # Extract raw times
    planned_time_raw = movement_elem.get('pt')
    print(f"planned_time_raw:{planned_time_raw}")
    actual_time_raw = movement_elem.get('ct')
    print(f"actual_time_raw:{actual_time_raw}")
    cancellation_time_raw = movement_elem.get('clt')
    # Parse times (or None)
    planned_time = parse_db_time(planned_time_raw)
    actual_time = parse_db_time(actual_time_raw)
    cancellation_time = parse_db_time(cancellation_time_raw)
    # Format (for database)
    planned_time_str = planned_time.strftime('%Y-%m-%d %H:%M:%S') if planned_time else None
    actual_time_str = actual_time.strftime('%Y-%m-%d %H:%M:%S') if actual_time else None
    cancellation_time_str = cancellation_time.strftime('%Y-%m-%d %H:%M:%S') if cancellation_time else None
    
    event_type = movement_elem.tag.upper()
    event_status = movement_elem.get('cs')
    planned_platform = movement_elem.get('pp')
    actual_platform = movement_elem.get('cp')
    planned_path = movement_elem.get('ppth')
    actual_path = movement_elem.get('cpth')
    planned_destination = movement_elem.get('pde')
    transition_reference = movement_elem.get('tra')
    line_number = movement_elem.get('l')
    distance_change = movement_elem.get('dc')
    is_hidden = (movement_elem.get('hi', '0') == '1')
    is_cancelled = (event_status == 'c')
    has_disruption = movement_elem.find('m') is not None

    delay_minutes = None
    if planned_time and actual_time:
        delay_minutes = int((actual_time - planned_time).total_seconds() // 60)

    blank_to_none = lambda x: x if x not in ('', 'None', None) else None

    planned_platform = blank_to_none(planned_platform)
    actual_platform = blank_to_none(actual_platform)
    planned_path = blank_to_none(planned_path)
    actual_path = blank_to_none(actual_path)
    planned_destination = blank_to_none(planned_destination)
    transition_reference = blank_to_none(transition_reference)
    cancellation_time_str = blank_to_none(cancellation_time_str)
    line_number = blank_to_none(line_number)
    distance_change = int(distance_change) if distance_change not in (None, '', 'None') else None

    return (
        event_type,                # string
        event_status,              # string
        planned_time_str,          # timestamp string (or None)
        actual_time_str,           # timestamp string (or None)
        planned_platform,          # string
        actual_platform,           # string
        delay_minutes,             # int (or None)
        is_cancelled,              # bool
        cancellation_time_str,     # timestamp string (or None)
        is_hidden,                 # bool
        has_disruption,            # bool
        distance_change,           # int (or None)
        line_number,               # string
        planned_path,              # string
        actual_path,               # string
        planned_destination,       # string
        transition_reference       # string
    )

def load_fact_train(conn, data_folder):
    timetables_dir = os.path.join(data_folder, 'timetables')
    station_key_map = build_station_key_map(conn)
    for period_folder in os.listdir(timetables_dir):
        period_path = os.path.join(timetables_dir, period_folder)
        if not os.path.isdir(period_path):
            continue
        for hour_folder in os.listdir(period_path):
            hour_path = os.path.join(period_path, hour_folder)
            if not os.path.isdir(hour_path):
                continue
            for filename in os.listdir(hour_path):
                
                if not filename.endswith('.xml'):
                    continue
                file_path = os.path.join(hour_path, filename)
                print(f"Processing {file_path}...")
                try:
                    # --- FIX: Get station_key from filename directly ---
                    
                    tree = ET.parse(file_path)
                    root = tree.getroot()
                    xml_station_name = get_station_name_from_root(root)
                    file_station_name = extract_station_name_from_filename(filename)
                    station_key = get_station_key_any(station_key_map, xml_station_name, file_station_name)
                    if station_key is None:
                        print(f"Station not found: XML='{xml_station_name}', File='{file_station_name}'")
                        continue
                    #station_key = get_station_key_from_name(conn, station_name)
                    # normalized_station_name = normalize_station_name(station_name)
                    # station_key = station_key_map.get(normalized_station_name)
                    # if station_key is None:
                    #     print(f"Station '{station_name}' (normalized: '{normalized_station_name}') not found in dim_station!")
                    #     continue
                    # if station_key is None:
                    #     print(f" Station '{station_name}' not found in dim_station (from file {filename}).")
                    #     continue

                    for movement in root.findall('.//s'):

                        print("dataaaaaaaa:")
                        
                        train_elem = movement.find('tl')
                        if train_elem is None:
                            continue
                        train_category = train_elem.get('c')
                        train_number = train_elem.get('n')
                        operator_code = train_elem.get('o')
                        train_id = f"{train_category}_{train_number}_{operator_code}" if operator_code else f"{train_category}_{train_number}_UNK"
                        train_data = {
                            'train_id': train_id,
                            'train_number': train_number,
                            'operator_code': operator_code,
                            'operator_name': None,
                            'train_category': train_category,
                            'train_category_desc': None,
                            'train_class': train_elem.get('f'),
                            'train_type': train_elem.get('t'),
                            'train_type_desc': None
                        }
                        train_key = get_train_key(conn, train_data)
                        for event_elem in movement:  # movement is <s> - so event_elem is <ar>, <dp>, etc
                            event_type = event_elem.tag.upper()      # Correct! 'AR', 'DP'
                            planned_time_str = event_elem.get('pt')  # Correct! Like '2509090907'
                            planned_time = parse_db_time(planned_time_str)  # Correctly parses string, returns datetime
                            print(f"planned_time_str: {planned_time_str}")
                            if event_elem.tag == "tl":
                                print("event_elem:{event_elem}")
                                continue  # skip
                            print(f"station_key:{station_key}")
                            fact_values = parse_movement_event(event_elem)
                            planned_time = fact_values[0]
                            time_key = get_or_create_time_key(conn, planned_time) if planned_time else None
                            print(f"station_key:{station_key}")
                            print(f"train_key:{train_key}")
                            print(f"time_key:{time_key}")
                            if None in (station_key, train_key, time_key):
                                continue

                            print("CORRECT!")
                            cursor = conn.cursor()
                            cursor.execute("""
                            INSERT INTO fact_train_movement (
                                station_key, train_key, time_key,
                                event_type, event_status,
                                planned_time, actual_time,
                                planned_platform, actual_platform,
                                delay_minutes, is_cancelled, cancellation_time, is_hidden, has_disruption,
                                distance_change, line_number, planned_path, actual_path, planned_destination, transition_reference
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (station_key, train_key, time_key) + fact_values)
                            conn.commit()
                            cursor.close()
                except Exception as e:
                    print(f"Error parsing {file_path}: {e}")
                    conn.rollback()
# def load_fact_train(conn, data_folder):
#     station_json_path = os.path.join(data_folder, 'station_data.json')
#     # station_name_to_eva = build_station_name_to_eva(station_json_path)

#     timetables_dir = os.path.join(data_folder, 'timetables')
#     for period_folder in os.listdir(timetables_dir):
#         period_path = os.path.join(timetables_dir, period_folder)
#         if not os.path.isdir(period_path):
#             continue
#         for hour_folder in os.listdir(period_path):
#             hour_path = os.path.join(period_path, hour_folder)
#             if not os.path.isdir(hour_path):
#                 continue
#             for filename in os.listdir(hour_path):
#                 print(f"filename :{filename}")
#                 if not filename.endswith('.xml'):
#                     continue
#                 file_path = os.path.join(hour_path, filename)
#                 print(f"Processing {file_path}...")
#                 try:
#                     tree = ET.parse(file_path)
#                     root = tree.getroot()
#                     eva_number = get_eva_number(file_path, root, station_name_to_eva)
#                     if eva_number is None:
#                         continue
#                     station_key = get_station_key(conn, eva_number)
#                     if station_key is None:
#                         print(f" Station EVA {eva_number} not found in dim_station.")
#                         continue
                 
#                     for movement in root.findall('.//s'):
#                         # Get station_key from earlier logic (it is the same for all events in this <s> group)
#                         # Get train_elem from movement
#                         train_elem = movement.find('tl')
#                         if train_elem is None:
#                             continue
                        
#                         # Build train_id and get train_key (same for all events under this <s>)
#                         train_category = train_elem.get('c')
#                         train_number = train_elem.get('n')
#                         operator_code = train_elem.get('o')
#                         # ... normalization ...
#                         train_id = f"{train_category}_{train_number}_{operator_code}" if operator_code else f"{train_category}_{train_number}_UNK"
#                         train_data = {
#                                 'train_id': train_id,
#                                 'train_number': train_number,
#                                 'operator_code': operator_code,
#                                 'operator_name': None,  # you may add mapping logic!
#                                 'train_category': train_category,
#                                 'train_category_desc': None,
#                                 'train_class': train_elem.get('f'),
#                                 'train_type': train_elem.get('t'),
#                                 'train_type_desc': None
#                         }
#                         train_key = get_train_key(conn, train_data)

#                         # Now, iterate over each event (ar, dp, etc) inside <s>:
#                         for event_elem in movement:
#                             if event_elem.tag == "tl":
#                                 continue  # skip
#                             fact_values = parse_movement_event(event_elem)
#                             planned_time = fact_values[0]
#                             print(f"planned_time: {planned_time}")
#                             time_key = get_or_create_time_key(conn, planned_time) if planned_time else None
#                             if None in (station_key, train_key, time_key):
#                                 continue
#                             # Insert row:
#                             cursor = conn.cursor()
#                             cursor.execute("""
#                             INSERT INTO fact_train_movement (
#                                 station_key, train_key, time_key,
#                                 event_type, event_status,
#                                 planned_time, actual_time,
#                                 planned_platform, actual_platform,
#                                 delay_minutes, is_cancelled, cancellation_time, is_hidden, has_disruption,
#                                 distance_change, line_number, planned_path, actual_path, planned_destination, transition_reference
#                             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#                             """, (station_key, train_key, time_key) + fact_values)
#                             conn.commit()
#                             cursor.close()
#                 except Exception as e:
#                     print(f"Error parsing {file_path}: {e}")
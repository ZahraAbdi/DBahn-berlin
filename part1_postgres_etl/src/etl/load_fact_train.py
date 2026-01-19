import os
import xml.etree.ElementTree as ET
from datetime import datetime
import unicodedata
import re
from rapidfuzz import process, fuzz

# want to normlize the station name in the file name like berlin_s_dkreuz_timetable.xml or station name like <timetable station="Berlin Südkreuz">
def normalize_station_name(name):
    if not name:
        return ""
    
    # basic german character replacements
    name = name. replace("ß", "ss")
    name = name.replace("ä", "ae") 
    name = name.replace("ö", "oe")
    name = name.replace("ü", "ue")
    name = name.replace("Ä", "Ae")
    name = name.replace("Ö", "Oe") 
    name = name.replace("Ü", "Ue")
    
    # remove berlin from start
    if name.lower().startswith("berlin "):
        name = name[7:]
    
    # fix street abbreviations - need regex for this
    name = re.sub(r"(str\.|stra_e)", "strasse", name, flags=re.IGNORECASE)
    
    # remove stuff in brackets - regex is easier here
    name = re.sub(r"\s*\(.*\)", "", name)
    
    # replace spaces and dots with underscores
    name = name.replace(" ", "_")
    name = name.replace(".", "_")
    name = name.replace("-", "_")
    
    # cleanup multiple underscores
    while "__" in name:
        name = name.replace("__", "_")
    
    return name.lower().strip("_")

def get_train_from_db(conn, train_id):
    cursor = conn.cursor()
    
    cursor.execute(
        "select train_key from dim_train where train_id = %s",
        (train_id,)
    )

    result = cursor.fetchone()
    cursor.close()

    if result:
        return result[0]

## search  the primary key for a given train_id in the database.
def get_train_key(conn, train_id):
    
    train_key = get_train_from_db(conn, train_id)
    if train_key:
        return train_key

    cursor = conn.cursor()
    cursor.execute("""
        insert into dim_train (train_id)
        values (%s)
    """, (train_id,))

    conn.commit()
    cursor.close()

    train_key = get_train_from_db(conn, train_id)
    
    if not train_key:
        cursor.close()
        raise RuntimeError("Failed to fetch train_key after insert")

    return train_key


## pars time string into a python datetime
def parse_db_time(ts):

    if ts is None or not ts.isdigit():
        return None
    
    ts = str(ts)
    # for the case in 10 digits: YYMMDDHHMM
    if len(ts) == 10:
        year = int(ts[0:2])
        year += 2000 if year < 60 else 1900  
        month =int(ts[2:4])
        day = int(ts[4:6])
        hour = int(ts[6:8])
        minute = int(ts[8:10])
 
        return datetime(year, month, day, hour, minute)
        
    # for the case in 12 digits: YYYYMMDDHHMM
    elif len(ts) == 12:
        year = int(ts[0:4])
        month =int(ts[4:6])
        day = int(ts[6:8])
        hour = int(ts[8:10])
        minute =int(ts[10:12])
        
        return datetime(year, month, day, hour, minute)
    
    return None

## read all timetale_changes files from subdirectories of changes_week_dir matching the given hour_prefix
def load_changes_map_for_period(changes_week_dir, hour_prefix):
   
    changes_map = {}
    # get all matching subfolders
    for element in os.scandir(changes_week_dir):
        if element.is_dir() and element.name.startswith(hour_prefix):
            #print(f"Exploring changes in folder: {element.path}")
            # Now process each .xml in this folder
            for filename in os.listdir(element.path):

                if not filename.endswith('.xml'):
                    continue

                path = os.path.join(element.path, filename)
                #print(f"file_path : {path}")

                tree = ET.parse(path)
                root = tree.getroot()
                #print("test")
                for s in root.findall(".//s"):
                    sid = s.get("id")
                    if not sid:
                        continue
                    for ev in s:
                        if ev.tag not in ("ar", "dp"):
                            continue
                        station_has_disruption = 1 if s.findall("m") else 0
                        event_type = ev.tag.upper()
                        actual_time = parse_db_time(ev.get("ct"))
                        actual_platform = ev.get("cp") or ev.get("l")
                        actual_path = ev.get("cpth")
                        is_cancelled = ev.get("cs", "").strip().lower() == "c"
                        #print(f"is_cancelled: {is_cancelled}")
                        #print(f"clt: {clt}")
                        cancellation_time =  parse_db_time(ev.get("clt"))
                        #print(f"cancellation_time: {cancellation_time}")
                        has_disruption = ev.get("dis") == "1" if ev.get("dis") is not None else False
                        distance_change = None
                        changes_map[(sid, event_type)] = {
                            "actual_time": actual_time,
                            "actual_platform": actual_platform,
                            "actual_path": actual_path,
                            "is_cancelled": is_cancelled,
                            "cancellation_time": cancellation_time,
                            "has_disruption": has_disruption,
                            "distance_change": distance_change,
                        }
                        #print(f"actual time: {actual_time}")
    return changes_map


## convert dattime in string format into datetime objects
# def parse_db_time_from_str(time_str):

#     if not time_str:
#         return None
#     try:
#         # standard format
#         return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
#     except ValueError:
#         try:
#             return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
#         except ValueError:
#             try:
#                 # compact format like '2509221939' -> YYMMDDHHMM
#                 return datetime.strptime(time_str, "%y%m%d%H%M")
#             except ValueError:
#                 print(f"Warning: cannot parse time string '{time_str}'")
#                 return None


# here we compute the delay time between planned and actual datetime objects
def compute_delay_minutes(planned, actual):
    # Return difference in minutes
    if not planned or not actual:
        return None
    return int((actual - planned).total_seconds() // 60)

def insert_fact_train_movement_batch(conn, rows):

    cur = conn.cursor()
    ## insert batches at once - faster
    cur.executemany("""
        insert into fact_train_movement (
            station_key, train_key, time_key, event_type, event_status,
            planned_time, actual_time, planned_platform, actual_platform, delay_minutes,
            is_cancelled, cancellation_time, is_hidden, has_disruption,
            distance_change, line_number, planned_path, actual_path,
            planned_destination, sid)
                    
        values (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )""", rows)
        
    conn.commit()

    cur.close()


#### get the station name from station (normlize) to comapre with station info in timetable and timetablechanges
def build_station_key_map(conn):
    cur = conn.cursor()
    cur.execute("select station_key, station_name from dim_station")
    name_map = {}
    for station_key, station_name in cur.fetchall():
        normalized = normalize_station_name(station_name)
        name_map[normalized] = (station_key, station_name)
    return name_map      



def get_station_key(name_map, xml_station, file_station):
    
    # try exact matches first to get 
    for station in [xml_station, file_station]: 
        normalized = normalize_station_name(station)
        if normalized in name_map:
            return name_map[normalized]
    
    #if exact match fails, do fuzzy match 
    return try_fuzzy_match(name_map, xml_station, file_station)

## since sometimes it can not exactly match the name in the folder with that staion name in table, so we did fuzzy match
def try_fuzzy_match(name_map, xml_station, file_station):
    
    choices = list(name_map.keys())
    
    for station in [xml_station, file_station]:
        normalized = normalize_station_name(station)
        match, score, _ = process.extractOne(normalized, choices, scorer=fuzz.ratio)

        if score >= 85:
            return name_map[match]
    
    return None


## check if the time is not inserted before in time table , then insert the new one
def get_time_from_db(conn, dt):
    cursor = conn.cursor()
    
    cursor.execute(
        "select time_key from dim_time where timestamp = %s",
        (dt,)
    )

    result = cursor.fetchone()
    cursor.close()

    if result:
        return result[0]

## search the primary key for a given timestamp in the database.
def get_time_key(conn, dt):
    
    time_key = get_time_from_db(conn, dt)
    if time_key:
        return time_key

    cursor = conn.cursor()
    cursor.execute("""
        insert into dim_time (timestamp, date, year, month, day, hour, minute, weekday, is_weekend)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        dt, dt.date(), dt.year, dt.month, dt.day, dt.hour, dt.minute,
        dt.weekday(), dt.weekday() in (5, 6)
    ))

    conn.commit()
    cursor.close()

    time_key = get_time_from_db(conn, dt)
    
    if not time_key:
        print("Failed to fetch time_key after insert")

    return time_key


def process_timetable_files(conn, file_path, filename, station_key_maps, changes_map):

    tree = ET.parse(file_path)
    print(f"file_path :{file_path}")
    root = tree.getroot()
    ### get the station info from the station table 
    xml_station = root.get("station")
    filename_station = filename.replace("_timetable.xml", "").replace("_", " ")
    #print(f"filename_station : {filename_station}")
    ## get station key- so we have to lookup the name in filename_station(in timetable file) in station table
    result = get_station_key(station_key_maps, xml_station, filename_station)
    
    if result is None:
        print(f"WARNING: Could not find station for {xml_station} / {filename_station}")
        return  # skip or handle gracefully
    
    station_key, station_name = result
    #print(f"station_name : {station_name}")
        # #print(f"station_name:{station_name}")
        # if station_key is None:
        #     print(f"Could not find station '{xml_station} or {filename_station}' in dim_station for file {filename}.")
        #     return
    
    rows_to_insert = []
    for s in root.findall(".//s"):
        sid = s.get("id")
        tl = s.find("tl")

        if not sid or tl is None:
            continue
        train_id = f"{tl.get('c')}_{tl.get('n')}_{tl.get('o') or 'UNK'}"
        train_key = get_train_key(conn, train_id)

        if train_key is None:
            continue

        for ev in s:
            if ev.tag not in ("ar", "dp"):
                continue

            event_type = ev.tag.upper()
            planned_time = parse_db_time(ev.get("pt"))
            planned_platform = ev.get("pp")
            planned_path = ev.get("ppth")
            planned_destination = ev.get("pdest")
            #time_key = None  # fill in if you use time dimension
            time_key = get_time_key(conn, planned_time)
            change = changes_map.get((sid, event_type), {})
            actual_time = change.get("actual_time")
            actual_platform = change.get("actual_platform")
            actual_path = change.get("actual_path")
            delay_minutes = compute_delay_minutes(planned_time, actual_time)
            is_cancelled = change.get("is_cancelled", False)
            cancellation_time = change.get("cancellation_time")
            is_hidden = False
            has_disruption = change.get("has_disruption", False)
            distance_change = change.get("distance_change")
            line_number = ev.get("l")
            event_status = ev.get("status")  # Or as appropriate (some systems have this, others not)

            rows_to_insert.append((
                station_key,            # foreign key
                train_key,              # foreign key
                time_key,               # foreign key
                event_type,
                event_status,
                planned_time,
                actual_time,
                planned_platform,
                actual_platform,
                delay_minutes,
                is_cancelled,
                cancellation_time,
                is_hidden,
                has_disruption,
                distance_change,
                line_number,
                planned_path,
                actual_path,
                planned_destination,
                sid
            ))
    #print(f"rows_to_insert: {rows_to_insert}")   
    return rows_to_insert
      
def load_fact_train(conn, data_folder):
    timetables_dir = os.path.join(data_folder, "timetables")
    station_key_maps=build_station_key_map(conn)

    for period_folder in os.listdir(timetables_dir):
        period_path = os.path.join(timetables_dir, period_folder)

        ## for the same period, check the time table change to avoid check all of the timetablechange files
        ## this reduce computation. only check a period 
        changes_week_dir = os.path.join(data_folder, "timetable_changes", period_folder)

        for hour_folder in os.listdir(period_path):
            hour_path = os.path.join(period_path, hour_folder)

            print(f"Processing {hour_path} folder")
            hour_prefix = hour_folder[:6]
            ## extract the relavant data from timetablechange files
            changes_map = load_changes_map_for_period(changes_week_dir, hour_prefix)
            #print(f"loaded: {len(changes_map) } changes data")
            for filename in os.listdir(hour_path):
                #print(f"Processing {filename}")
                if not filename.endswith(".xml"):
                    continue

                file_path = os.path.join(hour_path, filename)
                
                rows = process_timetable_files(conn, file_path, filename, station_key_maps, changes_map)

                if rows:
                    insert_fact_train_movement_batch(conn, rows)
            
            
                conn.rollback()

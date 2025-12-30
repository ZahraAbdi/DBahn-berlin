import os
import xml.etree.ElementTree as ET
from datetime import datetime
import unicodedata
import re



#--- STATION NAME NORMALIZATION ---
# want to normlize the station name in the file name like berlin_s_dkreuz_timetable.xml or station name like <timetable station="Berlin Südkreuz">

def normalize_station_name(name):

    """ Standardizes a German station name for reliable matching.
    Handles umlauts, ß, Berlin prefix, street abbreviations,
    removes punctuation and casing discrepancies.

    Example: "Berlin-Hauptbahnhof (Süd)" -> "hauptbahnhof"
    """

    if not name:
        return ""
    # Replace ß with ss
    name = name.replace("ß", "ss")
    # Replace common German umlauts
    name = re.sub("ä", "ae", name, flags=re.IGNORECASE)
    name = re.sub("ö", "oe", name, flags=re.IGNORECASE)
    name = re.sub("ü", "ue", name, flags=re.IGNORECASE)
    # Remove Berlin prefix (optional, see below)
    name = re.sub(r"\bberlin\b\s*", "", name, flags=re.IGNORECASE)
    # Replace abbreviations/ASCII fallbacks for street ("str.", "stra_e", etc.)
    name = re.sub(r"(str\.|stra_e)", "strasse", name, flags=re.IGNORECASE)
    # Remove parenthesis and everything inside
    name = re.sub(r"\s*\(.*\)", "", name)
    # Strip non-ascii chars
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    # Remove spaces, dots, hyphens
    name = re.sub(r"[\s\.\-]+", "_", name)
    name = name.lower()
    name = re.sub(r"_+", "_", name) # collapse underscores
    return name.strip("_")


# want to xtract the station name from a timetable filename. 
#    Removes common suffix and extension like 'yorckstrasse_timetable.xml' -> 'yorckstrasse'
def extract_station_name_from_filename(filename):
    return filename[:-len('_timetable.xml')] if filename.endswith('_timetable.xml') else filename.rsplit('.', 1)[0]



def get_train_key(conn, train_id):
    """
    Looks up the primary key for a given train_id in the database.
    """
    cursor = conn.cursor()

    cursor.execute(
        "SELECT train_key FROM dim_train WHERE train_id = %s",
        (train_id,)
    )
    result = cursor.fetchone()
    if result:
        cursor.close()
        return result[0]

    cursor.execute("""
        INSERT INTO dim_train (train_id)
        VALUES (%s)
        RETURNING train_key
    """, (train_id,))

    train_key = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return train_key


def parse_db_time(ts):

    """
    pars time string into a Python datetime.
    """
    if ts is None or not ts.isdigit():
        return None
    ts = str(ts)
    # 10 digits: YYMMDDHHMM
    if len(ts) == 10:
        year = int(ts[0:2])
        year += 2000 if year < 60 else 1900  
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


def load_changes_map_for_period(changes_week_dir, hour_prefix):
    """
    reads all timetables_changea  files from subdirectories of changes_week_dir matching the given hour_prefix.
    Loads relevant change events into a memory map for fast lookup.
    prevent huge loopk up, focuses only on directory with given prefix
    """
    changes_map = {}
    # Get all matching subfolders
    for entry in os.scandir(changes_week_dir):
        if entry.is_dir() and entry.name.startswith(hour_prefix):
            #print(f"Exploring changes in folder: {entry.path}")
            # Now process each .xml in this folder
            for filename in os.listdir(entry.path):
                if not filename.endswith('.xml'):
                    continue
                file_path = os.path.join(entry.path, filename)
                #print(f"file_path : {file_path}")
                try:
                    tree = ET.parse(file_path)
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
                            actual_time = parse_db_time_from_str(ev.get("ct"))
                            actual_platform = ev.get("cp") or ev.get("l")
                            actual_path = ev.get("cpth")
                            is_cancelled = ev.get("cs") == "1" if ev.get("cncl") is not None else False
                            cancellation_time =  parse_db_time(ev.get("clt"))
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
                except Exception as e:
                    print(f"Error parsing changes file {file_path}: {e}")
    
    return changes_map



def parse_db_time_from_str(time_str):
    """
    parses timetable time strings into datetime objects.
    """
    if not time_str:
        return None
    try:
        # standard format
        return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            try:
                # compact format like '2509221939' -> YYMMDDHHMM
                return datetime.strptime(time_str, "%y%m%d%H%M")
            except ValueError:
                print(f"Warning: cannot parse time string '{time_str}'")
                return None


def compute_delay_minutes(planned, actual):
    """
    computes delay in minutes between planned and actual (changed) datetime objects.
    """
    # Return difference in minutes
    if not planned or not actual:
        return None
    return int((actual - planned).total_seconds() // 60)

def insert_fact_train_movement_batch(conn, rows):
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO fact_train_movement (
                station_key, train_key, time_key, event_type, event_status,
                planned_time, actual_time, planned_platform, actual_platform, delay_minutes,
                is_cancelled, cancellation_time, is_hidden, has_disruption,
                distance_change, line_number, planned_path, actual_path,
                planned_destination, sid)
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s
            )""", rows)
        conn.commit()


#### get the station name from station (normlize) to comapre with station info in timetable and timetablechanges
def build_station_key_map(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT station_key, station_name FROM dim_station")
        name_map = {}
        for station_key, station_name in cur.fetchall():
            normalized = normalize_station_name(station_name)
            name_map[normalized] = (station_key, station_name)
    return name_map      


## compare the name of station with normlized name in filename.xml and the station name inside the file 
## if dont catch exact match, we go to fuzzy match, since we can not explor all the rules 
def get_station_key(name_map, xml_station, file_station):
    norm_xml = normalize_station_name(xml_station)
    norm_file = normalize_station_name(file_station)
    #print(f"Loaded {len(name_map)} stations into map. Example: {list(name_map.items())[:5]}")
    # print(f"norm_file:{norm_file}")
    # print(f"norm_xml:{norm_xml}")
    # Quick exact match first (much faster than fuzzy!)
    if norm_xml in name_map:
        #print(f"name_map:{name_map[norm_xml]}")
        return name_map[norm_xml]
    if norm_file in name_map:
        #print(f"name_map:{name_map[norm_xml]}")
        return name_map[norm_file]
    
    # Fuzzy match fallback (RapidFuzz or fuzzywuzzy), threshold 85
    choices = list(name_map.keys())
    try:
        from rapidfuzz import process, fuzz
    except ImportError:
        print("RapidFuzz not installed!")
        return None

    match_xml, score_xml, _ = process.extractOne(norm_xml, choices, scorer=fuzz.ratio)
    if score_xml >= 85:
        #print(f"name_map:{name_map[norm_xml]}")
        return name_map[match_xml]
    
    match_file, score_file, _ = process.extractOne(norm_file, choices, scorer=fuzz.ratio)
    if score_file >= 85:
        #print(f"name_map:{name_map[norm_xml]}")
        return name_map[match_file]
    
    return None

def get_time_key(conn, dt):
    with conn.cursor() as cur:
        cur.execute("SELECT time_key FROM dim_time WHERE timestamp = %s", (dt,))
        result = cur.fetchone()
        if result:
            return result[0]
        # INSERT exactly as in scan_and_insert_time_dimension
        cur.execute("""
            INSERT INTO dim_time (timestamp, date, year, month, day, hour, minute, weekday, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING RETURNING time_key
        """, (
            dt, dt.date(), dt.year, dt.month, dt.day, dt.hour, dt.minute,
            dt.weekday(), dt.weekday() in (5, 6)
        ))
        # fetchone may be None if ON CONFLICT
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        # Otherwise, need to select again
        cur.execute("SELECT time_key FROM dim_time WHERE timestamp = %s", (dt,))
        return cur.fetchone()[0]

def load_fact_train(conn, data_folder):
    timetables_dir = os.path.join(data_folder, "timetables")
    station_key_maps=build_station_key_map(conn)
    for period_folder in os.listdir(timetables_dir):
        period_path = os.path.join(timetables_dir, period_folder)
        if not os.path.isdir(period_path):
            continue

        changes_week_dir = os.path.join(data_folder, "timetable_changes", period_folder)
        if not os.path.isdir(changes_week_dir):
            continue

        for hour_folder in os.listdir(period_path):
            hour_path = os.path.join(period_path, hour_folder)
            if not os.path.isdir(hour_path):
                continue
            print(f"Processing {hour_path} folder")
            hour_prefix = hour_folder[:6]
            changes_map = load_changes_map_for_period(changes_week_dir, hour_prefix)
            print(f"loaded: {len(changes_map) } changes data")
            for filename in os.listdir(hour_path):
                #print(f"Processing {filename}")
                if not filename.endswith(".xml"):
                    continue
                file_path = os.path.join(hour_path, filename)
                try:
                    tree = ET.parse(file_path)
                    root = tree.getroot()
                    

                    ### get the station info from the station table 
                
                    xml_station = root.get("station")
                    filename_station = filename.replace("_timetable.xml", "").replace("_", " ")
                    station_key, station_name = get_station_key(station_key_maps, xml_station, filename_station)
                    #print(f"station_name:{station_name}")
                    if station_key is None:
                        print(f"Could not find station '{xml_station} or {filename_station}' in dim_station for file {filename}, skipping.")
                        continue

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
                            planned_time = parse_db_time_from_str(ev.get("pt"))
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
                                station_key,            # FK
                                train_key,              # FK
                                time_key,               # Fill if using dim_time
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
                    insert_fact_train_movement_batch(conn, rows_to_insert)
                except Exception as e:
                    print(f"Error parsing {file_path}: {e}")
                    conn.rollback()
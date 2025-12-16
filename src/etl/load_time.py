from datetime import datetime
import os

def get_or_create_time_key(conn, dt):
    """
    Takes a datetime and either finds it in dim_time or adds it. 
    Returns the time_key we need for foreign keys. 
    """

    if type(dt) == str:
        dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
    
    # Pull out all the pieces we need
    full_date = dt.date()
    yr = dt.year
    mon = dt.month
    dy = dt.day
    hr = dt.hour
    mins = dt.minute
    day_name = dt.weekday()
    weekend = True if dt.weekday() in [5, 6] else False
    
    cur = conn.cursor()
    
    # See if we already have this timestamp
    cur.execute("SELECT time_key FROM dim_time WHERE timestamp = %s", (dt,))
    row = cur.fetchone()
    
    if row:
        key = row[0]
    else:
       
        cur. execute("""
            INSERT INTO dim_time 
            (timestamp, date, year, month, day, hour, minute, weekday, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING time_key
        """, (dt, full_date, yr, mon, dy, hr, mins, day_name, weekend))
        key = cur.fetchone()[0]
        conn.commit()
    
    cur.close()
    return key


def extract_datetime_from_folder(folder):
    """
    Folder names look like: 2509051100 or 2509051116
    That's:  YY MM DD HH MM
    """
    if len(folder) != 10:
        return None
    
    try:
        yy = int(folder[0:2])
        mm = int(folder[2:4])
        dd = int(folder[4:6])
        hh = int(folder[6:8])
        minute = int(folder[8:10])
        
        full_year = 2000 + yy
        dt = datetime(full_year, mm, dd, hh, minute)
        return dt
    except: 
        return None


def scan_folders_and_load_times(conn, base_path):
    """
    Walk through timetables and timetable_changes folders. 
    Pull timestamps from folder names and add them to dim_time.
    """
    print("Scanning folders for timestamps...")
    count = 0
    
    # Check timetables first
    tt_path = os.path.join(base_path, 'timetables')
    if os.path.exists(tt_path):
        for week_folder in os.listdir(tt_path):
            week_path = os.path.join(tt_path, week_folder)
            if not os.path.isdir(week_path):
                continue
            
            for hour_folder in os.listdir(week_path):
                full_path = os.path.join(week_path, hour_folder)
                if os.path.isdir(full_path):
                    dt = extract_datetime_from_folder(hour_folder)
                    if dt:
                        get_or_create_time_key(conn, dt)
                        count += 1
    
    # Now check timetable_changes
    changes_path = os.path.join(base_path, 'timetable_changes')
    if os.path.exists(changes_path):
        for week_folder in os.listdir(changes_path):
            week_path = os.path.join(changes_path, week_folder)
            if not os.path. isdir(week_path):
                continue
            
            for time_folder in os.listdir(week_path):
                full_path = os.path.join(week_path, time_folder)
                if os.path.isdir(full_path):
                    dt = extract_datetime_from_folder(time_folder)
                    if dt:
                        get_or_create_time_key(conn, dt)
                        count += 1
    
    conn.commit()
    print(f"Added {count} time entries to dim_time")


def parse_time_from_xml(date_code, time_code):
    """
    XML gives us date like '250905' and time like '14:30'
    Turn that into a proper datetime
    """
    try:
        yy = int(date_code[0:2])
        mm = int(date_code[2:4])
        dd = int(date_code[4:6])
        
        parts = time_code.split(':')
        hh = int(parts[0])
        mins = int(parts[1])
        
        full_year = 2000 + yy
        return datetime(full_year, mm, dd, hh, mins)
    except:
        return None
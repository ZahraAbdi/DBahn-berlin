

from datetime import datetime
import os

# def get_or_create_time_key(conn, dt):
#     """
#     Takes a datetime and either finds it in dim_time or adds it. 
#     Returns the time_key we need for foreign keys. 
#     """

#     if type(dt) == str:
#         dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
    
#     # Pull out all the pieces we need
#     full_date = dt.date()
#     yr = dt.year
#     mon = dt.month
#     dy = dt.day
#     hr = dt.hour
#     mins = dt.minute
#     day_name = dt.weekday()
#     weekend = True if dt.weekday() in [5, 6] else False
    
#     cur = conn.cursor()
    
#     # See if we already have this timestamp
#     cur.execute("SELECT time_key FROM dim_time WHERE timestamp = %s", (dt,))
#     row = cur.fetchone()
    
#     if row:
#         key = row[0]
#     else:
       
#         cur. execute("""
#             INSERT INTO dim_time 
#             (timestamp, date, year, month, day, hour, minute, weekday, is_weekend)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#             RETURNING time_key
#         """, (dt, full_date, yr, mon, dy, hr, mins, day_name, weekend))
#         key = cur.fetchone()[0]
#         conn.commit()
    
#     cur.close()
#     return key


from datetime import datetime
import os

def parse_timestamp_from_folder(folder):
    if not (folder.isdigit() and len(folder) == 10):
        return None
    try:
        yy = int(folder[0:2])
        mm = int(folder[2:4])
        dd = int(folder[4:6])
        hh = int(folder[6:8])
        mi = int(folder[8:10])
        return datetime(2000+yy, mm, dd, hh, mi)
    except Exception:
        return None

def scan_and_insert_time_dimension(conn, base_path):
    time_stamps = set()

    # Scan timetables/
    tt_base = os.path.join(base_path, 'timetables')
    if os.path.exists(tt_base):
        for week in os.listdir(tt_base):
            week_path = os.path.join(tt_base, week)
            if not os.path.isdir(week_path):
                continue
            for time_folder in os.listdir(week_path):
                fpath = os.path.join(week_path, time_folder)
                if not os.path.isdir(fpath):
                    continue
                dt = parse_timestamp_from_folder(time_folder)
                if dt:
                    time_stamps.add(dt)

    # Scan timetable_changes/
    tc_base = os.path.join(base_path, 'timetable_changes')
    if os.path.exists(tc_base):
        for week in os.listdir(tc_base):
            week_path = os.path.join(tc_base, week)
            if not os.path.isdir(week_path):
                continue
            for time_folder in os.listdir(week_path):
                fpath = os.path.join(week_path, time_folder)
                if not os.path.isdir(fpath):
                    continue
                dt = parse_timestamp_from_folder(time_folder)
                if dt:
                    time_stamps.add(dt)

    print(f"Discovered {len(time_stamps)} unique timestamps.")

    with conn.cursor() as cur:
        for dt in sorted(time_stamps):
            cur.execute("""
                INSERT INTO dim_time (timestamp, date, year, month, day, hour, minute, weekday, is_weekend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp) DO NOTHING
            """, (
                dt, dt.date(), dt.year, dt.month, dt.day, dt.hour, dt.minute,
                dt.weekday(), dt.weekday() in (5, 6)
            ))
        conn.commit()
    print("Inserted all timestamps.")
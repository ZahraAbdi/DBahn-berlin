
from datetime import datetime
import os

def convert_datetime(folder):
    if not (folder.isdigit() and len(folder) == 10):
        return None
    
    year = int(folder[0:2]) + 2000
    month = int(folder[2:4])
    day = int(folder[4:6])
    hour = int(folder[6:8])
    minute = int(folder[8:10])
    parsed_datatime = None

    if not( 1<=month<=12 and 0<=hour<24 and 0<=minute<60):
       return None
    
    parsed_datatime = datetime(year, month, day, hour, minute)
    return parsed_datatime


def check_folder_time(base_path, folder_path):
    time_dict = {}
    base_timetables = os.path.join(base_path, folder_path) 
    
    if os.path.exists(base_timetables):
        for week_foler in os.listdir(base_timetables):
            week_path = os.path.join(base_timetables, week_foler)

            if os.path.isdir(week_path):
                for time_folder in os.listdir(week_path):
                    file_path =  os.path.join(week_path, time_folder)

                    if  os.path.isdir(file_path):
                        date_time = convert_datetime(time_folder)
                        if date_time:
                            time_dict[date_time] = True
    
    return time_dict


def load_time(conn, base_path):
    ## first we scan timetable and teimatables folders to get the dattime from there 
    time_data = check_folder_time(base_path, 'timetables' )
    time_data.update(check_folder_time(base_path, 'timetable_changes' ))

    ####### insert all readed timestamps form timetables and timetable_changes
    print(f"Loaded {len(time_data)} timestamps.")
    cur = conn.cursor()

    query =  "insert into dim_time (timestamp, date, year, month, day, hour, minute, weekday, is_weekend) "
    query += "values (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
    ## timestamp has to be unique in time dim, so ignore if it duplicate
    query += "on conflict (timestamp) do nothing"  
    
    for dt in time_data:
           cur.execute(
               query, (
                   dt, 
                   dt.date(), 
                   dt.year,
                   dt.month, 
                   dt.day, 
                   dt.hour, 
                   dt.minute,
                   dt.weekday(),
                   dt.weekday() >= 5)
                   )
           
    conn.commit()
    cur.close()
    print("all timestamps is inserted.")
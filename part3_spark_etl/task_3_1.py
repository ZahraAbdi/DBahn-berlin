import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import xml.etree.ElementTree as ET
from datetime import datetime
import tarfile
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor

def setup_spark():
    spark = SparkSession.builder \
    .appName("train data") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

    return spark


def convert_time(timestr):
    if not timestr or len(timestr) != 10:
        return None
    year = int(timestr[0:2])
    
    month  = int(timestr[2:4])
    day = int(timestr[4:6])
    hour = int(timestr[6:8])
    minutes = int(timestr[8:10])
    year = year +2000

    return datetime(year, month, day, hour, minutes)


# def unzip_folders(parent_dir):
#     print(f"parent_dir: {parent_dir}")
#     for name in os.listdir(parent_dir):
#         if not name.endswith(".tar.gz"):
#             continue

#         tar_path = os.path.join(parent_dir, name)
#         target_dir = os.path.join(parent_dir, name[:-7])

#         if os.path.exists(target_dir):
#             print(f"unzipped before: {name}")
#             continue

#         print(f"unzipping floder:  {name}")
#         with tarfile.open(tar_path, "r:gz") as tar:
#             tar.extractall(target_dir)

def unzip_file(tar_path, target_dir):
    if os.path.exists(target_dir):
        print(f"Already unzipped: {os.path.basename(tar_path)}")
        return
    print(f"Unzipping: {os.path.basename(tar_path)}")
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(target_dir)

def unzip_folders_parallel(parent_dir, max_workers=4):
    tar_files = [f for f in os.listdir(parent_dir) if f.endswith(".tar.gz")]
    tasks = [(os.path.join(parent_dir, f), os.path.join(parent_dir, f[:-7])) for f in tar_files]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda p: unzip_file(*p), tasks)


def extract_timetable(path):

    raws = []
    tree = ET.parse(path)
    root = tree.getroot()
    station_name = root.get('station')

    for s in root.findall('s'):
        s_id = s.get("id")
        train_line = s.find('tl')

        if train_line is not None:
            category = train_line.get('c')
            number = train_line.get('n')
            owner = train_line.get('o')

        else:
            category = ''
            number = ''
            owner = ''
        
        ## for avoid the reapting I compact two type of event codes in one loop
        for event_tag, event_type in (('ar', 'arrival'), ('dp', 'departure')):
            element = s.find(event_tag)

            if element is not None:
                raws.append({
                'sid' : s_id, 
                'station' : station_name,
                'event_type' : event_type, 
                'category' : category,
                'number' : number,
                'owner' : owner,
                ## this info espical for the event
                'line' : element.get('l'),
                'planned_time' : convert_time(element.get('pt')),
                'planned_platform' : element.get('pp'),
                'planned_path': element.get('ppth')
            })
               
    return raws



def extract_changes(path):
    raws = []
    tree = ET.parse(path)
    root = tree.getroot()
    ## get the station name from the root
    station_name = root.get('station')
    
    for s in root.findall('s'):
        s_id = s.get('id')
        #train_line = s.find('tl')

        ### To Do, it is optional to have train info here-- I will add if we need 
        # if train_line is not None:
        #     category = train_line.get('c')
        #     number = train_line.get('n')
        #     owner = train_line.get('o')
            
        # else:
        #     category = ''
        #     number = ''
        #     owner = ''

        for event_tag, event_type in (('ar', 'arrival'), ('dp', 'departure')):
            element = s.find(event_tag)

            if element is not None:
                raws.append({
                    'sid': s_id ,
                    'station': station_name,
                    'event_type': event_type, 
                    'line': element.get('l', ''),
                    'changed_time': convert_time(element.get('ct')),
                    'cancellation_time': convert_time(element.get('clt')),
                    'cancellation_status': element.get('cs', ''),
                    'planned_platform': element.get('pp', ''),
                    'changed_path': element.get('cpth', '')
                })
    
    return raws


def define_schema():
    time_schema = StructType([
        StructField("sid", StringType()),
        StructField("station", StringType()),
        StructField("event_type", StringType()),
        StructField("category", StringType()),
        StructField("number", StringType()),
        StructField("owner",StringType()),
        StructField("line", StringType()),
        StructField("planned_time", TimestampType()),
        StructField("planned_platform",StringType()),
        StructField("planned_path", StringType())
])

    change_schema = StructType([
        StructField("sid", StringType()),
        StructField("station", StringType()),
        StructField("event_type", StringType()),
        StructField("line", StringType()),
        StructField("changed_time", TimestampType()),
        StructField("cancellation_time", TimestampType()),  # actual time
        StructField("cancellation_status", StringType()),  # p / a / c
        StructField("planned_platform", StringType()),
        StructField("changed_path", StringType())
])
    return time_schema, change_schema


## get the weeks files of the corresponding path : timetables or timetablechanges
def find_weeks_periods(base_path ):
    weeks = []
    for d in os.listdir(base_path):
        if os.path.isdir(os.path.join(base_path, d)):
            #print(f"week :{d}")
            weeks.append(d)

    print("weeks:", weeks)
    return weeks


## extraxt .xml files in timetable or timetable_changes 
def find_xml_files(weeks , tima_base_path, time_change_path):
    
    tt_files = []
    chg_files = []
    for week in weeks:

        tt_pattern = f"{tima_base_path}/{week}/**/*.xml" 
        chg_pattern = f"{time_change_path}/{week}/**/*.xml"
        
        week_tt = glob.glob(tt_pattern, recursive=True)
        week_chg = glob.glob(chg_pattern, recursive=True)
        
        print(week, len(week_tt), len(week_chg))
        
        tt_files.extend(week_tt)
        chg_files.extend(week_chg)

    print("total:", len(tt_files), len(chg_files))
    return tt_files, chg_files


def process_time_files_parallel(spark, timetable_files, time_schema, batch_size, max_workers=8):
    all_tt_data = []

    # function to parse a single file
    def parse_file(f):
        return extract_timetable(f)

    for i in range(0, len(timetable_files), batch_size):
        batch_files = timetable_files[i:i+batch_size]

        # Parse all files in the batch in parallel using threads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            batch_results = list(executor.map(parse_file, batch_files))

        # flatten the results
        for r in batch_results:
            all_tt_data.extend(r)

        print(f"processed {i + len(batch_files)} files")

        # write batch to parquet
        df = spark.createDataFrame(all_tt_data, time_schema)
        mode = "overwrite" if i == 0 else "append"
        df.write.mode(mode).parquet("./timetables.parquet")
        all_tt_data = []  # clear memory

def process_change_files_parallel(spark, change_files, change_schema, batch_size, max_workers=8):
    all_chg_data = []

    def parse_file(f):
        return extract_changes(f)

    for i in range(0, len(change_files), batch_size):
        batch_files = change_files[i:i+batch_size]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            batch_results = list(executor.map(parse_file, batch_files))

        for r in batch_results:
            all_chg_data.extend(r)

        print(f"processed {i + len(batch_files)} files")

        df = spark.createDataFrame(all_chg_data, change_schema)
        mode = "overwrite" if i == 0 else "append"
        df.write.mode(mode).parquet("./timetable_changes.parquet")
        all_chg_data = []



def main():
    spark= setup_spark()
    ## deine schemas
    time_schema, change_schema = define_schema()

    tt_tar = "./timetables.tar.gz"
    chg_tar = "./timetable_changes.tar.gz"
 
    weeks = []
    tt_base = "./timetables"
    chg_base = "./timetable_changes"


    path = "./"
    print("Unzipping raw data")
    print("time_table: ")

    unzip_folders_parallel(os.path.join(path, "timetables"), max_workers=4)
    unzip_folders_parallel(os.path.join(path, "timetable_changes"), max_workers=4)

    weeks = find_weeks_periods(tt_base)
    time_files , change_files = find_xml_files(weeks, tt_base, chg_base )

    # process timetables data in batch size of 1000 since I faced with memory full issue
    batch_size = 1000

    ## after getting all data of time table, want to process them 
    process_time_files_parallel(spark, time_files, time_schema, batch_size, max_workers=4)
    process_change_files_parallel(spark, change_files, change_schema, batch_size, max_workers=4)


    # check
    tt_df = spark.read.parquet("./timetables.parquet")
    chg_df = spark.read.parquet("./timetable_changes.parquet") 

    print("finished successfully:", tt_df.count(), chg_df.count())



if __name__== '__main__':
    main()
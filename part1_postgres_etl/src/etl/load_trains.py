import os
import xml.etree.ElementTree as ET


OPERATOR_CODE_TO_NAME = {
    "08": "S-Bahn Berlin GmbH",
    "OWRE": "Ostdeutsche Eisenbahn GmbH (ODEG)",
    "DB": "DB Regio AG",
    "800165": "DB Regio AG Nordost",
    "SNCF": "French National Railways",
    "BVG": "Berliner Verkehrsbetriebe",

}

TRAIN_CATEGORY_DESC = {
    "S": "S-Bahn",
    "RE": "Regional Express",
    "RB": "Regionalbahn",
    "IC": "Intercity",
    "ICE": "Intercity Express",
    
}

TRAIN_CLASS_DESC = {
    "N": "Normal",
    "S": "Special",
    "": None,
    None: None
}

TRAIN_TYPE_DESC = {
    "p": "Passenger",
    "g": "Freight",
    "": None,
    None: None
}


def get_train_key(conn, train_list):

    cursor = conn.cursor()

    train_id = train_list.get('train_id')
    ## first check if it is already in DB or not
    if train_id is not None:
        cursor.execute(
            "select train_key from dim_train where train_id = %s",
            (train_id,)
        )

        #print(cursor.fetchone())
        raw = cursor.fetchone()
        if raw:
            cursor.close()
            return raw[0]
        
    ## if it is not in Db, insert it
    cursor.execute("""
        insert into dim_train (
            train_id, train_number, operator_code, operator_name,
            train_category, train_category_desc, train_class, train_class_desc,
            train_type, train_type_desc
        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (train_id) DO NOTHING
    """, (
        train_list['train_id'],
        train_list['train_number'],
        train_list['operator_code'],
        train_list['operator_name'],
        train_list['train_category'],
        train_list['train_category_desc'],
        train_list['train_class'],
        train_list['train_class_desc'],
        train_list['train_type'],
        train_list['train_type_desc']
    ))
    
    conn.commit()
    cursor.execute("select train_key from dim_train where train_id = %s" ,
                (train_list['train_id'],) )

    row = cursor.fetchone()
    
    if not row:
        cursor.close()
        raise RuntimeError("Failed to fetch train_key after insert")

    insreted_key = row[0]
    cursor.close()
    return insreted_key


def clean_data(item):
    if item is None:
        return None
    item  = item.strip()
    return item  if item else None


def load_trains(conn, data_folder):

    timetables_dir = os.path.join(data_folder, "timetables")
    trains_list = []

    for week_folder in os.listdir(timetables_dir):
        week_path = os.path.join(timetables_dir, week_folder)
        if not os.path.isdir(week_path):
            continue
        
        for hour_folder in os.listdir(week_path):
            hour_path = os.path.join(week_path, hour_folder)

            if not os.path.isdir(hour_path):
                continue

            for filename in os.listdir(hour_path):
                if not filename.endswith(".xml"):
                    continue

                file_path = os.path.join(hour_path, filename)
                
                tree = ET.parse(file_path)
                root = tree.getroot()
                for m in root.findall('.//s'):

                    train_data = m.find('tl')

                    if train_data is None:
                        continue

                    train_category = train_data.get('c')
                    train_number = train_data.get('n')
                    operator_code= train_data.get('o')
                    train_type = train_data.get('t')
                    train_class= train_data.get('f')

                    if not train_category or not train_number:
                        continue
                  
                    train_category = clean_data(train_category)
                    train_number = clean_data(train_number)
                    operator_code = clean_data(operator_code)
                    train_type = clean_data(train_type)
                    train_class = clean_data(train_class)
                    
                    if operator_code:
                        train_id = f"{train_category}_{train_number}_{operator_code}"
                    else:
                        train_id = f"{train_category}_{train_number}_UNK"
                    
                    train_data = {}
                    train_data['train_id'] = train_id
                    train_data['train_number']= train_number
                    train_data['operator_code'] = operator_code
                    train_data['operator_name']= OPERATOR_CODE_TO_NAME.get(operator_code)
                    train_data['train_category']= train_category
                    train_data['train_category_desc'] =TRAIN_CATEGORY_DESC.get(train_category)
                    train_data['train_class']= train_class
                    train_data['train_class_desc']= TRAIN_CLASS_DESC.get(train_class)
                    train_data['train_type']= train_type
                    train_data['train_type_desc']= TRAIN_TYPE_DESC.get(train_type)
                        
                    # print("data:", train_data)
                        
                    if train_id not in trains_list:
                        get_train_key(conn, train_data)
                        trains_list.append(train_id)
                

    print(f"Loaded {len(trains_list)} unique trains into dim_train.")
import os
import xml.etree.ElementTree as ET

# === Fill these dicts as you encounter more unique codes in your data ===
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
    
    # Add more seen in your DEBUG output!
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


def get_train_key(conn, train_data):
    cursor = conn.cursor()
    train_id = train_data.get('train_id')
    if train_id:
        cursor.execute("SELECT train_key FROM dim_train WHERE train_id = %s", (train_id,))
        result = cursor.fetchone()
        if result:
            cursor.close()
            return result[0]
    cursor.execute("""
    INSERT INTO dim_train (
        train_id, train_number, operator_code, operator_name,
        train_category, train_category_desc, train_class, train_class_desc,
        train_type, train_type_desc
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING train_key
""", (
    train_data['train_id'], train_data['train_number'], train_data['operator_code'],
    train_data['operator_name'], train_data['train_category'], train_data['train_category_desc'],
    train_data['train_class'], train_data['train_class_desc'],
    train_data['train_type'], train_data['train_type_desc']
))
    train_key = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return train_key

def load_all_trains(conn, data_folder):
    timetables_dir = os.path.join(data_folder, "timetables")
    loaded = set()
    for period_folder in os.listdir(timetables_dir):
        period_path = os.path.join(timetables_dir, period_folder)
        if not os.path.isdir(period_path):
            continue
        for hour_folder in os.listdir(period_path):
            hour_path = os.path.join(period_path, hour_folder)
            if not os.path.isdir(hour_path):
                continue
            for filename in os.listdir(hour_path):
                if not filename.endswith(".xml"):
                    continue
                file_path = os.path.join(hour_path, filename)
                try:
                    tree = ET.parse(file_path)
                    root = tree.getroot()
                    for movement in root.findall('.//s'):
                        train_elem = movement.find('tl')
                        if train_elem is None:
                            continue
                        train_category = train_elem.get('c')
                        train_number = train_elem.get('n')
                        operator_code = train_elem.get('o')
                        train_type = train_elem.get('t')
                        train_class = train_elem.get('f')
                        if not train_category or not train_number:
                            continue
                        # --- Normalization ---
                        train_category = train_category.strip() if train_category else None
                        train_number = train_number.strip() if train_number else None
                        operator_code = operator_code.strip() if operator_code else None
                        train_type = train_type.strip() if train_type else None
                        train_class = train_class.strip() if train_class else None
                        # --- ID construction ---
                        if operator_code:
                            train_id = f"{train_category}_{train_number}_{operator_code}"
                        else:
                            train_id = f"{train_category}_{train_number}_UNK"
                        train_data = {
                            'train_id': train_id,
                            'train_number': train_number,
                            'operator_code': operator_code,
                            'operator_name': OPERATOR_CODE_TO_NAME.get(operator_code),
                            'train_category': train_category,
                            'train_category_desc': TRAIN_CATEGORY_DESC.get(train_category),
                            'train_class': train_class,
                            'train_class_desc': TRAIN_CLASS_DESC.get(train_class),
                            'train_type': train_type,
                            'train_type_desc': TRAIN_TYPE_DESC.get(train_type)
                        }
                        
                        # print("DEBUG TRAIN:", train_data)
                        
                        if train_id not in loaded:
                            get_train_key(conn, train_data)
                            loaded.add(train_id)
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")
    print(f"Loaded {len(loaded)} unique trains into dim_train.")
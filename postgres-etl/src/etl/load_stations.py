import json


def insert_station_into_db(cursor, values):
    ## insert one station data into dim_station
    query = """
            insert into dim_station (
            station_id, station_number, station_name, eva_number, ifopt,
            street, zipcode, city, category, price_category,
            has_parking, has_bicycle_parking, has_local_public_transport,
            has_public_facilities, has_locker_system, has_taxi_rank,
            has_travel_necessities, has_stepless_access, has_mobility_service,
            has_wifi, has_travel_center, has_railway_mission, has_db_lounge,
            has_lost_and_found, has_car_rental, longitude, latitude,
            regional_number, regional_name, regional_shortname,
            aufgabentraeger_shortname, aufgabentraeger_name,
            szentrale_number, szentrale_name, szentrale_phone,
            station_mgmt_number, station_mgmt_name, product_line, segment,
            ril100_identifier, primary_location_code, steam_permission, federal_state
        ) """
    
    query +="""
            values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """
    
    cursor.execute(query, values)


def extract_data(station):
    eva_number = None
    longitude = None
    latitude = None
    ril100_id = None
    location_code = None
    steam_perm = None
    
    ## address info
    address = station.get('mailingAddress', {})

    ## eva number and coordinates
    if station.get('evaNumbers'):
        first_eva = station['evaNumbers'][0]
        eva_number = first_eva.get('number')
        coords = first_eva.get('geographicCoordinates', {})
        if coords.get('coordinates'):
            longitude , latitude = coords['coordinates'][:2]

    ## ril100Identifiers
    if station.get('ril100Identifiers'):
        first_ril = station['ril100Identifiers'][0]
        ril100_id = first_ril.get('rilIdentifier')
        location_code = first_ril.get('primaryLocationCode')
        steam_perm= first_ril.get('steamPermission')

    
    regional =station.get('regionalbereich', {})
    aufgaben = station.get('aufgabentraeger', {})
    szentrale = station.get('szentrale',{})
    station_mgmt = station.get('stationManagement', {})
    product = station.get('productLine',{})

    return (
        station.get('number'),
        station.get('number'),
        station.get('name'),

        eva_number,

        station.get('ifopt'),
        address.get('street'),
        address.get('zipcode'),
        address.get('city'),
        station.get('category'),
        station.get('priceCategory'),
        station.get('hasParking'),
        station.get('hasBicycleParking'),
        station.get('hasLocalPublicTransport'),
        station.get('hasPublicFacilities'),
        station.get('hasLockerSystem'),
        station.get('hasTaxiRank'),
        station.get('hasTravelNecessities'),
        station.get('hasSteplessAccess'),
        station.get('hasMobilityService'),
        station.get('hasWiFi'),
        station.get('hasTravelCenter'),
        station.get('hasRailwayMission'),
        station.get('hasDBLounge'),
        station.get('hasLostAndFound'),
        station.get('hasCarRental'),

        longitude,
        latitude,

        regional.get('number'),
        regional.get('name'),
        regional.get('shortName'),
        aufgaben.get('shortName'),
        aufgaben.get('name'),
        szentrale.get('number'),
        szentrale.get('name'),
        szentrale.get('publicPhoneNumber'),
        station_mgmt.get('number'),
        station_mgmt.get('name'),
        product.get('productLine'),
        product.get('segment'),

        ril100_id,
        location_code,
        steam_perm,

        station.get('federalState')
    )


def load_stations(conn, json_file):
    ## first read data from station_data.json file
    print(f"Loading stations data from {json_file} file")

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
     
    stations = data.get('result')
    if stations is None:
        stations = []

    print(f"There are {len(stations)} stations in the file")

    cursor = conn.cursor()
    loaded = 0

    for station in stations:
        ## get relavant data from the station 
        station_data = extract_data(station)
        ## insert the extracted data into db
        insert_station_into_db(cursor, station_data)
        loaded += 1
  
    conn.commit()
    cursor.close()

    print(f"Successfully loaded {loaded} stations")
    return loaded
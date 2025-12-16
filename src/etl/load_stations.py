import json


def load_stations(conn, json_file):
    
    print(f"Loading stations from: {json_file}")
  
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    stations = data.get('result', [])
    print(f"Found {len(stations)} stations")
    
    cursor = conn. cursor()
    loaded = 0
    
    for station in stations:
        
        eva_number = None
        longitude = None
        latitude = None
        
        if station.get('evaNumbers'):
            first_eva = station['evaNumbers'][0]
            eva_number = first_eva.get('number')
            
            coords = first_eva.get('geographicCoordinates', {})
            if coords.get('coordinates'):
                longitude = coords['coordinates'][0]
                latitude = coords['coordinates'][1]
        
        # extract address information
        address = station.get('mailingAddress', {})
        
        # extract ril100 information
        ril100_id = None
        location_code = None
        steam_perm = None
        
        if station.get('ril100Identifiers'):
            first_ril = station['ril100Identifiers'][0]
            ril100_id = first_ril. get('rilIdentifier')
            location_code = first_ril. get('primaryLocationCode')
            steam_perm = first_ril.get('steamPermission')
        
        # extract regional information
        regional = station.get('regionalbereich', {})
        aufgaben = station.get('aufgabentraeger', {})
        szentrale = station.get('szentrale', {})
        station_mgmt = station.get('stationManagement', {})
        product = station.get('productLine', {})
        
        # insert all station data
        cursor.execute("""
            INSERT INTO dim_station (
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
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
        """, (
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
            station. get('hasLocalPublicTransport'),
            station.get('hasPublicFacilities'),
            station.get('hasLockerSystem'),
            station.get('hasTaxiRank'),
            station.get('hasTravelNecessities'),
            station.get('hasSteplessAccess'),
            station.get('hasMobilityService'),
            station.get('hasWiFi'),
            station.get('hasTravelCenter'),
            station. get('hasRailwayMission'),
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
            station. get('federalState')
        ))
        
        loaded += 1

        if loaded % 100 == 0:  # print every 100 stations
            print(f"Loaded {loaded} stations so far...")

    conn.commit()
    cursor.close()
    
    print(f"Successfully loaded {loaded} stations")
    return loaded
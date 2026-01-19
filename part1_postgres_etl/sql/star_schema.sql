-- Star Schema for Deutsche Bahn Train Movement Analysis

--- Drop tables in correct order (fact table first, then dimensions)
drop table if exists fact_train_movement CASCADE;
drop table if exists dim_message CASCADE;
drop table if exists dim_time CASCADE;
drop table if exists dim_train CASCADE;
drop table if exists dim_station CASCADE;

-- create Station dimension
create table dim_station (
    station_key SERIAL PRIMARY KEY,
    
    -- basic info
    station_id INTEGER,
    station_number INTEGER,
    station_name VARCHAR(255),
    eva_number INTEGER,
    ifopt VARCHAR(100),
    
    -- address
    street VARCHAR(255),
    zipcode VARCHAR(20),
    city VARCHAR(100),
    federal_state VARCHAR(100),
    
    -- category
    category INTEGER,
    price_category INTEGER,
    
    -- facilities of the station
    has_parking BOOLEAN,
    has_bicycle_parking BOOLEAN,
    has_local_public_transport BOOLEAN,
    has_public_facilities BOOLEAN,
    has_locker_system BOOLEAN,
    has_taxi_rank BOOLEAN,
    has_travel_necessities BOOLEAN,
    has_stepless_access VARCHAR(50),
    has_mobility_service TEXT,
    has_wifi BOOLEAN,
    has_travel_center BOOLEAN,
    has_railway_mission BOOLEAN,
    has_db_lounge BOOLEAN,
    has_lost_and_found BOOLEAN,
    has_car_rental BOOLEAN,
    
    -- location 
    longitude DECIMAL(10, 7),
    latitude DECIMAL(10, 7),
    
    -- organizational structure
    regional_number INTEGER,
    regional_name VARCHAR(255),
    regional_shortname VARCHAR(100),
    aufgabentraeger_shortname VARCHAR(100),
    aufgabentraeger_name VARCHAR(255),
    szentrale_number INTEGER,
    szentrale_name VARCHAR(255),
    szentrale_phone VARCHAR(50),
    station_mgmt_number INTEGER,
    station_mgmt_name VARCHAR(255),
    
    -- product info
    product_line VARCHAR(100),
    segment VARCHAR(100),
    
    -- technical codes
    ril100_identifier VARCHAR(50),
    primary_location_code VARCHAR(50),
    steam_permission VARCHAR(50)
);

create INDEX idx_station_eva ON dim_station(eva_number);


-- create train dimention
create TABLE dim_train (
    train_key SERIAL PRIMARY KEY,
    train_id VARCHAR(100) UNIQUE,
    train_number VARCHAR(50),
    operator_code VARCHAR(50),
    operator_name VARCHAR(255),
    train_category VARCHAR(50),
    train_category_desc VARCHAR(255),
    train_class VARCHAR(10),           -- have to be string 'S' or 'N'
    train_class_desc VARCHAR(255),     -- for description of the train
    train_type VARCHAR(50),
    train_type_desc VARCHAR(255)
);

create INDEX idx_train_id ON dim_train(train_id);

-- Time dimension
-- timestamp is extracted from folder structure or content of the .xml files
create table dim_time (
    time_key SERIAL PRIMARY KEY,
    timestamp TIMESTAMP UNIQUE,
    date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    minute INTEGER, 
    weekday INTEGER,
    is_weekend BOOLEAN
);


create INDEX idx_time_timestamp ON dim_time(timestamp);


-- Fact table - each row represent the 
create table fact_train_movement (
    movement_id SERIAL PRIMARY KEY,
    
    -- foreign keys to dimentions
    station_key INTEGER REFERENCES dim_station(station_key),
    train_key INTEGER REFERENCES dim_train(train_key),
    time_key INTEGER REFERENCES dim_time(time_key),
  
    -- event info
    event_type VARCHAR(50),
    event_status VARCHAR(50),
    
    -- time info
    planned_time TIMESTAMP,
    actual_time TIMESTAMP,
    
    -- platform info
    planned_platform VARCHAR(50),
    actual_platform VARCHAR(50),
    
    -- delays and cancellations
    delay_minutes INTEGER,
    is_cancelled BOOLEAN,
    cancellation_time TIMESTAMP,
    is_hidden BOOLEAN,
    has_disruption BOOLEAN,
    
    -- additional details
    distance_change INTEGER,
    line_number VARCHAR(50),
    planned_path TEXT,
    actual_path TEXT,
    planned_destination VARCHAR(255),
    sid VARCHAR(100)
);
create INDEX idx_fact_station ON fact_train_movement(station_key);
create INDEX idx_fact_train ON fact_train_movement(train_key);
create INDEX idx_fact_time ON fact_train_movement(time_key);
--CREATE INDEX idx_fact_message ON fact_train_movement(message_key);
create INDEX idx_fact_event_type ON fact_train_movement(event_type);

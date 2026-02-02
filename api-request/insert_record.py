import psycopg2
import os
from api_request import Data_Ingestion as DI
from dotenv import load_dotenv
import json
import pandas as pd

def connect_db():
    print("Connecting to the database...")
    try:
        conn = psycopg2.connect(
            dbname="nyc311",
            user="nyc311_user",
            password="nyc311_password",
            port = 5432,
            host="nyc311_db",  
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise
def create_table(conn):
    print("Creating table if not exists...")
    create_table_query = """
    CREATE SCHEMA IF NOT EXISTS raw_data;
    CREATE TABLE IF NOT EXISTS raw_data.nyc311_land (
                        unique_key TEXT PRIMARY KEY, 
                        created_date TIMESTAMP, 
                        agency TEXT, 
                        agency_name TEXT, 
                        complaint_type TEXT,
                        descriptor TEXT, 
                        incident_zip TEXT, 
                        intersection_street_1 TEXT,
                        intersection_street_2 TEXT, 
                        address_type TEXT, 
                        city TEXT, 
                        facility_type TEXT,
                        status TEXT, 
                        resolution_description TEXT, 
                        resolution_action_updated_date TEXT,
                        community_board TEXT, 
                        council_district TEXT, 
                        police_precinct TEXT, 
                        borough TEXT,
                        x_coordinate_state_plane TEXT, 
                        y_coordinate_state_plane TEXT,
                        open_data_channel_type TEXT, 
                        park_facility_name TEXT, 
                        park_borough TEXT,
                        latitude TEXT, 
                        longitude TEXT, 
                        location TEXT, 
                        ":@computed_region_f5dn_yrer" TEXT,
                        ":@computed_region_yeji_bk3q" TEXT, 
                        ":@computed_region_sbqj_enih" TEXT,
                        ":@computed_region_92fq_4b7q" TEXT, 
                        location_type TEXT, 
                        incident_address TEXT,
                        street_name TEXT, 
                        cross_street_1 TEXT, 
                        cross_street_2 TEXT, 
                        landmark TEXT, 
                        bbl TEXT,
                        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        source_system TEXT DEFAULT 'socrata',
                        batch_id TEXT DEFAULT gen_random_uuid()
        );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        print("Table created or already exists.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise

def insert_record(conn):
    print("Inserting records into the database...")
    data_ingestion = DI()
    df = data_ingestion.get_data_from_socrata(offset=1)
    insert_query = f"""
            INSERT INTO raw_data.nyc311_land (
            unique_key, 
            created_date, 
            agency, 
            agency_name, 
            complaint_type,
            descriptor, 
            incident_zip, 
            intersection_street_1,
            intersection_street_2, 
            address_type, 
            city, 
            facility_type,
            status, 
            resolution_description, 
            resolution_action_updated_date,
            community_board, 
            council_district, 
            police_precinct, 
            borough,
            x_coordinate_state_plane, 
            y_coordinate_state_plane,
            open_data_channel_type, 
            park_facility_name, 
            park_borough,
            latitude, 
            longitude, 
            location, 
            ":@computed_region_f5dn_yrer",
            ":@computed_region_yeji_bk3q", 
            ":@computed_region_sbqj_enih",
            ":@computed_region_92fq_4b7q", 
            location_type, 
            incident_address,
            street_name, 
            cross_street_1, 
            cross_street_2, 
            landmark, 
            bbl )
            VALUES ({", ".join(["%s"] * 38)})
    ON CONFLICT (unique_key) DO NOTHING;
    """
    try:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row.get('unique_key'), 
                row.get('created_date'), 
                row.get('agency'), 
                row.get('agency_name'), 
                row.get('complaint_type'),
                row.get('descriptor'), 
                row.get('incident_zip'), 
                row.get('intersection_street_1'),
                row.get('intersection_street_2'), 
                row.get('address_type'), 
                row.get('city'), 
                row.get('facility_type'),
                row.get('status'), 
                row.get('resolution_description'), 
                row.get('resolution_action_updated_date'),
                row.get('community_board'), 
                row.get('council_district'), 
                row.get('police_precinct'), 
                row.get('borough'),
                row.get('x_coordinate_state_plane'), 
                row.get('y_coordinate_state_plane'),
                row.get('open_data_channel_type'), 
                row.get('park_facility_name'), 
                row.get('park_borough'),
                row.get('latitude'), 
                row.get('longitude'), 
                json.dumps(row.get('location')) if pd.notnull(row.get('location')) else None, 
                row.get(':@computed_region_f5dn_yrer'),
                row.get(':@computed_region_yeji_bk3q'), 
                row.get(':@computed_region_sbqj_enih'),
                row.get(':@computed_region_92fq_4b7q'), 
                row.get('location_type'), 
                row.get('incident_address'),
                row.get('street_name'), 
                row.get('cross_street_1'), 
                row.get('cross_street_2'), 
                row.get('landmark'), 
                row.get('bbl')  
                    ))
        conn.commit()
        cursor.close()
        print("Records inserted successfully.")
    except psycopg2.Error as e:
        print(f"Error inserting records: {e}")
        raise   

def main():
    try:
        connection = connect_db()
        create_table(connection)
        insert_record(connection)
        connection.close()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
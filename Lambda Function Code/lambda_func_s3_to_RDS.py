import json
import boto3
import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String
from io import StringIO

# AWS services
s3_client = boto3.client('s3')

# Lambda function handler
def lambda_handler(event, context):
    try:
        print(f"Received event: {json.dumps(event)}")  # Log the received event
        
        # Extract S3 bucket and file details from the event
        s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        
        print(f"Processing file: {s3_file_name} in bucket: {s3_bucket_name}")  # Log file processing
        
        # Retrieve the CSV file content from S3
        s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_name)
        csv_string = s3_object['Body'].read().decode('utf-8')
    
        if s3_file_name.startswith('virtual_sensors'):
            columns = ["index", "HPC_outlet_pressure (P30)", "engine_pressure_ratio (epr)", "fuel_ps30_ratio (phi)",
               "corrected_fan_speed (NRf)", "corrected_core_speed (NRc)", "bypass_ratio (BPR)", "burner_fuel_air_ratio (farB)",
               "bleed_enthalpy (htBleed)", "demanded_fan_speed (Nf_dmd)", "demanded_corrected_fan_speed (PCNfR_dmd)",
               "HPT_coolant_bleed (W31)", "LPT_coolant_bleed (W32)"]
        elif s3_file_name.startswith('measurements'):
            columns = ["index", "LPC_outlet_temperature (T24)", "HPC_outlet_temperature (T30)", "LPT_outlet_temperature (T50)",
               "fan_inlet_pressure (P2)", "bypass_duct_pressure (P15)", "fan_speed (Nf)", "core_speed (Nc)",
               "HPC_outlet_static_pressure (Ps30)"]
        elif s3_file_name.startswith('scenario_descriptors'):
            columns = ["index", "altitude (alt)", "mach_number (mach)", "throttle_resolver_angle (TRA)", "fan_inlet_temperature (T2)"]
        elif s3_file_name.startswith('auxiliary_data'):
            columns = ["index", "engine", "cycle", "source"]
        else:
            print(f"Warning: Unexpected file name: {s3_file_name}")
            return {
                'statusCode': 400,
                'body': json.dumps(f'Warning: Unexpected file name: {s3_file_name}')
            }
        
        # Read CSV data into a DataFrame without headers
        df = pd.read_csv(StringIO(csv_string), header=None, names=columns)
        
        # Database connection parameters
        db_host = os.environ['DB_HOST']
        db_name = os.environ['DB_NAME']
        db_user = os.environ['DB_USER']
        db_password = os.environ['DB_PASSWORD']
        db_table = os.environ['DB_TABLE']
        
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}')
        
        # Use MetaData to reflect existing tables
        meta = MetaData()
        meta.reflect(bind=engine)
        
        # Check if the table exists
        if db_table not in meta.tables:
            columns = [
                Column("index", Integer, primary_key=True),
                Column("HPC_outlet_pressure (P30)", Float),
                Column("engine_pressure_ratio (epr)", Float),
                Column("fuel_ps30_ratio (phi)", Float),
                Column("corrected_fan_speed (NRf)", Float),
                Column("corrected_core_speed (NRc)", Float),
                Column("bypass_ratio (BPR)", Float),
                Column("burner_fuel_air_ratio (farB)", Float),
                Column("bleed_enthalpy (htBleed)", Float),
                Column("demanded_fan_speed (Nf_dmd)", Float),
                Column("demanded_corrected_fan_speed (PCNfR_dmd)", Float),
                Column("HPT_coolant_bleed (W31)", Float),
                Column("LPT_coolant_bleed (W32)", Float),
                Column("LPC_outlet_temperature (T24)", Float),
                Column("HPC_outlet_temperature (T30)", Float),
                Column("LPT_outlet_temperature (T50)", Float),
                Column("fan_inlet_pressure (P2)", Float),
                Column("bypass_duct_pressure (P15)", Float),
                Column("fan_speed (Nf)", Float),
                Column("core_speed (Nc)", Float),
                Column("HPC_outlet_static_pressure (Ps30)", Float),
                Column("altitude (alt)", Float),
                Column("mach_number (mach)", Float),
                Column("throttle_resolver_angle (TRA)", Float),
                Column("fan_inlet_temperature (T2)", Float),
                Column("engine", Integer),
                Column("cycle", Integer),
                Column("source", String)
            ]
            table = Table(db_table, meta, *columns)
            table.create(bind=engine)
        
        # Merge new data with existing data if the table exists
        existing_data = pd.read_sql_table(db_table, engine)
        
        # Perform outer merge to combine existing and new data
        df_merged = pd.merge(existing_data, df, on='index', how='outer', suffixes=('_existing', '_new'))
        
        # Iterate through new columns and update existing ones
        for column in df.columns:
            if column != 'index':
                df_merged[column] = df_merged[f"{column}_new"].combine_first(df_merged[f"{column}_existing"])
                df_merged.drop(columns=[f"{column}_new", f"{column}_existing"], inplace=True)
        
        # Drop duplicates based on 'index'
        df_merged.drop_duplicates(subset='index', keep='last', inplace=True)
        
        # Write merged data back to database
        df_merged.to_sql(db_table, engine, if_exists='replace', index=False)
        
        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps('Data inserted successfully into RDS!')
        }
    
    except Exception as e:
        # Print the error and return error response
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }


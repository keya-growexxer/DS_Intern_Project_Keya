import json
import boto3
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String
from io import StringIO

# AWS services
s3_client = boto3.client('s3')

# Lambda function handler
def lambda_handler(event, context):
    try:
        # Extract S3 bucket and file details from the event
        s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        
        # Retrieve the CSV file content from S3
        s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_name)
        csv_string = s3_object['Body'].read().decode('utf-8')
        
        # Determine columns based on file prefix
        if s3_file_name.startswith('virtual_sensors'):
            columns = ["index", "p30", "epr", "phi", "nrf", "nrc", "bpr", "farb", "htbleed", "nf_dmd", "pcnfr_dmd", "w31", "w32"]
        elif s3_file_name.startswith('measurements'):
            columns = ["index", "t24", "t30", "t50", "p2", "p15", "nf", "nc", "ps30"]
        elif s3_file_name.startswith('scenario_descriptors'):
            columns = ["index", "alt", "mach", "tra", "t2"]
        elif s3_file_name.startswith('auxiliary_data'):
            columns = ["index", "engine", "cycle", "source"]
        else:
            raise ValueError(f"Unexpected file name: {s3_file_name}")
        
        # Read CSV data into a DataFrame without headers
        df = pd.read_csv(StringIO(csv_string), header=None, names=columns)
        
        # Database connection parameters
        db_host = 'turbofan-engine-database-instance.cru0o4s60i3o.ap-south-1.rds.amazonaws.com'  # Change this to your RDS endpoint
        db_name = 'turbofanenginedatabase'
        db_user = 'postgres'
        db_password = 'project12'
        db_table = 'turbofan_jet_engine_dataset'
        
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}')
        
        # Use MetaData to reflect existing tables
        meta = MetaData()
        meta.reflect(bind=engine)
        
        # Check if the table exists
        if db_table not in meta.tables:
            # Define table schema
            columns = [
                Column("index", Integer, primary_key=True),
                Column("p30", Float),
                Column("epr", Float),
                Column("phi", Float),
                Column("nrf", Float),
                Column("nrc", Float),
                Column("bpr", Float),
                Column("farb", Float),
                Column("htbleed", Float),
                Column("nf_dmd", Float),
                Column("pcnfr_dmd", Float),
                Column("w31", Float),
                Column("w32", Float),
                Column("t24", Float),
                Column("t30", Float),
                Column("t50", Float),
                Column("p2", Float),
                Column("p15", Float),
                Column("nf", Float),
                Column("nc", Float),
                Column("ps30", Float),
                Column("alt", Float),
                Column("mach", Float),
                Column("tra", Float),
                Column("t2", Float),
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

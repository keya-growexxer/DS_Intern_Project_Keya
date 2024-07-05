import json
import boto3
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Retrieve bucket and file name from the event
        s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        
        # Get the file object from S3
        s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_name)
        body = s3_object['Body']
        csv_string = body.read().decode('utf-8')
        
        # Load CSV data into a DataFrame
        dataframe = pd.read_csv(StringIO(csv_string))
        logger.info(f'First 3 rows of the DataFrame:\n{dataframe.head(3)}')
        
        # Database connection parameters
        db_host = 'turbofan-engine-database-instance.cru0o4s60i3o.ap-south-1.rds.amazonaws.com'
        db_port = '5432'
        db_name = 'turbofanenginedatabase'
        db_user = 'postgres'
        db_password = 'project12'
        
        # Create the database connection string
        db_url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        # Create a SQLAlchemy engine
        engine = create_engine(db_url)
        
        # Define the table name
        table_name = 'turbofan_rul_data'
        
        # Insert data into the PostgreSQL database using pandas
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
        
    except Exception as err:
        logger.error(f'Error: {err}')
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing file: {err}')
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps('File processed successfully!')
    }


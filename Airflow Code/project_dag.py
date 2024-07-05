from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
import time
 
# Define the paths to the local CSV files
LOCAL_CSV_FILES = [
    '/home/growlt248/Final_Project/Dataset/train/auxiliary_data_train.csv',
    '/home/growlt248/Final_Project/Dataset/train/measurements_train.csv',
    '/home/growlt248/Final_Project/Dataset/train/scenario_descriptors_train.csv',
    '/home/growlt248/Final_Project/Dataset/train/virtual_sensors_train.csv',
    '/home/growlt248/Final_Project/Dataset/test/auxiliary_data_test.csv',
    '/home/growlt248/Final_Project/Dataset/test/measurements_test.csv',
    '/home/growlt248/Final_Project/Dataset/test/scenario_descriptors_test.csv',
    '/home/growlt248/Final_Project/Dataset/test/virtual_sensors_test.csv'
]
BATCH_SIZE = 21000
S3_BUCKET_NAME = 'turbofan-engine-data-bucket'
 
# Define a mapping of file path substrings to S3 folders
FILE_TYPE_TO_S3_FOLDER = {
    'auxiliary': 'auxiliary_data',
    'measurements': 'measurements',
    'scenario_descriptors': 'scenario_descriptors',
    'virtual_sensors': 'virtual_sensors'
}
 
def get_s3_folder(file_path):
    for key, folder in FILE_TYPE_TO_S3_FOLDER.items():
        if key in file_path:
            return folder
    return 'other'
 
def upload_to_s3(batch_num, batch_data, s3_key, s3_hook):
    try:
        # Save the batch data to a temporary CSV file without headers
        temp_file = f'/tmp/{os.path.basename(s3_key)}'
        batch_data.to_csv(temp_file, index=False, header=False)
        logging.info(f"Temporary file created at {temp_file}")
        
        # Upload the temporary CSV file to S3
        s3_hook.load_file(temp_file, key=s3_key, bucket_name=S3_BUCKET_NAME, replace=True)
        logging.info(f"File uploaded to S3 at {s3_key}")
        
        # Remove the temporary file
        os.remove(temp_file)
        logging.info(f"Temporary file {temp_file} removed after upload")
    except Exception as e:
        logging.error(f"Failed to upload batch {batch_num} to S3: {e}")
 
def process_and_upload(file_path):
    try:
        # Read the entire CSV file without headers
        df = pd.read_csv(file_path, header=None)
        logging.info(f"Read {len(df)} rows from {file_path}")
        
        # Calculate today's date for batching
        today_date = datetime.now().date()
        
        # Get the last batch number processed for this file
        last_batch_num_var = f"last_batch_num_{os.path.basename(file_path)}"
        last_batch_num = int(Variable.get(last_batch_num_var, default_var=0))
        logging.info(f"Starting batch processing from batch number {last_batch_num}")
        
        # Calculate the start and end indices for the current batch
        start_idx = last_batch_num * BATCH_SIZE
        end_idx = min((last_batch_num + 1) * BATCH_SIZE, len(df))
        batch_data = df.iloc[start_idx:end_idx]
        
        if batch_data.empty:
            logging.info(f"No new data found in batch {last_batch_num} of {file_path} to process and upload.")
            return
        
        # Determine the S3 folder based on the file type
        s3_folder = get_s3_folder(file_path)
        
        # Define the S3 object key with today's date and folder
        s3_key = f'{s3_folder}/{os.path.basename(file_path).replace(".csv", "")}_{today_date}_batch_{last_batch_num}.csv'
        
        # Initialize the S3Hook
        s3_hook = S3Hook(aws_conn_id='local_to_s3_conn')
        
        # Upload the current batch to S3
        upload_to_s3(last_batch_num, batch_data, s3_key, s3_hook)
        
        # Update the last processed batch number in Airflow Variables
        Variable.set(last_batch_num_var, last_batch_num + 1)
        
        # Log completion of file processing
        logging.info(f"Processed and uploaded batch {last_batch_num} for {file_path}")
    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
 
def add_delay():
    time.sleep(20)
    logging.info("Delay of 20 seconds added between file processing tasks.")
 
# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
# Define the DAG
dag = DAG(
    'project_airflow_dag_local_to_s3',
    default_args=default_args,
    description='Extract data from local CSVs and upload to S3 in batches daily',
    schedule_interval="@once",
)
 
# Define tasks to process and upload each file
previous_task = None
for file_path in LOCAL_CSV_FILES:
    task_id = f'process_and_upload_to_s3_{os.path.basename(file_path).replace(".", "_")}'
    upload_task = PythonOperator(
        task_id=task_id,
        python_callable=process_and_upload,
        op_args=[file_path],
        dag=dag,
    )
    delay_task_id = f'delay_task_{os.path.basename(file_path).replace(".", "_")}'
    delay_task = PythonOperator(
        task_id=delay_task_id,
        python_callable=add_delay,
        dag=dag,
    )
    if previous_task:
        previous_task >> upload_task >> delay_task
    else:
        upload_task >> delay_task
    previous_task = delay_task
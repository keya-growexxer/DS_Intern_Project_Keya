import os
from datetime import datetime, timedelta
import pandas as pd
import logging
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.utils.email import send_email

logging.basicConfig(level=logging.INFO)

# Paths to the local CSV files
LOCAL_CSV_FILES = [
    '/home/growlt248/Final_Project/Dataset/FD002/train/auxiliary_data_train_FD002.csv',
    '/home/growlt248/Final_Project/Dataset/FD002/train/measurements_train_FD002.csv',
    '/home/growlt248/Final_Project/Dataset/FD002/train/scenario_descriptors_train_FD002.csv',
    '/home/growlt248/Final_Project/Dataset/FD002/train/virtual_sensors_train_FD002.csv',
    '/home/growlt248/Final_Project/Dataset/FD002/test/auxiliary_data_test_FD002.csv',
    '/home/growlt248/Final_Project/Dataset/FD002/test/measurements_test_FD002.csv',
    '/home/growlt248/Final_Project/Dataset/FD002/test/scenario_descriptors_test_FD002.csv',
    '/home/growlt248/Final_Project/Dataset/FD002/test/virtual_sensors_test_FD002.csv'
]

BATCH_SIZE = 2000
S3_BUCKET_NAME = 'turbofan-engine-data-bucket'

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
        # Save the batch data to a temporary CSV file
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
        raise  # Raise the exception to trigger Airflow task failure

def process_and_upload(file_path):
    try:
        # Read the entire CSV file
        df = pd.read_csv(file_path, header=None)
        logging.info(f"Read {len(df)} rows from {file_path}")
        
        today_date = datetime.now().date()
        
        # Get the last batch number processed for this file
        last_batch_num_var = f"last_batch_num_{os.path.basename(file_path).replace('.', '_')}"
        last_batch_num = int(Variable.get(last_batch_num_var, default_var=0))
        logging.info(f"Starting batch processing from batch number {last_batch_num}")
        
        start_idx = last_batch_num * BATCH_SIZE
        end_idx = min((last_batch_num + 1) * BATCH_SIZE, len(df))
        batch_data = df.iloc[start_idx:end_idx]
        
        if batch_data.empty:
            logging.info(f"No new data found in batch {last_batch_num} of {file_path} to process and upload.")
            return
        
        # Determine the S3 folder based on the file type
        s3_folder = get_s3_folder(file_path)
        
        s3_key = f'{s3_folder}/{os.path.basename(file_path).replace(".csv", "")}_{today_date}_batch_{last_batch_num}.csv'
        
        s3_hook = S3Hook(aws_conn_id='local_to_s3_conn')
        
        # Upload to S3
        upload_to_s3(last_batch_num, batch_data, s3_key, s3_hook)
        
        # Update the batch number variable for next run
        Variable.set(last_batch_num_var, last_batch_num + 1)
        
        logging.info(f"Processed and uploaded batch {last_batch_num} for {file_path}")
    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        raise  # Raise the exception to trigger Airflow task failure

def add_delay():
    time.sleep(20)
    logging.info("Delay of 20 seconds added between file processing tasks.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 13),
    'email_on_failure': True,
    'email': ['keya.shah@growexx.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=40),
}

dag = DAG(
    'project_airflow_dag',
    default_args=default_args,
    description='Extract data from local CSVs and upload to S3 in batches daily',
    schedule_interval="@daily",
)

# Add tasks to the DAG
previous_task = None
for file_path in LOCAL_CSV_FILES:
    task_id = f'process_and_upload_to_s3_{os.path.basename(file_path).replace(".", "_")}'
    upload_task = PythonOperator(
        task_id=task_id,
        python_callable=process_and_upload,
        op_args=[file_path],
        dag=dag,
        trigger_rule='all_done',  # Ensure the task runs regardless of the previous task's success
        on_failure_callback=lambda context: send_email(
            to=['keya.shah@growexx.com'],
            subject=f'Airflow Task Failed: {context["task_instance"].task_id}',
            html_content=f'Task {context["task_instance"].task_id} failed. <br> Log: {context["task_instance"].log_url}',
        )
    )
    
    delay_task_id = f'delay_task_{os.path.basename(file_path).replace(".", "_")}'
    delay_task = PythonOperator(
        task_id=delay_task_id,
        python_callable=add_delay,
        dag=dag,
        trigger_rule='all_done',  # Ensure the delay task runs regardless of the previous task's success
    )
    
    if previous_task:
        previous_task >> upload_task >> delay_task
    else:
        upload_task >> delay_task
        
    previous_task = delay_task

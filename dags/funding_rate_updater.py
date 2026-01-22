"""
Bitcoin Funding Rate Data Pipeline

This module implements an 8-hourly update pipeline for Bitcoin funding rate data.
Funding rates are collected every 8 hours at 02:00, 10:00, and 18:00 UTC.

Features:
- Downloads hourly funding rate data with full datetime precision
- Uses MERGE statements for efficient upsert operations
- Handles datetime strings with optional milliseconds
- Stores data directly in the DATA schema for production use
- Primary key constraints for data integrity
- Comprehensive error handling and logging
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
import json
import os
import tempfile
import socket

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'execution_timeout': timedelta(minutes=5),
}

# DAG definition - Run every 8 hours at 30 minutes past the hour
dag = DAG(
    'btc_funding_rate_updater',
    default_args=default_args,
    description='8-hourly update of Bitcoin funding rate data',
    schedule='30 21 * * *',  # Run at 22h30 UTC+2
    max_active_runs=1,
    tags=['bitcoin', 'funding-rate', '8hourly', 'snowflake']
)

def download_and_upload_funding_rate(**context):
    """
    Download funding rate JSON data from API and upload to Snowflake stage
    """
    api_url = 'https://bitcoin-data.com/v1/funding-rate'
    
    try:
        print(f"Downloading funding rate data from: {api_url}")
        
        try:
            response = requests.get(
                api_url, 
                timeout=600
            )
            response.raise_for_status()
            
        except (requests.exceptions.RequestException, socket.gaierror) as e:
            raise Exception(f"Failed to download funding rate data: {str(e)}")
    
        # Validate JSON
        json_data = response.json()
        print(f"Downloaded {len(json_data)} records for funding rate")
        
        # Validate that we have data
        if not json_data or len(json_data) == 0:
            raise Exception("No funding rate data received")
            
        # Log sample data for verification
        if len(json_data) > 0:
            print(f"Sample record: {json_data[0]}")
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(json_data, temp_file, indent=2)
            temp_file_path = temp_file.name
        
        # Upload to Snowflake stage
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        stage_filename = f"funding_rate-{timestamp}.json"
        
        # Upload file to stage
        print(f"Uploading file to stage as: {stage_filename}")
        
        # Use Snowflake PUT command to upload file
        put_sql = f"PUT file://{temp_file_path} @BITCOIN_DATA.FORECASTER.my_stage/{stage_filename}"
        snowflake_hook.run(put_sql)
        
        # Clean up temporary file
        os.unlink(temp_file_path)
        
        # Store filename in XCom for next task
        return stage_filename
        
    except Exception as e:
        print(f"Error details: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        raise Exception(f"Error in download_and_upload_funding_rate: {str(e)}")

def merge_funding_rate_data(**context):
    """
    Merge funding rate data using MERGE statement (upsert)
    """
    filename = context['task_instance'].xcom_pull(task_ids='download_funding_rate')
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Create table if not exists first
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS BITCOIN_DATA.DATA.FUNDING_RATE (
        datetime TIMESTAMP,
        unix_ts BIGINT,
        funding_rate FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (datetime)
    );
    """
    
    # MERGE statement with robust datetime parsing
    merge_sql = f"""
    MERGE INTO BITCOIN_DATA.DATA.FUNDING_RATE AS target
    USING (
        SELECT 
            CASE 
                -- Handle datetime with milliseconds (e.g., "2023-07-16 02:00:00.001")
                WHEN $1:d::STRING LIKE '%.%' THEN
                    TO_TIMESTAMP(SUBSTR($1:d::STRING, 1, 19), 'YYYY-MM-DD HH24:MI:SS')
                -- Handle standard datetime (e.g., "2023-07-09 18:00:00")
                ELSE
                    TO_TIMESTAMP($1:d::STRING, 'YYYY-MM-DD HH24:MI:SS')
            END as datetime,
            CASE 
                -- Handle unix timestamp with milliseconds
                WHEN $1:unixTs::STRING LIKE '%.%' THEN
                    FLOOR($1:unixTs::FLOAT)::BIGINT
                ELSE
                    $1:unixTs::BIGINT
            END as unix_ts,
            $1:fundingRate::FLOAT as funding_rate
        FROM @BITCOIN_DATA.FORECASTER.my_stage/{filename} (FILE_FORMAT => BITCOIN_DATA.FORECASTER.json_format)
        WHERE $1:d IS NOT NULL AND $1:fundingRate IS NOT NULL
    ) AS source
    ON target.datetime = source.datetime
    WHEN MATCHED THEN
        UPDATE SET 
            unix_ts = source.unix_ts,
            funding_rate = source.funding_rate,
            updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (datetime, unix_ts, funding_rate)
        VALUES (source.datetime, source.unix_ts, source.funding_rate);
    """
    
    print("Creating funding rate table if not exists...")
    snowflake_hook.run(create_table_sql)
    
    print(f"Merging funding rate data from file: {filename}")
    result = snowflake_hook.run(merge_sql)
    print(f"Merge completed for funding rate: {result}")
    
    return result

def cleanup_stage_files(**context):
    """
    Clean up the uploaded file from the Snowflake stage
    """
    filename = context['task_instance'].xcom_pull(task_ids='download_funding_rate')
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Remove the specific file from the stage
    cleanup_sql = f"REMOVE @BITCOIN_DATA.FORECASTER.my_stage/{filename}"
    
    print(f"Cleaning up file from stage: {filename}")
    result = snowflake_hook.run(cleanup_sql)
    print(f"Stage cleanup completed: {result}")
    
    return result

# Create file format task
create_file_format = SnowflakeOperator(
    task_id='create_file_format',
    snowflake_conn_id='snowflake_default',
    sql="""
    CREATE FILE FORMAT IF NOT EXISTS BITCOIN_DATA.FORECASTER.json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;
    """,
    dag=dag
)

# Download task
download_task = PythonOperator(
    task_id='download_funding_rate',
    python_callable=download_and_upload_funding_rate,
    dag=dag
)

# Merge task
merge_task = PythonOperator(
    task_id='merge_funding_rate',
    python_callable=merge_funding_rate_data,
    dag=dag
)

# Cleanup task
cleanup_task = PythonOperator(
    task_id='cleanup_stage',
    python_callable=cleanup_stage_files,
    dag=dag
)

# Set task dependencies
create_file_format >> download_task >> merge_task >> cleanup_task

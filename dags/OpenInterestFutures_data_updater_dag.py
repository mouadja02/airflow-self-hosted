"""
Bitcoin Open Futures Interest Data Pipeline

This module implements a daily update pipeline for Bitcoin open futures interest data
from multiple exchanges including Binance, Bybit, OKX, Deribit, BitMEX, Huobi, Bitfinex, etc.

Features:
- Downloads daily open interest data from all major exchanges
- Uses MERGE statements for efficient upsert operations
- Handles null values for exchanges that don't have historical data
- Calculates total open interest across all exchanges
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
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'execution_timeout': timedelta(minutes=30),
    'retry_exponential_backoff': True,
}

# DAG definition
dag = DAG(
    'btc_open_interest_futures_updater',
    default_args=default_args,
    description='Daily update of Bitcoin open futures interest data',
    schedule='45 0 * * *', 
    max_active_runs=1,
    tags=['bitcoin', 'futures', 'open-interest', 'daily', 'snowflake']
)

def download_and_upload_open_interest(**context):
    """
    Download open futures interest JSON data from API and upload to Snowflake stage
    """
    api_url = 'https://bitcoin-data.com/v1/open-interest-futures'
    
    try:
        print(f"Downloading open futures interest data from: {api_url}")
        
        try:
            response = requests.get(
                api_url, 
                timeout=600
            )
            response.raise_for_status()
            
        except (requests.exceptions.RequestException, socket.gaierror) as e:
            raise Exception(f"Failed to download open futures interest data: {str(e)}")
    
        # Validate JSON
        json_data = response.json()
        print(f"Downloaded {len(json_data)} records for open futures interest")
        
        # Validate that we have data
        if not json_data or len(json_data) == 0:
            raise Exception("No open futures interest data received")
            
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
        stage_filename = f"open_interest_futures-{timestamp}.json"
        
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
        raise Exception(f"Error in download_and_upload_open_interest: {str(e)}")

def merge_open_interest_data(**context):
    """
    Merge open futures interest data using MERGE statement (upsert)
    """
    filename = context['task_instance'].xcom_pull(task_ids='download_open_interest')
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Create table if not exists first
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS BITCOIN_DATA.DATA.OPEN_INTEREST_FUTURES (
        date DATE,
        unix_ts BIGINT,
        binance FLOAT,
        bybit FLOAT,
        okx FLOAT,
        bitget FLOAT,
        deribit FLOAT,
        bitmex FLOAT,
        huobi FLOAT,
        bitfinex FLOAT,
        gate_io FLOAT,
        kucoin FLOAT,
        kraken FLOAT,
        crypto_com FLOAT,
        dydx FLOAT,
        delta_exchange FLOAT,
        total_open_interest FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (date)
    );
    """
    
    # MERGE statement with calculated total open interest
    merge_sql = f"""
    MERGE INTO BITCOIN_DATA.DATA.OPEN_INTEREST_FUTURES AS target
    USING (
        SELECT 
            TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
            $1:unixTs::BIGINT as unix_ts,
            $1:binance::FLOAT as binance,
            $1:bybit::FLOAT as bybit,
            $1:okx::FLOAT as okx,
            $1:bitget::FLOAT as bitget,
            $1:deribit::FLOAT as deribit,
            $1:bitmex::FLOAT as bitmex,
            $1:huobi::FLOAT as huobi,
            $1:bitfinex::FLOAT as bitfinex,
            $1:gateIo::FLOAT as gate_io,
            $1:kucoin::FLOAT as kucoin,
            $1:kraken::FLOAT as kraken,
            $1:cryptoCom::FLOAT as crypto_com,
            $1:dydx::FLOAT as dydx,
            $1:deltaExchange::FLOAT as delta_exchange,
            -- Calculate total open interest (sum of all non-null exchanges)
            COALESCE($1:binance::FLOAT, 0) + 
            COALESCE($1:bybit::FLOAT, 0) + 
            COALESCE($1:okx::FLOAT, 0) + 
            COALESCE($1:bitget::FLOAT, 0) + 
            COALESCE($1:deribit::FLOAT, 0) + 
            COALESCE($1:bitmex::FLOAT, 0) + 
            COALESCE($1:huobi::FLOAT, 0) + 
            COALESCE($1:bitfinex::FLOAT, 0) + 
            COALESCE($1:gateIo::FLOAT, 0) + 
            COALESCE($1:kucoin::FLOAT, 0) + 
            COALESCE($1:kraken::FLOAT, 0) + 
            COALESCE($1:cryptoCom::FLOAT, 0) + 
            COALESCE($1:dydx::FLOAT, 0) + 
            COALESCE($1:deltaExchange::FLOAT, 0) as total_open_interest
        FROM @BITCOIN_DATA.FORECASTER.my_stage/{filename} (FILE_FORMAT => BITCOIN_DATA.FORECASTER.json_format)
    ) AS source
    ON target.date = source.date
    WHEN MATCHED THEN
        UPDATE SET 
            unix_ts = source.unix_ts,
            binance = source.binance,
            bybit = source.bybit,
            okx = source.okx,
            bitget = source.bitget,
            deribit = source.deribit,
            bitmex = source.bitmex,
            huobi = source.huobi,
            bitfinex = source.bitfinex,
            gate_io = source.gate_io,
            kucoin = source.kucoin,
            kraken = source.kraken,
            crypto_com = source.crypto_com,
            dydx = source.dydx,
            delta_exchange = source.delta_exchange,
            total_open_interest = source.total_open_interest,
            updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (date, unix_ts, binance, bybit, okx, bitget, deribit, bitmex, 
                huobi, bitfinex, gate_io, kucoin, kraken, crypto_com, dydx, 
                delta_exchange, total_open_interest)
        VALUES (source.date, source.unix_ts, source.binance, source.bybit, 
                source.okx, source.bitget, source.deribit, source.bitmex,
                source.huobi, source.bitfinex, source.gate_io, source.kucoin,
                source.kraken, source.crypto_com, source.dydx, source.delta_exchange,
                source.total_open_interest);
    """
    
    print("Creating open interest futures table if not exists...")
    snowflake_hook.run(create_table_sql)
    
    print(f"Merging open interest futures data from file: {filename}")
    result = snowflake_hook.run(merge_sql)
    print(f"Merge completed for open interest futures: {result}")
    
    return result

def cleanup_stage_files(**context):
    """
    Clean up the uploaded file from the Snowflake stage
    """
    filename = context['task_instance'].xcom_pull(task_ids='download_open_interest')
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
    task_id='download_open_interest',
    python_callable=download_and_upload_open_interest,
    dag=dag
)

# Merge task
merge_task = PythonOperator(
    task_id='merge_open_interest',
    python_callable=merge_open_interest_data,
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

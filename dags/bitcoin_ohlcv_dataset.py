"""
BTC Price Dataset DAG
Replicates the n8n workflow for fetching Bitcoin hourly price data and storing in Snowflake
Includes database initialization with schema/table creation and historical OHLCV batching since 2010-07-18
CryptoCompare API limit: 2000 records per call
"""

from datetime import datetime, timedelta
import os
import json
import requests
import time
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.empty import EmptyOperator

# Default arguments
default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'btc_price_dataset',
    default_args=default_args,
    description='Fetch Bitcoin hourly price data and store in Snowflake',
    schedule='5 0 * * *',  # Every day after midnight UTC
    catchup=False,
    tags=['bitcoin', 'cryptocurrency', 'snowflake'],
)

# Bitcoin started trading in July 2010 (first exchange Mt.Gox)
BTC_START_TIMESTAMP = int(datetime(2010, 7, 18).timestamp())

# ─── Database Initialization ───────────────────────────────────────────

def ensure_schema_and_table(**context):
    """Create database, schema and table in Snowflake if they don't exist"""
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Create database if not exists
    snowflake_hook.run("CREATE DATABASE IF NOT EXISTS BITCOIN_DATA")
    print("✅ Database BITCOIN_DATA ensured")
    
    # Create schema if not exists
    snowflake_hook.run("CREATE SCHEMA IF NOT EXISTS BITCOIN_DATA.DATA")
    print("✅ Schema BITCOIN_DATA.DATA ensured")
    
    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS BITCOIN_DATA.DATA.BTC_HOURLY_DATA (
        UNIX_TIMESTAMP NUMBER(38,0) PRIMARY KEY,
        DATE DATE,
        HOUR_OF_DAY NUMBER(2,0),
        OPEN FLOAT,
        HIGH FLOAT,
        CLOSE FLOAT,
        LOW FLOAT,
        VOLUME_BTC FLOAT,
        VOLUME_USD FLOAT,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    snowflake_hook.run(create_table_query)
    print("✅ Table BITCOIN_DATA.DATA.BTC_HOURLY_DATA ensured")


def check_historical_data_exists(**context):
    """Check if historical data exists in Snowflake from BTC start"""
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Check if table exists and has data from early BTC history
    check_query = f"""
    SELECT COUNT(*) as count, MIN(UNIX_TIMESTAMP) as min_timestamp
    FROM BITCOIN_DATA.DATA.BTC_HOURLY_DATA
    WHERE UNIX_TIMESTAMP >= {BTC_START_TIMESTAMP}
    """
    
    try:
        result = snowflake_hook.get_first(check_query)
        print(f"🔍 Query result: {result}")
        
        if result and result[0] > 0:
            min_timestamp = result[1]
            context['task_instance'].xcom_push(key='min_timestamp', value=min_timestamp)
            
            # Check if we have data from at least 2011 (when BTC trading started)
            if min_timestamp <= int(datetime(2011, 1, 1).timestamp()):
                print(f"✅ Historical data exists from timestamp {min_timestamp}")
                print(f"➡️  Branching to: fetch_btc_data")
                return 'fetch_btc_data'
            else:
                print(f"⚠️ Data exists but starts too late (timestamp {min_timestamp}). Need initialization.")
                print(f"➡️  Branching to: initialize_historical_data")
                return 'initialize_historical_data'
        else:
            print("❌ No historical data found. Starting initialization.")
            print(f"➡️  Branching to: initialize_historical_data")
            return 'initialize_historical_data'
            
    except Exception as e:
        print(f"⚠️ Error checking data: {str(e)}")
        print(f"➡️  Branching to: initialize_historical_data")
        return 'initialize_historical_data'

# ─── Historical Batch Initialization ───────────────────────────────────

def initialize_historical_data(**context):
    """Prepare batch list for fetching all historical data in batches of 2000
    
    CryptoCompare API limit is 2000 records per call.
    We need to fetch hourly data since 2010-07-18 to present.
    Each batch covers 2000 hours (~83 days).
    """
    
    current_timestamp = int(datetime.now().timestamp())
    batches = []
    
    # Calculate number of batches needed (2000 hours per batch)
    hours_diff = (current_timestamp - BTC_START_TIMESTAMP) // 3600
    num_batches = (hours_diff // 2000) + 1
    
    print(f"📊 Need to fetch {hours_diff} hours of data in {num_batches} batches")
    print(f"📅 From: {datetime.fromtimestamp(BTC_START_TIMESTAMP)} to: {datetime.now()}")
    
    # Create batch list with toTs parameter for each batch
    to_ts = current_timestamp
    for i in range(num_batches):
        batches.append({
            'batch_num': i + 1,
            'to_ts': to_ts,
            'limit': 2000
        })
        to_ts = to_ts - (2000 * 3600)  # Go back 2000 hours
    
    context['task_instance'].xcom_push(key='batches', value=batches)
    context['task_instance'].xcom_push(key='total_batches', value=len(batches))
    
    print(f"✅ Prepared {len(batches)} batches for historical data fetch")
    return len(batches)


def generate_merge_query(bulk_values_str):
    """Generate a MERGE query for bulk insert/update"""
    
    return f"""
MERGE INTO BITCOIN_DATA.DATA.BTC_HOURLY_DATA AS target
USING (
  SELECT column1 AS UNIX_TIMESTAMP, 
         column2 AS DATE, 
         column3 AS HOUR_OF_DAY, 
         column4 AS OPEN, 
         column5 AS HIGH, 
         column6 AS CLOSE, 
         column7 AS LOW, 
         column8 AS VOLUME_BTC,
         column9 AS VOLUME_USD,
         column10 AS CREATED_AT
  FROM VALUES
  {bulk_values_str}
) AS source
ON target.UNIX_TIMESTAMP = source.UNIX_TIMESTAMP
WHEN MATCHED THEN UPDATE SET
  target.OPEN = source.OPEN,
  target.HIGH = source.HIGH,
  target.CLOSE = source.CLOSE,
  target.LOW = source.LOW,
  target.VOLUME_BTC = source.VOLUME_BTC,
  target.VOLUME_USD = source.VOLUME_USD,
  target.CREATED_AT = source.CREATED_AT
WHEN NOT MATCHED THEN INSERT
  (UNIX_TIMESTAMP, DATE, HOUR_OF_DAY, OPEN, HIGH, CLOSE, LOW, VOLUME_BTC, VOLUME_USD, CREATED_AT)
VALUES
  (source.UNIX_TIMESTAMP, source.DATE, source.HOUR_OF_DAY, source.OPEN, source.HIGH, source.CLOSE, source.LOW, source.VOLUME_BTC, source.VOLUME_USD, source.CREATED_AT);
"""


def process_all_historical_batches(**context):
    """Process all historical batches with rate limiting
    
    Fetches data from CryptoCompare API in batches of 2000 (API limit),
    transforms each batch, and merges into Snowflake immediately.
    Includes 1-second rate limiting between API calls.
    """
    
    batches = context['task_instance'].xcom_pull(task_ids='initialize_historical_data', key='batches')
    total_records = 0
    
    # Suppress verbose Snowflake logging during bulk operations
    sf_connector_logger = logging.getLogger('snowflake.connector')
    sf_hook_logger = logging.getLogger('airflow.task.hooks.airflow.providers.snowflake.hooks.snowflake.SnowflakeHook')
    original_connector_level = sf_connector_logger.level
    original_hook_level = sf_hook_logger.level
    sf_connector_logger.setLevel(logging.WARNING)
    sf_hook_logger.setLevel(logging.WARNING)
    
    try:
        for i, batch in enumerate(batches):
            print(f"📥 Processing batch {i+1}/{len(batches)}")
            
            url = "https://min-api.cryptocompare.com/data/v2/histohour"
            params = {
                'fsym': 'BTC',
                'tsym': 'USD',
                'limit': batch['limit'],
                'toTs': batch['to_ts']
            }
            
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get('Response') != 'Success':
                    raise Exception(f"API returned error: {data.get('Message', 'Unknown error')}")
                
                response_data = data['Data']['Data']
                current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Transform data
                bulk_values = []
                for record in response_data:
                    date_obj = datetime.fromtimestamp(record['time'])
                    date = date_obj.strftime('%Y-%m-%d')
                    hour = date_obj.hour
                    
                    value_string = f"({record['time']}, '{date}', {hour}, {record['open']}, {record['high']}, {record['close']}, {record['low']}, {record['volumefrom']}, {record['volumeto']}, '{current_timestamp}')"
                    bulk_values.append(value_string)
                
                if bulk_values:
                    bulk_values_str = ',\n  '.join(bulk_values)
                    merge_query = generate_merge_query(bulk_values_str)
                    
                    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
                    snowflake_hook.run(merge_query)
                    
                    total_records += len(bulk_values)
                    print(f"✅ Batch {i+1}/{len(batches)} - Inserted/Updated {len(bulk_values)} records")
                
                # Rate limiting: 1 second between API calls
                if i < len(batches) - 1:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"❌ Error processing batch {i+1}: {str(e)}")
                raise
    finally:
        # Restore logging levels
        sf_connector_logger.setLevel(original_connector_level)
        sf_hook_logger.setLevel(original_hook_level)
    
    context['task_instance'].xcom_push(key='init_record_count', value=total_records)
    print(f"🎉 Historical initialization complete! Total records: {total_records}")
    return total_records

# ─── Daily Delta Update ────────────────────────────────────────────────

def fetch_btc_data(**context):
    """Fetch Bitcoin hourly data from CryptoCompare API (delta update)"""
    
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    params = {
        'fsym': 'BTC',
        'tsym': 'USD',
        'limit': '100'  # Fetch last 100 hours
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get('Response') == 'Success':
            context['task_instance'].xcom_push(key='btc_raw_data', value=data)
            return data
        else:
            raise Exception(f"API returned error: {data.get('Message', 'Unknown error')}")
            
    except Exception as e:
        raise Exception(f"Failed to fetch BTC data: {str(e)}")

def transform_btc_data(**context):
    """Transform Bitcoin data for Snowflake insertion"""
    
    raw_data = context['task_instance'].xcom_pull(task_ids='fetch_btc_data', key='btc_raw_data')
    
    if not raw_data or 'Data' not in raw_data or 'Data' not in raw_data['Data']:
        raise Exception("No valid data received from API")
    
    response_data = raw_data['Data']['Data']
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Transform data for Snowflake
    bulk_values = []
    for record in response_data:
        date_obj = datetime.fromtimestamp(record['time'])
        date = date_obj.strftime('%Y-%m-%d')
        hour = date_obj.hour
        
        value_string = f"({record['time']}, '{date}', {hour}, {record['open']}, {record['high']}, {record['close']}, {record['low']}, {record['volumefrom']}, {record['volumeto']}, '{current_timestamp}')"
        bulk_values.append(value_string)
    
    bulk_values_str = ',\n  '.join(bulk_values)
    bulk_merge_query = generate_merge_query(bulk_values_str)
    
    context['task_instance'].xcom_push(key='merge_query', value=bulk_merge_query)
    context['task_instance'].xcom_push(key='record_count', value=len(bulk_values))
    
    print(f"✅ Transformed {len(bulk_values)} records")
    return len(bulk_values)

# ─── Notification ──────────────────────────────────────────────────────

def send_telegram_notification(**context):
    """Send success notification via Telegram"""
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("Telegram credentials not found, skipping notification")
        return
    
    # Check if this was initialization or delta update
    init_count = context['task_instance'].xcom_pull(task_ids='process_all_historical_batches', key='init_record_count')
    delta_count = context['task_instance'].xcom_pull(task_ids='transform_btc_data', key='record_count')
    
    if init_count:
        message = f"✅ Historical BTC data initialization complete! 🎉\n📊 Loaded {init_count} records from 2010-07-18 to now"
    elif delta_count:
        message = f"✅ Hourly Price dataset successfully refreshed! 🔄 ❄️\nProcessed {delta_count} records"
    else:
        message = "✅ BTC data pipeline completed successfully!"
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {
        'chat_id': chat_id,
        'text': message
    }
    
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
        print("Telegram notification sent successfully")
    except Exception as e:
        print(f"Failed to send Telegram notification: {str(e)}")

# ─── Task Definitions ──────────────────────────────────────────────────

# Step 1: Ensure DB infrastructure exists
ensure_db_task = PythonOperator(
    task_id='ensure_schema_and_table',
    python_callable=ensure_schema_and_table,
    dag=dag,
)

# Step 2: Check if historical data needs initialization
check_data_task = BranchPythonOperator(
    task_id='check_historical_data',
    python_callable=check_historical_data_exists,
    dag=dag,
)

# Path A: Historical initialization
init_historical_task = PythonOperator(
    task_id='initialize_historical_data',
    python_callable=initialize_historical_data,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

process_batches_task = PythonOperator(
    task_id='process_all_historical_batches',
    python_callable=process_all_historical_batches,
    dag=dag,
)

# Path B: Daily delta update
fetch_data_task = PythonOperator(
    task_id='fetch_btc_data',
    python_callable=fetch_btc_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_btc_data',
    python_callable=transform_btc_data,
    dag=dag,
)

# Snowflake connection parameters
snowflake_conn_params = {
    'snowflake_conn_id': 'snowflake_default',
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'BITCOIN_DATA'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'DATA'),
}

execute_merge_task = SnowflakeOperator(
    task_id='execute_snowflake_merge',
    sql="{{ task_instance.xcom_pull(task_ids='transform_btc_data', key='merge_query') }}",
    dag=dag,
    **snowflake_conn_params,
)

# Join point
join_task = EmptyOperator(
    task_id='join_paths',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Notification
telegram_notification_task = PythonOperator(
    task_id='send_telegram_notification',
    python_callable=send_telegram_notification,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# ─── Task Dependencies ─────────────────────────────────────────────────
#
#  ensure_schema_and_table >> check_historical_data
#       ├── initialize_historical_data >> process_all_historical_batches >> join_paths
#       └── fetch_btc_data >> transform_btc_data >> execute_snowflake_merge >> join_paths
#  join_paths >> send_telegram_notification
#

ensure_db_task >> check_data_task >> [init_historical_task, fetch_data_task]

# Path A: Init >> Process batches
init_historical_task >> process_batches_task

# Path B: Fetch >> Transform >> Merge
fetch_data_task >> transform_data_task >> execute_merge_task

# All paths converge at join
[process_batches_task, execute_merge_task] >> join_task

# Final notification
join_task >> telegram_notification_task

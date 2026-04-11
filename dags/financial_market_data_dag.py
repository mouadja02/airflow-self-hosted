"""
Financial Market Data DAG
Replicates the n8n workflow for fetching financial market data (NASDAQ, VIX, DXY, Gold)
Includes database initialization with schema/table creation and historical data recovery
"""

from datetime import datetime, timedelta
import os
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
    'financial_market_data',
    default_args=default_args,
    description='Fetch financial market data for correlation analysis',
    schedule='30 22 * * *',  # Daily at 23:30
    catchup=False,
    tags=['financial', 'market', 'nasdaq', 'vix', 'dxy', 'gold', 'snowflake'],
)

# Snowflake connection parameters
snowflake_conn_params = {
    'snowflake_conn_id': 'snowflake_default',
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'INT_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'BITCOIN_DATA'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'DATA'),
}

# ─── Database Initialization ───────────────────────────────────────────

def ensure_schema_and_table(**context):
    """Create schema and table in Snowflake if they don't exist"""
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Create database if not exists
    snowflake_hook.run("CREATE DATABASE IF NOT EXISTS BITCOIN_DATA")
    print("✅ Database BITCOIN_DATA ensured")
    
    # Create schema if not exists
    snowflake_hook.run("CREATE SCHEMA IF NOT EXISTS BITCOIN_DATA.DATA")
    print("✅ Schema BITCOIN_DATA.DATA ensured")
    
    # Create table if not exists
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS BITCOIN_DATA.DATA.FINANCIAL_MARKET_DATA (
        DATE DATE PRIMARY KEY,
        NASDAQ FLOAT,
        VIX FLOAT,
        DXY FLOAT,
        GOLD FLOAT,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    snowflake_hook.run(create_table_sql)
    print("✅ Table BITCOIN_DATA.DATA.FINANCIAL_MARKET_DATA ensured")


def check_historical_data(**context):
    """Check if historical data exists; if not, branch to initialization"""
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    try:
        result = snowflake_hook.get_first("""
            SELECT COUNT(*) as cnt, MIN(DATE) as min_date
            FROM BITCOIN_DATA.DATA.FINANCIAL_MARKET_DATA
        """)
        
        count = result[0] if result else 0
        min_date = result[1] if result and result[1] else None
        
        print(f"🔍 Found {count} records, earliest date: {min_date}")
        
        if count == 0:
            print("❌ No historical data found. Will initialize.")
            return 'initialize_historical_market_data'
        
        # Check if we have at least a year of data
        if count < 250:  # ~1 trading year
            print(f"⚠️ Only {count} records found. Will initialize to recover full history.")
            return 'initialize_historical_market_data'
        
        print("✅ Historical data exists. Proceeding to daily update.")
        return 'check_if_weekend'
        
    except Exception as e:
        print(f"⚠️ Error checking data: {str(e)}. Will initialize.")
        return 'initialize_historical_market_data'


def _fetch_stooq_all_data(symbol, value_name):
    """Fetch full historical CSV data from Stooq for a given symbol"""
    
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    lines = response.text.strip().split('\n')
    if len(lines) < 2:
        raise Exception(f"Insufficient data for {symbol}")
    
    headers = [h.strip() for h in lines[0].split(',')]
    date_idx = headers.index('Date')
    close_idx = headers.index('Close')
    
    records = []
    for line in lines[1:]:
        parts = line.split(',')
        try:
            records.append({
                'date': parts[date_idx].strip(),
                value_name: float(parts[close_idx].strip())
            })
        except (ValueError, IndexError):
            continue
    
    return records


def initialize_historical_market_data(**context):
    """Fetch all available historical data from Stooq and load into Snowflake"""
    
    symbols = {
        '^ndq': 'NASDAQ',
        'vi.c': 'VIX',
        'dx.c': 'DXY',
        'xauusd': 'GOLD',
    }
    
    # Fetch all datasets
    all_data = {}  # date -> {NASDAQ: ..., VIX: ..., ...}
    
    for symbol, col_name in symbols.items():
        print(f"📥 Fetching historical {col_name} data from Stooq...")
        try:
            records = _fetch_stooq_all_data(symbol, col_name)
            for r in records:
                date_key = r['date']
                if date_key not in all_data:
                    all_data[date_key] = {}
                all_data[date_key][col_name] = r[col_name]
            print(f"✅ Fetched {len(records)} records for {col_name}")
        except Exception as e:
            print(f"⚠️ Failed to fetch {col_name}: {str(e)}")
        
        time.sleep(2)  # Rate limiting between Stooq requests
    
    if not all_data:
        raise Exception("Failed to fetch any historical market data")
    
    # Sort dates
    sorted_dates = sorted(all_data.keys())
    print(f"📊 Total unique dates: {len(sorted_dates)} (from {sorted_dates[0]} to {sorted_dates[-1]})")
    
    # Insert in batches of 500
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    batch_size = 500
    total_merged = 0
    
    # Suppress verbose logging
    sf_connector_logger = logging.getLogger('snowflake.connector')
    sf_hook_logger = logging.getLogger('airflow.task.hooks.airflow.providers.snowflake.hooks.snowflake.SnowflakeHook')
    original_connector_level = sf_connector_logger.level
    original_hook_level = sf_hook_logger.level
    sf_connector_logger.setLevel(logging.WARNING)
    sf_hook_logger.setLevel(logging.WARNING)
    
    try:
        for i in range(0, len(sorted_dates), batch_size):
            batch_dates = sorted_dates[i:i + batch_size]
            
            values_list = []
            for date_str in batch_dates:
                data = all_data[date_str]
                nasdaq = data.get('NASDAQ', 'NULL')
                vix = data.get('VIX', 'NULL')
                dxy = data.get('DXY', 'NULL')
                gold = data.get('GOLD', 'NULL')
                
                values_list.append(
                    f"('{date_str}'::DATE, {nasdaq}, {vix}, {dxy}, {gold})"
                )
            
            values_str = ',\n  '.join(values_list)
            
            merge_query = f"""
MERGE INTO BITCOIN_DATA.DATA.FINANCIAL_MARKET_DATA AS target
USING (
  SELECT column1 AS DATE, column2 AS NASDAQ, column3 AS VIX, column4 AS DXY, column5 AS GOLD
  FROM VALUES
  {values_str}
) AS source
ON target.DATE = source.DATE
WHEN MATCHED THEN
  UPDATE SET
    NASDAQ = source.NASDAQ,
    VIX = source.VIX,
    DXY = source.DXY,
    GOLD = source.GOLD,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (DATE, NASDAQ, VIX, DXY, GOLD)
  VALUES (source.DATE, source.NASDAQ, source.VIX, source.DXY, source.GOLD);
"""
            snowflake_hook.run(merge_query)
            total_merged += len(batch_dates)
            batch_num = i // batch_size + 1
            total_batches = (len(sorted_dates) - 1) // batch_size + 1
            print(f"✅ Batch {batch_num}/{total_batches} - Merged {len(batch_dates)} records (total: {total_merged})")
    finally:
        sf_connector_logger.setLevel(original_connector_level)
        sf_hook_logger.setLevel(original_hook_level)
    
    print(f"🎉 Historical initialization complete! Total records: {total_merged}")
    context['task_instance'].xcom_push(key='init_record_count', value=total_merged)
    return total_merged


# ─── Daily Update Functions ────────────────────────────────────────────

def check_if_weekend(**context):
    """Check if current day is weekend"""
    
    now = datetime.now()
    day_of_week = now.weekday()  # 0 = Monday, 6 = Sunday
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    is_weekend = day_of_week >= 5  # Saturday or Sunday
    
    result = {
        'day_of_week': day_names[day_of_week],
        'is_weekend': is_weekend,
        'current_date': now.strftime('%Y-%m-%d')
    }
    
    context['task_instance'].xcom_push(key='date_check', value=result)
    
    if is_weekend:
        print(f"Today is {day_names[day_of_week]} (weekend), skipping market data fetch")
        return False
    else:
        print(f"Today is {day_names[day_of_week]} (weekday), proceeding with market data fetch")
        return True

def fetch_nasdaq_data(**context):
    """Fetch NASDAQ data from Stooq"""
    
    url = "https://stooq.com/q/d/l/?s=^ndq&d1&i=d"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse CSV data
        lines = response.text.strip().split('\n')
        
        if len(lines) >= 2:
            headers = lines[0].split(',')
            latest_data = lines[-1].split(',')
            
            # Find indices for Date and Close
            date_index = next((i for i, header in enumerate(headers) if header.strip() == 'Date'), None)
            close_index = next((i for i, header in enumerate(headers) if header.strip() == 'Close'), None)
            
            if date_index is not None and close_index is not None:
                result = {
                    'date': latest_data[date_index],
                    'nasdaq': float(latest_data[close_index])
                }
                context['task_instance'].xcom_push(key='nasdaq_data', value=result)
                return result
        
        raise Exception("Could not parse NASDAQ data")
        
    except Exception as e:
        raise Exception(f"Failed to fetch NASDAQ data: {str(e)}")

def fetch_vix_data(**context):
    """Fetch VIX data from Stooq"""
    
    url = "https://stooq.com/q/d/l/?s=vi.c&d1&i=d"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse CSV data
        lines = response.text.strip().split('\n')
        
        if len(lines) >= 2:
            headers = lines[0].split(',')
            latest_data = lines[-1].split(',')
            
            # Find indices for Date and Close
            date_index = next((i for i, header in enumerate(headers) if header.strip() == 'Date'), None)
            close_index = next((i for i, header in enumerate(headers) if header.strip() == 'Close'), None)
            
            if date_index is not None and close_index is not None:
                result = {
                    'date': latest_data[date_index],
                    'vix': float(latest_data[close_index])
                }
                context['task_instance'].xcom_push(key='vix_data', value=result)
                return result
        
        raise Exception("Could not parse VIX data")
        
    except Exception as e:
        raise Exception(f"Failed to fetch VIX data: {str(e)}")

def fetch_dxy_data(**context):
    """Fetch DXY data from Stooq"""
    
    url = "https://stooq.com/q/d/l/?s=dx.c&d1&i=d"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse CSV data
        lines = response.text.strip().split('\n')
        
        if len(lines) >= 2:
            headers = lines[0].split(',')
            latest_data = lines[-1].split(',')
            
            # Find indices for Date and Close
            date_index = next((i for i, header in enumerate(headers) if header.strip() == 'Date'), None)
            close_index = next((i for i, header in enumerate(headers) if header.strip() == 'Close'), None)
            
            if date_index is not None and close_index is not None:
                result = {
                    'date': latest_data[date_index],
                    'dxy': float(latest_data[close_index])
                }
                context['task_instance'].xcom_push(key='dxy_data', value=result)
                return result
        
        raise Exception("Could not parse DXY data")
        
    except Exception as e:
        raise Exception(f"Failed to fetch DXY data: {str(e)}")

def fetch_gold_data(**context):
    """Fetch Gold data from Stooq"""
    
    url = "https://stooq.com/q/d/l/?s=xauusd&d1&i=d"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse CSV data
        lines = response.text.strip().split('\n')
        
        if len(lines) >= 2:
            headers = lines[0].split(',')
            latest_data = lines[-1].split(',')
            
            # Find indices for Date and Close
            date_index = next((i for i, header in enumerate(headers) if header.strip() == 'Date'), None)
            close_index = next((i for i, header in enumerate(headers) if header.strip() == 'Close'), None)
            
            if date_index is not None and close_index is not None:
                result = {
                    'date': latest_data[date_index],
                    'gold': float(latest_data[close_index])
                }
                context['task_instance'].xcom_push(key='gold_data', value=result)
                return result
        
        raise Exception("Could not parse Gold data")
        
    except Exception as e:
        raise Exception(f"Failed to fetch Gold data: {str(e)}")

def combine_market_data(**context):
    """Combine all market data into a single record"""
    
    nasdaq_data = context['task_instance'].xcom_pull(task_ids='fetch_nasdaq_data', key='nasdaq_data') or {}
    vix_data = context['task_instance'].xcom_pull(task_ids='fetch_vix_data', key='vix_data') or {}
    dxy_data = context['task_instance'].xcom_pull(task_ids='fetch_dxy_data', key='dxy_data') or {}
    gold_data = context['task_instance'].xcom_pull(task_ids='fetch_gold_data', key='gold_data') or {}
    
    # Extract the common date and individual values
    combined_data = {
        'date': nasdaq_data.get('date') or vix_data.get('date') or dxy_data.get('date') or gold_data.get('date'),
        'nasdaq': nasdaq_data.get('nasdaq'),
        'vix': vix_data.get('vix'),
        'dxy': dxy_data.get('dxy'),
        'gold': gold_data.get('gold')
    }
    
    print(f"Combined market data: {combined_data}")
    
    context['task_instance'].xcom_push(key='combined_data', value=combined_data)
    return combined_data

def generate_snowflake_query(**context):
    """Generate Snowflake MERGE query"""
    
    combined_data = context['task_instance'].xcom_pull(task_ids='combine_market_data', key='combined_data')
    
    if not combined_data or not combined_data.get('date'):
        raise Exception("No valid market data to insert")
    
    # Generate MERGE query
    merge_query = f"""
MERGE INTO BITCOIN_DATA.DATA.FINANCIAL_MARKET_DATA AS target
USING (
  SELECT 
    '{combined_data['date']}'::DATE as DATE,
    {combined_data['nasdaq']} as NASDAQ,
    {combined_data['vix']} as VIX,
    {combined_data['dxy']} as DXY,
    {combined_data['gold']} as GOLD
) AS source
ON target.DATE = source.DATE
WHEN MATCHED THEN
  UPDATE SET
    NASDAQ = source.NASDAQ,
    VIX = source.VIX,
    DXY = source.DXY,
    GOLD = source.GOLD,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (DATE, NASDAQ, VIX, DXY, GOLD)
  VALUES (source.DATE, source.NASDAQ, source.VIX, source.DXY, source.GOLD);
"""
    
    context['task_instance'].xcom_push(key='merge_query', value=merge_query)
    return merge_query

def log_success(**context):
    """Log successful data update"""
    
    combined_data = context['task_instance'].xcom_pull(task_ids='combine_market_data', key='combined_data')
    init_count = context['task_instance'].xcom_pull(task_ids='initialize_historical_market_data', key='init_record_count')
    
    if init_count:
        print(f'🎉 Historical initialization complete: {init_count} records loaded')
    
    print('Successfully updated financial market data')
    print(f'Data inserted/updated: {combined_data}')
    
    return {
        'status': 'success',
        'message': 'Financial market data updated successfully',
        'timestamp': datetime.now().isoformat(),
        'data': combined_data
    }

# ─── Task Definitions ──────────────────────────────────────────────────

# Step 1: Ensure DB infrastructure exists
ensure_db_task = PythonOperator(
    task_id='ensure_schema_and_table',
    python_callable=ensure_schema_and_table,
    dag=dag,
)

# Step 2: Check if historical data needs initialization
check_history_task = BranchPythonOperator(
    task_id='check_historical_data',
    python_callable=check_historical_data,
    dag=dag,
)

# Step 3a: Initialize historical data (recovery path)
init_historical_task = PythonOperator(
    task_id='initialize_historical_market_data',
    python_callable=initialize_historical_market_data,
    dag=dag,
)

# Step 3b: Normal daily path - check weekend
date_check_task = PythonOperator(
    task_id='check_if_weekend',
    python_callable=check_if_weekend,
    dag=dag,
)

# Join point after init or daily path
join_task = EmptyOperator(
    task_id='join_paths',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Daily fetch tasks
fetch_nasdaq_task = PythonOperator(
    task_id='fetch_nasdaq_data',
    python_callable=fetch_nasdaq_data,
    dag=dag,
)

fetch_vix_task = PythonOperator(
    task_id='fetch_vix_data',
    python_callable=fetch_vix_data,
    dag=dag,
)

fetch_dxy_task = PythonOperator(
    task_id='fetch_dxy_data',
    python_callable=fetch_dxy_data,
    dag=dag,
)

fetch_gold_task = PythonOperator(
    task_id='fetch_gold_data',
    python_callable=fetch_gold_data,
    dag=dag,
)

combine_data_task = PythonOperator(
    task_id='combine_market_data',
    python_callable=combine_market_data,
    dag=dag,
)

generate_query_task = PythonOperator(
    task_id='generate_snowflake_query',
    python_callable=generate_snowflake_query,
    dag=dag,
)

execute_merge_task = SnowflakeOperator(
    task_id='execute_snowflake_merge',
    sql="{{ task_instance.xcom_pull(task_ids='generate_snowflake_query', key='merge_query') }}",
    dag=dag,
    **snowflake_conn_params,
)

success_log_task = PythonOperator(
    task_id='log_success',
    python_callable=log_success,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# ─── Task Dependencies ─────────────────────────────────────────────────
# 
#  ensure_schema_and_table >> check_historical_data
#       ├── initialize_historical_market_data >> join_paths
#       └── check_if_weekend >> [fetch_*] >> combine >> generate >> merge >> join_paths
#  join_paths >> log_success

ensure_db_task >> check_history_task >> [init_historical_task, date_check_task]

# Recovery path
init_historical_task >> join_task

# Normal daily path
date_check_task >> [fetch_nasdaq_task, fetch_vix_task, fetch_dxy_task, fetch_gold_task] >> combine_data_task >> generate_query_task >> execute_merge_task >> join_task

# Final
join_task >> success_log_task

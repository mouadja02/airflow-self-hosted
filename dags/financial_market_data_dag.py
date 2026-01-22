"""
Financial Market Data DAG
Replicates the n8n workflow for fetching financial market data (NASDAQ, VIX, DXY, Gold)
"""

from datetime import datetime, timedelta
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Default arguments
default_args = {
    'owner': 'airflow',
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
    GOLD = source.GOLD
WHEN NOT MATCHED THEN
  INSERT (DATE, NASDAQ, VIX, DXY, GOLD)
  VALUES (source.DATE, source.NASDAQ, source.VIX, source.DXY, source.GOLD);
"""
    
    context['task_instance'].xcom_push(key='merge_query', value=merge_query)
    return merge_query

def log_success(**context):
    """Log successful data update"""
    
    combined_data = context['task_instance'].xcom_pull(task_ids='combine_market_data', key='combined_data')
    
    print('Successfully updated financial market data')
    print(f'Data inserted/updated: {combined_data}')
    
    return {
        'status': 'success',
        'message': 'Financial market data updated successfully',
        'timestamp': datetime.now().isoformat(),
        'data': combined_data
    }

# Snowflake connection parameters
snowflake_conn_params = {
    'snowflake_conn_id': 'snowflake_default',
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'INT_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'BITCOIN_DATA'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'DATA'),
}

# Define tasks
date_check_task = PythonOperator(
    task_id='check_if_weekend',
    python_callable=check_if_weekend,
    dag=dag,
)

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
    dag=dag,
)

# Set task dependencies
date_check_task >> [fetch_nasdaq_task, fetch_vix_task, fetch_dxy_task, fetch_gold_task] >> combine_data_task >> generate_query_task >> execute_merge_task >> success_log_task 

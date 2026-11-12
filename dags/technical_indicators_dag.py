"""
Technical Indicators DAG
Automatically updates the HOURLY_TA table with technical indicators
when new data is available in BTC_HOURLY_DATA
Includes historical initialization with batching and MERGE logic to avoid duplicates
Uses TA-Lib for comprehensive technical analysis
"""

from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from bitcoin_indicators import add_technical_indicators_talib, merge_records
from bitcoin_cloud import BitcoinCloudOperator, cloud_enabled
from bitcoin_pipeline_helpers import (
    PRICE_ASSET, INDICATORS_ASSET, HOUR, asset_update_range,
    closed_hour_cutoff, validate_indicator_alignment,
)

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
    'technical_indicators_update',
    default_args=default_args,
    description='Update HOURLY_TA table with technical indicators using TA-Lib',
    schedule=[PRICE_ASSET],
    catchup=False,
    tags=['bitcoin', 'technical-analysis', 'hourly', 'snowflake'],
    max_active_runs=1,
)


def ensure_schema_and_table(**context):
    """Create database, schema and HOURLY_TA table in Snowflake if they don't exist"""
    
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Create database if not exists
    hook.run("CREATE DATABASE IF NOT EXISTS BITCOIN_DATA")
    print("✅ Database BITCOIN_DATA ensured")
    
    # Create schema if not exists
    hook.run("CREATE SCHEMA IF NOT EXISTS BITCOIN_DATA.DATA")
    print("✅ Schema BITCOIN_DATA.DATA ensured")
    
    # Create HOURLY_TA table if not exists with primary key
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS BITCOIN_DATA.DATA.HOURLY_TA (
        UNIX_TIMESTAMP NUMBER(38,0) PRIMARY KEY,
        datetime TIMESTAMP_NTZ,
        OPEN FLOAT,
        HIGH FLOAT,
        CLOSE FLOAT,
        LOW FLOAT,
        VOLUME FLOAT,
        -- Overlap Studies
        sma_5 FLOAT, sma_10 FLOAT, sma_20 FLOAT, sma_50 FLOAT, sma_100 FLOAT, sma_200 FLOAT,
        ema_5 FLOAT, ema_10 FLOAT, ema_12 FLOAT, ema_20 FLOAT, ema_26 FLOAT, ema_50 FLOAT,
        wma_10 FLOAT, wma_20 FLOAT, dema_10 FLOAT, dema_20 FLOAT,
        tema_10 FLOAT, tema_20 FLOAT, trima_20 FLOAT, kama_20 FLOAT, t3_5 FLOAT,
        bb_upper FLOAT, bb_middle FLOAT, bb_lower FLOAT,
        -- Momentum Indicators
        rsi_7 FLOAT, rsi_14 FLOAT, rsi_21 FLOAT,
        macd FLOAT, macd_signal FLOAT, macd_hist FLOAT,
        slowk FLOAT, slowd FLOAT, fastk FLOAT, fastd FLOAT,
        stochrsi_fastk FLOAT, stochrsi_fastd FLOAT,
        cci_14 FLOAT, cci_20 FLOAT, cmo_14 FLOAT,
        mom_10 FLOAT, roc_10 FLOAT, rocp_10 FLOAT, rocr_10 FLOAT,
        willr_14 FLOAT, ppo FLOAT, apo FLOAT, bop FLOAT, ultosc FLOAT,
        -- Volume Indicators
        ad FLOAT, adosc FLOAT, obv FLOAT, mfi_14 FLOAT,
        -- Volatility Indicators
        atr_14 FLOAT, natr_14 FLOAT, trange FLOAT,
        -- Price Transform
        avgprice FLOAT, medprice FLOAT, typprice FLOAT, wclprice FLOAT,
        -- Trend Indicators
        adx_14 FLOAT, adxr_14 FLOAT, dx_14 FLOAT,
        minus_di FLOAT, plus_di FLOAT, minus_dm FLOAT, plus_dm FLOAT,
        aroon_down FLOAT, aroon_up FLOAT, aroonosc FLOAT, sar FLOAT,
        -- Statistical Functions
        beta FLOAT, correl FLOAT, linearreg FLOAT, linearreg_angle FLOAT,
        linearreg_intercept FLOAT, linearreg_slope FLOAT,
        stddev FLOAT, tsf FLOAT, var FLOAT,
        -- Hilbert Transform
        ht_dcperiod FLOAT, ht_dcphase FLOAT, ht_trendmode FLOAT,
        ht_sine FLOAT, ht_leadsine FLOAT, ht_inphase FLOAT, ht_quadrature FLOAT, ht_trendline FLOAT,
        -- Cycle Indicators
        mama FLOAT, fama FLOAT,
        -- Pattern Recognition
        cdl_doji FLOAT, cdl_hammer FLOAT, cdl_inverted_hammer FLOAT,
        cdl_hanging_man FLOAT, cdl_shooting_star FLOAT, cdl_engulfing FLOAT,
        cdl_morning_star FLOAT, cdl_evening_star FLOAT,
        cdl_three_white_soldiers FLOAT, cdl_three_black_crows FLOAT,
        cdl_harami FLOAT, cdl_dark_cloud_cover FLOAT, cdl_piercing FLOAT,
        cdl_marubozu FLOAT, cdl_spinning_top FLOAT,
        -- Custom Features
        price_change FLOAT, high_low_ratio FLOAT, close_open_ratio FLOAT,
        volatility_30d FLOAT, price_volatility_30d FLOAT, hl_volatility_30d FLOAT,
        -- Metadata
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    hook.run(create_table_sql)
    print("✅ Table BITCOIN_DATA.DATA.HOURLY_TA ensured")


def check_table_status(**context):
    """
    Check if HOURLY_TA table needs initialization or just delta update
    Returns: 'initialize' or 'delta_update'
    """
    try:
        print("🔍 Checking HOURLY_TA table status...")
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Check if HOURLY_TA table has data
        count_query = """
        SELECT COUNT(*) as record_count
        FROM BITCOIN_DATA.DATA.HOURLY_TA
        """
        
        result = hook.get_first(count_query)
        record_count = result[0] if result else 0
        
        print(f"📊 HOURLY_TA has {record_count} records")
        
        # Get source table record count
        source_count_query = """
        SELECT COUNT(*) as record_count
        FROM BITCOIN_DATA.DATA.BTC_HOURLY_DATA
        """
        
        source_result = hook.get_first(source_count_query)
        source_count = source_result[0] if source_result else 0
        
        print(f"📊 BTC_HOURLY_DATA has {source_count} records")
        
        # Decide initialization vs delta
        if record_count == 0 and source_count > 0:
            print("🔄 Table is empty - will initialize with historical data")
            context['ti'].xcom_push(key='operation_mode', value='initialize')
            context['ti'].xcom_push(key='source_count', value=source_count)
            return 'initialize_historical_data'
        elif record_count > 0:
            # Check if we have new data to process
            latest_ta_query = """
            SELECT COALESCE(MAX(UNIX_TIMESTAMP), 0) as latest_timestamp
            FROM BITCOIN_DATA.DATA.HOURLY_TA
            """
            
            latest_ta_result = hook.get_first(latest_ta_query)
            latest_ta_timestamp = latest_ta_result[0] if latest_ta_result else 0
            
            latest_btc_query = """
            SELECT COALESCE(MAX(UNIX_TIMESTAMP), 0) as latest_timestamp
            FROM BITCOIN_DATA.DATA.BTC_HOURLY_DATA
            """
            
            latest_btc_result = hook.get_first(latest_btc_query)
            latest_btc_timestamp = latest_btc_result[0] if latest_btc_result else 0
            
            missing_result = hook.get_first("""
                SELECT MIN(s.UNIX_TIMESTAMP) FROM BITCOIN_DATA.DATA.BTC_HOURLY_DATA s
                LEFT JOIN BITCOIN_DATA.DATA.HOURLY_TA t ON s.UNIX_TIMESTAMP = t.UNIX_TIMESTAMP
                WHERE s.UNIX_TIMESTAMP >= (SELECT MIN(UNIX_TIMESTAMP) FROM BITCOIN_DATA.DATA.HOURLY_TA)
                  AND t.UNIX_TIMESTAMP IS NULL
            """)
            missing_timestamp = missing_result[0] if missing_result else None
            change = asset_update_range(context)
            if latest_btc_timestamp > latest_ta_timestamp or missing_timestamp is not None or change:
                gap = (latest_btc_timestamp - latest_ta_timestamp) / 3600  # Convert to hours
                print(f"✅ Found new data - {gap:.1f} hours gap")
                context['ti'].xcom_push(key='operation_mode', value='delta')
                context['ti'].xcom_push(key='latest_ta_timestamp', value=latest_ta_timestamp)
                context['ti'].xcom_push(key='latest_btc_timestamp', value=latest_btc_timestamp)
                starts = [int(latest_ta_timestamp)]
                if missing_timestamp is not None:
                    starts.append(int(missing_timestamp))
                if change:
                    starts.append(change['start_timestamp'])
                context['ti'].xcom_push(key='calculation_start', value=min(starts))
                return 'process_delta_data'
            else:
                print("ℹ️ No new data to process")
                context['ti'].xcom_push(key='operation_mode', value='skip')
                return 'skip_processing'
        else:
            print("⚠️ Source table is empty - skipping")
            context['ti'].xcom_push(key='operation_mode', value='skip')
            return 'skip_processing'
            
    except Exception as e:
        print(f"❌ Error checking table status: {str(e)}")
        raise

def initialize_historical_data(**context):
    """
    Initialize HOURLY_TA with all historical data from BTC_HOURLY_DATA
    Processes data in batches for memory efficiency
    Uses MERGE to handle duplicates
    """
    try:
        print("🚀 Starting historical data initialization...")
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Get all data from BTC_HOURLY_DATA (we need enough history for 30-day indicators)
        query = """
        SELECT 
            UNIX_TIMESTAMP,
            OPEN,
            HIGH,
            CLOSE,
            LOW,
            VOLUME_USD as VOLUME
        FROM BITCOIN_DATA.DATA.BTC_HOURLY_DATA
        ORDER BY UNIX_TIMESTAMP ASC
        """
        
        print("📥 Fetching all historical data...")
        results = hook.get_records(query)
        
        if not results:
            print("❌ No data found in BTC_HOURLY_DATA")
            return 0
        
        # Convert to DataFrame
        columns = ['UNIX_TIMESTAMP', 'OPEN', 'HIGH', 'CLOSE', 'LOW', 'VOLUME']
        df = pd.DataFrame(results, columns=columns)
        
        print(f"✅ Fetched {len(df)} historical records")
        
        # Add datetime column
        df['datetime'] = pd.to_datetime(df['UNIX_TIMESTAMP'], unit='s')
        
        # Calculate technical indicators for ALL data
        print("🔧 Calculating comprehensive technical indicators using TA-Lib...")
        df = add_technical_indicators_talib(df)
        
        # Skip first 720 rows (30 days) as they won't have complete 30-day indicators
        df_to_insert = df.iloc[720:].copy()
        
        print(f"📊 Prepared {len(df_to_insert)} records for insertion (skipped first 720 for indicator warmup)")
        
        # Insert in batches of 2000 to avoid memory issues
        batch_size = 2000
        total_merged = 0
        
        for i in range(0, len(df_to_insert), batch_size):
            batch_df = df_to_insert.iloc[i:i+batch_size]
            
            print(f"📤 Merging batch {i//batch_size + 1}/{(len(df_to_insert)-1)//batch_size + 1} ({len(batch_df)} records)...")
            merged_count = merge_records(hook, batch_df)
            
            total_merged += merged_count
            print(f"✅ Progress: {total_merged}/{len(df_to_insert)} records processed")
        
        print(f"🎉 Historical initialization complete! Processed {total_merged} records")
        return total_merged
        
    except Exception as e:
        print(f"❌ Error during historical initialization: {str(e)}")
        raise

def process_delta_data(**context):
    """
    Process the full changed range, including repaired gaps, with indicator warmup.
    Uses MERGE to handle duplicates
    """
    try:
        print("🔄 Processing delta update...")
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        latest_ta_timestamp = context['ti'].xcom_pull(key='latest_ta_timestamp', task_ids='check_table_status')
        calculation_start = context['ti'].xcom_pull(key='calculation_start', task_ids='check_table_status')
        if calculation_start is None:
            calculation_start = int(latest_ta_timestamp) + HOUR
        calculation_start = int(calculation_start)
        cutoff = closed_hour_cutoff()
        
        # Fetch last 1000 hours to ensure we have enough data for all indicators
        # This ensures continuity for 30-day (720 hour) indicators
        query = f"""
        SELECT 
            UNIX_TIMESTAMP,
            OPEN,
            HIGH,
            CLOSE,
            LOW,
            VOLUME_USD as VOLUME
        FROM BITCOIN_DATA.DATA.BTC_HOURLY_DATA
        WHERE UNIX_TIMESTAMP >= {calculation_start - 1000 * HOUR}
          AND UNIX_TIMESTAMP < {cutoff}
        ORDER BY UNIX_TIMESTAMP ASC
        """
        
        print("📥 Fetching recent data for delta update...")
        results = hook.get_records(query)
        
        if not results:
            print("❌ No data found")
            return 0
        
        # Convert to DataFrame
        columns = ['UNIX_TIMESTAMP', 'OPEN', 'HIGH', 'CLOSE', 'LOW', 'VOLUME']
        df = pd.DataFrame(results, columns=columns)
        
        # Sort ascending for proper calculation
        df = df.sort_values('UNIX_TIMESTAMP').reset_index(drop=True)
        
        print(f"✅ Fetched {len(df)} records for processing")
        
        # Add datetime column
        df['datetime'] = pd.to_datetime(df['UNIX_TIMESTAMP'], unit='s')
        
        # Calculate technical indicators
        print("🔧 Calculating comprehensive technical indicators using TA-Lib...")
        df = add_technical_indicators_talib(df)
        
        # Filter only new records (but we'll merge all to handle updates)
        new_df = df[df['UNIX_TIMESTAMP'] >= calculation_start].copy()
        
        if len(new_df) == 0:
            print("ℹ️ No new records to process after filtering")
            return 0
        
        print(f"📊 Prepared {len(new_df)} new records for merge")
        
        # Merge records (insert new, update existing)
        merged_count = 0
        for offset in range(0, len(new_df), 1000):
            merged_count += merge_records(hook, new_df.iloc[offset:offset + 1000])
        
        print(f"✅ Delta update complete! Processed {merged_count} records")
        return merged_count
        
    except Exception as e:
        print(f"❌ Error during delta update: {str(e)}")
        raise


def validate_indicators(**context):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    through = validate_indicator_alignment(hook, closed_hour_cutoff())
    change = asset_update_range(context) or {'start_timestamp': through, 'through_timestamp': through}
    change['through_timestamp'] = through
    context['outlet_events'][INDICATORS_ASSET].extra = change
    print(f"Verified complete indicator coverage through {through}")
    return change



# Define tasks
ensure_db_task = PythonOperator(
    task_id='ensure_schema_and_table',
    python_callable=ensure_schema_and_table,
    dag=dag,
)

check_status_task = BranchPythonOperator(
    task_id='check_table_status',
    python_callable=check_table_status,
    dag=dag,
)

if cloud_enabled():
    initialize_task = BitcoinCloudOperator(
        task_id='initialize_historical_data', action='calculate', initialize=True, dag=dag)
    delta_update_task = BitcoinCloudOperator(
        task_id='process_delta_data', action='calculate', dag=dag)
else:
    initialize_task = PythonOperator(
        task_id='initialize_historical_data', python_callable=initialize_historical_data, dag=dag)
    delta_update_task = PythonOperator(
        task_id='process_delta_data', python_callable=process_delta_data, dag=dag)

skip_task = EmptyOperator(
    task_id='skip_processing',
    dag=dag,
)

# Join task to merge branches
end_task = EmptyOperator(
    task_id='end_processing',
    dag=dag,
    trigger_rule='none_failed_min_one_success',
)

validate_indicators_task = PythonOperator(
    task_id='validate_indicators',
    python_callable=validate_indicators,
    outlets=[INDICATORS_ASSET],
    dag=dag,
)

# Set task dependencies
ensure_db_task >> check_status_task >> [initialize_task, delta_update_task, skip_task]
initialize_task >> end_task
delta_update_task >> end_task
skip_task >> end_task
end_task >> validate_indicators_task


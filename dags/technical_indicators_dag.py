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
import talib

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
    'technical_indicators_update',
    default_args=default_args,
    description='Update HOURLY_TA table with technical indicators using TA-Lib',
    schedule='15 * * * *',
    catchup=False,
    tags=['bitcoin', 'technical-analysis', 'hourly', 'snowflake'],
    max_active_runs=1,
)


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
            
            if latest_btc_timestamp > latest_ta_timestamp:
                gap = (latest_btc_timestamp - latest_ta_timestamp) / 3600  # Convert to hours
                print(f"✅ Found new data - {gap:.1f} hours gap")
                context['ti'].xcom_push(key='operation_mode', value='delta')
                context['ti'].xcom_push(key='latest_ta_timestamp', value=latest_ta_timestamp)
                context['ti'].xcom_push(key='latest_btc_timestamp', value=latest_btc_timestamp)
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
    Process only new data (last 1000+ hours for indicator continuity)
    Uses MERGE to handle duplicates
    """
    try:
        print("🔄 Processing delta update...")
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        latest_ta_timestamp = context['ti'].xcom_pull(key='latest_ta_timestamp', task_ids='check_table_status')
        
        # Fetch last 1000 hours to ensure we have enough data for all indicators
        # This ensures continuity for 30-day (720 hour) indicators
        query = """
        SELECT 
            UNIX_TIMESTAMP,
            OPEN,
            HIGH,
            CLOSE,
            LOW,
            VOLUME_USD as VOLUME
        FROM BITCOIN_DATA.DATA.BTC_HOURLY_DATA
        ORDER BY UNIX_TIMESTAMP DESC
        LIMIT 1000
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
        new_df = df[df['UNIX_TIMESTAMP'] > latest_ta_timestamp].copy()
        
        if len(new_df) == 0:
            print("ℹ️ No new records to process after filtering")
            return 0
        
        print(f"📊 Prepared {len(new_df)} new records for merge")
        
        # Merge records (insert new, update existing)
        merged_count = merge_records(hook, new_df)
        
        print(f"✅ Delta update complete! Processed {merged_count} records")
        return merged_count
        
    except Exception as e:
        print(f"❌ Error during delta update: {str(e)}")
        raise

def add_technical_indicators_talib(df: pd.DataFrame) -> pd.DataFrame:
    """Add comprehensive technical indicators using TA-Lib"""
    print("🔧 Calculating comprehensive technical indicators with TA-Lib...")
    
    try:
        # Convert to numpy arrays for TA-Lib
        open_prices = df['OPEN'].values
        high_prices = df['HIGH'].values
        low_prices = df['LOW'].values
        close_prices = df['CLOSE'].values
        volume = df['VOLUME'].values
        
        # Dictionary to store all new columns (more efficient than adding one by one)
        new_columns = {}
        
        # === OVERLAP STUDIES ===
        new_columns['sma_5'] = talib.SMA(close_prices, timeperiod=5)
        new_columns['sma_10'] = talib.SMA(close_prices, timeperiod=10)
        new_columns['sma_20'] = talib.SMA(close_prices, timeperiod=20)
        new_columns['sma_50'] = talib.SMA(close_prices, timeperiod=50)
        new_columns['sma_100'] = talib.SMA(close_prices, timeperiod=100)
        new_columns['sma_200'] = talib.SMA(close_prices, timeperiod=200)
        
        new_columns['ema_5'] = talib.EMA(close_prices, timeperiod=5)
        new_columns['ema_10'] = talib.EMA(close_prices, timeperiod=10)
        new_columns['ema_12'] = talib.EMA(close_prices, timeperiod=12)
        new_columns['ema_20'] = talib.EMA(close_prices, timeperiod=20)
        new_columns['ema_26'] = talib.EMA(close_prices, timeperiod=26)
        new_columns['ema_50'] = talib.EMA(close_prices, timeperiod=50)
        
        new_columns['wma_10'] = talib.WMA(close_prices, timeperiod=10)
        new_columns['wma_20'] = talib.WMA(close_prices, timeperiod=20)
        
        new_columns['dema_10'] = talib.DEMA(close_prices, timeperiod=10)
        new_columns['dema_20'] = talib.DEMA(close_prices, timeperiod=20)
        
        new_columns['tema_10'] = talib.TEMA(close_prices, timeperiod=10)
        new_columns['tema_20'] = talib.TEMA(close_prices, timeperiod=20)
        
        new_columns['trima_20'] = talib.TRIMA(close_prices, timeperiod=20)
        new_columns['kama_20'] = talib.KAMA(close_prices, timeperiod=20)
        new_columns['t3_5'] = talib.T3(close_prices, timeperiod=5)
        
        # Bollinger Bands
        new_columns['bb_upper'], new_columns['bb_middle'], new_columns['bb_lower'] = talib.BBANDS(close_prices, timeperiod=20)
        
        # === MOMENTUM INDICATORS ===
        new_columns['rsi_7'] = talib.RSI(close_prices, timeperiod=7)
        new_columns['rsi_14'] = talib.RSI(close_prices, timeperiod=14)
        new_columns['rsi_21'] = talib.RSI(close_prices, timeperiod=21)
        
        # MACD
        new_columns['macd'], new_columns['macd_signal'], new_columns['macd_hist'] = talib.MACD(close_prices)
        
        # Stochastic
        new_columns['slowk'], new_columns['slowd'] = talib.STOCH(high_prices, low_prices, close_prices)
        new_columns['fastk'], new_columns['fastd'] = talib.STOCHF(high_prices, low_prices, close_prices)
        
        # Stochastic RSI
        new_columns['stochrsi_fastk'], new_columns['stochrsi_fastd'] = talib.STOCHRSI(close_prices)
        
        new_columns['cci_14'] = talib.CCI(high_prices, low_prices, close_prices, timeperiod=14)
        new_columns['cci_20'] = talib.CCI(high_prices, low_prices, close_prices, timeperiod=20)
        
        new_columns['cmo_14'] = talib.CMO(close_prices, timeperiod=14)
        new_columns['mom_10'] = talib.MOM(close_prices, timeperiod=10)
        new_columns['roc_10'] = talib.ROC(close_prices, timeperiod=10)
        new_columns['rocp_10'] = talib.ROCP(close_prices, timeperiod=10)
        new_columns['rocr_10'] = talib.ROCR(close_prices, timeperiod=10)
        
        new_columns['willr_14'] = talib.WILLR(high_prices, low_prices, close_prices, timeperiod=14)
        new_columns['ppo'] = talib.PPO(close_prices)
        new_columns['apo'] = talib.APO(close_prices)
        
        new_columns['bop'] = talib.BOP(open_prices, high_prices, low_prices, close_prices)
        new_columns['ultosc'] = talib.ULTOSC(high_prices, low_prices, close_prices)
        
        # === VOLUME INDICATORS ===
        new_columns['ad'] = talib.AD(high_prices, low_prices, close_prices, volume)
        new_columns['adosc'] = talib.ADOSC(high_prices, low_prices, close_prices, volume)
        new_columns['obv'] = talib.OBV(close_prices, volume)
        new_columns['mfi_14'] = talib.MFI(high_prices, low_prices, close_prices, volume, timeperiod=14)
        
        # === VOLATILITY INDICATORS ===
        new_columns['atr_14'] = talib.ATR(high_prices, low_prices, close_prices, timeperiod=14)
        new_columns['natr_14'] = talib.NATR(high_prices, low_prices, close_prices, timeperiod=14)
        new_columns['trange'] = talib.TRANGE(high_prices, low_prices, close_prices)
        
        # === PRICE TRANSFORM ===
        new_columns['avgprice'] = talib.AVGPRICE(open_prices, high_prices, low_prices, close_prices)
        new_columns['medprice'] = talib.MEDPRICE(high_prices, low_prices)
        new_columns['typprice'] = talib.TYPPRICE(high_prices, low_prices, close_prices)
        new_columns['wclprice'] = talib.WCLPRICE(high_prices, low_prices, close_prices)
        
        # === TREND INDICATORS ===
        new_columns['adx_14'] = talib.ADX(high_prices, low_prices, close_prices, timeperiod=14)
        new_columns['adxr_14'] = talib.ADXR(high_prices, low_prices, close_prices, timeperiod=14)
        new_columns['dx_14'] = talib.DX(high_prices, low_prices, close_prices, timeperiod=14)
        
        new_columns['minus_di'] = talib.MINUS_DI(high_prices, low_prices, close_prices, timeperiod=14)
        new_columns['plus_di'] = talib.PLUS_DI(high_prices, low_prices, close_prices, timeperiod=14)
        new_columns['minus_dm'] = talib.MINUS_DM(high_prices, low_prices, timeperiod=14)
        new_columns['plus_dm'] = talib.PLUS_DM(high_prices, low_prices, timeperiod=14)
        
        # Aroon
        new_columns['aroon_down'], new_columns['aroon_up'] = talib.AROON(high_prices, low_prices, timeperiod=14)
        new_columns['aroonosc'] = talib.AROONOSC(high_prices, low_prices, timeperiod=14)
        
        # Parabolic SAR
        new_columns['sar'] = talib.SAR(high_prices, low_prices)
        
        # === STATISTICAL FUNCTIONS ===
        new_columns['beta'] = talib.BETA(high_prices, low_prices, timeperiod=5)
        new_columns['correl'] = talib.CORREL(high_prices, low_prices, timeperiod=30)
        new_columns['linearreg'] = talib.LINEARREG(close_prices, timeperiod=14)
        new_columns['linearreg_angle'] = talib.LINEARREG_ANGLE(close_prices, timeperiod=14)
        new_columns['linearreg_intercept'] = talib.LINEARREG_INTERCEPT(close_prices, timeperiod=14)
        new_columns['linearreg_slope'] = talib.LINEARREG_SLOPE(close_prices, timeperiod=14)
        new_columns['stddev'] = talib.STDDEV(close_prices, timeperiod=20)
        new_columns['tsf'] = talib.TSF(close_prices, timeperiod=14)
        new_columns['var'] = talib.VAR(close_prices, timeperiod=20)
        
        # === HILBERT TRANSFORM ===
        new_columns['ht_dcperiod'] = talib.HT_DCPERIOD(close_prices)
        new_columns['ht_dcphase'] = talib.HT_DCPHASE(close_prices)
        new_columns['ht_trendmode'] = talib.HT_TRENDMODE(close_prices)
        
        new_columns['ht_sine'], new_columns['ht_leadsine'] = talib.HT_SINE(close_prices)
        new_columns['ht_inphase'], new_columns['ht_quadrature'] = talib.HT_PHASOR(close_prices)
        new_columns['ht_trendline'] = talib.HT_TRENDLINE(close_prices)
        
        # === CYCLE INDICATORS ===
        new_columns['mama'], new_columns['fama'] = talib.MAMA(close_prices)
        
        # === PATTERN RECOGNITION (Candlestick Patterns) ===
        new_columns['cdl_doji'] = talib.CDLDOJI(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_hammer'] = talib.CDLHAMMER(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_inverted_hammer'] = talib.CDLINVERTEDHAMMER(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_hanging_man'] = talib.CDLHANGINGMAN(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_shooting_star'] = talib.CDLSHOOTINGSTAR(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_engulfing'] = talib.CDLENGULFING(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_morning_star'] = talib.CDLMORNINGSTAR(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_evening_star'] = talib.CDLEVENINGSTAR(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_three_white_soldiers'] = talib.CDL3WHITESOLDIERS(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_three_black_crows'] = talib.CDL3BLACKCROWS(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_harami'] = talib.CDLHARAMI(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_dark_cloud_cover'] = talib.CDLDARKCLOUDCOVER(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_piercing'] = talib.CDLPIERCING(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_marubozu'] = talib.CDLMARUBOZU(open_prices, high_prices, low_prices, close_prices)
        new_columns['cdl_spinning_top'] = talib.CDLSPINNINGTOP(open_prices, high_prices, low_prices, close_prices)
        
        # === CUSTOM FEATURES ===
        new_columns['price_change'] = close_prices / np.roll(close_prices, 1) - 1
        new_columns['high_low_ratio'] = high_prices / low_prices
        new_columns['close_open_ratio'] = close_prices / open_prices
        
        # 30-day volatility features (720 hours = 30 days)
        window = 720
        new_columns['volatility_30d'] = pd.Series(new_columns['price_change']).rolling(window=window).std() * np.sqrt(24)
        new_columns['price_volatility_30d'] = pd.Series(close_prices).rolling(window=window).std()
        new_columns['hl_volatility_30d'] = pd.Series((high_prices - low_prices) / close_prices).rolling(window=window).mean()
        
        # Merge all new columns at once (avoids DataFrame fragmentation)
        df = pd.concat([df, pd.DataFrame(new_columns, index=df.index)], axis=1)
        
        print(f"✅ Calculated {len(new_columns)} technical indicators")
        return df
        
    except Exception as e:
        print(f"❌ Error calculating technical indicators: {str(e)}")
        raise

def merge_records(hook: SnowflakeHook, df: pd.DataFrame) -> int:
    """
    Merge records into HOURLY_TA table using MERGE statement
    This handles both inserts and updates, avoiding duplicates
    """
    if len(df) == 0:
        return 0
    
    try:
        # Get all column names (excluding UNIX_TIMESTAMP which is the key)
        indicator_columns = [col for col in df.columns if col not in ['UNIX_TIMESTAMP', 'OPEN', 'HIGH', 'CLOSE', 'LOW', 'VOLUME', 'datetime']]
        
        # Prepare VALUES clause
        values_list = []
        for _, row in df.iterrows():
            values = [str(int(row['UNIX_TIMESTAMP']))]
            values.append(f"'{row['datetime'].strftime('%Y-%m-%d %H:%M:%S')}'")
            values.append(f"{float(row['OPEN'])}")
            values.append(f"{float(row['HIGH'])}")
            values.append(f"{float(row['CLOSE'])}")
            values.append(f"{float(row['LOW'])}")
            values.append(f"{float(row['VOLUME'])}")
            
            # Add all indicator values
            for col in indicator_columns:
                val = row[col]
                if pd.isna(val):
                    values.append('NULL')
                else:
                    values.append(f"{float(val)}")
            
            values_list.append(f"({', '.join(values)})")
        
        values_str = ',\n  '.join(values_list)
        
        # Build column list for UPDATE clause
        update_clauses = []
        update_clauses.append("target.datetime = source.datetime")
        update_clauses.append("target.OPEN = source.OPEN")
        update_clauses.append("target.HIGH = source.HIGH")
        update_clauses.append("target.CLOSE = source.CLOSE")
        update_clauses.append("target.LOW = source.LOW")
        update_clauses.append("target.VOLUME = source.VOLUME")
        
        for col in indicator_columns:
            update_clauses.append(f"target.{col} = source.{col}")
        
        # Build INSERT column list
        insert_columns = ['UNIX_TIMESTAMP', 'datetime', 'OPEN', 'HIGH', 'CLOSE', 'LOW', 'VOLUME'] + indicator_columns
        insert_columns_str = ', '.join(insert_columns)
        insert_values_str = ', '.join([f'source.{col}' for col in insert_columns])
        
        # Build MERGE query
        merge_query = f"""
MERGE INTO BITCOIN_DATA.DATA.HOURLY_TA AS target
USING (
  SELECT 
    column1 AS UNIX_TIMESTAMP,
    column2 AS datetime,
    column3 AS OPEN,
    column4 AS HIGH,
    column5 AS CLOSE,
    column6 AS LOW,
    column7 AS VOLUME"""
        
        # Add column definitions for indicators
        if indicator_columns:
            merge_query += ",\n"
            indicator_mappings = []
            for i, col in enumerate(indicator_columns, start=8):
                indicator_mappings.append(f"    column{i} AS {col}")
            merge_query += ",\n".join(indicator_mappings)
        
        merge_query += "\n"
        
        merge_query += f"""  FROM VALUES
  {values_str}
) AS source
ON target.UNIX_TIMESTAMP = source.UNIX_TIMESTAMP
WHEN MATCHED THEN UPDATE SET
  {',\n  '.join(update_clauses)}
WHEN NOT MATCHED THEN INSERT
  ({insert_columns_str})
VALUES
  ({insert_values_str})
"""
        
        # Suppress verbose logging for large MERGE statements
        sf_connector_logger = logging.getLogger('snowflake.connector')
        sf_hook_logger = logging.getLogger('airflow.task.hooks.airflow.providers.snowflake.hooks.snowflake.SnowflakeHook')
        
        original_connector_level = sf_connector_logger.level
        original_hook_level = sf_hook_logger.level
        
        sf_connector_logger.setLevel(logging.WARNING)
        sf_hook_logger.setLevel(logging.WARNING)
        
        # Execute MERGE
        hook.run(merge_query)
        
        # Restore logging levels
        sf_connector_logger.setLevel(original_connector_level)
        sf_hook_logger.setLevel(original_hook_level)
        
        return len(df)
        
    except Exception as e:
        print(f"❌ Error merging records: {str(e)}")
        raise

# Define tasks
check_status_task = BranchPythonOperator(
    task_id='check_table_status',
    python_callable=check_table_status,
    dag=dag,
)

initialize_task = PythonOperator(
    task_id='initialize_historical_data',
    python_callable=initialize_historical_data,
    dag=dag,
)

delta_update_task = PythonOperator(
    task_id='process_delta_data',
    python_callable=process_delta_data,
    dag=dag,
)

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

# Set task dependencies
check_status_task >> [initialize_task, delta_update_task, skip_task]
initialize_task >> end_task
delta_update_task >> end_task
skip_task >> end_task

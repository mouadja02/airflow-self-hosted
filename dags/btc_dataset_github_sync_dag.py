from datetime import datetime, timedelta
import os
import requests
import subprocess
import tempfile
import shutil
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException

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

# BTC DATASET DAG definition
dag = DAG(
    'btc_technical_indicators_dataset',
    default_args=default_args,
    description='Daily refresh of Bitcoin OHLCV with 90+ Technical Indicators to GitHub',
    schedule='30 0 * * *',  # Daily at 00:30 UTC
    catchup=False,
    tags=['bitcoin', 'technical-indicators', 'dataset', 'github'],
)

def get_config(**context):
    """Get and validate configuration"""
    config = {
        'GITHUB_USERNAME': os.getenv('GITHUB_USERNAME', 'mouadja02'),
        'GITHUB_REPO': os.getenv('GITHUB_REPO_BTC_HOURLY', 'bitcoin-technical-indicators-dataset'),
        'GITHUB_TOKEN': os.getenv('GITHUB_TOKEN'),
        'GITHUB_EMAIL': 'mouadpro02@gmail.com',
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE', 'BITCOIN_DATA'),
        'SNOWFLAKE_SCHEMA': 'DATA', 
        'SNOWFLAKE_TABLE': 'HOURLY_TA',
        'SNOWFLAKE_OHLCV_TABLE': 'BTC_HOURLY_DATA',
        'LOCAL_FILE_PATH_WITH_INDICATORS': '/tmp/bitcoin-hourly-technical-indicators.csv',
        'LOCAL_FILE_PATH_OHLCV_ONLY': '/tmp/bitcoin-hourly-ohlcv.csv',
        'CHUNK_SIZE': 10000,
        'TELEGRAM_BOT_TOKEN': os.getenv('TELEGRAM_BOT_TOKEN'),
        'TELEGRAM_CHAT_ID': os.getenv('TELEGRAM_CHAT_ID'),
    }
    
    # Validate required configs
    if not config['GITHUB_TOKEN']:
        raise AirflowException("GITHUB_TOKEN is required but not set")
    
    print("✅ Configuration loaded successfully")
    return config

def export_data_from_snowflake(**context):
    """Export data from Snowflake to CSV using chunked processing"""
    try:
        config = context['task_instance'].xcom_pull(task_ids='get_config')
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # ========== EXPORT 1: OHLCV ONLY (Raw Data) ==========
        print("📊 Step 1a: Exporting raw OHLCV data...")
        
        ohlcv_query = f"""
        SELECT 
            UNIX_TIMESTAMP,
            TO_VARCHAR(DATETIME, 'YYYY-MM-DD HH24:MI:SS') AS DATETIME,
            OPEN, HIGH, CLOSE, LOW, VOLUME_USD AS VOLUME
        FROM {config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}.{config['SNOWFLAKE_OHLCV_TABLE']}
        ORDER BY UNIX_TIMESTAMP ASC;
        """
        
        df_ohlcv = hook.get_pandas_df(ohlcv_query)
        print(f"✅ Fetched {len(df_ohlcv):,} OHLCV rows")
        
        # Write OHLCV-only CSV
        print(f"💾 Writing OHLCV CSV file to {config['LOCAL_FILE_PATH_OHLCV_ONLY']}...")
        df_ohlcv.to_csv(
            config['LOCAL_FILE_PATH_OHLCV_ONLY'],
            index=False,
            encoding='utf-8',
            float_format='%.8f',
            chunksize=config['CHUNK_SIZE']
        )
        
        ohlcv_file_size = os.path.getsize(config['LOCAL_FILE_PATH_OHLCV_ONLY'])
        ohlcv_file_size_mb = ohlcv_file_size / (1024 * 1024)
        print(f"✅ OHLCV CSV file created: {ohlcv_file_size_mb:.2f} MB")
        
        # ========== EXPORT 2: OHLCV + Technical Indicators ==========
        print("\n📊 Step 1b: Counting total records with technical indicators...")
        
        # Get total count
        count_sql = f"SELECT COUNT(*) FROM {config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}.{config['SNOWFLAKE_TABLE']};"
        result = hook.get_first(count_sql)
        total_records = result[0] if result else 0
        
        print(f"📈 Total records to export: {total_records:,}")
        
        print(f"💾 Step 2: Exporting data with technical indicators in chunks of {config['CHUNK_SIZE']:,} rows...")
        
        # Query to fetch data with technical indicators
        query = f"""
        SELECT 
            UNIX_TIMESTAMP,
            TO_VARCHAR(DATETIME, 'YYYY-MM-DD HH24:MI:SS') AS DATETIME,
            OPEN, HIGH, CLOSE, LOW, VOLUME,
            -- Moving Averages
            SMA_5, SMA_10, SMA_20, SMA_50, SMA_100, SMA_200,
            EMA_5, EMA_10, EMA_12, EMA_20, EMA_26, EMA_50,
            WMA_10, WMA_20,
            DEMA_10, DEMA_20, TEMA_10, TEMA_20,
            TRIMA_20, KAMA_20, T3_5,
            -- Bollinger Bands
            BB_UPPER, BB_MIDDLE, BB_LOWER,
            -- Momentum Indicators
            RSI_7, RSI_14, RSI_21,
            MACD, MACD_SIGNAL, MACD_HIST,
            -- Stochastic
            SLOWK, SLOWD, FASTK, FASTD,
            STOCHRSI_FASTK, STOCHRSI_FASTD,
            -- Other Oscillators
            CCI_14, CCI_20, CMO_14,
            MOM_10, ROC_10, WILLR_14,
            PPO, APO, BOP, ULTOSC,
            -- Volume Indicators
            AD, ADOSC, OBV, MFI_14,
            -- Volatility
            ATR_14, NATR_14, TRANGE, TYPPRICE,
            -- Trend Indicators
            ADX_14,
            MINUS_DI, PLUS_DI, MINUS_DM, PLUS_DM,
            AROON_DOWN, AROON_UP, AROONOSC,
            SAR,
            -- Statistical
            BETA, CORREL,
            LINEARREG, LINEARREG_ANGLE, LINEARREG_SLOPE,
            STDDEV,
            -- Hilbert Transform
            HT_TRENDMODE, HT_SINE, HT_LEADSINE, HT_TRENDLINE, MAMA,
            -- Custom Features
            PRICE_CHANGE, HIGH_LOW_RATIO, CLOSE_OPEN_RATIO,
            VOLATILITY_30D, PRICE_VOLATILITY_30D
        FROM {config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}.{config['SNOWFLAKE_TABLE']}
        ORDER BY UNIX_TIMESTAMP ASC;
        """
        
        # Get pandas DataFrame
        df_indicators = hook.get_pandas_df(query)
        
        print(f"✅ Fetched {len(df_indicators):,} rows with indicators")
        
        # Write to CSV with indicators
        print(f"💾 Writing Technical Indicators CSV file to {config['LOCAL_FILE_PATH_WITH_INDICATORS']}...")
        
        df_indicators.to_csv(
            config['LOCAL_FILE_PATH_WITH_INDICATORS'],
            index=False,
            encoding='utf-8',
            float_format='%.8f',
            chunksize=config['CHUNK_SIZE']
        )
        
        # Get file size
        indicators_file_size = os.path.getsize(config['LOCAL_FILE_PATH_WITH_INDICATORS'])
        indicators_file_size_mb = indicators_file_size / (1024 * 1024)
        
        print(f"✅ Technical Indicators CSV file created: {indicators_file_size_mb:.2f} MB")
        
        # Clean up DataFrames from memory
        del df_ohlcv
        del df_indicators
        
        return {
            'record_count': total_records,
            'ohlcv_record_count': len(df_ohlcv) if 'df_ohlcv' in locals() else 0,
            'exported_count': len(df_indicators) if 'df_indicators' in locals() else total_records,
            'column_count': 92,
            'ohlcv_file_path': config['LOCAL_FILE_PATH_OHLCV_ONLY'],
            'indicators_file_path': config['LOCAL_FILE_PATH_WITH_INDICATORS'],
            'ohlcv_file_size_mb': ohlcv_file_size_mb,
            'indicators_file_size_mb': indicators_file_size_mb,
            'success': True
        }
        
    except Exception as e:
        print(f"❌ Error exporting data: {str(e)}")
        raise AirflowException(f"Data export failed: {str(e)}")

def upload_to_github_with_lfs(**context):
    """Upload large CSV files to GitHub using Git LFS"""
    try:
        config = context['task_instance'].xcom_pull(task_ids='get_config')
        export_info = context['task_instance'].xcom_pull(task_ids='export_data')
        
        if not export_info or not export_info.get('success'):
            raise AirflowException("No valid data to upload")
        
        print(f"📤 Uploading files:")
        print(f"  - OHLCV: {export_info['ohlcv_file_path']} ({export_info['ohlcv_file_size_mb']:.2f} MB)")
        print(f"  - With Indicators: {export_info['indicators_file_path']} ({export_info['indicators_file_size_mb']:.2f} MB)")
        
        # Create temporary directory for git operations
        with tempfile.TemporaryDirectory() as tmp_dir:
            print(f"📁 Working directory: {tmp_dir}")
            
            repo_url = f"https://{config['GITHUB_TOKEN']}@github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}.git"
            
            # Clone repository
            print("📥 Cloning repository...")
            subprocess.run(
                ['git', 'clone', repo_url, tmp_dir],
                check=True,
                capture_output=True,
                text=True
            )
            
            # Configure git
            print("🔧 Configuring git...")
            subprocess.run(['git', 'config', 'user.name', config['GITHUB_USERNAME']], cwd=tmp_dir, check=True)
            subprocess.run(['git', 'config', 'user.email', config['GITHUB_EMAIL']], cwd=tmp_dir, check=True)
            
            # Initialize Git LFS
            print("🔧 Initializing Git LFS...")
            subprocess.run(['git', 'lfs', 'install'], cwd=tmp_dir, check=True)
            
            # Track CSV files with LFS
            print("📝 Configuring LFS tracking...")
            subprocess.run(['git', 'lfs', 'track', '*.csv'], cwd=tmp_dir, check=True)
            
            # Add .gitattributes if it was created/modified
            gitattributes_path = os.path.join(tmp_dir, '.gitattributes')
            if os.path.exists(gitattributes_path):
                subprocess.run(['git', 'add', '.gitattributes'], cwd=tmp_dir, check=False)
            
            # Copy both CSV files to repo
            ohlcv_filename = 'bitcoin-hourly-ohlcv.csv'
            indicators_filename = 'bitcoin-hourly-technical-indicators.csv'
            docs_filename = 'TECHNICAL_INDICATORS_CALCULATION.md'
            script_filename = 'calculate_technical_indicators.py'
            
            ohlcv_dest_path = os.path.join(tmp_dir, ohlcv_filename)
            indicators_dest_path = os.path.join(tmp_dir, indicators_filename)
            docs_dest_path = os.path.join(tmp_dir, docs_filename)
            script_dest_path = os.path.join(tmp_dir, script_filename)
            
            # Get the file paths (they're in the parent directory of dags)
            base_dir = os.path.dirname(os.path.dirname(__file__))
            docs_source_path = os.path.join(base_dir, docs_filename)
            script_source_path = os.path.join(base_dir, script_filename)
            
            print(f"📋 Copying files to repository...")
            shutil.copy2(export_info['ohlcv_file_path'], ohlcv_dest_path)
            shutil.copy2(export_info['indicators_file_path'], indicators_dest_path)
            
            # Copy documentation if it exists
            if os.path.exists(docs_source_path):
                shutil.copy2(docs_source_path, docs_dest_path)
                print(f"📄 Copied documentation file")
            
            # Copy Python script if it exists
            if os.path.exists(script_source_path):
                shutil.copy2(script_source_path, script_dest_path)
                print(f"🐍 Copied Python calculation script")
            
            # Add files to git
            print("➕ Adding files to git...")
            subprocess.run(['git', 'add', ohlcv_filename], cwd=tmp_dir, check=True)
            subprocess.run(['git', 'add', indicators_filename], cwd=tmp_dir, check=True)
            if os.path.exists(docs_dest_path):
                subprocess.run(['git', 'add', docs_filename], cwd=tmp_dir, check=True)
            if os.path.exists(script_dest_path):
                subprocess.run(['git', 'add', script_filename], cwd=tmp_dir, check=True)
            
            # Check if there are changes to commit
            print("🔍 Checking for changes...")
            status_result = subprocess.run(
                ['git', 'status', '--porcelain'],
                cwd=tmp_dir,
                capture_output=True,
                text=True,
                check=True
            )
            
            if not status_result.stdout.strip():
                print("ℹ️ No changes detected - files are identical to existing versions")
                print("✅ Repository is already up to date")
                
                return {
                    'success': True,
                    'commit_message': 'No changes - files already up to date',
                    'ohlcv_url': f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{ohlcv_filename}",
                    'indicators_url': f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{indicators_filename}",
                    'skipped': True
                }
            
            # Create commit message
            now = datetime.now()
            date_str = now.strftime('%Y-%m-%d')
            commit_message = f"""📊 Daily Update: Bitcoin Dataset

📅 Date: {date_str}
📈 Records: {export_info['record_count']:,}

📦 Files Updated:
  • bitcoin-hourly-ohlcv.csv ({export_info['ohlcv_file_size_mb']:.2f} MB)
    - Raw OHLCV data (UNIX_TIMESTAMP, DATETIME, OPEN, HIGH, CLOSE, LOW, VOLUME)
  
  • bitcoin-hourly-technical-indicators.csv ({export_info['indicators_file_size_mb']:.2f} MB)
    - OHLCV + {export_info['column_count']} Technical Indicators

Automated update via Airflow"""
            
            print("💾 Creating commit...")
            subprocess.run(['git', 'commit', '-m', commit_message], cwd=tmp_dir, check=True)
            
            # Push to GitHub
            print("⬆️ Pushing to GitHub...")
            result = subprocess.run(
                ['git', 'push', 'origin', 'main'],
                cwd=tmp_dir,
                capture_output=True,
                text=True,
                check=True
            )
            
            print("✅ Successfully uploaded both files to GitHub with LFS!")
            print(f"🔗 OHLCV: https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{ohlcv_filename}")
            print(f"🔗 With Indicators: https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{indicators_filename}")
        
        # Cleanup local files
        if os.path.exists(export_info['ohlcv_file_path']):
            os.remove(export_info['ohlcv_file_path'])
        if os.path.exists(export_info['indicators_file_path']):
            os.remove(export_info['indicators_file_path'])
        print("🧹 Cleaned up local files")
        
        return {
            'success': True,
            'commit_message': commit_message,
            'ohlcv_url': f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{ohlcv_filename}",
            'indicators_url': f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{indicators_filename}",
            'skipped': False
        }
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Git command failed: {e.stderr if e.stderr else e.stdout}")
        raise AirflowException(f"Git operation failed: {e.stderr if e.stderr else e.stdout}")
    except Exception as e:
        print(f"❌ Error uploading to GitHub: {str(e)}")
        raise AirflowException(f"GitHub upload failed: {str(e)}")

def send_telegram_notification(**context):
    """Send success notification via Telegram"""
    try:
        config = context['task_instance'].xcom_pull(task_ids='get_config')
        export_info = context['task_instance'].xcom_pull(task_ids='export_data')
        upload_info = context['task_instance'].xcom_pull(task_ids='upload_to_github')
        
        if not config['TELEGRAM_BOT_TOKEN']:
            print("⚠️ TELEGRAM_BOT_TOKEN not found, skipping notification")
            return
        
        now = datetime.now()
        date_str = now.strftime('%Y-%m-%d %H:%M UTC')
        
        # Check if upload was skipped
        if upload_info.get('skipped'):
            message = f"""ℹ️ *Bitcoin Dataset - No Changes*

📅 *Date*: {date_str}
📈 *Records*: {export_info['record_count']:,}
💾 *Total Size*: {export_info['ohlcv_file_size_mb'] + export_info['indicators_file_size_mb']:.2f} MB
✅ *Status*: Already up-to-date

📦 *Files*:
  • [OHLCV Only]({upload_info['ohlcv_url']})
  • [With Indicators]({upload_info['indicators_url']})

_Automated by Airflow DAG_"""
        else:
            message = f"""✅ *Bitcoin Dataset Updated*

📅 *Date*: {date_str}
📈 *Records*: {export_info['record_count']:,}

📦 *Files Updated*:
  • *OHLCV Only* ({export_info['ohlcv_file_size_mb']:.2f} MB)
    [View on GitHub]({upload_info['ohlcv_url']})
    Raw OHLCV data without indicators
  
  • *With Technical Indicators* ({export_info['indicators_file_size_mb']:.2f} MB)
    [View on GitHub]({upload_info['indicators_url']})
    OHLCV + {export_info['column_count']} indicators

📦 *Storage*: Git LFS

_Automated by Airflow DAG_"""
        
        url = f"https://api.telegram.org/bot{config['TELEGRAM_BOT_TOKEN']}/sendMessage"
        data = {
            'chat_id': config['TELEGRAM_CHAT_ID'],
            'text': message,
            'parse_mode': 'Markdown'
        }
        
        response = requests.post(url, data=data)
        response.raise_for_status()
        
        print("✅ Telegram notification sent successfully")
        return True
        
    except Exception as e:
        print(f"⚠️ Failed to send Telegram notification: {str(e)}")
        return False

# Define tasks
get_config_task = PythonOperator(
    task_id='get_config',
    python_callable=get_config,
    dag=dag,
)

export_data_task = PythonOperator(
    task_id='export_data',
    python_callable=export_data_from_snowflake,
    dag=dag,
)

upload_github_task = PythonOperator(
    task_id='upload_to_github',
    python_callable=upload_to_github_with_lfs,
    dag=dag,
)

telegram_notification_task = PythonOperator(
    task_id='send_telegram_notification',
    python_callable=send_telegram_notification,
    dag=dag,
)

# Set task dependencies
get_config_task >> export_data_task >> upload_github_task >> telegram_notification_task

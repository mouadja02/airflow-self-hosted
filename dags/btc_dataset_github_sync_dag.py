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
        'LOCAL_FILE_PATH': '/tmp/bitcoin-hourly-technical-indicators.csv',
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
        
        print("📊 Step 1: Counting total records...")
        
        # Get total count
        count_sql = f"SELECT COUNT(*) FROM {config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}.{config['SNOWFLAKE_TABLE']};"
        result = hook.get_first(count_sql)
        total_records = result[0] if result else 0
        
        print(f"📈 Total records to export: {total_records:,}")
        
        print(f"💾 Step 2: Exporting data in chunks of {config['CHUNK_SIZE']:,} rows...")
        
        # Query to fetch data
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
        df = hook.get_pandas_df(query)
        
        print(f"✅ Fetched {len(df):,} rows")
        
        # Write to CSV
        print(f"💾 Writing CSV file to {config['LOCAL_FILE_PATH']}...")
        
        df.to_csv(
            config['LOCAL_FILE_PATH'],
            index=False,
            encoding='utf-8',
            float_format='%.8f',
            chunksize=config['CHUNK_SIZE']
        )
        
        # Get file size
        file_size = os.path.getsize(config['LOCAL_FILE_PATH'])
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"✅ CSV file created: {file_size_mb:.2f} MB")
        
        # Clean up DataFrame from memory
        del df
        
        return {
            'record_count': total_records,
            'exported_count': len(df) if 'df' in locals() else total_records,
            'column_count': 92,
            'file_path': config['LOCAL_FILE_PATH'],
            'file_size_mb': file_size_mb,
            'success': True
        }
        
    except Exception as e:
        print(f"❌ Error exporting data: {str(e)}")
        raise AirflowException(f"Data export failed: {str(e)}")

def upload_to_github_with_lfs(**context):
    """Upload large CSV file to GitHub using Git LFS"""
    try:
        config = context['task_instance'].xcom_pull(task_ids='get_config')
        export_info = context['task_instance'].xcom_pull(task_ids='export_data')
        
        if not export_info or not export_info.get('success'):
            raise AirflowException("No valid data to upload")
        
        print(f"📤 Uploading file: {export_info['file_path']} ({export_info['file_size_mb']:.2f} MB)")
        
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
            
            # Copy CSV file to repo
            filename = 'bitcoin-hourly-technical-indicators.csv'
            dest_path = os.path.join(tmp_dir, filename)
            print(f"📋 Copying file to repository...")
            shutil.copy2(export_info['file_path'], dest_path)
            
            # Add file to git
            print("➕ Adding file to git...")
            subprocess.run(['git', 'add', filename], cwd=tmp_dir, check=True)
            
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
                print("ℹ️ No changes detected - file is identical to existing version")
                print("✅ Repository is already up to date")
                
                return {
                    'success': True,
                    'commit_message': 'No changes - file already up to date',
                    'file_url': f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{filename}",
                    'skipped': True
                }
            
            # Create commit message
            now = datetime.now()
            date_str = now.strftime('%Y-%m-%d')
            commit_message = f"""📊 Daily Update: Bitcoin Technical Indicators Dataset

📅 Date: {date_str}
📈 Records: {export_info['record_count']:,}
💾 Size: {export_info['file_size_mb']:.2f} MB
🔍 {export_info['column_count']} Technical Indicators + OHLCV

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
            
            print("✅ Successfully uploaded to GitHub with LFS!")
            print(f"🔗 URL: https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{filename}")
        
        # Cleanup local file
        if os.path.exists(export_info['file_path']):
            os.remove(export_info['file_path'])
            print("🧹 Cleaned up local file")
        
        return {
            'success': True,
            'commit_message': commit_message,
            'file_url': f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{filename}",
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
💾 *File Size*: {export_info['file_size_mb']:.2f} MB
✅ *Status*: Already up-to-date

🔗 [View on GitHub]({upload_info['file_url']})

_Automated by Airflow DAG_"""
        else:
            message = f"""✅ *Bitcoin Technical Indicators Dataset Updated*

📅 *Date*: {date_str}
📈 *Records*: {export_info['record_count']:,}
💾 *File Size*: {export_info['file_size_mb']:.2f} MB
🔍 *Indicators*: {export_info['column_count']} Technical Indicators
📦 *Storage*: Git LFS

🔗 [View on GitHub]({upload_info['file_url']})

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

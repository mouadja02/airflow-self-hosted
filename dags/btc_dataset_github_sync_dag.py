from datetime import datetime, timedelta
import os
import requests
import subprocess
import tempfile
import shutil
import time
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from bitcoin_pipeline_helpers import INDICATORS_ASSET, closed_hour_cutoff, validate_indicator_alignment
from bitcoin_cloud import BitcoinCloudOperator, cloud_enabled, finish_cloud_publication

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
    schedule=[INDICATORS_ASSET],  # Publish only after the indicator update succeeds.
    catchup=False,
    max_active_runs=1,
    tags=['bitcoin', 'technical-indicators', 'dataset', 'github'],
)

def get_config(**context):
    """Get and validate configuration"""
    config = {
        'GITHUB_USERNAME': os.getenv('GITHUB_USERNAME', 'mouadja02'),
        'GITHUB_REPO': os.getenv('GITHUB_REPO_BTC_HOURLY', 'bitcoin-technical-indicators-dataset'),
        'GITHUB_TOKEN': None if cloud_enabled() else os.getenv('GITHUB_TOKEN'),
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
    if not config['GITHUB_TOKEN'] and not cloud_enabled():
        raise AirflowException("GITHUB_TOKEN is required but not set")

    print("✅ Configuration loaded successfully")
    return config


def export_data_from_snowflake(**context):
    """Export data from Snowflake to CSV using chunked processing"""
    try:
        config = context['task_instance'].xcom_pull(task_ids='get_config')
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        cutoff = closed_hour_cutoff()
        through_timestamp = validate_indicator_alignment(hook, cutoff)

        # ========== EXPORT 1: OHLCV ONLY (Raw Data) ==========
        print("📊 Step 1a: Exporting raw OHLCV data...")

        ohlcv_query = f"""
        SELECT
            UNIX_TIMESTAMP,
            TO_TIMESTAMP_NTZ(UNIX_TIMESTAMP) AS DATETIME,
            OPEN, HIGH, CLOSE, LOW, VOLUME_USD, VOLUME_BTC
        FROM {config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}.{config['SNOWFLAKE_OHLCV_TABLE']}
        WHERE UNIX_TIMESTAMP <= {through_timestamp}
        ORDER BY UNIX_TIMESTAMP ASC;
        """

        df_ohlcv = hook.get_pandas_df(ohlcv_query)
        ohlcv_record_count = len(df_ohlcv)
        if df_ohlcv.empty or int(df_ohlcv['UNIX_TIMESTAMP'].max()) != through_timestamp:
            raise AirflowException("OHLCV export does not reach the verified data watermark")
        print(f"✅ Fetched {ohlcv_record_count:,} OHLCV rows")

        print(f"💾 Writing OHLCV CSV file to {config['LOCAL_FILE_PATH_OHLCV_ONLY']}...")
        df_ohlcv.to_csv(
            config['LOCAL_FILE_PATH_OHLCV_ONLY'],
            index=False,
            encoding='utf-8',
            float_format='%.8f',
            chunksize=config['CHUNK_SIZE']
        )
        del df_ohlcv  # Free memory immediately after writing

        ohlcv_file_size_mb = os.path.getsize(config['LOCAL_FILE_PATH_OHLCV_ONLY']) / (1024 * 1024)
        print(f"✅ OHLCV CSV file created: {ohlcv_file_size_mb:.2f} MB")

        # ========== EXPORT 2: OHLCV + Technical Indicators ==========
        print("\n📊 Step 1b: Counting total records with technical indicators...")

        count_sql = f"SELECT COUNT(*) FROM {config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}.{config['SNOWFLAKE_TABLE']} WHERE UNIX_TIMESTAMP <= {through_timestamp};"
        result = hook.get_first(count_sql)
        total_records = result[0] if result else 0
        print(f"📈 Total records to export: {total_records:,}")

        print(f"💾 Step 2: Exporting data with technical indicators in chunks of {config['CHUNK_SIZE']:,} rows...")

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
        WHERE UNIX_TIMESTAMP <= {through_timestamp}
        ORDER BY UNIX_TIMESTAMP ASC;
        """

        df_indicators = hook.get_pandas_df(query)
        indicators_record_count = len(df_indicators)
        if df_indicators.empty or int(df_indicators['UNIX_TIMESTAMP'].max()) != through_timestamp:
            raise AirflowException("Indicator export does not reach the verified data watermark")
        print(f"✅ Fetched {indicators_record_count:,} rows with indicators")

        print(f"💾 Writing Technical Indicators CSV file to {config['LOCAL_FILE_PATH_WITH_INDICATORS']}...")
        df_indicators.to_csv(
            config['LOCAL_FILE_PATH_WITH_INDICATORS'],
            index=False,
            encoding='utf-8',
            float_format='%.8f',
            chunksize=config['CHUNK_SIZE']
        )
        del df_indicators  # Free memory immediately after writing

        indicators_file_size_mb = os.path.getsize(config['LOCAL_FILE_PATH_WITH_INDICATORS']) / (1024 * 1024)
        print(f"✅ Technical Indicators CSV file created: {indicators_file_size_mb:.2f} MB")

        return {
            'record_count': total_records,
            'ohlcv_record_count': ohlcv_record_count,
            'exported_count': indicators_record_count,
            'column_count': 92,
            'ohlcv_file_path': config['LOCAL_FILE_PATH_OHLCV_ONLY'],
            'indicators_file_path': config['LOCAL_FILE_PATH_WITH_INDICATORS'],
            'ohlcv_file_size_mb': ohlcv_file_size_mb,
            'indicators_file_size_mb': indicators_file_size_mb,
            'success': True,
            'through_timestamp': through_timestamp,
        }

    except Exception as e:
        print(f"❌ Error exporting data: {str(e)}")
        raise AirflowException(f"Data export failed: {str(e)}")


def _run_git(cmd, cwd=None, env=None, capture=True):
    """Helper to run git commands with consistent error handling"""
    result = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        capture_output=capture,
        text=True,
    )
    if result.returncode != 0:
        output = result.stderr or result.stdout or ""
        raise subprocess.CalledProcessError(result.returncode, cmd, output=output)
    return result


def _push_commit_with_lfs(repo_dir, branch='main', max_attempts=3):
    """Upload the new commit's LFS objects, then push its Git ref.

    The clone intentionally contains only LFS pointers for existing history, so
    ``git lfs push --all`` cannot be used without downloading every historical
    object first. A normal ref push uploads only objects not already referenced
    by the cloned remote, which includes the files created by this task.
    """
    version = _run_git(['git', 'lfs', 'version'], cwd=repo_dir)
    print(f"🔎 {version.stdout.strip()}")

    print("🔍 Validating LFS pointers in the new commit...")
    _run_git(
        ['git', 'lfs', 'fsck', '--pointers', 'HEAD^..HEAD'],
        cwd=repo_dir,
    )

    for attempt in range(1, max_attempts + 1):
        print(
            f"⬆️ Uploading LFS objects for HEAD "
            f"(attempt {attempt}/{max_attempts})..."
        )
        _run_git(['git', 'lfs', 'push', 'origin', 'HEAD'], cwd=repo_dir)

        try:
            _run_git(
                ['git', 'push', 'origin', f'HEAD:{branch}'],
                cwd=repo_dir,
            )
            return
        except subprocess.CalledProcessError as exc:
            error_msg = (exc.output or str(exc)).lower()
            is_gh009 = (
                'gh009' in error_msg
                or 'lfs object integrity could not be checked' in error_msg
            )
            if not is_gh009 or attempt == max_attempts:
                raise

            delay_seconds = 10 * attempt
            print(
                "⚠️ GitHub could not complete its LFS integrity check; "
                f"retrying in {delay_seconds}s..."
            )
            time.sleep(delay_seconds)


def upload_to_github_with_lfs(**context):
    """Upload large CSV files to GitHub using Git LFS.

    Key fix: GIT_LFS_SKIP_SMUDGE=1 during clone so LFS objects are NOT
    downloaded (only pointers), eliminating bandwidth consumption and the
    'LFS budget exceeded' error on clone.  We only push new content, we
    never need to download the old blobs.
    """
    try:
        config = context['task_instance'].xcom_pull(task_ids='get_config')
        export_info = context['task_instance'].xcom_pull(task_ids='export_data')

        if not export_info or not export_info.get('success'):
            raise AirflowException("No valid data to upload")

        print(f"📤 Uploading files:")
        print(f"  - OHLCV: {export_info['ohlcv_file_path']} ({export_info['ohlcv_file_size_mb']:.2f} MB)")
        print(f"  - With Indicators: {export_info['indicators_file_path']} ({export_info['indicators_file_size_mb']:.2f} MB)")

        # Build env with LFS smudge disabled so clone never downloads LFS blobs
        git_env = os.environ.copy()
        git_env["GIT_LFS_SKIP_SMUDGE"] = "1"

        with tempfile.TemporaryDirectory() as tmp_dir:
            print(f"📁 Working directory: {tmp_dir}")

            repo_url = (
                f"https://{config['GITHUB_TOKEN']}@github.com"
                f"/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}.git"
            )

            # Clone — pointers only, no LFS download
            print("📥 Cloning repository (LFS smudge disabled — pointers only)...")
            _run_git(['git', 'clone', repo_url, tmp_dir], env=git_env)

            # Configure git identity
            print("🔧 Configuring git...")
            _run_git(['git', 'config', 'user.name', config['GITHUB_USERNAME']], cwd=tmp_dir)
            _run_git(['git', 'config', 'user.email', config['GITHUB_EMAIL']], cwd=tmp_dir)

            # Initialize Git LFS and track CSVs
            print("🔧 Initializing repository-local Git LFS hooks...")
            _run_git(
                ['git', 'lfs', 'install', '--local', '--force'],
                cwd=tmp_dir,
            )

            print("📝 Configuring LFS tracking for *.csv...")
            _run_git(['git', 'lfs', 'track', '*.csv'], cwd=tmp_dir)

            gitattributes_path = os.path.join(tmp_dir, '.gitattributes')
            if os.path.exists(gitattributes_path):
                _run_git(['git', 'add', '.gitattributes'], cwd=tmp_dir)

            # File names
            ohlcv_filename = 'bitcoin-hourly-ohlcv.csv'
            indicators_filename = 'bitcoin-hourly-technical-indicators.csv'
            docs_filename = 'TECHNICAL_INDICATORS_CALCULATION.md'
            script_filename = 'calculate_technical_indicators.py'

            ohlcv_dest = os.path.join(tmp_dir, ohlcv_filename)
            indicators_dest = os.path.join(tmp_dir, indicators_filename)
            docs_dest = os.path.join(tmp_dir, docs_filename)
            script_dest = os.path.join(tmp_dir, script_filename)

            base_dir = os.path.dirname(os.path.dirname(__file__))
            docs_src = os.path.join(base_dir, docs_filename)
            script_src = os.path.join(base_dir, script_filename)

            print("📋 Copying files to repository...")
            shutil.copy2(export_info['ohlcv_file_path'], ohlcv_dest)
            shutil.copy2(export_info['indicators_file_path'], indicators_dest)

            if os.path.exists(docs_src):
                shutil.copy2(docs_src, docs_dest)
                print("📄 Copied documentation file")

            if os.path.exists(script_src):
                shutil.copy2(script_src, script_dest)
                print("🐍 Copied Python calculation script")

            # Stage files
            print("➕ Adding files to git...")
            _run_git(['git', 'add', ohlcv_filename], cwd=tmp_dir)
            _run_git(['git', 'add', indicators_filename], cwd=tmp_dir)
            if os.path.exists(docs_dest):
                _run_git(['git', 'add', docs_filename], cwd=tmp_dir)
            if os.path.exists(script_dest):
                _run_git(['git', 'add', script_filename], cwd=tmp_dir)

            # Check for changes
            print("🔍 Checking for changes...")
            status = _run_git(['git', 'status', '--porcelain'], cwd=tmp_dir)

            if not status.stdout.strip():
                print("ℹ️ No changes detected — repository is already up to date")
                return {
                    'success': True,
                    'commit_message': 'No changes - files already up to date',
                    'ohlcv_url': f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{ohlcv_filename}",
                    'indicators_url': f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{indicators_filename}",
                    'skipped': True,
                }

            # Commit
            date_str = datetime.now().strftime('%Y-%m-%d')
            commit_message = f"""📊 Daily Update: Bitcoin Dataset

📅 Date: {date_str}
📈 Records: {export_info['record_count']:,}
🕒 Data through: {datetime.utcfromtimestamp(export_info['through_timestamp']).isoformat()} UTC

📦 Files Updated:
  • bitcoin-hourly-ohlcv.csv ({export_info['ohlcv_file_size_mb']:.2f} MB)
    - Raw OHLCV data (UNIX_TIMESTAMP, DATETIME, OPEN, HIGH, CLOSE, LOW, VOLUME)

  • bitcoin-hourly-technical-indicators.csv ({export_info['indicators_file_size_mb']:.2f} MB)
    - OHLCV + {export_info['column_count']} Technical Indicators

Automated update via Airflow"""

            print("💾 Creating commit...")
            _run_git(['git', 'commit', '-m', commit_message], cwd=tmp_dir)

            # Explicitly upload LFS objects before advancing the Git ref.
            print("⬆️ Uploading LFS objects and pushing to GitHub...")
            _push_commit_with_lfs(tmp_dir)
            commit_sha = _run_git(['git', 'rev-parse', 'HEAD'], cwd=tmp_dir).stdout.strip()
            print(f"Published dataset commit: {commit_sha}")

            print("✅ Successfully uploaded both files to GitHub with LFS!")
            ohlcv_url = f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{ohlcv_filename}"
            indicators_url = f"https://github.com/{config['GITHUB_USERNAME']}/{config['GITHUB_REPO']}/blob/main/{indicators_filename}"
            print(f"🔗 OHLCV: {ohlcv_url}")
            print(f"🔗 With Indicators: {indicators_url}")

        # Cleanup local temp files
        for path in [export_info['ohlcv_file_path'], export_info['indicators_file_path']]:
            if os.path.exists(path):
                os.remove(path)
        print("🧹 Cleaned up local files")

        return {
            'success': True,
            'commit_message': commit_message,
            'ohlcv_url': ohlcv_url,
            'indicators_url': indicators_url,
            'skipped': False,
            'commit_sha': commit_sha,
            'through_timestamp': export_info['through_timestamp'],
        }

    except subprocess.CalledProcessError as e:
        error_msg = e.output or str(e)
        print(f"❌ Git command failed: {error_msg}")
        raise AirflowException(f"Git operation failed: {error_msg}")
    except Exception as e:
        print(f"❌ Error uploading to GitHub: {str(e)}")
        raise AirflowException(f"GitHub upload failed: {str(e)}")


def send_telegram_notification(**context):
    """Send success notification via Telegram"""
    try:
        config = context['task_instance'].xcom_pull(task_ids='get_config')
        export_info = context['task_instance'].xcom_pull(task_ids='export_data')
        upload_info = context['task_instance'].xcom_pull(task_ids='upload_to_github')
        export_info = upload_info.get('export_info', export_info)

        if not config['TELEGRAM_BOT_TOKEN']:
            print("⚠️ TELEGRAM_BOT_TOKEN not found, skipping notification")
            return

        date_str = datetime.now().strftime('%Y-%m-%d %H:%M UTC')

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
            'parse_mode': 'Markdown',
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

if cloud_enabled():
    export_data_task = BitcoinCloudOperator(task_id='export_data', action='publish', dag=dag)
    upload_github_task = PythonOperator(
        task_id='upload_to_github', python_callable=finish_cloud_publication, dag=dag)
else:
    export_data_task = PythonOperator(
        task_id='export_data', python_callable=export_data_from_snowflake, dag=dag)
    upload_github_task = PythonOperator(
        task_id='upload_to_github', python_callable=upload_to_github_with_lfs, dag=dag)

telegram_notification_task = PythonOperator(
    task_id='send_telegram_notification',
    python_callable=send_telegram_notification,
    dag=dag,
)

# Set task dependencies
get_config_task >> export_data_task >> upload_github_task >> telegram_notification_task

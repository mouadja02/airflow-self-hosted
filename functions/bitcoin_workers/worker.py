"""AWS workers: data and CSV bytes stay between Snowflake, Lambda, and GitHub."""

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import boto3
import pandas as pd
import snowflake.connector

from bitcoin_indicators import add_technical_indicators_talib, merge_records


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
RAW = 'BITCOIN_DATA.DATA.BTC_HOURLY_DATA'
TA = 'BITCOIN_DATA.DATA.HOURLY_TA'
HOUR = 3600
INDICATOR_COLUMNS = '''UNIX_TIMESTAMP,DATETIME,OPEN,HIGH,CLOSE,LOW,VOLUME,
SMA_5,SMA_10,SMA_20,SMA_50,SMA_100,SMA_200,EMA_5,EMA_10,EMA_12,EMA_20,EMA_26,EMA_50,
WMA_10,WMA_20,DEMA_10,DEMA_20,TEMA_10,TEMA_20,TRIMA_20,KAMA_20,T3_5,
BB_UPPER,BB_MIDDLE,BB_LOWER,RSI_7,RSI_14,RSI_21,MACD,MACD_SIGNAL,MACD_HIST,
SLOWK,SLOWD,FASTK,FASTD,STOCHRSI_FASTK,STOCHRSI_FASTD,CCI_14,CCI_20,CMO_14,
MOM_10,ROC_10,WILLR_14,PPO,APO,BOP,ULTOSC,AD,ADOSC,OBV,MFI_14,
ATR_14,NATR_14,TRANGE,TYPPRICE,ADX_14,MINUS_DI,PLUS_DI,MINUS_DM,PLUS_DM,
AROON_DOWN,AROON_UP,AROONOSC,SAR,BETA,CORREL,LINEARREG,LINEARREG_ANGLE,LINEARREG_SLOPE,
STDDEV,HT_TRENDMODE,HT_SINE,HT_LEADSINE,HT_TRENDLINE,MAMA,
PRICE_CHANGE,HIGH_LOW_RATIO,CLOSE_OPEN_RATIO,VOLATILITY_30D,PRICE_VOLATILITY_30D'''.replace('\n', '').split(',')


def secret(arn):
    response = boto3.client('secretsmanager').get_secret_value(SecretId=arn)
    return json.loads(response['SecretString'])


class SnowflakeSession:
    """Reuse one connection for all batches rather than reconnecting per MERGE."""

    def __init__(self, connection):
        self.connection = connection

    def run(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)

    def get_records(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def get_first(self, sql):
        return self.get_records(sql)[0]

    def dataframe(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetch_pandas_all()


def aligned_watermark(session, cutoff):
    source = session.get_first(f'SELECT MAX(UNIX_TIMESTAMP) FROM {RAW} WHERE UNIX_TIMESTAMP < {cutoff}')[0]
    target = session.get_first(f'SELECT MAX(UNIX_TIMESTAMP) FROM {TA} WHERE UNIX_TIMESTAMP < {cutoff}')[0]
    missing = session.get_first(f'''SELECT COUNT(*) FROM {RAW} s LEFT JOIN {TA} t
        ON s.UNIX_TIMESTAMP=t.UNIX_TIMESTAMP
        WHERE s.UNIX_TIMESTAMP >= (SELECT MIN(UNIX_TIMESTAMP) FROM {TA})
          AND s.UNIX_TIMESTAMP < {cutoff} AND t.UNIX_TIMESTAMP IS NULL''')[0]
    if source is None or target != source or missing:
        raise ValueError(f'Incomplete indicator coverage: source={source}, indicators={target}, missing={missing}')
    return int(source)


def calculate(session, event):
    initialize = bool(event.get('initialize', False))
    latest = session.get_first(f'SELECT MAX(UNIX_TIMESTAMP) FROM {TA}')[0]
    start_value = event.get('start_timestamp')
    start = int(start_value) if start_value not in (None, '', 'None') else int(latest or 0)
    cutoff = int(event.get('through_timestamp') or 0) + HOUR
    if cutoff <= HOUR:
        cutoff = int(datetime.now(timezone.utc).timestamp()) // HOUR * HOUR
    lower = '' if initialize or latest is None else f'AND UNIX_TIMESTAMP >= {start - 1000 * HOUR}'
    rows = session.get_records(f'''SELECT UNIX_TIMESTAMP,OPEN,HIGH,CLOSE,LOW,VOLUME_USD
        FROM {RAW} WHERE UNIX_TIMESTAMP < {cutoff} {lower} ORDER BY UNIX_TIMESTAMP''')
    if not rows:
        raise ValueError('No source rows available for indicator calculation')
    frame = pd.DataFrame(rows, columns=['UNIX_TIMESTAMP', 'OPEN', 'HIGH', 'CLOSE', 'LOW', 'VOLUME'])
    frame['datetime'] = pd.to_datetime(frame['UNIX_TIMESTAMP'], unit='s')
    frame = add_technical_indicators_talib(frame)
    changed = frame.iloc[720:].copy() if initialize or latest is None else frame[frame['UNIX_TIMESTAMP'] >= start]
    processed = 0
    for offset in range(0, len(changed), 1000):
        processed += merge_records(session, changed.iloc[offset:offset + 1000])
    through = aligned_watermark(session, cutoff)
    if event.get('through_timestamp') and through < int(event['through_timestamp']):
        raise ValueError('Source data does not reach the upstream asset watermark')
    return {'success': True, 'processed_records': processed, 'through_timestamp': through}


def git(args, cwd, env):
    result = subprocess.run(['git', *args], cwd=cwd, env=env, capture_output=True, text=True, timeout=600)
    if result.returncode:
        # Credentials are supplied through askpass, never embedded in the URL or argv.
        raise RuntimeError(f'Git {args[0]} failed: {result.stderr[-2000:]}')
    return result.stdout.strip()


def publish(session, event):
    cutoff = int(datetime.now(timezone.utc).timestamp()) // HOUR * HOUR
    through = aligned_watermark(session, cutoff)
    if event.get('through_timestamp') and through < int(event['through_timestamp']):
        raise ValueError('Source data does not reach the upstream asset watermark')
    credentials = secret(os.environ['GITHUB_SECRET_ARN'])
    owner, repository = credentials['username'], credentials['repository']
    with tempfile.TemporaryDirectory(prefix='bitcoin-') as directory:
        folder = Path(directory)
        paths = [folder / 'bitcoin-hourly-ohlcv.csv', folder / 'bitcoin-hourly-technical-indicators.csv']
        raw = session.dataframe(f'''SELECT UNIX_TIMESTAMP,TO_TIMESTAMP_NTZ(UNIX_TIMESTAMP) AS DATETIME,
            OPEN,HIGH,CLOSE,LOW,VOLUME_USD,VOLUME_BTC FROM {RAW}
            WHERE UNIX_TIMESTAMP <= {through} ORDER BY UNIX_TIMESTAMP''')
        raw_count = len(raw)
        if raw.empty or int(raw['UNIX_TIMESTAMP'].max()) != through:
            raise ValueError('OHLCV export is stale')
        raw.to_csv(paths[0], index=False, encoding='utf-8', float_format='%.8f', chunksize=10000)
        del raw
        columns = ','.join("TO_VARCHAR(DATETIME, 'YYYY-MM-DD HH24:MI:SS') AS DATETIME" if col == 'DATETIME' else col for col in INDICATOR_COLUMNS)
        indicators = session.dataframe(f'SELECT {columns} FROM {TA} WHERE UNIX_TIMESTAMP <= {through} ORDER BY UNIX_TIMESTAMP')
        indicator_count = len(indicators)
        if indicators.empty or int(indicators['UNIX_TIMESTAMP'].max()) != through:
            raise ValueError('Indicator export is stale')
        indicators.to_csv(paths[1], index=False, encoding='utf-8', float_format='%.8f', chunksize=10000)
        del indicators
        result = {
            'success': True, 'record_count': indicator_count, 'exported_count': indicator_count,
            'ohlcv_record_count': raw_count, 'column_count': len(INDICATOR_COLUMNS) - 7, 'through_timestamp': through,
            'ohlcv_file_size_mb': paths[0].stat().st_size / (1024 * 1024),
            'indicators_file_size_mb': paths[1].stat().st_size / (1024 * 1024),
            'ohlcv_url': f'https://github.com/{owner}/{repository}/blob/main/{paths[0].name}',
            'indicators_url': f'https://github.com/{owner}/{repository}/blob/main/{paths[1].name}',
        }
        askpass = folder / 'askpass.sh'
        askpass.write_text('#!/bin/sh\ncase "$1" in\n*Username*) printf "%s\\n" "$BITCOIN_GIT_USERNAME" ;;\n*) printf "%s\\n" "$BITCOIN_GIT_TOKEN" ;;\nesac\n')
        askpass.chmod(0o700)
        env = {**os.environ, 'GIT_LFS_SKIP_SMUDGE': '1', 'GIT_TERMINAL_PROMPT': '0',
               'GIT_ASKPASS': str(askpass), 'BITCOIN_GIT_USERNAME': owner,
               'BITCOIN_GIT_TOKEN': credentials['token'], 'GIT_CONFIG_GLOBAL': '/dev/null'}
        checkout = folder / 'repository'
        git(['clone', '--depth', '1', '--branch', 'main', f'https://github.com/{owner}/{repository}.git', str(checkout)], folder, env)
        git(['config', 'user.name', owner], checkout, env)
        git(['config', 'user.email', credentials.get('email', 'mouadpro02@gmail.com')], checkout, env)
        git(['lfs', 'install', '--local', '--force'], checkout, env)
        git(['lfs', 'track', '*.csv'], checkout, env)
        for path in paths:
            shutil.copy2(path, checkout / path.name)
        git(['add', '.gitattributes', *(path.name for path in paths)], checkout, env)
        result['skipped'] = not bool(git(['status', '--porcelain'], checkout, env))
        if not result['skipped']:
            message = f'Update Bitcoin dataset through {datetime.fromtimestamp(through, timezone.utc).isoformat()} via AWS Lambda'
            git(['commit', '-m', message], checkout, env)
            git(['lfs', 'fsck', '--pointers', 'HEAD^..HEAD'], checkout, env)
            git(['lfs', 'push', 'origin', 'HEAD'], checkout, env)
            git(['push', 'origin', 'HEAD:main'], checkout, env)
        result['commit_sha'] = git(['rev-parse', 'HEAD'], checkout, env)
        return result


def handler(event, context):
    action = event.get('action')
    if action not in ('calculate', 'publish'):
        raise ValueError('Unsupported worker action')
    configuration = secret(os.environ['SNOWFLAKE_SECRET_ARN'])
    logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
    with snowflake.connector.connect(**configuration, login_timeout=30, network_timeout=90) as connection:
        session = SnowflakeSession(connection)
        result = calculate(session, event) if action == 'calculate' else publish(session, event)
    LOGGER.info('Completed %s: %s', action, json.dumps(result))
    return result

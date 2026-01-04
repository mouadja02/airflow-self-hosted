"""
Bitcoin Data Updater Pipeline
===========================================================
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
from functools import partial

# Default arguments
default_args = {
    'owner': 'mouadja02',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'catchup': False,
    'execution_timeout': timedelta(minutes=60),
}

# Define 4 DAGs
batch_schedules = {
    1: '45 6 * * *', 
    2: '50 7 * * *',
    3: '55 8 * * *',
    4: '00 10 * * *',
}

# Create DAG objects
btc_updater_batch1 = DAG(
    'btc_updater_batch1',
    default_args=default_args,
    description='Bitcoin metrics updater - Batch 1',
    schedule=batch_schedules[1],
    max_active_runs=1,
    tags=['bitcoin', 'dataset_updater', 'batch1']
)

btc_updater_batch2 = DAG(
    'btc_updater_batch2',
    default_args=default_args,
    description='Bitcoin metrics updater - Batch 2',
    schedule=batch_schedules[2],
    max_active_runs=1,
    tags=['bitcoin', 'dataset_updater', 'batch2']
)

btc_updater_batch3 = DAG(
    'btc_updater_batch3',
    default_args=default_args,
    description='Bitcoin metrics updater - Batch 3',
    schedule=batch_schedules[3],
    max_active_runs=1,
    tags=['bitcoin', 'dataset_updater', 'batch3']
)

btc_updater_batch4 = DAG(
    'btc_updater_batch4',
    default_args=default_args,
    description='Bitcoin metrics updater - Batch 4',
    schedule=batch_schedules[4],
    max_active_runs=1,
    tags=['bitcoin', 'dataset_updater', 'batch4']
)

# Dictionary for easy access in loops
dags = {
    1: btc_updater_batch1,
    2: btc_updater_batch2,
    3: btc_updater_batch3,
    4: btc_updater_batch4
}

# OPTIMIZED METRIC BATCHES - 4 metrics per batch
batch_metrics = {
    1: [
        # Core Price & Technical Analysis (8 metrics)
        'market_price',           # Current BTC price
        'delta_price_pct',       # Daily momentum
        'mvrv',                  # Market cycle indicator
        'mvrv_zscore',          # Extreme cycle readings
        'nupl',                   # Net unrealized profit/loss
        'puell_multiple',       # Miner revenue/capitulation
        'aviv',             # Active Value to Invested Value
        'asol'                 # The Average Lifespan of Spent Output
    ],
    2: [
        # On-chain Sentiment & Holder Behavior (8 metrics)
        'asopr',                 # Adjusted SOPR (cleaner)
        'sth_mvrv',             # Short-term holder profitability
        'lth_mvrv',             # Long-term holder profitability
        'rhodl_ratio',          # HODL accumulation waves
        'supply_profit',        # % BTC in profit
        'supply_loss',          # % BTC in loss (capitulation)
        'reserve_risk',         # HODLer confidence vs price
        'investor_price'        # Investor price

    ],
    3: [
        # Supply Metrics & HODLer Analysis (8 metrics)
        'supply_current',        # Total BTC supply
        'long_term_hodler_supply',   # LTH accumulation
        'short_term_hodler_supply',  # STH speculation
        'realized_price',        # Average cost basis
        'realized_profit_sth',   # STH profit taking
        'realized_profit_lth',   # LTH profit taking
        'terminal_price',         # Fair value model
        'stocks'                   # Gold, Silver, SP500, Strategy, Microsoft, Meta, Google, Apple, Amazon and Nvidia 
    ],
    4: [
        # Mining, Network & Market (8 metrics)
        'etf_flow_btc',         # ETF flows (CRITICAL for 2024+)
        'bitcoin_dominance',    # BTC vs altcoin cycle
        'nvts',                 # Network value signal
        'hashrate',             # Network security
        'active_addresses',     # Network activity
        'mvocdd',                  # Median Value of Coin days destroyed
        'nrpl_btc',            # Net realized profit/loss
        'miner_balances'        # Miner selling pressure
    ]

}

def get_metrics_config(metric_name):
    """Return API and table configuration for each metric"""
    
    configs = {
        'market_price': {
            'api_url': 'https://bitcoin-data.com/v1/btc-price',
            'table_name': 'MARKET_PRICE',
            'columns': '(date, unix_ts, market_price)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:btcPrice::FLOAT as market_price
            '''
        },
        'delta_price_pct': {
            'api_url': 'https://bitcoin-data.com/v1/delta-price-pct',
            'table_name': 'DELTA_PRICE_PCT',
            'columns': '(date, unix_ts, delta_price_pct)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:deltaPricePct::FLOAT as delta_price_pct
            '''
        },
        'mvrv': {
            'api_url': 'https://bitcoin-data.com/v1/mvrv',
            'table_name': 'MVRV',
            'columns': '(date, unix_ts, mvrv)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:mvrv::FLOAT as mvrv
            '''
        },
        'mvrv_zscore': {
            'api_url': 'https://bitcoin-data.com/v1/mvrv-zscore',
            'table_name': 'MVRV_ZSCORE',
            'columns': '(date, unix_ts, mvrv_zscore)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:mvrvZscore::FLOAT as mvrv_zscore
            '''
        },
        'nupl': {
            'api_url': 'https://bitcoin-data.com/v1/nupl',
            'table_name': 'NUPL',
            'columns': '(date, unix_ts, nupl)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:nupl::FLOAT as nupl
            '''
        },
        'asol': {
            'api_url': 'https://bitcoin-data.com/v1/asol',
            'table_name': 'ASOL',
            'columns': '(date, unix_ts, asol)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:asol::FLOAT as asol
            '''
        },
        'asopr': {
            'api_url': 'https://bitcoin-data.com/v1/asopr',
            'table_name': 'ASOPR',
            'columns': '(date, unix_ts, asopr)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:asopr::FLOAT as asopr
            '''
        },
        'aviv': {
            'api_url': 'https://bitcoin-data.com/v1/aviv',
            'table_name': 'AVIV',
            'columns': '(date, unix_ts, aviv)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:aviv::FLOAT as aviv
            '''
        },
        'sth_mvrv': {
            'api_url': 'https://bitcoin-data.com/v1/sth-mvrv',
            'table_name': 'STH_MVRV',
            'columns': '(date, unix_ts, sth_mvrv)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:sthMvrv::FLOAT as sth_mvrv
            '''
        },
        'lth_mvrv': {
            'api_url': 'https://bitcoin-data.com/v1/lth-mvrv',
            'table_name': 'LTH_MVRV',
            'columns': '(date, unix_ts, lth_mvrv)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:lthMvrv::FLOAT as lth_mvrv
            '''
        },
        'rhodl_ratio': {
            'api_url': 'https://bitcoin-data.com/v1/rhodl-ratio',
            'table_name': 'RHODL_RATIO',
            'columns': '(date, unix_ts, rhodl_ratio)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:rhodlRatio::FLOAT as rhodl_ratio
            '''
        },
        'supply_profit': {
            'api_url': 'https://bitcoin-data.com/v1/supply-profit',
            'table_name': 'SUPPLY_PROFIT',
            'columns': '(date, unix_ts, supply_profit)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:supplyProfit::FLOAT as supply_profit
            '''
        },
        'supply_loss': {
            'api_url': 'https://bitcoin-data.com/v1/supply-loss',
            'table_name': 'SUPPLY_LOSS',
            'columns': '(date, unix_ts, supply_loss)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:supplyLoss::FLOAT as supply_loss
            '''
        },
        'supply_current': {
            'api_url': 'https://bitcoin-data.com/v1/supply-current',
            'table_name': 'SUPPLY_CURRENT',
            'columns': '(date, unix_ts, supply_current)',
            'select_clause': '''
                TO_DATE($1:theDay::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:supplyCurrent::FLOAT as supply_current
            '''
        },
        'long_term_hodler_supply': {
            'api_url': 'https://bitcoin-data.com/v1/long-term-hodler-supply',
            'table_name': 'LONG_TERM_HODLER_SUPPLY',
            'columns': '(date, unix_ts, lth_supply)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:longTermHodlerSupply::FLOAT as lth_supply
            '''
        },
        'short_term_hodler_supply': {
            'api_url': 'https://bitcoin-data.com/v1/short-term-hodler-supply',
            'table_name': 'SHORT_TERM_HODLER_SUPPLY',
            'columns': '(date, unix_ts, sth_supply)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:shortTermHodlerSupply::FLOAT as sth_supply
            '''
        },
        'realized_price': {
            'api_url': 'https://bitcoin-data.com/v1/realized-price',
            'table_name': 'REALIZED_PRICE',
            'columns': '(date, unix_ts, realized_price)',
            'select_clause': '''
                TO_DATE($1:theDay::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:realizedPrice::FLOAT as realized_price
            '''
        },
        'etf_flow_btc': {
            'api_url': 'https://bitcoin-data.com/v1/etf-flow-btc',
            'table_name': 'ETF_FLOW_BTC',
            'columns': '(date, unix_ts, etf_flow)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:etfFlow::FLOAT as etf_flow
            '''
        },
        'bitcoin_dominance': {
            'api_url': 'https://bitcoin-data.com/v1/bitcoin-dominance',
            'table_name': 'BITCOIN_DOMINANCE',
            'columns': '(date, unix_ts, bitcoin_dominance)',
            'select_clause': '''
                TO_DATE(SUBSTR($1:d::STRING, 1, 10), 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:bitcoinDominance::FLOAT as bitcoin_dominance
            '''
        },
        'funding_rate': {
            'api_url': 'https://bitcoin-data.com/v1/funding-rate',
            'table_name': 'FUNDING_RATE',
            'columns': '(date, unix_ts, funding_rate)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:fundingRate::FLOAT as funding_rate
            '''
        },
        'nvts': {
            'api_url': 'https://bitcoin-data.com/v1/nvts',
            'table_name': 'NVTS',
            'columns': '(date, unix_ts, nvts)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:nvts::FLOAT as nvts
            '''
        },
        'hashrate': {
            'api_url': 'https://bitcoin-data.com/v1/hashrate',
            'table_name': 'HASHRATE',
            'columns': '(date, unix_ts, hashrate)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:hashrate::FLOAT as hashrate
            '''
        },
        'puell_multiple': {
            'api_url': 'https://bitcoin-data.com/v1/puell-multiple',
            'table_name': 'PUELL_MULTIPLE',
            'columns': '(date, unix_ts, puell_multiple)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:puellMultiple::FLOAT as puell_multiple
            '''
        },
        'reserve_risk': {
            'api_url': 'https://bitcoin-data.com/v1/reserve-risk',
            'table_name': 'RESERVE_RISK',
            'columns': '(date, unix_ts, reserve_risk)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:reserveRisk::FLOAT as reserve_risk
            '''
        },
        'investor_price': {
            'api_url': 'https://bitcoin-data.com/v1/investor-price',
            'table_name': 'INVESTOR_PRICE',
            'columns': '(date, unix_ts, investor_price)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:investorPrice::FLOAT as investor_price
            '''
        },
        'realized_profit_sth': {
            'api_url': 'https://bitcoin-data.com/v1/realized_profit_sth',
            'table_name': 'REALIZED_PROFIT_STH',
            'columns': '(date, unix_ts, realized_profit_sth)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:realizedProfitSth::FLOAT as realized_profit_sth
            '''
        },
        'realized_profit_lth': {
            'api_url': 'https://bitcoin-data.com/v1/realized_profit_lth',
            'table_name': 'REALIZED_PROFIT_LTH',
            'columns': '(date, unix_ts, realized_profit_lth)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:realizedProfitLth::FLOAT as realized_profit_lth
            '''
        },
        'stocks': {
            'api_url': 'https://bitcoin-data.com/v1/stock',
            'table_name': 'STOCK',
            'columns': '(date, unix_ts, sp500, gold, silver, strategy, microsoft, meta, google, apple, amazon, nvidia)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:sp500::FLOAT as sp500,
                $1:gold::FLOAT as gold,
                $1:silver::FLOAT as silver,
                $1:strategy::FLOAT as strategy,
                $1:microsoft::FLOAT as microsoft,
                $1:meta::FLOAT as meta,
                $1:google::FLOAT as google,
                $1:apple::FLOAT as apple,
                $1:amazon::FLOAT as amazon,
                $1:nvidia::FLOAT as nvidia
            '''
        },
        'terminal_price': {
            'api_url': 'https://bitcoin-data.com/v1/terminal-price',
            'table_name': 'TERMINAL_PRICE',
            'columns': '(date, unix_ts, terminal_price)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:terminalPrice::FLOAT as terminal_price
            '''
        },
        'active_addresses': {
            'api_url': 'https://bitcoin-data.com/v1/active-addresses',
            'table_name': 'ACTIVE_ADDRESSES',
            'columns': '(date, unix_ts, active_addresses)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:activeAddresses::BIGINT as active_addresses
            '''
        },
        'mvocdd': {
            'api_url': 'https://bitcoin-data.com/v1/mvocdd',
            'table_name': 'MVOCDD',
            'columns': '(date, unix_ts, mvocdd)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:mvocdd::FLOAT as mvocdd
            '''
        },
        'nrpl_btc': {
            'api_url': 'https://bitcoin-data.com/v1/nrpl-btc',
            'table_name': 'NRPL_BTC',
            'columns': '(date, unix_ts, nrpl_btc)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:nrplBtc::FLOAT as nrpl_btc
            '''
        },
        'miner_balances': {
            'api_url': 'https://bitcoin-data.com/v1/miner-balances',
            'table_name': 'MINER_BALANCES',
            'columns': '(date, unix_ts, miner_balance)',
            'select_clause': '''
                TO_DATE($1:d::STRING, 'YYYY-MM-DD') as date,
                $1:unixTs::BIGINT as unix_ts,
                $1:minerBalances::FLOAT as miner_balance
            '''
        }
    }
    
    if metric_name not in configs:
        raise ValueError(f"Unknown metric: {metric_name}")
    
    return configs[metric_name]

def download_and_upload_metric(metric_name, **context):
    """Download JSON from API and upload to Snowflake stage"""
    config = get_metrics_config(metric_name)
    api_url = config['api_url']

    try:
        print(f"[{datetime.now()}] Downloading {metric_name} from: {api_url}")
        
        response = requests.get(api_url, timeout=600)
        response.raise_for_status()
        
        json_data = response.json()
        print(f"✓ Downloaded {len(json_data)} records for {metric_name}")
        
        if not json_data:
            raise Exception(f"No data received for {metric_name}")
        
        # Save to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(json_data, temp_file, indent=2)
            temp_file_path = temp_file.name
        
        # Upload to Snowflake stage
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        stage_filename = f"{metric_name}-{timestamp}.json"
        
        put_sql = f"PUT file://{temp_file_path} @BITCOIN_DATA.DATA.my_stage/{stage_filename}"
        snowflake_hook.run(put_sql)
        
        os.unlink(temp_file_path)
        
        print(f"✓ Uploaded {metric_name} to stage as {stage_filename}")
        return stage_filename
        
    except Exception as e:
        print(f"✗ Error in {metric_name}: {str(e)}")
        raise

def merge_metric_data(metric_name, **context):
    """Merge data for a specific metric using UPSERT"""
    config = get_metrics_config(metric_name)
    filename = context['task_instance'].xcom_pull(task_ids=f'download_{metric_name}')
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    columns_str = config['columns'].strip('()')
    columns_list = [col.strip() for col in columns_str.split(',')]
    value_column = columns_list[-1]
    
    if config['table_name'] == 'STOCK':
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS BITCOIN_DATA.DATA.{config['table_name']} (
            date DATE,
            unix_ts BIGINT,
            sp500 FLOAT,
            gold FLOAT,
            silver FLOAT,
            strategy FLOAT,
            microsoft FLOAT,
            meta FLOAT,
            google FLOAT,
            apple FLOAT,
            amazon FLOAT,
            nvidia FLOAT,
            PRIMARY KEY (date)
        );
        """

        merge_sql = f"""
            MERGE INTO BITCOIN_DATA.DATA.{config['table_name']} AS target
            USING (
                SELECT {config['select_clause']}
                FROM @BITCOIN_DATA.DATA.my_stage/{filename} (FILE_FORMAT => BITCOIN_DATA.DATA.json_format)
            ) AS source
            ON target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET unix_ts = source.unix_ts, sp500 = source.sp500, gold = source.gold, 
                           silver = source.silver, strategy = source.strategy, microsoft = source.microsoft, 
                           meta = source.meta, google = source.google, apple = source.apple, 
                           amazon = source.amazon, nvidia = source.nvidia
            WHEN NOT MATCHED THEN
                INSERT {config['columns']}
                VALUES (source.date, source.unix_ts, source.sp500, source.gold, source.silver, 
                        source.strategy, source.microsoft, source.meta, source.google, source.apple, 
                        source.amazon, source.nvidia);
            """
    else:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS BITCOIN_DATA.DATA.{config['table_name']} (
                date DATE,
                unix_ts BIGINT,
                {value_column} FLOAT,
                PRIMARY KEY (date)
            );
            """
        
        merge_sql = f"""
            MERGE INTO BITCOIN_DATA.DATA.{config['table_name']} AS target
            USING (
                SELECT {config['select_clause']}
                FROM @BITCOIN_DATA.DATA.my_stage/{filename} (FILE_FORMAT => BITCOIN_DATA.DATA.json_format)
            ) AS source
            ON target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET unix_ts = source.unix_ts, {value_column} = source.{value_column}
            WHEN NOT MATCHED THEN
                INSERT {config['columns']}
                VALUES (source.date, source.unix_ts, source.{value_column});
            """
        
    print(f"Creating table for {metric_name}...")
    snowflake_hook.run(create_table_sql)
    
    print(f"Merging {metric_name} data...")
    result = snowflake_hook.run(merge_sql)
    print(f"✓ Merge completed for {metric_name}")
    
    return result

def create_consolidated_table(**context):
    """Create/update consolidated table with all optimized metrics"""
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Create table with all optimized metrics
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS BITCOIN_DATA.DATA.GLOBAL_DATA (
        date DATE,
        unix_ts BIGINT,
        -- Core Price & Technical
        market_price FLOAT,
        delta_price_pct FLOAT,
        -- On-chain Sentiment
        mvrv FLOAT,
        mvrv_zscore FLOAT,
        nupl FLOAT,
        asol FLOAT,
        asopr FLOAT,
        aviv FLOAT,
        -- Holder Behavior
        sth_mvrv FLOAT,
        lth_mvrv FLOAT,
        rhodl_ratio FLOAT,
        -- Stocks
        sp500 FLOAT,
        gold FLOAT,
        silver FLOAT,
        strategy FLOAT,
        microsoft FLOAT,
        meta FLOAT,
        google FLOAT,
        apple FLOAT,
        amazon FLOAT,
        nvidia FLOAT,
        -- Supply Metrics
        supply_profit FLOAT,
        supply_loss FLOAT,
        supply_current FLOAT,
        lth_supply FLOAT,
        sth_supply FLOAT,
        realized_price FLOAT,
        -- Institutional & Liquidity
        etf_flow FLOAT,
        bitcoin_dominance FLOAT,
        nvts FLOAT,
        -- Mining & Network
        hashrate FLOAT,
        puell_multiple FLOAT,
        active_addresses FLOAT,
        mvocdd FLOAT,
        nrpl_btc FLOAT,
        miner_balance FLOAT,
        -- Value & Risk
        reserve_risk FLOAT,
        investor_price FLOAT,
        realized_profit_sth FLOAT,
        realized_profit_lth FLOAT,
        terminal_price FLOAT,
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (date)
    );
    """
    
    print("Creating optimized consolidated table...")
    snowflake_hook.run(create_table_sql)
    
    # Comprehensive MERGE statement for all metrics
    merge_sql = """
    MERGE INTO BITCOIN_DATA.DATA.GLOBAL_DATA AS target
    USING (
        SELECT 
            all_dates.date,
            COALESCE(mp.unix_ts, dp.unix_ts, mv.unix_ts) as unix_ts,
            -- Core Price & Technical
            mp.market_price,
            dp.delta_price_pct,
            -- On-chain Sentiment
            mv.mvrv, mvz.mvrv_zscore, np.nupl, asol_tbl.asol, asp.asopr, av.aviv,
            -- Holder Behavior
            smv.sth_mvrv, lmv.lth_mvrv, rr_ratio.rhodl_ratio,
            -- Supply
            spr.supply_profit, spl.supply_loss, sc.supply_current,
            lth.lth_supply, sth.sth_supply, rp.realized_price,
            -- Institutional
            etf.etf_flow, bd.bitcoin_dominance, nvts.nvts,
            -- Mining & Network
            hr.hashrate, pm.puell_multiple,
            aa.active_addresses, mvocdd.mvocdd, nrpl.nrpl_btc, mb.miner_balance,
            -- Value & Risk
            rsk.reserve_risk, ip.investor_price,
            rps.realized_profit_sth, rpl.realized_profit_lth, tp.terminal_price, st.sp500, st.gold, st.silver, st.strategy, st.microsoft, st.meta, st.google, st.apple, st.amazon, st.nvidia
        FROM (
            SELECT DISTINCT date FROM BITCOIN_DATA.DATA.MARKET_PRICE
            UNION SELECT DISTINCT date FROM BITCOIN_DATA.DATA.MVRV
            UNION SELECT DISTINCT date FROM BITCOIN_DATA.DATA.NUPL
            UNION SELECT DISTINCT date FROM BITCOIN_DATA.DATA.ASOL
            UNION SELECT DISTINCT date FROM BITCOIN_DATA.DATA.ETF_FLOW_BTC
            UNION SELECT DISTINCT date FROM BITCOIN_DATA.DATA.BITCOIN_DOMINANCE
            UNION SELECT DISTINCT date FROM BITCOIN_DATA.DATA.NVTS
            UNION SELECT DISTINCT date FROM BITCOIN_DATA.DATA.HASHRATE
            UNION SELECT DISTINCT date FROM BITCOIN_DATA.DATA.INVESTOR_PRICE
            UNION SELECT DISTINCT date FROM BITCOIN_DATA.DATA.STOCK
        ) all_dates
        LEFT JOIN BITCOIN_DATA.DATA.MARKET_PRICE mp ON all_dates.date = mp.date
        LEFT JOIN BITCOIN_DATA.DATA.DELTA_PRICE_PCT dp ON all_dates.date = dp.date
        LEFT JOIN BITCOIN_DATA.DATA.MVRV mv ON all_dates.date = mv.date
        LEFT JOIN BITCOIN_DATA.DATA.MVRV_ZSCORE mvz ON all_dates.date = mvz.date
        LEFT JOIN BITCOIN_DATA.DATA.NUPL np ON all_dates.date = np.date
        LEFT JOIN BITCOIN_DATA.DATA.ASOL asol_tbl ON all_dates.date = asol_tbl.date
        LEFT JOIN BITCOIN_DATA.DATA.ASOPR asp ON all_dates.date = asp.date
        LEFT JOIN BITCOIN_DATA.DATA.AVIV av ON all_dates.date = av.date
        LEFT JOIN BITCOIN_DATA.DATA.STH_MVRV smv ON all_dates.date = smv.date
        LEFT JOIN BITCOIN_DATA.DATA.LTH_MVRV lmv ON all_dates.date = lmv.date
        LEFT JOIN BITCOIN_DATA.DATA.RHODL_RATIO rr_ratio ON all_dates.date = rr_ratio.date
        LEFT JOIN BITCOIN_DATA.DATA.SUPPLY_PROFIT spr ON all_dates.date = spr.date
        LEFT JOIN BITCOIN_DATA.DATA.SUPPLY_LOSS spl ON all_dates.date = spl.date
        LEFT JOIN BITCOIN_DATA.DATA.SUPPLY_CURRENT sc ON all_dates.date = sc.date
        LEFT JOIN BITCOIN_DATA.DATA.LONG_TERM_HODLER_SUPPLY lth ON all_dates.date = lth.date
        LEFT JOIN BITCOIN_DATA.DATA.SHORT_TERM_HODLER_SUPPLY sth ON all_dates.date = sth.date
        LEFT JOIN BITCOIN_DATA.DATA.REALIZED_PRICE rp ON all_dates.date = rp.date
        LEFT JOIN BITCOIN_DATA.DATA.ETF_FLOW_BTC etf ON all_dates.date = etf.date
        LEFT JOIN BITCOIN_DATA.DATA.BITCOIN_DOMINANCE bd ON all_dates.date = bd.date
        LEFT JOIN BITCOIN_DATA.DATA.NVTS nvts ON all_dates.date = nvts.date
        LEFT JOIN BITCOIN_DATA.DATA.HASHRATE hr ON all_dates.date = hr.date
        LEFT JOIN BITCOIN_DATA.DATA.PUELL_MULTIPLE pm ON all_dates.date = pm.date
        LEFT JOIN BITCOIN_DATA.DATA.ACTIVE_ADDRESSES aa ON all_dates.date = aa.date
        LEFT JOIN BITCOIN_DATA.DATA.MVOCDD mvocdd ON all_dates.date = mvocdd.date
        LEFT JOIN BITCOIN_DATA.DATA.NRPL_BTC nrpl ON all_dates.date = nrpl.date
        LEFT JOIN BITCOIN_DATA.DATA.MINER_BALANCES mb ON all_dates.date = mb.date
        LEFT JOIN BITCOIN_DATA.DATA.RESERVE_RISK rsk ON all_dates.date = rsk.date
        LEFT JOIN BITCOIN_DATA.DATA.INVESTOR_PRICE ip ON all_dates.date = ip.date
        LEFT JOIN BITCOIN_DATA.DATA.REALIZED_PROFIT_STH rps ON all_dates.date = rps.date
        LEFT JOIN BITCOIN_DATA.DATA.REALIZED_PROFIT_LTH rpl ON all_dates.date = rpl.date
        LEFT JOIN BITCOIN_DATA.DATA.TERMINAL_PRICE tp ON all_dates.date = tp.date
        LEFT JOIN BITCOIN_DATA.DATA.STOCK st ON all_dates.date = st.date
        WHERE all_dates.date IS NOT NULL
    ) AS source
    ON target.date = source.date
    WHEN MATCHED THEN
        UPDATE SET 
            unix_ts = source.unix_ts,
            market_price = source.market_price,
            delta_price_pct = source.delta_price_pct,
            mvrv = source.mvrv, mvrv_zscore = source.mvrv_zscore, nupl = source.nupl,
            asol = source.asol, asopr = source.asopr, aviv = source.aviv,
            sth_mvrv = source.sth_mvrv, lth_mvrv = source.lth_mvrv, rhodl_ratio = source.rhodl_ratio,
            supply_profit = source.supply_profit, supply_loss = source.supply_loss,
            supply_current = source.supply_current, lth_supply = source.lth_supply,
            sth_supply = source.sth_supply, realized_price = source.realized_price,
            etf_flow = source.etf_flow, bitcoin_dominance = source.bitcoin_dominance, nvts = source.nvts,
            hashrate = source.hashrate, puell_multiple = source.puell_multiple,
            active_addresses = source.active_addresses, mvocdd = source.mvocdd, 
            nrpl_btc = source.nrpl_btc, miner_balance = source.miner_balance,
            reserve_risk = source.reserve_risk, investor_price = source.investor_price,
            realized_profit_sth = source.realized_profit_sth, realized_profit_lth = source.realized_profit_lth,
            terminal_price = source.terminal_price, sp500 = source.sp500, gold = source.gold, silver = source.silver, strategy = source.strategy, microsoft = source.microsoft, meta = source.meta, google = source.google, apple = source.apple, amazon = source.amazon, nvidia = source.nvidia, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (date, unix_ts, market_price, delta_price_pct,
                mvrv, mvrv_zscore, nupl, asol, asopr, aviv, sth_mvrv, lth_mvrv, rhodl_ratio,
                supply_profit, supply_loss, supply_current, lth_supply, sth_supply, realized_price,
                etf_flow, bitcoin_dominance, nvts,
                hashrate, puell_multiple, active_addresses, mvocdd, nrpl_btc, miner_balance,
                reserve_risk, investor_price, realized_profit_sth, realized_profit_lth, terminal_price, sp500, gold, silver, strategy, microsoft, meta, google, apple, amazon, nvidia)
        VALUES (source.date, source.unix_ts, source.market_price, source.delta_price_pct, 
                source.mvrv, source.mvrv_zscore, source.nupl,
                source.asol, source.asopr, source.aviv, source.sth_mvrv, source.lth_mvrv, source.rhodl_ratio,
                source.supply_profit, source.supply_loss, source.supply_current, source.lth_supply,
                source.sth_supply, source.realized_price, source.etf_flow, source.bitcoin_dominance, source.nvts,
                source.hashrate, source.puell_multiple, source.active_addresses, source.mvocdd, 
                source.nrpl_btc, source.miner_balance,
                source.reserve_risk, source.investor_price, source.realized_profit_sth,
                source.realized_profit_lth, source.terminal_price, source.sp500, source.gold, source.silver, source.strategy, source.microsoft, source.meta, source.google, source.apple, source.amazon, source.nvidia);
    """
    
    print("Merging data into global table...")
    result = snowflake_hook.run(merge_sql)
    print(f"✓ Consolidation completed")
    
    return result

def cleanup_stage(**context):
    """Clean up all files from Snowflake stage"""
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    cleanup_sql = "REMOVE @BITCOIN_DATA.DATA.my_stage"
    
    print("Cleaning up stage files...")
    result = snowflake_hook.run(cleanup_sql)
    print("✓ Stage cleanup completed")
    
    return result



# Create file format tasks for all DAGs
for i in range(1, 5):
    create_format = SnowflakeOperator(
        task_id='create_file_format',
        snowflake_conn_id='snowflake_default',
        sql="""
        CREATE FILE FORMAT IF NOT EXISTS BITCOIN_DATA.DATA.json_format
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = TRUE;
        """,
        dag=dags[i]
    )

# Create download and merge tasks for batches 1-4
for batch_num in range(1, 5):
    prev_task = None
    dag = dags[batch_num]
    
    # Start with file format
    format_task = [task for task in dag.tasks if task.task_id == 'create_file_format'][0]
    prev_task = format_task
    
    # Create sequential download and merge for each metric
    for metric_name in batch_metrics[batch_num]:
        download_task = PythonOperator(
            task_id=f'download_{metric_name}',
            python_callable=partial(download_and_upload_metric, metric_name),
            dag=dag
        )
        
        merge_task = PythonOperator(
            task_id=f'merge_{metric_name}',
            python_callable=partial(merge_metric_data, metric_name),
            dag=dag
        )
        
        # Chain: previous_task >> download >> merge
        prev_task >> download_task >> merge_task
        prev_task = merge_task


    if batch_num == 4: # Consolidation and cleanup at the end of batch 4
        consolidate_task = PythonOperator(
            task_id='create_consolidated_table',
            python_callable=create_consolidated_table,
            dag=dags[batch_num]
        )

        cleanup_task = PythonOperator(
            task_id='cleanup_stage',
            python_callable=cleanup_stage,
            dag=dags[batch_num]
        )

        merge_task >> consolidate_task >> cleanup_task
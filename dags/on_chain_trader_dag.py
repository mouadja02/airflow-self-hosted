from datetime import datetime, timedelta
import os
import yfinance as yf
import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
import re
import logging
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

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
    'bitcoin_on_chain_trader_fixed',
    default_args=default_args,
    description='Calculate Bitcoin on-chain metrics for trading indicators - FIXED VERSION',
    schedule='5 23 * * *',  # Every day at 23:05
    catchup=False,
    tags=['bitcoin', 'onchain', 'trading', 'snowflake', 'fixed'],
)

# Snowflake connection parameters
snowflake_conn_params = {
    'snowflake_conn_id': 'snowflake_default',
    'warehouse': 'INT_WH',
    'database': os.getenv('SNOWFLAKE_DATABASE', 'BITCOIN_DATA'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'DATA'),
}


def merge_btc_ohclv(**context):
    """
    Pull 7 days of BTC data from Yahoo Finance and up-sert into Snowflake.
    """
    btc = yf.Ticker("BTC-USD")                  
    end_date   = datetime.utcnow() + timedelta(days=2)
    start_date = datetime.utcnow() - timedelta(days=7) # 7 days backup

    try:
        df = (btc.history(start=start_date.strftime('%Y-%m-%d'),end=end_date.strftime('%Y-%m-%d')).reset_index().dropna(subset=['Close']))
    
        df = df.rename(columns={
            'Date':   'DATE',
            'Open':   'OPEN',
            'High':   'HIGH',
            'Low':    'LOW',
            'Close':  'CLOSE',
            'Volume': 'VOLUME',
        })
    
        values_clause = ",\n        ".join(
            f"('{r.DATE:%Y-%m-%d}', {r.OPEN:.2f}, {r.HIGH:.2f}, "
            f"{r.LOW:.2f}, {r.CLOSE:.2f}, {r.VOLUME:.0f})"
            for r in df.itertuples(index=False)
        )
    
        sql  = f"""
        MERGE INTO BITCOIN_DATA.DATA.OHCLV_DATA   AS tgt
        USING (
            SELECT column1  AS DATE,
                   column2  AS OPEN,
                   column3  AS HIGH,
                   column4  AS LOW,
                   column5  AS CLOSE,
                   column6  AS VOLUME
            FROM   VALUES
            {values_clause}
        ) AS src
        ON tgt.DATE = src.DATE
        WHEN MATCHED THEN UPDATE SET
            OPEN   = src.OPEN,
            HIGH   = src.HIGH,
            LOW    = src.LOW,
            CLOSE  = src.CLOSE,
            VOLUME = src.VOLUME
        WHEN NOT MATCHED THEN INSERT
            (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
            VALUES
            (src.DATE, src.OPEN, src.HIGH, src.LOW, src.CLOSE, src.VOLUME);
        """
    
        context['ti'].xcom_push(key='btc_merge_sql', value=sql )
        return sql 
        
    except Exception as e:
        print(f"Error fetching BTC data: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise

def run_bitcoin_metrics_updater(**context):
    """
    Bitcoin metrics updater function integrated into Airflow
    FIXED VERSION: Uses CLOSE price from OHCLV_DATA table instead of web scraping
    Fetches realized price using Firecrawl API with estimation fallback
    """
    # Configure logging
    logger = logging.getLogger(__name__)
    
    # Firecrawl API configuration
    FIRECRAWL_API_KEY = os.getenv('FIRECRAWL_API_KEY')
    FIRECRAWL_BASE_URL = "https://api.firecrawl.dev/v2"
    
    if not FIRECRAWL_API_KEY:
        logger.warning("Firecrawl API key not found, some features may not work")
    
    def get_btc_close_price_from_snowflake():
        """Get Bitcoin CLOSE price from OHCLV_DATA table - most recent date"""
        try:
            hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
            
            query = """
            SELECT DATE, CLOSE 
            FROM BITCOIN_DATA.DATA.OHCLV_DATA 
            ORDER BY DATE DESC 
            LIMIT 1
            """
            
            result = hook.get_pandas_df(query)
            
            if result.empty:
                raise ValueError("No data found in OHCLV_DATA table")
            
            close_price = float(result['CLOSE'].iloc[0])
            date = result['DATE'].iloc[0]
            
            logger.info(f"Retrieved CLOSE price from Snowflake for {date}: ${close_price:,.2f}")
            return close_price
            
        except Exception as e:
            logger.error(f"Error fetching CLOSE price from Snowflake: {e}")
            raise
    
    def scrape_with_firecrawl(url, selector_description=""):
        """Generic function to scrape data using Firecrawl API"""
        try:
            headers = {
                "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "url": url,
                "formats": ["html", "markdown"],
                "onlyMainContent": True,
                "waitFor": 2000,  # Wait 2 seconds for page to load
                "timeout": 60000  # 60 second timeout
            }
            
            response = requests.post(
                f"{FIRECRAWL_BASE_URL}/scrape", 
                json=payload, 
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get('success'):
                return result.get('data', {})
            else:
                raise ValueError(f"Firecrawl API returned success=False: {result}")
                
        except Exception as e:
            logger.error(f"Firecrawl scraping failed for {url}: {e}")
            return None
    
    def fetch_realized_price(market_price):
        """Fetch current Bitcoin realized price using Firecrawl with estimation fallback"""
        try:
            url = "https://newhedge.io/bitcoin/realized-price"
            data = scrape_with_firecrawl(url)
            
            if data and 'html' in data:
                soup = BeautifulSoup(data['html'], 'html.parser')
                
                # Try multiple selectors for realized price
                price_selectors = [
                    '.realized-price-selector',
                    '[class*="realized"]',
                    '[class*="price"]'
                ]
                
                for selector in price_selectors:
                    price_element = soup.select_one(selector)
                    if price_element:
                        price_text = price_element.text.strip()
                        price_match = re.search(r'[\d,]+\.?\d*', price_text.replace('$', '').replace(',', ''))
                        if price_match:
                            realized_price = float(price_match.group().replace(',', ''))
                            logger.info(f"Successfully scraped realized price: ${realized_price:,.2f}")
                            return realized_price
                
                # Try markdown extraction
                if 'markdown' in data:
                    markdown_text = data['markdown']
                    price_patterns = [
                        r'Realized Price:?\s*\$?([\d,]+\.?\d*)',
                        r'\$?([\d,]+\.?\d*)'
                    ]
                    for pattern in price_patterns:
                        price_match = re.search(pattern, markdown_text)
                        if price_match:
                            price_str = price_match.group(1).replace(',', '')
                            try:
                                price = float(price_str)
                                if 5000 <= price <= 100000:  # Reasonable realized price range
                                    logger.info(f"Successfully extracted realized price from markdown: ${price:,.2f}")
                                    return price
                            except ValueError:
                                continue
            
            # If no price found, estimate realized price as 60% of market price
            estimated_realized = market_price * 0.60  # Rough estimation
            logger.warning(f"Could not fetch realized price, estimating as 60% of market price: ${estimated_realized:.2f}")
            return estimated_realized
            
        except Exception as e:
            logger.error(f"Error fetching realized price: {e}")
            # Fallback: estimate as percentage of market price
            estimated_realized = market_price * 0.60
            logger.warning(f"Using estimated realized price (60% of market): ${estimated_realized:.2f}")
            return estimated_realized
    
    def calculate_metrics(market_price, realized_price):
        """Calculate MVRV and NUPL metrics"""
        mvrv = market_price / realized_price
        nupl = (market_price - realized_price) / market_price
        
        # Get current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        return {
            'date': current_date,
            'marketPrice': market_price,
            'realizedPrice': realized_price,
            'mvrv': mvrv,
            'nupl': nupl
        }
    
    def insert_to_snowflake(metrics):
        """Insert metrics into Snowflake tables using Airflow hook with MERGE statements"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        try:
            # MERGE into MVRV table
            mvrv_sql = f"""
            MERGE INTO BITCOIN_DATA.DATA.MVRV AS target
            USING (
                SELECT '{metrics['date']}' AS DATE, {metrics['mvrv']} AS MVRV
            ) AS source
            ON target.DATE = source.DATE
            WHEN MATCHED THEN 
                UPDATE SET MVRV = source.MVRV
            WHEN NOT MATCHED THEN 
                INSERT (DATE, MVRV) VALUES (source.DATE, source.MVRV);
            """
            hook.run(mvrv_sql)
            logger.info(f"Successfully merged MVRV: {metrics['mvrv']}")
            
            # MERGE into REALIZED_PRICE table  
            realized_price_sql = f"""
            MERGE INTO BITCOIN_DATA.DATA.REALIZED_PRICE AS target
            USING (
                SELECT '{metrics['date']}' AS DATE, {metrics['realizedPrice']} AS REALIZED_PRICE
            ) AS source
            ON target.DATE = source.DATE
            WHEN MATCHED THEN 
                UPDATE SET REALIZED_PRICE = source.REALIZED_PRICE
            WHEN NOT MATCHED THEN 
                INSERT (DATE, REALIZED_PRICE) VALUES (source.DATE, source.REALIZED_PRICE);
            """
            hook.run(realized_price_sql)
            logger.info(f"Successfully merged Realized Price: {metrics['realizedPrice']}")
            
            # MERGE into NUPL table
            nupl_sql = f"""
            MERGE INTO BITCOIN_DATA.DATA.NUPL AS target
            USING (
                SELECT '{metrics['date']}' AS DATE, {metrics['nupl']} AS NUPL
            ) AS source
            ON target.DATE = source.DATE
            WHEN MATCHED THEN 
                UPDATE SET NUPL = source.NUPL
            WHEN NOT MATCHED THEN 
                INSERT (DATE, NUPL) VALUES (source.DATE, source.NUPL);
            """
            hook.run(nupl_sql)
            logger.info(f"Successfully merged NUPL: {metrics['nupl']}")
            
            # Execute the MERGE query to update ONCHAIN_STRATEGY table
            merge_query = """
            MERGE INTO BITCOIN_DATA.DATA.ONCHAIN_STRATEGY AS target
            USING (
                SELECT 
                    COALESCE(o.DATE, m.DATE, n.DATE) as DATE,
                    o.OPEN,
                    o.HIGH,
                    o.LOW,
                    o.CLOSE,
                    o.VOLUME,
                    m.MVRV,
                    n.NUPL
                FROM (
                    SELECT * FROM BITCOIN_DATA.DATA.OHCLV_DATA 
                    ORDER BY DATE DESC 
                    LIMIT 60
                ) o
                FULL OUTER JOIN (
                    SELECT * FROM BITCOIN_DATA.DATA.MVRV 
                    ORDER BY DATE DESC 
                    LIMIT 60
                ) m ON o.DATE = m.DATE
                FULL OUTER JOIN (
                    SELECT * FROM BITCOIN_DATA.DATA.NUPL 
                    ORDER BY DATE DESC 
                    LIMIT 60
                ) n ON COALESCE(o.DATE, m.DATE) = n.DATE
                WHERE COALESCE(o.DATE, m.DATE, n.DATE) IS NOT NULL
            ) AS source
            ON target.DATE = source.DATE
            WHEN MATCHED THEN
                UPDATE SET
                    OPEN = source.OPEN,
                    HIGH = source.HIGH,
                    LOW = source.LOW,
                    CLOSE = source.CLOSE,
                    VOLUME = source.VOLUME,
                    MVRV = source.MVRV,
                    NUPL = source.NUPL
            WHEN NOT MATCHED THEN
                INSERT (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, MVRV, NUPL)
                VALUES (source.DATE, source.OPEN, source.HIGH, source.LOW, 
                        source.CLOSE, source.VOLUME, source.MVRV, source.NUPL)
            """
            
            hook.run(merge_query)
            logger.info("Successfully updated ONCHAIN_STRATEGY table")
            
        except Exception as e:
            logger.error(f"Error inserting data to Snowflake: {e}")
            raise
    
    try:
        logger.info("Starting Bitcoin metrics update...")
        
        # FIXED: Get market price from Snowflake OHCLV_DATA table
        logger.info("Fetching market price from Snowflake OHCLV_DATA...")
        market_price = get_btc_close_price_from_snowflake()
        
        logger.info("Fetching realized price...")
        realized_price = fetch_realized_price(market_price)
        
        logger.info(f"Market Price (CLOSE): ${market_price:,.2f}")
        logger.info(f"Realized Price: ${realized_price:,.2f}")
        
        # Calculate metrics
        metrics = calculate_metrics(market_price, realized_price)
        
        logger.info(f"MVRV: {metrics['mvrv']:.4f}")
        logger.info(f"NUPL: {metrics['nupl']:.4f}")
        
        # Insert to Snowflake
        insert_to_snowflake(metrics)
        
        logger.info("Bitcoin metrics update completed successfully!")
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

def run_btc_strategy_with_logging(**context):
    """
    Run the Bitcoin trading strategy, log results to Snowflake and send to all subscribers
    """    
    logger = logging.getLogger(__name__)
    
    # Strategy parameters - UPDATED WITH NEW OPTIMIZED VALUES
    OPTIMIZED_PARAMS = {
        'combine_method': 'weighted',
        'ma_type': 'WMA',
        'ma_length': 300,
        'zscore_lookback': 50,
        'long_threshold': -0.52,
        'short_threshold': -1.32,
        'mvrv_weight': 0.80,
        'nupl_weight': 0.20,
        'initial_capital': 10000
    }
    
    def load_data_from_snowflake():
        """Load data from Snowflake using Airflow hook - FULL DATASET ORDERED BY DATE ASC"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        query = """
        SELECT DATE,
            CAST(OPEN AS FLOAT) AS OPEN,
            CAST(HIGH AS FLOAT) AS HIGH,
            CAST(LOW AS FLOAT) AS LOW,
            CAST(CLOSE AS FLOAT) AS CLOSE,
            CAST(VOLUME AS FLOAT) AS VOLUME,
            CAST(MVRV AS FLOAT) AS MVRV,
            CAST(NUPL AS FLOAT) AS NUPL 
        FROM BITCOIN_DATA.DATA.ONCHAIN_STRATEGY 
        ORDER BY DATE ASC
        """
        
        result = hook.get_pandas_df(query)
        result['DATE'] = pd.to_datetime(result['DATE'])
        result = result.set_index('DATE')
        
        # Interpolate missing values
        result = result.interpolate(method='time')
        
        return result
    
    def extract_last_signals_from_backtest(df):
        """Extract the last 4 LONG/SHORT signals from the backtest dataframe"""
        # Filter only rows where SIGNAL is 1 (LONG) or -1 (SHORT)
        signal_switches = df[df['SIGNAL'].isin([1, -1])].copy()
        
        # Get the last 4 signals
        last_4_signals = signal_switches.tail(4)
        
        # Create a list of signal dictionaries
        signals_list = []
        for idx, row in last_4_signals.iterrows():
            signal_type = 'LONG' if row['SIGNAL'] == 1 else 'SHORT'
            price_col = 'CLOSE' if 'CLOSE' in row else 'PRICE'
            signals_list.append({
                'DATE': idx,
                'SIGNAL': signal_type,
                'BTC_PRICE': row[price_col]
            })
        
        # Reverse to show most recent first
        signals_list.reverse()
        
        return pd.DataFrame(signals_list)
    
    def calculate_mvrv_zscore(df, ma_type='SMA', ma_length=220, lookback=200):
        """Calculate MVRV Z-Score"""
        if 'MVRV' not in df.columns:
            raise ValueError("MVRV column not found in the dataframe")
        
        if ma_type == 'SMA':
            df['MVRV_MA'] = df['MVRV'].rolling(window=ma_length).mean()
        elif ma_type == 'EMA':
            df['MVRV_MA'] = df['MVRV'].ewm(span=ma_length, adjust=False).mean()
        elif ma_type == 'DEMA':
            ema1 = df['MVRV'].ewm(span=ma_length, adjust=False).mean()
            ema2 = ema1.ewm(span=ma_length, adjust=False).mean()
            df['MVRV_MA'] = 2 * ema1 - ema2
        elif ma_type == 'WMA':
            weights = np.arange(1, ma_length + 1)
            df['MVRV_MA'] = df['MVRV'].rolling(window=ma_length).apply(
                lambda x: np.sum(weights * x) / weights.sum(), raw=True)
        else:
            df['MVRV_MA'] = df['MVRV'].rolling(window=ma_length).mean()
        
        df['MVRV_STD'] = df['MVRV'].rolling(window=lookback).std()
        df['MVRV_ZSCORE'] = (df['MVRV'] - df['MVRV_MA']) / df['MVRV_STD']
        
        return df
    
    def calculate_nupl_zscore(df, ma_type='SMA', ma_length=220, lookback=200):
        """Calculate NUPL Z-Score"""
        if 'NUPL' not in df.columns:
            raise ValueError("NUPL column not found in the dataframe")
        
        if ma_type == 'SMA':
            df['NUPL_MA'] = df['NUPL'].rolling(window=ma_length).mean()
        elif ma_type == 'EMA':
            df['NUPL_MA'] = df['NUPL'].ewm(span=ma_length, adjust=False).mean()
        elif ma_type == 'DEMA':
            ema1 = df['NUPL'].ewm(span=ma_length, adjust=False).mean()
            ema2 = ema1.ewm(span=ma_length, adjust=False).mean()
            df['NUPL_MA'] = 2 * ema1 - ema2
        elif ma_type == 'WMA':
            weights = np.arange(1, ma_length + 1)
            df['NUPL_MA'] = df['NUPL'].rolling(window=ma_length).apply(
                lambda x: np.sum(weights * x) / weights.sum(), raw=True)
        else:
            df['NUPL_MA'] = df['NUPL'].rolling(window=ma_length).mean()
        
        df['NUPL_STD'] = df['NUPL'].rolling(window=lookback).std()
        df['NUPL_ZSCORE'] = (df['NUPL'] - df['NUPL_MA']) / df['NUPL_STD']
        
        return df
    
    def calculate_combined_signal(df, method='average', mvrv_weight=0.5, nupl_weight=0.5):
        """Calculate combined Z-Score from MVRV and NUPL Z-Scores"""
        if 'MVRV_ZSCORE' not in df.columns:
            raise ValueError("MVRV_ZSCORE column not found in the dataframe")
        if 'NUPL_ZSCORE' not in df.columns:
            raise ValueError("NUPL_ZSCORE column not found in the dataframe")
        
        if method == 'average':
            df['COMBINED_ZSCORE'] = (df['MVRV_ZSCORE'] + df['NUPL_ZSCORE']) / 2
        elif method == 'weighted':
            total_weight = mvrv_weight + nupl_weight
            mvrv_weight = mvrv_weight / total_weight
            nupl_weight = nupl_weight / total_weight
            
            df['COMBINED_ZSCORE'] = (df['MVRV_ZSCORE'] * mvrv_weight) + (df['NUPL_ZSCORE'] * nupl_weight)
        elif method == 'consensus':
            conditions = [
                (df['MVRV_ZSCORE'] > 0) & (df['NUPL_ZSCORE'] > 0),
                (df['MVRV_ZSCORE'] < 0) & (df['NUPL_ZSCORE'] < 0)
            ]
            choices = [
                (df['MVRV_ZSCORE'] + df['NUPL_ZSCORE']) / 2,
                (df['MVRV_ZSCORE'] + df['NUPL_ZSCORE']) / 2
            ]
            default = 0
            
            df['COMBINED_ZSCORE'] = np.select(conditions, choices, default)
        else:
            df['COMBINED_ZSCORE'] = (df['MVRV_ZSCORE'] + df['NUPL_ZSCORE']) / 2
        
        return df
    
    def generate_signals(df, long_threshold=0.35, short_threshold=-0.35, z_score_col='COMBINED_ZSCORE'):
        """Generate trading signals based on Z-Score crossing thresholds"""
        df = df.copy()
        df['SIGNAL'] = 0
        
        current_position = 0
        
        for i in range(1, len(df)):
            if df[z_score_col].iloc[i-1] <= long_threshold and df[z_score_col].iloc[i] > long_threshold and current_position == 0:
                df.loc[df.index[i], 'SIGNAL'] = 1
                current_position = 1
            elif df[z_score_col].iloc[i-1] >= short_threshold and df[z_score_col].iloc[i] < short_threshold and current_position == 1:
                df.loc[df.index[i], 'SIGNAL'] = -1
                current_position = 0
        
        df['POSITION'] = 0
        position = 0
        
        for i in range(len(df)):
            if df['SIGNAL'].iloc[i] == 1:
                position = 1
            elif df['SIGNAL'].iloc[i] == -1:
                position = 0
                
            df.loc[df.index[i], 'POSITION'] = position
        
        return df
    
    def backtest_strategy(df, initial_capital=1000):
        """Backtest the combined Z-Score strategy and calculate returns"""
        bt_df = df.copy()
        
        if 'CLOSE' in bt_df.columns:
            bt_df['PRICE'] = bt_df['CLOSE']
        elif 'BTC_PRICE' in bt_df.columns:
            bt_df['PRICE'] = bt_df['BTC_PRICE']
        elif 'PRICE' not in bt_df.columns:
            raise ValueError("No price column found in the dataframe")
        
        bt_df['PORTFOLIO_VALUE'] = pd.Series([float(initial_capital)] * len(bt_df), index=bt_df.index)
        bt_df['BUY_HOLD_VALUE'] = pd.Series([float(initial_capital)] * len(bt_df), index=bt_df.index)
        
        initial_btc = initial_capital / bt_df['PRICE'].iloc[0]
        bt_df['BUY_HOLD_VALUE'] = initial_btc * bt_df['PRICE']
        
        position = 0
        btc_held = 0
        cash = initial_capital
        
        for i in range(1, len(bt_df)):
            if position == 1:
                bt_df.loc[bt_df.index[i], 'PORTFOLIO_VALUE'] = float(btc_held * bt_df['PRICE'].iloc[i])
            else:
                bt_df.loc[bt_df.index[i], 'PORTFOLIO_VALUE'] = float(cash)
            
            if bt_df['SIGNAL'].iloc[i] == 1 and position == 0:
                position = 1
                btc_held = cash / bt_df['PRICE'].iloc[i]
                cash = 0
            elif bt_df['SIGNAL'].iloc[i] == -1 and position == 1:
                position = 0
                cash = btc_held * bt_df['PRICE'].iloc[i]
                btc_held = 0
        
        bt_df['STRATEGY_RETURNS'] = bt_df['PORTFOLIO_VALUE'].pct_change()
        bt_df['BUY_HOLD_RETURNS'] = bt_df['BUY_HOLD_VALUE'].pct_change()
        
        bt_df['STRATEGY_CUM_RETURNS'] = (1 + bt_df['STRATEGY_RETURNS'].fillna(0)).cumprod() - 1
        bt_df['BUY_HOLD_CUM_RETURNS'] = (1 + bt_df['BUY_HOLD_RETURNS'].fillna(0)).cumprod() - 1
        
        return bt_df
    
    def log_strategy_result_to_snowflake(strategy_result):
        """Log the strategy execution results to Snowflake"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        escaped_message = strategy_result['formatted_message'].replace("'", "''")
        
        # Merge strategy result to avoid duplicates
        merge_sql = f"""
        MERGE INTO BITCOIN_DATA.BOT.STRATEGY_RESULTS AS target
        USING (
            SELECT 
                '{strategy_result['execution_date']}' AS EXECUTION_DATE,
                '{strategy_result['signal']}' AS SIGNAL,
                {strategy_result['current_position']} AS CURRENT_POSITION,
                '{strategy_result['current_signal']}' AS CURRENT_SIGNAL,
                {strategy_result['btc_price']} AS BTC_PRICE,
                {strategy_result['mvrv_zscore']} AS MVRV_ZSCORE,
                {strategy_result['nupl_zscore']} AS NUPL_ZSCORE,
                {strategy_result['combined_zscore']} AS COMBINED_ZSCORE,
                {strategy_result['total_return']} AS TOTAL_RETURN,
                {strategy_result['buy_hold_return']} AS BUY_HOLD_RETURN,
                {strategy_result['outperformance']} AS OUTPERFORMANCE,
                {strategy_result['month_return']} AS MONTH_RETURN,
                {strategy_result['market_month_return']} AS MARKET_MONTH_RETURN,
                '{escaped_message}' AS STRATEGY_MESSAGE
        ) AS source
        ON DATE(target.EXECUTION_DATE) = DATE(source.EXECUTION_DATE)
        WHEN MATCHED THEN 
            UPDATE SET
                SIGNAL = source.SIGNAL,
                CURRENT_POSITION = source.CURRENT_POSITION,
                CURRENT_SIGNAL = source.CURRENT_SIGNAL,
                BTC_PRICE = source.BTC_PRICE,
                MVRV_ZSCORE = source.MVRV_ZSCORE,
                NUPL_ZSCORE = source.NUPL_ZSCORE,
                COMBINED_ZSCORE = source.COMBINED_ZSCORE,
                TOTAL_RETURN = source.TOTAL_RETURN,
                BUY_HOLD_RETURN = source.BUY_HOLD_RETURN,
                OUTPERFORMANCE = source.OUTPERFORMANCE,
                MONTH_RETURN = source.MONTH_RETURN,
                MARKET_MONTH_RETURN = source.MARKET_MONTH_RETURN,
                STRATEGY_MESSAGE = source.STRATEGY_MESSAGE
        WHEN NOT MATCHED THEN 
            INSERT (
                EXECUTION_DATE, SIGNAL, CURRENT_POSITION, CURRENT_SIGNAL, BTC_PRICE,
                MVRV_ZSCORE, NUPL_ZSCORE, COMBINED_ZSCORE, TOTAL_RETURN, 
                BUY_HOLD_RETURN, OUTPERFORMANCE, MONTH_RETURN, MARKET_MONTH_RETURN,
                STRATEGY_MESSAGE
            ) VALUES (
                source.EXECUTION_DATE, source.SIGNAL, source.CURRENT_POSITION, 
                source.CURRENT_SIGNAL, source.BTC_PRICE, source.MVRV_ZSCORE, 
                source.NUPL_ZSCORE, source.COMBINED_ZSCORE, source.TOTAL_RETURN,
                source.BUY_HOLD_RETURN, source.OUTPERFORMANCE, source.MONTH_RETURN,
                source.MARKET_MONTH_RETURN, source.STRATEGY_MESSAGE
            );
        """
        hook.run(merge_sql)
        logger.info("Strategy result logged to Snowflake successfully")
    
    def get_signal_emoji(signal):
        """Return emoji based on signal"""
        if signal == "LONG":
            return "🟢"
        elif signal == "SHORT":
            return "🔴"
        elif signal == "HOLD BTC":
            return "💎"
        elif signal == "HOLD FIAT":
            return "💵"
        else:
            return "⚪"
    
    def format_last_signals(last_signals_df):
        """Format the last 4 signals for display"""
        if last_signals_df.empty:
            return "\n_Aucun signal historique disponible_"
        
        signals_text = ""
        for idx, row in last_signals_df.iterrows():
            signal_emoji = "🟢" if row['SIGNAL'] == 'LONG' else "🔴"
            date_str = pd.to_datetime(row['DATE']).strftime('%Y-%m-%d')
            price_str = f"${row['BTC_PRICE']:,.2f}"
            signals_text += f"\n{signal_emoji} *{row['SIGNAL']}* - {date_str} - Prix: {price_str}"
        
        return signals_text
    
    logger.info(f"Running BTC strategy - {datetime.now()}")
    
    # Load data from Snowflake - FULL DATASET
    logger.info("Loading FULL dataset from Snowflake...")
    df = load_data_from_snowflake()
    
    # Apply strategy calculations
    logger.info("Calculating MVRV Z-Score indicators...")
    df = calculate_mvrv_zscore(
        df.copy(),
        ma_type=OPTIMIZED_PARAMS['ma_type'],
        ma_length=OPTIMIZED_PARAMS['ma_length'],
        lookback=OPTIMIZED_PARAMS['zscore_lookback']
    )
    
    logger.info("Calculating NUPL Z-Score indicators...")
    df = calculate_nupl_zscore(
        df,
        ma_type=OPTIMIZED_PARAMS['ma_type'],
        ma_length=OPTIMIZED_PARAMS['ma_length'],
        lookback=OPTIMIZED_PARAMS['zscore_lookback']
    )
    
    logger.info("Calculating combined signal...")
    df = calculate_combined_signal(
        df,
        method=OPTIMIZED_PARAMS['combine_method'],
        mvrv_weight=OPTIMIZED_PARAMS['mvrv_weight'],
        nupl_weight=OPTIMIZED_PARAMS['nupl_weight']
    )
    
    logger.info("Generating trading signals...")
    df = generate_signals(
        df,
        long_threshold=OPTIMIZED_PARAMS['long_threshold'],
        short_threshold=OPTIMIZED_PARAMS['short_threshold'],
        z_score_col='COMBINED_ZSCORE'
    )
    
    logger.info("Backtesting strategy...")
    df = backtest_strategy(df, initial_capital=OPTIMIZED_PARAMS['initial_capital'])
    
    # Extract last 4 signals from backtest
    logger.info("Extracting last 4 LONG/SHORT signals from backtest...")
    last_signals_df = extract_last_signals_from_backtest(df)
    
    # Extract latest signal and position
    latest_date = df.index[-1]
    latest_signal = df['SIGNAL'].iloc[-1]
    current_position = df['POSITION'].iloc[-1]
    price_col = 'CLOSE' if 'CLOSE' in df.columns else 'PRICE'
    latest_price = df[price_col].iloc[-1]
    
    # Find last non-zero signal
    last_action_signal = None
    for i in range(len(df) - 1, -1, -1):
        if df['SIGNAL'].iloc[i] != 0:
            last_action_signal = df['SIGNAL'].iloc[i]
            break
    
    # Determine current signal
    if latest_signal == 1:
        current_signal = "LONG"
        signal_context = "🚀 Achat de BTC recommandé"
    elif latest_signal == -1:
        current_signal = "SHORT" 
        signal_context = "💰 Vente de BTC recommandée - Passage en fiat"
    else:
        if current_position == 1:
            current_signal = "HOLD BTC"
            if last_action_signal == 1:
                signal_context = "💎 Conserver vos BTCs (dernier signal: LONG)"
            else:
                signal_context = "💎 Conserver vos BTCs"
        else:
            current_signal = "HOLD FIAT"
            if last_action_signal == -1:
                signal_context = "💵 Conserver votre fiat (dernier signal: SHORT)"
            else:
                signal_context = "💵 Conserver votre fiat"
    
    # Calculate performance metrics
    initial_value = df['PORTFOLIO_VALUE'].iloc[0]
    final_value = df['PORTFOLIO_VALUE'].iloc[-1]
    buy_hold_final = df['BUY_HOLD_VALUE'].iloc[-1]
    
    total_return = (final_value / initial_value - 1) * 100
    buy_hold_return = (buy_hold_final / initial_value - 1) * 100
    outperformance = total_return - buy_hold_return
    
    # Calculate 30-day return
    if len(df) > 30:
        month_return = (df['PORTFOLIO_VALUE'].iloc[-1] / df['PORTFOLIO_VALUE'].iloc[-30] - 1) * 100
        market_month_return = (df['BUY_HOLD_VALUE'].iloc[-1] / df['BUY_HOLD_VALUE'].iloc[-30] - 1) * 100
    else:
        month_return = 0
        market_month_return = 0
    
    # Extract current Z-Score values
    current_mvrv_zscore = df['MVRV_ZSCORE'].iloc[-1]
    current_nupl_zscore = df['NUPL_ZSCORE'].iloc[-1]
    current_combined_zscore = df['COMBINED_ZSCORE'].iloc[-1]
    
    # Get signal emoji
    emoji = get_signal_emoji(current_signal)
    
    # Format last signals
    last_signals_text = format_last_signals(last_signals_df)
    
    # Create formatted message with last signals history
    formatted_message = f"""*Rapport Quotidien de la Stratégie BTC*

*Prix de fermeture du BTC pour {latest_date.strftime('%Y-%m-%d')}*: ${latest_price:,.2f}

*SIGNAL ACTUEL*: {emoji} *{current_signal}*

{signal_context}

*Indicateurs Z-Score*:
- MVRV Z-Score: {current_mvrv_zscore:.3f}
- NUPL Z-Score: {current_nupl_zscore:.3f}
- *Z-Score Combiné*: {current_combined_zscore:.3f}

*Historique des 4 derniers signaux LONG/SHORT*:{last_signals_text}

*Résultats du backtest de la stratégie sur le marché BTC*:
- Rendement Total: {total_return:.2f}%
- Rendement Buy & Hold: {buy_hold_return:.2f}%
- Surperformance: {outperformance:.2f}%

*Performance sur 30 jours*:
- Stratégie: {month_return:.2f}%
- Marché: {market_month_return:.2f}%

*Paramètres de la stratégie* ⚙️:
- Méthode: {OPTIMIZED_PARAMS['combine_method']}
- MA Type: {OPTIMIZED_PARAMS['ma_type']} (Weighted Moving Average)
- MA Length: {OPTIMIZED_PARAMS['ma_length']} jours
- Lookback: {OPTIMIZED_PARAMS['zscore_lookback']} jours
- Seuil d'achat: {OPTIMIZED_PARAMS['long_threshold']}
- Seuil de vente: {OPTIMIZED_PARAMS['short_threshold']}
- Poids MVRV: {OPTIMIZED_PARAMS['mvrv_weight']} (80%)
- Poids NUPL: {OPTIMIZED_PARAMS['nupl_weight']} (20%)

**📋 Commandes programmées:**
/news - Dernières actualités Bitcoin
/action - Signal de trading actuel  
/price - Prix BTC temps réel
/help - Afficher l'aide
/stop - Se désabonner

⚠️ *AVERTISSEMENT* ⚠️
_Ceci est uniquement une recommandation de trading basée sur des indicateurs on-chain. Une analyse approfondie doit être effectuée par le destinataire avant toute décision d'investissement._"""
    
    # Prepare strategy result for logging
    strategy_result = {
        'execution_date': latest_date.strftime('%Y-%m-%d %H:%M:%S'),
        'signal': current_signal,
        'current_position': current_position,
        'current_signal': current_signal,
        'btc_price': latest_price,
        'mvrv_zscore': current_mvrv_zscore,
        'nupl_zscore': current_nupl_zscore,
        'combined_zscore': current_combined_zscore,
        'total_return': total_return,
        'buy_hold_return': buy_hold_return,
        'outperformance': outperformance,
        'month_return': month_return,
        'market_month_return': market_month_return,
        'formatted_message': formatted_message
    }
    
    # Log to Snowflake
    log_strategy_result_to_snowflake(strategy_result)
    
    logger.info("BTC strategy execution completed successfully!")
    
    return strategy_result


# Task definitions
fetch_BITCOIN_DATA_task = PythonOperator(
    task_id='fetch_BITCOIN_DATA',
    python_callable=merge_btc_ohclv,
    dag=dag,
)

execute_btc_insert_task = SnowflakeOperator(
    task_id='insert_BITCOIN_DATA',
    sql="{{ ti.xcom_pull(key='btc_merge_sql', task_ids='fetch_BITCOIN_DATA') }}",
    dag=dag,
    **snowflake_conn_params,
)

bitcoin_metrics_task = PythonOperator(
    task_id='bitcoin_metrics_updater',
    python_callable=run_bitcoin_metrics_updater,
    dag=dag,
)

btc_strategy_task = PythonOperator(
    task_id='btc_strategy_execution_with_broadcast',
    python_callable=run_btc_strategy_with_logging,
    dag=dag,
)

# Set task dependencies
fetch_BITCOIN_DATA_task >> execute_btc_insert_task >> bitcoin_metrics_task >> btc_strategy_task

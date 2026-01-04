"""
DAG to test Airflow and Snowflake connection.
This DAG will:
1. Test Snowflake connection
2. Create a test table
3. Insert sample data
4. Query the data
5. Clean up the test table
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os

# Default arguments for the DAG
default_args = {
    'owner': 'mj',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'test_snowflake_connection',
    default_args=default_args,
    description='Test Airflow and Snowflake connectivity',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['test', 'snowflake'],
)

def test_connection():
    """Test the Snowflake connection using environment variables"""
    try:
    
        # Create Snowflake hook
        hook = SnowflakeHook(
            snowflake_conn_id='snowflake_default'
        )
        
        # Test the connection
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Execute a simple query
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        result = cursor.fetchone()
        
        print("\n" + "="*60)
        print("✅ SNOWFLAKE CONNECTION SUCCESSFUL!")
        print("="*60)
        print(f"Snowflake Version: {result[0]}")
        print(f"Current User: {result[1]}")
        print(f"Current Role: {result[2]}")
        print(f"Current Database: {result[3]}")
        print(f"Current Schema: {result[4]}")
        print("="*60 + "\n")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print("\n" + "="*60)
        print("❌ SNOWFLAKE CONNECTION FAILED!")
        print("="*60)
        print(f"Error: {str(e)}")
        print("="*60 + "\n")
        raise

def query_test_data():
    """Query the test data from Snowflake"""
    try:

        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM AIRFLOW_TEST_TABLE ORDER BY ID")
        results = cursor.fetchall()
        
        print("\n" + "="*60)
        print("📊 QUERY RESULTS FROM TEST TABLE")
        print("="*60)
        for row in results:
            print(f"ID: {row[0]}, Name: {row[1]}, Created: {row[2]}")
        print("="*60 + "\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error querying data: {str(e)}")
        raise

# Task 1: Test connection
test_connection_task = PythonOperator(
    task_id='test_connection',
    python_callable=test_connection,
    dag=dag,
)

# Task 2: Create test table
create_table_task = SnowflakeOperator(
    task_id='create_test_table',
    sql="""
        CREATE OR REPLACE TABLE AIRFLOW_TEST_TABLE (
            ID INTEGER,
            NAME VARCHAR(100),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    snowflake_conn_id='snowflake_default',
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    dag=dag,
)

# Task 3: Insert test data
insert_data_task = SnowflakeOperator(
    task_id='insert_test_data',
    sql="""
        INSERT INTO AIRFLOW_TEST_TABLE (ID, NAME) VALUES
        (1, 'Airflow Test 1'),
        (2, 'Airflow Test 2'),
        (3, 'Airflow Test 3'),
        (4, 'Connection Successful'),
        (5, 'Raspberry Pi Rules!')
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag,
)

# Task 4: Query the data
query_data_task = PythonOperator(
    task_id='query_test_data',
    python_callable=query_test_data,
    dag=dag,
)

# Task 5: Clean up - drop test table
cleanup_task = SnowflakeOperator(
    task_id='cleanup_test_table',
    sql="DROP TABLE IF EXISTS AIRFLOW_TEST_TABLE",
    snowflake_conn_id='snowflake_default',
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    dag=dag,
)

# Define task dependencies
test_connection_task >> create_table_task >> insert_data_task >> query_data_task >> cleanup_task
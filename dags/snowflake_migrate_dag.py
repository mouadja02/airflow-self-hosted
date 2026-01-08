"""
Airflow DAG: Snowflake Trial Account Migration
Automates migrating multiple databases from expiring trial to new trial account.
Database list is configured via Airflow variable: SNOWFLAKE_MIGRATION_DATABASES
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable
import json
import logging

# Import TaskGroup at module level
try:
    from airflow.utils.task_group import TaskGroup
except ImportError:
    # Fallback for older Airflow versions
    TaskGroup = None
    logger = logging.getLogger(__name__)
    logger.warning("TaskGroup not available, will run databases sequentially")

# Set up logging
logger = logging.getLogger(__name__)
# Suppress verbose logging for large MERGE statements
sf_connector_logger = logging.getLogger('snowflake.connector')
sf_hook_logger = logging.getLogger('airflow.task.hooks.airflow.providers.snowflake.hooks.snowflake.SnowflakeHook')
original_connector_level = sf_connector_logger.level
original_hook_level = sf_hook_logger.level
sf_connector_logger.setLevel(logging.WARNING)
sf_hook_logger.setLevel(logging.WARNING)

def send_telegram_failure_notification(context):
    """Send simple Telegram notification when a task fails"""
    import requests

    try:
        # Get Telegram credentials
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        # Extract basic info
        task_instance = context.get('task_instance')
        dag_id = context.get('dag').dag_id
        task_id = task_instance.task_id if task_instance else "Unknown"

        message = (
            f"🚨 Snowflake Migration DAG Failed!\n\n"
            f"DAG: {dag_id}\n"
            f"Failed Task: {task_id}\n\n"
            f"Please check the Airflow UI for details:\n"
            f"https://airflow.mj-dev.net/dags/{dag_id}"
        )

        # Send Telegram notification
        telegram_url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        payload = {
            "chat_id": telegram_chat_id,
            "text": message
        }

        response = requests.post(telegram_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("✅ Telegram failure notification sent successfully!")

    except Exception as e:
        logger.error(f"❌ Failed to send Telegram failure notification: {str(e)}")

# Default arguments
default_args = {
    'owner': 'mj',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': send_telegram_failure_notification,
}

# DAG definition
dag = DAG(
    'snowflake_trial_migration',
    default_args=default_args,
    description='Migrate multiple databases from expiring trial to new trial account',
    schedule='0 0 1 */3 *', # Run every 3 months
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['snowflake', 'migration', 'trial'],
)

# Configuration
source_conn_id = 'snowflake_default'
target_conn_id = 'snowflake_new'

# Get list of databases to migrate from Airflow variable
# Expected format: ["DATABASE1", "DATABASE2", "DATABASE3"]
try:
    databases_to_migrate = Variable.get("SNOWFLAKE_MIGRATION_DATABASES", deserialize_json=True)
    if not isinstance(databases_to_migrate, list):
        raise ValueError("SNOWFLAKE_MIGRATION_DATABASES must be a JSON list")
    logger.info(f"Databases to migrate: {databases_to_migrate}")
except Exception as e:
    logger.warning(f"Failed to get SNOWFLAKE_MIGRATION_DATABASES variable: {str(e)}")
    logger.warning("Falling back to default database: BITCOIN_DATA")
    databases_to_migrate = ['BITCOIN_DATA']

def test_connections(**context):
    """Verify both Snowflake connections are working"""
    try:
        # Test source connection
        logger.info("Testing source connection (snowflake_default)...")
        source_hook = SnowflakeHook(snowflake_conn_id=source_conn_id)
        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
        source_cursor.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER()")
        source_info = source_cursor.fetchone()
        logger.info(f"✅ Source Account: {source_info[0]}, User: {source_info[1]}")
        source_cursor.close()
        source_conn.close()

        # Test target connection
        logger.info("Testing target connection (snowflake_new)...")
        target_hook = SnowflakeHook(snowflake_conn_id=target_conn_id)
        target_conn = target_hook.get_conn()
        target_cursor = target_conn.cursor()
        target_cursor.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER()")
        target_info = target_cursor.fetchone()
        logger.info(f"✅ Target Account: {target_info[0]}, User: {target_info[1]}")
        target_cursor.close()
        target_conn.close()

        logger.info("✅ Both connections verified successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Connection test failed: {str(e)}")
        raise


def get_table_list(database_name: str, **context):
    """Get list of all tables in specified database"""
    try:
        source_hook = SnowflakeHook(snowflake_conn_id=source_conn_id, role='ACCOUNTADMIN')
        conn = source_hook.get_conn()
        cursor = conn.cursor()

        # Get all tables in the database
        cursor.execute(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = '{database_name}'
            AND TABLE_TYPE = 'BASE TABLE';

        """)

        tables = cursor.fetchall()
        tables_list = [{'schema': row[0], 'table': row[1]} for row in tables]

        logger.info(f"Found {len(tables_list)} tables in {database_name}")
        for table in tables_list:
            logger.info(f"  - {table['schema']}.{table['table']}")
        cursor.close()
        conn.close()

        # Store in XCom with database-specific key
        context['task_instance'].xcom_push(key=f'tables_list_{database_name}', value=tables_list)

    except Exception as e:
        logger.error(f"❌ Failed to get tables list for {database_name}: {str(e)}")
        raise

    return len(tables_list)

def get_schemas_list(database_name: str, **context):
    """Get list of all schemas in specified database"""
    try:
        source_hook = SnowflakeHook(snowflake_conn_id=source_conn_id, role='ACCOUNTADMIN')
        conn = source_hook.get_conn()
        cursor = conn.cursor()

        # Get all schemas in the database
        cursor.execute(f"""
            SELECT DISTINCT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE CATALOG_NAME = '{database_name}'
            AND SCHEMA_NAME <> 'INFORMATION_SCHEMA';

        """)

        schemas = cursor.fetchall()
        schemas_list = [{'schema': row[0]} for row in schemas]

        logger.info(f"Found {len(schemas_list)} schemas in {database_name}")
        for schema in schemas_list:
            logger.info(f"  - schema : {schema['schema']}")
        cursor.close()
        conn.close()

        # Store in XCom with database-specific key
        context['task_instance'].xcom_push(key=f'schemas_list_{database_name}', value=schemas_list)

        return len(schemas_list)

    except Exception as e:
        logger.error(f"❌ Failed to get schemas list for {database_name}: {str(e)}")
        raise

def get_tables_list(database_name: str, **context):
    """Get list of all tables in specified database (duplicate function, keeping for backward compatibility)"""
    try:
        source_hook = SnowflakeHook(snowflake_conn_id=source_conn_id, role='ACCOUNTADMIN')
        conn = source_hook.get_conn()
        cursor = conn.cursor()

        # Get all tables in the database
        cursor.execute(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = '{database_name}'
            AND TABLE_TYPE = 'BASE TABLE'
            AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA';

        """)

        tables = cursor.fetchall()
        tables_list = [{'schema': row[0], 'table': row[1]} for row in tables]

        logger.info(f"Found {len(tables_list)} tables in {database_name}")
        for table in tables_list:
            logger.info(f"  - {table['schema']}.{table['table']}")
        cursor.close()
        conn.close()

        # Store in XCom with database-specific key
        context['task_instance'].xcom_push(key=f'tables_list_{database_name}', value=tables_list)

    except Exception as e:
        logger.error(f"❌ Failed to get tables list for {database_name}: {str(e)}")
        raise

    return len(tables_list)

def init_environment(database_name: str, **context):
    """Initialize the target account environment for specified database"""
    target_hook = SnowflakeHook(snowflake_conn_id=target_conn_id, role='ACCOUNTADMIN')
    conn = target_hook.get_conn()
    cursor = conn.cursor()

    try:
        #Get current user
        cursor.execute("SELECT CURRENT_USER()")
        current_user = cursor.fetchone()[0]

        # Create warehouse (only once, check if exists)
        cursor.execute("""
            CREATE OR REPLACE WAREHOUSE COMPUTE_WH
            WITH WAREHOUSE_SIZE = 'XSMALL'
            AUTO_SUSPEND = 60
            AUTO_RESUME = TRUE
        """)

        # Set default role to SYSADMIN
        cursor.execute("GRANT OWNERSHIP ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN")
        cursor.execute(f"ALTER USER {current_user} SET DEFAULT_ROLE = 'SYSADMIN' DEFAULT_WAREHOUSE = 'COMPUTE_WH'")

        cursor.execute(f"CREATE OR REPLACE DATABASE {database_name}")

        logger.info(f"✅ Target account environment initialized successfully for {database_name}!")

    except Exception as e:
        logger.error(f"❌ Failed to initialize target account environment for {database_name}: {str(e)}")
        raise

    finally:
        cursor.close()
        conn.close()
        
def create_sharing(database_name: str, **context):
    """Create a share in the expiring account and add all objects for specified database"""
    conn_source = None
    cursor_source = None
    target_conn = None
    target_cursor = None

    try:
        source_hook = SnowflakeHook(snowflake_conn_id=source_conn_id, role='ACCOUNTADMIN')
        target_hook = SnowflakeHook(snowflake_conn_id=target_conn_id, role='ACCOUNTADMIN')

        conn_source = source_hook.get_conn()
        cursor_source = conn_source.cursor()

        # Create the share
        share_name = f'{database_name}_SNOWFLAKE_SHARE_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        cursor_source.execute(f'CREATE OR REPLACE SHARE "{share_name}"')
        logger.info(f"✅ Created share: {share_name}")

        # Store share name in XCom for use in create_database task with database-specific key
        context['task_instance'].xcom_push(key=f'share_name_{database_name}', value=share_name)

        # Grant database usage
        cursor_source.execute(f'GRANT USAGE ON DATABASE {database_name} TO SHARE "{share_name}"')
        logger.info(f"✅ Granted database usage for {database_name}")

        # Get schemas list from XCom and grant usage on each
        schemas_list = context['task_instance'].xcom_pull(
            key=f'schemas_list_{database_name}',
            task_ids=f'migrate_{database_name}.get_schemas_list'
        )
        for schema in schemas_list:
            cursor_source.execute(f'GRANT USAGE ON SCHEMA {database_name}.{schema["schema"]} TO SHARE "{share_name}"')
            cursor_source.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {database_name}.{schema["schema"]} TO SHARE "{share_name}"')
            logger.info(f"✅ Granted usage on schema: {schema['schema']}")

        # Get account identifier of source and target account
        target_conn = target_hook.get_conn()
        target_cursor = target_conn.cursor()

        cursor_source.execute("SELECT CURRENT_ORGANIZATION_NAME() || '.' || CURRENT_ACCOUNT_NAME()")
        source_account_identifier = cursor_source.fetchone()[0]
        context['task_instance'].xcom_push(key=f'source_account_identifier_{database_name}', value=source_account_identifier)

        target_cursor.execute("SELECT CURRENT_ORGANIZATION_NAME() || '.' || CURRENT_ACCOUNT_NAME()")
        target_account_identifier = target_cursor.fetchone()[0]

        # Share with the target account
        cursor_source.execute(f'ALTER SHARE "{share_name}" ADD ACCOUNTS = {target_account_identifier}')
        logger.info(f"✅ Shared '{share_name}' with target account: {target_account_identifier[:4]}****")

        logger.info(f"✅ Sharing process completed successfully for {database_name}!")
    except Exception as e:
        logger.error(f"❌ Sharing process failed for {database_name}: {str(e)}")
        raise
    finally:
        if cursor_source:
            cursor_source.close()
        if conn_source:
            conn_source.close()
        if target_cursor:
            target_cursor.close()
        if target_conn:
            target_conn.close()


def create_database(database_name: str, **context):
    """Create shared database from share, populate tables with historical data for specified database"""
    conn = None
    cursor = None

    try:
        target_hook = SnowflakeHook(snowflake_conn_id=target_conn_id, role='ACCOUNTADMIN')
        conn = target_hook.get_conn()
        cursor = conn.cursor()

        share_name = context['task_instance'].xcom_pull(
            key=f'share_name_{database_name}',
            task_ids=f'migrate_{database_name}.create_sharing'
        )
        source_account_identifier = context['task_instance'].xcom_pull(
            key=f'source_account_identifier_{database_name}',
            task_ids=f'migrate_{database_name}.create_sharing'
        )

        # Create shared database from the share
        cursor.execute(f'CREATE DATABASE {database_name}_SHARED FROM SHARE {source_account_identifier}."{share_name}"')
        logger.info(f"✅ Created shared database '{database_name}_SHARED' from share")

        # Create target database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        logger.info(f"✅ Created target database '{database_name}'")

        # Give ownership to SYSADMIN
        cursor.execute(f"GRANT OWNERSHIP ON DATABASE {database_name} TO ROLE SYSADMIN")
        logger.info(f"✅ Database creation process completed successfully for {database_name}!")

        # Get tables list from XCom
        tables_list = context['task_instance'].xcom_pull(
            key=f'tables_list_{database_name}',
            task_ids=f'migrate_{database_name}.get_table_list'
        )
        logger.info(f"Starting to populate {len(tables_list)} tables with historical data for {database_name}")

        for table in tables_list:
            schema = table['schema']
            table_name = table['table']

            # Create schema
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema}")

            if schema.upper() != 'ANALYTICS' or (schema.upper() != 'COINDESK' and table_name.upper() != 'NEWS'):
                # Create and populate table with CTAS
                cursor.execute(f"""
                    CREATE OR REPLACE TABLE {database_name}.{schema}.{table_name}
                    AS SELECT * FROM {database_name}_SHARED.{schema}.{table_name}
                """)

        # Grant ownership on all schemas and tables at the end
        cursor.execute(f"GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE {database_name} TO ROLE SYSADMIN")
        cursor.execute(f"GRANT OWNERSHIP ON ALL TABLES IN DATABASE {database_name} TO ROLE SYSADMIN")

        logger.info(f"✅ Database creation and population completed successfully for {database_name}!")

    except Exception as e:
        logger.error(f"❌ Database creation and population failed for {database_name}: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_sql_migrations(database_name: str, **context):
    """Execute SQL migration files from snowflake directory for specified database"""
    from pathlib import Path

    try:
        target_hook = SnowflakeHook(snowflake_conn_id=target_conn_id, role='ACCOUNTADMIN')
        conn = target_hook.get_conn()
        cursor = conn.cursor()

        # Set database context
        cursor.execute(f"USE DATABASE {database_name}")

        # Get migration files sorted by version
        migrations_dir = Path(__file__).parent.parent / 'snowflake' / 'migration'

        # Check if migrations directory exists
        if not migrations_dir.exists():
            logger.warning(f"⚠️ Migrations directory not found: {migrations_dir}")
            logger.info(f"Skipping SQL migrations for {database_name}")
            return

        sql_files = sorted(migrations_dir.glob('*.sql'))

        logger.info(f"Found {len(sql_files)} migration files to execute for {database_name}")

        for sql_file in sql_files:
            logger.info(f"Executing migration: {sql_file.name}")

            # Read SQL file
            with open(sql_file, 'r') as f:
                sql_content = f.read()

            # Smart split: handle stored procedures with $$ delimiters
            statements = []
            current_stmt = []
            in_procedure = False

            for line in sql_content.split('\n'):
                # Skip pure comment lines at the start of a new statement
                if not current_stmt and line.strip().startswith('--'):
                    continue

                current_stmt.append(line)

                # Check for procedure boundaries
                if '$$' in line:
                    in_procedure = not in_procedure

                # If not in procedure and line ends with semicolon, it's a statement boundary
                if not in_procedure and line.strip().endswith(';'):
                    stmt = '\n'.join(current_stmt).strip()
                    # Filter out comment-only statements
                    if stmt and not all(l.strip().startswith('--') or not l.strip() for l in stmt.split('\n')):
                        statements.append(stmt)
                    current_stmt = []

            # Add any remaining content
            if current_stmt:
                stmt = '\n'.join(current_stmt).strip()
                if stmt and not stmt.startswith('--'):
                    statements.append(stmt)

            for i, statement in enumerate(statements):
                try:
                    cursor.execute(statement)
                except Exception as stmt_error:
                    logger.warning(f"  ⚠️ Statement {i+1} failed (may be expected): {str(stmt_error)}")
                    # Continue with next statement even if one fails

            logger.info(f"✅ Completed migration: {sql_file.name}")

        cursor.close()
        conn.close()

        logger.info(f"✅ All SQL migrations executed successfully for {database_name}!")

    except Exception as e:
        logger.error(f"❌ SQL migration execution failed for {database_name}: {str(e)}")
        raise

def migration_success(**context):
    """Migration completed - cleanup resources and send Telegram notification"""
    import requests

    try:
        # Step 1: Cleanup shared databases and shares
        logger.info("=" * 80)
        logger.info("🧹 CLEANING UP TEMPORARY RESOURCES")
        logger.info("=" * 80)

        # Cleanup shared databases in target account
        target_hook = SnowflakeHook(snowflake_conn_id=target_conn_id, role='ACCOUNTADMIN')
        target_conn = target_hook.get_conn()
        target_cursor = target_conn.cursor()

        for database_name in databases_to_migrate:
            shared_db_name = f"{database_name}_SHARED"
            try:
                logger.info(f"Dropping shared database: {shared_db_name}")
                target_cursor.execute(f"DROP DATABASE IF EXISTS {shared_db_name}")
                logger.info(f"✅ Dropped shared database: {shared_db_name}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to drop shared database {shared_db_name}: {str(e)}")

        target_cursor.close()
        target_conn.close()

        # Cleanup shares in source account
        source_hook = SnowflakeHook(snowflake_conn_id=source_conn_id, role='ACCOUNTADMIN')
        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()

        for database_name in databases_to_migrate:
            # Get share name from XCom
            share_name = context['task_instance'].xcom_pull(
                key=f'share_name_{database_name}',
                task_ids=f'migrate_{database_name}.create_sharing'
            )

            if share_name:
                try:
                    logger.info(f"Dropping share: {share_name}")
                    source_cursor.execute(f'DROP SHARE IF EXISTS "{share_name}"')
                    logger.info(f"✅ Dropped share: {share_name}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to drop share {share_name}: {str(e)}")

        source_cursor.close()
        source_conn.close()

        logger.info("✅ Cleanup completed successfully!")
        logger.info("")

        # Step 2: Success notification
        # Get Telegram credentials from Airflow Variables
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN") or Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID") or Variable.get("TELEGRAM_CHAT_ID", default_var=None)

        logger.info("=" * 80)
        logger.info("🎉 MIGRATION PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info("")
        logger.info("✅ All data has been successfully migrated to the new Snowflake account")
        logger.info("✅ Temporary resources (shares and shared databases) have been cleaned up")

        # Send Telegram notification if credentials are configured
        if telegram_token and telegram_chat_id:
            # Create list of migrated databases for notification
            db_list = ", ".join(databases_to_migrate)

            message = (
                "🎉 *Snowflake Migration Completed Successfully!*\n\n"
                f"✅ Migrated databases: {db_list}\n"
                "✅ All data has been migrated to the new Snowflake account.\n"
                "✅ Temporary resources (shares and shared databases) have been cleaned up.\n\n"
                "📝 *NEXT STEP - Switch Airflow Connections:*\n"
                "1. Go to Admin > Connections in Airflow UI\n"
                f"2. Delete the `{source_conn_id}` connection\n"
                f"3. Edit `{target_conn_id}` and change its Connection Id to `{source_conn_id}`\n"
                "4. Save the changes\n\n"
                "⚠️ Your DAGs will use the new account after you complete the connection swap."
            )

            telegram_url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
            payload = {
                "chat_id": telegram_chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }

            try:
                response = requests.post(telegram_url, json=payload, timeout=10)
                response.raise_for_status()
                logger.info("✅ Telegram notification sent successfully!")
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ Failed to send Telegram notification: {str(e)}")
                logger.warning("   Migration completed successfully, but notification failed.")
        else:
            logger.warning("⚠️ Telegram credentials not provided")
            logger.warning("   Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to enable notifications")

        logger.info("")
        logger.info("📝 NEXT STEPS - Complete the connection swap in Airflow UI:")
        logger.info(f"   1. Delete the '{source_conn_id}' connection")
        logger.info(f"   2. Edit '{target_conn_id}' and change Connection Id to '{source_conn_id}'")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"❌ Migration success task failed: {str(e)}")
        raise


# Define tasks
test_connections_task = PythonOperator(
    task_id='test_connections',
    python_callable=test_connections,
    dag=dag,
)

# Create task groups for each database
from airflow.utils.task_group import TaskGroup

database_task_groups = []

for database_name in databases_to_migrate:
    with TaskGroup(group_id=f'migrate_{database_name}', dag=dag) as db_group:

        get_schemas_list_task = PythonOperator(
            task_id='get_schemas_list',
            python_callable=get_schemas_list,
            op_kwargs={'database_name': database_name},
            dag=dag,
        )

        get_table_list_task = PythonOperator(
            task_id='get_table_list',
            python_callable=get_table_list,
            op_kwargs={'database_name': database_name},
            dag=dag,
        )

        init_environment_task = PythonOperator(
            task_id='init_environment',
            python_callable=init_environment,
            op_kwargs={'database_name': database_name},
            dag=dag,
        )

        create_sharing_task = PythonOperator(
            task_id='create_sharing',
            python_callable=create_sharing,
            op_kwargs={'database_name': database_name},
            dag=dag,
        )

        create_database_task = PythonOperator(
            task_id='create_database',
            python_callable=create_database,
            op_kwargs={'database_name': database_name},
            dag=dag,
        )

        # Define flow within the task group
        # Only add execute_sql_migrations task for BITCOIN_DATA database
        if database_name.upper() == 'BITCOIN_DATA':
            execute_sql_migrations_task = PythonOperator(
                task_id='execute_sql_migrations',
                python_callable=execute_sql_migrations,
                op_kwargs={'database_name': database_name},
                dag=dag,
            )
            [get_schemas_list_task, get_table_list_task] >> init_environment_task >> create_sharing_task >> create_database_task >> execute_sql_migrations_task
        else:
            # For other databases, skip the SQL migrations task
            [get_schemas_list_task, get_table_list_task] >> init_environment_task >> create_sharing_task >> create_database_task

    database_task_groups.append(db_group)

migration_success_task = PythonOperator(
    task_id='migration_success',
    python_callable=migration_success,
    dag=dag,
)

# Define overall flow
test_connections_task >> database_task_groups >> migration_success_task
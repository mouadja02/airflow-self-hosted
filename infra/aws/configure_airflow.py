"""Enable cloud workers and install their encrypted, execution-only connection."""

import argparse
import configparser
import json
from pathlib import Path

import boto3
from cryptography.fernet import Fernet


ROOT = Path(__file__).resolve().parents[2]


def prepare_environment(state):
    path = ROOT / '.env'
    lines = path.read_text().splitlines()
    values = dict(line.split('=', 1) for line in lines if '=' in line and not line.lstrip().startswith('#'))
    key = values.get('AIRFLOW_FERNET_KEY', '').strip().strip('"\'')
    if not key:
        configuration = configparser.ConfigParser(interpolation=None)
        configuration.read(ROOT / 'docker/data/airflow/config/airflow.cfg')
        key = configuration.get('core', 'fernet_key', fallback='').strip() or Fernet.generate_key().decode()
    Fernet(key.encode())
    updates = {'AIRFLOW_FERNET_KEY': key, 'BITCOIN_WORKER_STATE_MACHINE_ARN': state['state_machine_arn']}
    lines = [line for line in lines if line.split('=', 1)[0].strip() not in updates]
    lines.extend(f'{name}={value}' for name, value in updates.items())
    temporary = path.with_name('.env.cloud-setup.tmp')
    temporary.write_text('\n'.join(lines) + '\n')
    temporary.chmod(0o600)
    temporary.replace(path)
    print('Configured cloud worker ARN and shared Fernet key; no secret values printed')


def install_connection(state, profile):
    from airflow.models.connection import Connection
    from airflow.utils.session import create_session
    manager = boto3.Session(profile_name=profile, region_name=state['region']).client('secretsmanager')
    credentials = json.loads(manager.get_secret_value(SecretId=state['runtime_secret_arn'])['SecretString'])
    with create_session() as session:
        connection = session.query(Connection).filter(Connection.conn_id == 'aws_bitcoin_workers').one_or_none()
        if connection is None:
            connection = Connection(conn_id='aws_bitcoin_workers', conn_type='aws')
            session.add(connection)
        connection.login = credentials['aws_access_key_id']
        connection.password = credentials['aws_secret_access_key']
        connection.extra = json.dumps({'region_name': state['region']})
        if not connection.is_encrypted:
            raise RuntimeError('Runtime connection must be encrypted before enabling the cloud DAGs')
    print('Installed encrypted aws_bitcoin_workers connection with execution-only permissions')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'install'])
    parser.add_argument('--profile', default='airflow-bitcoin-deployer')
    args = parser.parse_args()
    state = json.loads((ROOT / 'infra/aws/deployment-state.json').read_text())
    if args.action == 'prepare':
        prepare_environment(state)
    else:
        install_connection(state, args.profile)

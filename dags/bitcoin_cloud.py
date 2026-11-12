"""Small, deferrable Airflow control tasks for the AWS Bitcoin workers."""

import hashlib
import json
import os

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator

from bitcoin_pipeline_helpers import asset_update_range, closed_hour_cutoff, HOUR


CONNECTION_ID = 'aws_bitcoin_workers'


def cloud_enabled():
    return bool(os.environ.get('BITCOIN_WORKER_STATE_MACHINE_ARN'))


class BitcoinCloudOperator(StepFunctionStartExecutionOperator):
    """Send a small request, then release the Pi worker while AWS does the work."""

    def __init__(self, *, action, initialize=False, **kwargs):
        arn = os.environ.get('BITCOIN_WORKER_STATE_MACHINE_ARN')
        if not arn:
            raise AirflowException('The Bitcoin worker state machine is not configured')
        super().__init__(state_machine_arn=arn, aws_conn_id=CONNECTION_ID,
            region_name=arn.split(':')[3], deferrable=True,
            waiter_delay=30, waiter_max_attempts=40, **kwargs)
        self.action = action
        self.initialize = initialize

    def execute(self, context):
        ti = context['ti']
        ti.xcom_push(key='cloud_execution_try_number', value=ti.try_number)
        identity = f"{ti.dag_id}/{context['run_id']}/{ti.task_id}/{ti.try_number}"
        self.name = self.action + '-' + hashlib.sha256(identity.encode()).hexdigest()[:48]
        change = asset_update_range(context)
        self.input = {'action': self.action}
        if self.action == 'calculate':
            start = ti.xcom_pull(task_ids='check_table_status', key='calculation_start')
            through = ti.xcom_pull(task_ids='check_table_status', key='latest_btc_timestamp')
            self.input.update({'initialize': self.initialize, 'start_timestamp': start,
                'through_timestamp': int(through) if through else closed_hour_cutoff() - HOUR})
        elif change:
            self.input['through_timestamp'] = change['through_timestamp']
        self.log.info('Cloud worker action=%s; CSV data stays in AWS', self.action)
        return super().execute(context)

    def resume_execution(self, next_method, next_kwargs, context):
        # Airflow 3.0 can retain a failed trigger's resume arguments on retry.
        # A new attempt must submit a fresh execution rather than replay that failure.
        ti = context['ti']
        previous_try = ti.xcom_pull(task_ids=self.task_id, key='cloud_execution_try_number')
        if previous_try != ti.try_number:
            return self.execute(context)
        return super().resume_execution(next_method, next_kwargs, context)


def execution_result(arn):
    client = AwsBaseHook(aws_conn_id=CONNECTION_ID, client_type='stepfunctions',
        region_name=arn.split(':')[3]).get_conn()
    result = client.describe_execution(executionArn=arn)
    if result['status'] != 'SUCCEEDED':
        raise AirflowException(f"Cloud worker did not succeed: {result['status']}")
    output = json.loads(result['output'])
    if not output.get('success'):
        raise AirflowException('Cloud worker returned an unsuccessful result')
    return output


def finish_cloud_publication(**context):
    arn = context['ti'].xcom_pull(task_ids='export_data')
    result = execution_result(arn)
    return {**result, 'export_info': result.copy()}

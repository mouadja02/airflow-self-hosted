"""Control-plane payloads and deployment-mode regression tests."""

import os
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from bitcoin_cloud import BitcoinCloudOperator, execution_result


ARN = 'arn:aws:states:eu-west-3:434740914108:stateMachine:airflow-bitcoin-workers'


class CloudControlTests(unittest.TestCase):
    @patch.dict(os.environ, {'BITCOIN_WORKER_STATE_MACHINE_ARN': ARN})
    @patch.object(StepFunctionStartExecutionOperator, 'execute', return_value='execution-arn')
    def test_repair_range_is_sent_without_dataset_bytes(self, execute):
        task = BitcoinCloudOperator(task_id='process_delta_data', action='calculate')
        ti = MagicMock(dag_id='technical_indicators_update', task_id=task.task_id, try_number=1)
        ti.xcom_pull.side_effect = lambda task_ids, key: {
            'calculation_start': 1794009600, 'latest_btc_timestamp': 1794441600}[key]
        context = {'ti': ti, 'run_id': 'repair'}
        self.assertEqual(task.execute(context), 'execution-arn')
        self.assertEqual(task.input, {'action': 'calculate', 'initialize': False,
            'start_timestamp': 1794009600, 'through_timestamp': 1794441600})
        original_name = task.name
        task.execute(context)
        self.assertEqual(task.name, original_name)
        ti.try_number = 2
        task.execute(context)
        self.assertNotEqual(task.name, original_name)
        self.assertTrue(task.deferrable)

    @patch.dict(os.environ, {'BITCOIN_WORKER_STATE_MACHINE_ARN': ARN})
    @patch.object(StepFunctionStartExecutionOperator, 'execute')
    def test_publication_preserves_asset_watermark(self, execute):
        task = BitcoinCloudOperator(task_id='export_data', action='publish')
        context = {'ti': MagicMock(dag_id='publisher', task_id=task.task_id, try_number=1),
            'run_id': 'publish', 'triggering_asset_events': {'asset': [SimpleNamespace(
                extra={'start_timestamp': 1794009600, 'through_timestamp': 1794441600})]}}
        task.execute(context)
        self.assertEqual(task.input, {'action': 'publish', 'through_timestamp': 1794441600})

    @patch('bitcoin_cloud.AwsBaseHook')
    def test_failed_execution_cannot_be_reported_as_published(self, hook):
        hook.return_value.get_conn.return_value.describe_execution.return_value = {'status': 'FAILED'}
        with self.assertRaisesRegex(Exception, 'FAILED'):
            execution_result(ARN.replace('stateMachine:', 'execution:') + ':job')

    @patch.dict(os.environ, {'BITCOIN_WORKER_STATE_MACHINE_ARN': ARN})
    @patch.object(BitcoinCloudOperator, 'execute', return_value='new-execution')
    def test_retry_does_not_replay_a_failed_trigger(self, execute):
        task = BitcoinCloudOperator(task_id='export_data', action='publish')
        ti = MagicMock(try_number=2)
        ti.xcom_pull.return_value = 1
        context = {'ti': ti}
        result = task.resume_execution('__fail__', {'error': 'old trigger failure'}, context)
        self.assertEqual(result, 'new-execution')
        execute.assert_called_once_with(context)

    @patch.dict(os.environ, {'BITCOIN_WORKER_STATE_MACHINE_ARN': ARN})
    @patch.object(StepFunctionStartExecutionOperator, 'resume_execution', return_value='completed')
    def test_same_attempt_resumes_the_existing_execution(self, resume):
        task = BitcoinCloudOperator(task_id='export_data', action='publish')
        ti = MagicMock(try_number=1)
        ti.xcom_pull.return_value = 1
        context = {'ti': ti}
        self.assertEqual(task.resume_execution('execute_complete', {}, context), 'completed')
        resume.assert_called_once_with('execute_complete', {}, context)


if __name__ == '__main__':
    unittest.main()

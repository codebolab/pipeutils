import unittest
import os
from unittest.mock import patch
from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from pipeutils.operators import UploadToS3

fileDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.dirname(os.path.dirname(fileDir))


class TestMyOperator(unittest.TestCase):
    '''
    Review docs in https://bcb.github.io/airflow/testing-dags for dags test
    '''

    def setUp(self):
        self.kwargs = dict(
            model_name='test',
            django_manage='',
            source='',
            task_id='test_dag',
            dag=None
        )

    @patch('pipeutils.operators.logger.info')
    def test_s3_operator(self, info):

        dag = DAG(dag_id='foo', start_date=datetime(2018, 1, 1))
        files = os.path.join(parentDir, 'files', 'multiple')
        task = UploadToS3(task_id='django_1', dag=dag,
                          destination='test',
                          extension='txt',
                          source=files)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())

        info.assert_called()


if __name__ == '__main__':
    unittest.main()

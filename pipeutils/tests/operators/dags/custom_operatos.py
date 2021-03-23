import unittest
import os
from unittest.mock import patch
from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance as TI
from pipeutils.operators import (
    UploadToS3,
    PopulatePostgres
)

fileDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.dirname(os.path.dirname(fileDir))
DEFAULT_DATE = datetime.now()


class TestOperator(unittest.TestCase):
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
        '''
        Verifier number of files uploaded to s3.
          multiple:
            -file1.txt
            -file2.txt
          len(multiple) = 2
        '''
        dag = DAG(
            dag_id='test_s3_operator',
            start_date=DEFAULT_DATE
        )
        files = os.path.join(parentDir, 'files', 'multiple')
        dag_task = UploadToS3(task_id='django_1', dag=dag,
                              destination='test',
                              extension='txt',
                              source=files)

        ctx1 = {"ti": TI(task=dag_task, execution_date=DEFAULT_DATE)}

        dag_task.pre_execute(ctx1)
        self.assertEqual(len(dag_task.source), 51)
        result = dag_task.execute(ctx1)

        info.assert_called()
        self.assertEqual(result, 2)

    @patch('pipeutils.operators.logger.info')
    def test_postgres_operator(self, info):
        """
            CREATE TABLE test_job
        """
        dag = DAG(
            dag_id='test_postgres_operator',
            start_date=DEFAULT_DATE
        )
        file_to_load = os.path.join(parentDir, 'files', 'upload_postgres.csv')
        dag_task = PopulatePostgres(task_id='django_2',
                                    dag=dag,
                                    table="test_job",
                                    source=file_to_load,
                                    )

        ctx1 = {"ti": TI(task=dag_task, execution_date=DEFAULT_DATE)}
        dag_task.pre_execute(ctx1)
        result = dag_task.execute(ctx1)
        self.assertEqual(result, True)


if __name__ == '__main__':
    unittest.main()

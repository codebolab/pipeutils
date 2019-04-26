import unittest
from airflow.models import DagBag, DAG, TaskInstance
from datetime import datetime
from pipeutils.operators import XComParams, PopulateDjango


class TestDagIntegrity(unittest.TestCase):
    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag()

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )

    def test_execute(self):
        dag = DAG(dag_id='foo', start_date=datetime.now())
        task = XComParams(dag=dag, task_id='foo')
        ti = TaskInstance(task=task, execution_date=datetime.now())
        print(ti.get_template_context())

    def test_django(self):
        dag = DAG(dag_id='django', start_date=datetime.now())
        #for dag_id, dag in self.dagbag.dags.items():
        task = PopulateDjango(dag=dag, task_id='django', django_settings=None, model_name=None, source='Here')
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())
        response = ti.get_template_context()
        print(type(response['dag']))

suite = unittest.TestLoader().loadTestsFromTestCase(TestDagIntegrity)
unittest.TextTestRunner(verbosity=2).run(suite)

import logging as log
from datetime import datetime
from airflow import DAG
from pipeutils.operators import PopulateDjango, XComParams

args = {
    'task_id': 'runner',
    'owner': 'airflow',
    'start_date': datetime(2017, 3, 20),
    'provide_context': True
}

dag = DAG('dag_1',
          description='xComParams',
          schedule_interval='0 12 * * *',
          default_args=args)


class Test1(XComParams):
    def execute(self, context):
        log.info("TEST 1 ")
        return 'test1 '

test1 = Test1(task_id='test1', dag=dag, lookups={'source': 'test2'})
django = PopulateDjango(task_id='django_1', lookups={'source': 'test1'}, dag=dag, source=None, model_name=None, django_settings=None)

#test1 >> django
test1.set_upstream(django)




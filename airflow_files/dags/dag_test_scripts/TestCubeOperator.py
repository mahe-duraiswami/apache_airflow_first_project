import unittest

from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from custom_operator.cube_operator import CubeOperator

class TestCubeOperator(unittest.TestCase):
	
	def test_execute(self):
		dag = DAG(dag_id='any_dag',start_date=datetime.now())
		task = CubeOperator(my_input_val=3,dag=dag,task_id='any_task')
		task_inst = TaskInstance(task=task,execution_date=datetime.now())
		result = task.execute(task_inst.get_template_context())
		self.assertEqual(result,27)

if __name__ == "__main__":
	unittest.main()
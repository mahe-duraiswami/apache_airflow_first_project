###This script tests the definition of the dag - myfirstdag###

import unittest
from airflow.models import DagBag

class TestMyFirstDAG(unittest.TestCase):

	def setUp(self):
		self.dagbag=DagBag()
		self.dag_id='MyFirstDAG'

	def test_task_count(self):
		self.assertEqual(len(self.dagbag.get_dag(dag_id).tasks),5)

	def test_task_list(self):
		task_ids = list(map(lambda task: task.task_id, self.dagbag.get_bag(dag_id).tasks))
		self.assertListEqual(task_ids,['Step_1','Step_2','Step_3','Step_4','LastStep'])

	def test_dependencies_last_step(self):
		last_step_task_id = self.dagbag.get_bag(dag_id).get_task('LastStep')
		upstream_task_ids = list(map(lambda task: task.task_id,last_step_task_id.upstream_list))
		downstream_task_ids = list(map(lambda task: task.task_id,last_step_task_id.downstream_list))
		self.assertListEqual(upsteam_task_ids,['Step_4','Step_3'])
		self.assertListEqual(downstream_task_ids,[])

if __name__ == "__main__":
	unittest.main()

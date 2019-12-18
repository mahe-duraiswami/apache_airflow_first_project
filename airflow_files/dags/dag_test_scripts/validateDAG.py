###This script validates the integrity of the DAG. Checks for typo and cycles.
###Verify if alert email has been set for each DAG###


import unittest
from airflow.models import DagBag

class TestDAGValidity(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		print("Starting Validation of DAGs")

	@classmethod
	def tearDownClass(cls):
		print("Finished Validation of DAGs")
			

	def setUp(self):
		self.dagbag = DagBag()
		print("Initating test")

	def tearDown(self):
		print("Finished test")

	def test_import_status_dag(self):
		self.assertFalse(
			len(self.dagbag.import_errors),
			'DAG import failures. Errors : {}'.format(self.dagbag.import_errors)
		)

	def test_alert_email_configured(self):

		for dag_id,dag in self.dagbag.dags.items():
			email_info = dag.default_args.get('email',[])
			error_msg = 'Alert email not set for DAG {id}'.format(id=dag_id)
			self.assertIn('maheshwari.duraiswami@gmail.com',email_info,error_msg)

if __name__ == "__main__":
	unittest.main()


import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

default_args = {
	'owner': 'Mahe Duraiswami',
	'start_date': airflow.utils.dates.days_ago(1),
	'params': {
		'step_num':"1"
	}
}

with DAG(dag_id='MyFirstDAG',default_args=default_args,schedule_interval='0 0 * * *') as dag:
	
	Step1=BashOperator(
		task_id='Step_1',
		bash_command='echo I am in Step {{params.step_num}} in {{ var.json.env_config.env }} environment'
	)

	Step2=BashOperator(
		task_id='Step_2',
		bash_command='echo I am in Step {{params.step_num}} in {{ var.json.env_config.env }} environment',
		params={
			"step_num":"2"
		}
	)

	Step3=BashOperator(
		task_id='Step_3',
		bash_command='echo I am in Step {{params.step_num}} executing in parallel with Step 4 in {{ var.json.env_config.env }} environment',
		params={
			"step_num":"3"
		}
	)

	Step4=BashOperator(
		task_id='Step_4',
		bash_command='echo I am in Step {{params.step_num}} executing in parallel with Step 3 in {{ var.json.env_config.env }} environment',
		params={
			"step_num":"4"
		}
	)

	LastStep=BashOperator(
		task_id='LastStep',
		bash_command='echo I am the last step in {{ var.json.env_config.env }} environment'
	)

	Step1 >> Step2 >> [Step3,Step4] >> LastStep

if __name__ == "__main__":
	dag.cli()


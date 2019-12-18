import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class CubeOperator(BaseOperator):
	
	@apply_defaults
	def __init__(self,my_input_val,*args,**kwargs):
		self.input_val = my_input_val
		super(CubeOperator,self).__init__(*args,**kwargs)

	def execute(self,context):
		log.info('input_val: %s',self.input_val)
		return self.input_val * self.input_val * self.input_val

class CubePlugIn(AirflowPlugin):
	name = "cube_plugin"
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table='',
                 query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query  
        
    def execute(self, context):
        self.log.info(f'Loading fact table {self.table}')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run("DELETE FROM {}".format(self.table))    
                
        new_query = f'INSERT INTO {self.table} ' + self.query

        redshift.run(new_query)

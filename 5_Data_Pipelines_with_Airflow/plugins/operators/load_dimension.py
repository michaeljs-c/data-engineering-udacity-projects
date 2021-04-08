from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_sql = 'TRUNCATE TABLE {}'
   
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 table='',
                 query='',
                 truncate_table=False
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query     
        self.truncate_table=truncate_table

    def execute(self, context):
        self.log.info(f'Loading dimension table {self.table}')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #redshift.run("DELETE FROM {}".format(self.table))
        
        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.table))

        new_query = f'INSERT INTO {self.table} ' + self.query
        redshift.run(new_query)

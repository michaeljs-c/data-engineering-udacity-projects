from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = '',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Checking data quality')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info(f'Checking table {table}')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info(f'Data quality check failed on table {table} returning no results')
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            self.log.info(f'Data quality check passed on table {table} with {num_records} rows')

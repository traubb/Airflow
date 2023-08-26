from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsHook

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
<<<<<<< HEAD
                 target_sql='',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = f'public.{table}'  # table name with "public." due to Redshift schema
        self.target_sql = target_sql

    def execute(self, context):
        self.log.info(f'Loading data into {self.table}')

        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.target_sql}
            """
        
        self.log.info(f"Running SQL command: {insert_sql}")
        postgres_hook.run(insert_sql)
        self.log.info(f"SQL command: {insert_sql} completed")
=======
                 sql='',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Loading data into {self.table} fact table...')
        redshift_hook.run(self.sql)
        self.log.info(f'Data loaded into {self.table} fact table.')
>>>>>>> a872af15fe612615fba9087019550d80c84efa8e

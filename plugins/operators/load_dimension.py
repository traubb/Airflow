from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsHook

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 target_sql='',
                 append=True,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = f'public.{table}'  # table name with "public." due to Redshift schema
        self.target_sql = target_sql
        self.append = append

    def execute(self, context):
        self.log.info(f'Loading data into {self.table}')

        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append:
            truncate_sql = f"""
                TRUNCATE TABLE {self.table};
                """
            self.log.info(f"Running SQL command: {truncate_sql}")
            postgres_hook.run(truncate_sql)

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.target_sql}
            """
        
        self.log.info(f"Running SQL command: {insert_sql}")
        postgres_hook.run(insert_sql)
        self.log.info(f"SQL command: {insert_sql} completed")

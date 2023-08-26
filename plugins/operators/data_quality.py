import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=None,  # List of test cases
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tests = tests or []  # Default to an empty list if no tests are provided
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            sql, expected_result = test['sql'], test['expected_result']
            records = redshift_hook.get_records(sql)
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed for test '{sql}'. No results returned")
            
            test_result = records[0][0]
            if test_result != expected_result:
                raise ValueError(f"Data quality check failed for test '{sql}'. "
                                 f"Expected result: {expected_result}, Actual result: {test_result}")
            
            logging.info(f"Data quality check passed for test '{sql}'")

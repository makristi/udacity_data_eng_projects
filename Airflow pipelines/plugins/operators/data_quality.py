from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.data_checks = data_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for check in self.data_checks:
            sql = check.get('sql_stmt')
            expected_value = check.get('expected_value')
            records = redshift_hook.get_records(sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {sql} returned no results")
            if records[0][0] != expected_value:
                raise ValueError(f"Data quality check failed. {sql} returns {records[0]} instead of {expected_value}")
            self.log.info(f"Data quality on {sql} passed")
            
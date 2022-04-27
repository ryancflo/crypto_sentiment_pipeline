from typing import Any, Optional, Sequence
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                snowflake_conn_id="",
                warehouse: Optional[str] = None,
                database: Optional[str] = None,
                schema: Optional[str] = None,
                tables=[],
                where_parameters=None,
                role: Optional[str] = None,
                authenticator: Optional[str] = None,
                session_parameters: Optional[dict] = None,
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(**kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.tables = tables
        self.where_parameters = where_parameters
        self.role = role
        self.authenticator = authenticator
        self.session_parameters = session_parameters

    def execute(self, context):
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )

        self.recordCountCheck(snowflake_hook)


    def recordCountCheck(self, snowflake_hook):
        where_params = ''
        if self.where_parameters is not None:
            where_params = f"WHERE {self.where_parameters}"
        for table in self.tables:
            sql_statement = [f"SELECT COUNT(*) AS ROW_COUNT FROM {table} {where_params}"]
            self.log.info(sql_statement) 
            records = snowflake_hook.run(sql_statement)
            num_records = records[0]['ROW_COUNT']
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")



    def nullCheck(self):
        where_params = ''
        if self.where_parameters is not None:
            where_params = f"WHERE {self.where_parameters}"
        for table in self.tables:
            sql_statement = [f"SELECT COUNT(*) AS ROW_COUNT FROM {table} {where_params} IS NULL"]
            self.log.info(sql_statement) 
            records = snowflake_hook.run(sql_statement)
            num_records = records[0]['ROW_COUNT']
            if num_records > 1:
                raise ValueError(f"Data quality check failed. {table} has {num_records} null values")
            else:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")
            
    # def dateCheck(self):
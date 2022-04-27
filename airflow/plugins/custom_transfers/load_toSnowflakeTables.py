from typing import Any, Optional
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class LoadToSnowflakeOperator(BaseOperator):
    def __init__(self,
                 snowflake_conn_id = "",
                 source_table: Optional[str] = None,
                 target_table = "",
                 warehouse: Optional[str] = None,
                 database: Optional[str] = None,
                 role: Optional[str] = None,
                 schema: Optional[str] = None,
                 authenticator: Optional[str] = None,
                 select_query = "",
                 truncate_table=False,
                 autocommit: bool = True,
                 session_parameters: Optional[dict] = None,
                 *args, **kwargs):

        super(LoadToSnowflakeOperator, self).__init__(**kwargs)

        self.select_query = select_query
        self.truncate_table = truncate_table
        self.snowflake_conn_id = snowflake_conn_id
        self.source_table = source_table
        self.target_table = target_table
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.warehouse = warehouse
        self.database = database
        self.autocommit = autocommit
        self.session_parameters = session_parameters

    def execute(self, context: Any) -> None:
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )

        insert_sql_parts = [
            f"INSERT INTO {self.target_table}",
            f"{self.select_query}"
        ]

        truncate_sql_parts = [
            f"TRUNCATE TABLE {self.source_table}",
        ]

        insert_query = "\n".join(insert_sql_parts)
        truncate_query = "\n".join(truncate_sql_parts)

        self.log.info('Executing INSERT to snowflake json processing table command...')
        snowflake_hook.run(insert_query, self.autocommit)
        self.log.info("INSERT command completed")
        
        if self.truncate_table == True:
            self.log.info('Executing TRUNCATE to snowflake json raw table command...')
            snowflake_hook.run(truncate_query, self.autocommit)
            self.log.info("TRUNCATE command completed")
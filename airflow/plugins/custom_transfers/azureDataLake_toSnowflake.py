from typing import Any, Optional, Sequence
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class AzureDataLakeToSnowflakeTransferOperator(BaseOperator):

    template_fields: Sequence[str] = ("azure_keys",)
    template_fields_renderers = {"azure_keys": "json"}

    def __init__(self,
        *,
        azure_keys: Optional[list] = None,
        table: str,
        stage: str,
        prefix: Optional[str] = None,
        file_format: str,
        schema: Optional[str] = None,
        columns_array: Optional[list] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        autocommit: bool = True,
        snowflake_conn_id: str = 'snowflake_default',
        role: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.azure_keys = azure_keys
        self.table = table
        self.warehouse = warehouse
        self.database = database
        self.stage = stage
        self.prefix = prefix
        self.file_format = file_format
        self.schema = schema
        self.columns_array = columns_array
        self.autocommit = autocommit
        self.snowflake_conn_id = snowflake_conn_id
        self.role = role
        self.authenticator = authenticator
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

        if self.schema:
            into = f"{self.schema}.{self.table}"
        else:
            into = self.table
        if self.columns_array:
            into = f"{into}({','.join(self.columns_array)})"

        sql_parts = [
            f"COPY INTO {into}",
            f"FROM @{self.stage}/{self.prefix or ''}",
        ]
        if self.azure_keys:
            files = ", ".join(f"'{key}'" for key in self.azure_keys)
            sql_parts.append(f"files=({files})")
        sql_parts.append(f"file_format={self.file_format}")

        copy_query = "\n".join(sql_parts)

        self.log.info('Executing COPY command...')
        snowflake_hook.run(copy_query, self.autocommit)
        self.log.info("COPY command completed")
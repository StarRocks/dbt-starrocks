from contextlib import contextmanager
from dataclasses import dataclass
from typing import ContextManager, Optional, Union

import MySQLdb
import MySQLdb.cursors
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.events import AdapterLogger

from dbt import exceptions

logger = AdapterLogger("starrocks")


@dataclass
class StarRocksAdapterCredentials(Credentials):
    host: str = "127.0.0.1"
    port: int = 9030
    username: str = "root"
    password: str = ""
    database: Optional[str] = None
    schema: Optional[str] = None

    @property
    def type(self):
        return "starrocks"

    def _connection_keys(self):
        return "host", "port", "user", "schema"

    @property
    def unique_field(self) -> str:
        return self.host

    def __post_init__(self):
        if self.database is not None and self.database != self.schema:
            raise exceptions.RuntimeException(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On StarRocks, database must be omitted or have the same value as"
                f" schema."
            )


class StarRocksAdapterConnectionManager(SQLConnectionManager):
    TYPE = "starrocks"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open")
            return connection
        credentials = connection.credentials
        kwargs = {
            "host": credentials.host,
            "port": credentials.port,
            "user": credentials.username,
            "password": credentials.password,
            "database": "information_schema",  # we need special a default database when connecting
        }
        try:
            connection.handle = MySQLdb.connect(**kwargs)
            connection.state = ConnectionState.OPEN
        except MySQLdb.Error as e:
            logger.debug(f"Error connecting to database: {e}")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise exceptions.FailedToConnectException(str(e))
        return connection

    def cancel(self, connection: Connection):
        connection.handle.close()

    @classmethod
    def get_response(cls, cursor: MySQLdb.cursors.Cursor) -> Union[AdapterResponse, str]:
        code = "Unknown cursor state/status"
        rows = cursor.rowcount
        return AdapterResponse(
            code=code,
            _message=f"{rows} rows affected",
            rows_affected=rows,
        )

    @contextmanager  # type: ignore
    def exception_handler(self, sql: str) -> ContextManager:  # type: ignore
        try:
            yield
        except MySQLdb.DatabaseError as e:
            logger.debug(f"StarRocks database error: {e}, sql: {sql}")
            raise exceptions.DatabaseException(str(e)) from e
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            if isinstance(e, exceptions.RuntimeException):
                raise e
            raise exceptions.RuntimeException(str(e)) from e

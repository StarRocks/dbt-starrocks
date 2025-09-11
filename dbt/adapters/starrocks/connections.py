#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager

import mysql.connector

import dbt.exceptions
import dbt_common.exceptions
from dataclasses import dataclass

from dbt.adapters.contracts.connection import (
    Credentials,
    AdapterResponse,
    Connection
)
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.events.logging import AdapterLogger
from typing import Optional

logger = AdapterLogger("starrocks")


@dataclass
class StarRocksCredentials(Credentials):
    host: Optional[str] = None
    port: Optional[int] = None
    catalog: Optional[str] = 'default_catalog'
    database: Optional[str] = None
    schema: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    charset: Optional[str] = None
    version: Optional[str] = None
    use_pure: Optional[str] = None
    is_async: Optional[bool] = False
    async_query_timeout: Optional[int] = 300
    auth_plugin: Optional[str] = ''
    
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __post_init__(self):
        # starrocks classifies database and schema as the same thing
        if (
            self.database is not None and
            self.database != self.schema
        ):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On StarRocks, database must be omitted or have the same value as"
                f" schema."
            )

    @property
    def type(self):
        return 'starrocks'

    @property
    def unique_field(self):
        return self.schema

    def _connection_keys(self):
        """
        Returns an iterator of keys to pretty-print in 'dbt debug'
        """
        return (
            "host",
            "port",
            "schema",
            "catalog",
            "username",
            "use_pure",
            "is_async",
            "async_query_timeout",
            "auth_plugin",
        )


def _parse_version(result):
    default_version = (999, 999, 999)
    first_part = None

    if '-' in result:
        first_part = result.split('-')[0]
    if ' ' in result:
        first_part = result.split(' ')[0]

    if first_part and len(first_part.split('.')) == 3:
        return int(first_part[0]), int(first_part[2]), int(first_part[4])

    return default_version


class StarRocksConnectionManager(SQLConnectionManager):
    TYPE = 'starrocks'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {"host": credentials.host, "username": credentials.username,
                  "password": credentials.password, "database": credentials.catalog + "." + credentials.schema, "auth_plugin":credentials.auth_plugin}

        kwargs["buffered"] = True

        if credentials.port:
            kwargs["port"] = credentials.port

        if credentials.use_pure in ["true", "True"]:
            kwargs["use_pure"] = True

        try:
            connection.handle = mysql.connector.connect(**kwargs)
            connection.state = 'open'
        except mysql.connector.Error:

            try:
                logger.debug("Failed connection without supplying the `database`. "
                             "Trying again with `database` included.")

                # Try again with the database included
                database_toBeCreated = kwargs["database"]
                kwargs["database"] = "information_schema"

                connection.handle = mysql.connector.connect(**kwargs)
                connection.state = 'open'

                mycursor = connection.handle.cursor()

                mycursor.execute("CREATE DATABASE " + database_toBeCreated)
                kwargs["database"] = database_toBeCreated

                connection.handle = mysql.connector.connect(**kwargs)
                connection.state = 'open'

            except mysql.connector.Error as e:

                logger.debug("Got an error when attempting to open a StarRocks "
                             "connection: '{}'".format(e))

                connection.handle = None
                connection.state = 'fail'

                raise dbt_common.exceptions.ConnectionError(str(e))

        if credentials.version is None:
            cursor = connection.handle.cursor()
            try:
                cursor.execute("select current_version()")
                connection.handle.server_version = _parse_version(
                    cursor.fetchone()[0])
            except Exception as e:
                logger.debug(
                    "Got an error when obtain StarRocks version exception: '{}'".format(e))
        else:
            version = credentials.version.strip().split('.')
            if len(version) == 3:
                connection.handle.server_version = (
                    int(version[0]), int(version[1]), int(version[2]))
            elif len(version) == 2:
                connection.handle.server_version = (
                    int(version[0]), int(version[1]), 0)
            else:
                logger.debug("Config version '{}' is invalid".format(version))

        return connection

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    def cancel(self, connection: Connection):
        connection.handle.close()

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except mysql.connector.DatabaseError as e:
            logger.debug('StarRocks error: {}'.format(str(e)))

            try:
                self.rollback_if_open()
            except mysql.connector.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt_common.exceptions.DbtRuntimeError(str(e)) from e

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        code = "SUCCESS"
        num_rows = 0

        if cursor is not None and cursor.rowcount is not None:
            num_rows = cursor.rowcount

        # There's no real way to get the status from the mysql-connector-python driver.
        # So just return the default value.
        return AdapterResponse(
            _message="{} {}".format(code, num_rows),
            rows_affected=num_rows,
            code=code
        )

    def add_begin_query(self):
        return self.add_query("", auto_begin=False)

"""Serverless client for Neon database queries over HTTP."""

import json
import os
from typing import Any
from urllib.parse import urlparse

import httpx
import psycopg.types.datetime as psycopg_datetime
from dotenv import load_dotenv
from psycopg import Error as PsycopgError, sql
from psycopg.adapt import Transformer
from psycopg.postgres import register_default_adapters, register_default_types, types
from psycopg.pq import Format

from pyserverless.errors import (
    ConnectionStringFormattingError,
    ConnectionStringMissingError,
    InvalidAuthTokenError,
    NeonHTTPResponseError,
    PostgresAdaptationError,
    PythonAdaptationError,
)
from pyserverless.models import (
    FullQueryResults,
    HTTPQueryOptions,
    NeonTransactionOptions,
    ParameterizedQuery,
    QueryRows,
)

load_dotenv()


class Neon:
    """
    Client for executing queries against a Neon database over HTTP.

    This client provides methods to execute SQL queries and transactions
    against a Neon database using their HTTP API.

    Parameters
    ----------
    connection_string : str, optional
        A PostgreSQL connection string in the format
        `postgresql://user:pass@hostname/dbname`. If not provided,
        will look for DATABASE_URL environment variable.

    Raises
    ------
    ConnectionStringMissingError
        If no connection string is provided and DATABASE_URL environment variable is not found.
    ConnectionStringFormattingError
        If the connection string is not in the correct format.

    Examples
    --------
    >>> neon = Neon("postgresql://user:pass@hostname/dbname")
    >>> results = neon.query("SELECT * FROM users")
    >>> # Or if the DATABASE_URL environment variable is set:
    >>> neon = Neon()
    >>> results = neon.query("SELECT * FROM users")

    """

    def __init__(self, connection_string: str | None = None) -> None:
        if connection_string is None:
            connection_string = os.getenv("DATABASE_URL")
        if connection_string is None:
            raise ConnectionStringMissingError

        try:
            protocol, netloc, path, _, _, _ = urlparse(connection_string)
            username, rest = netloc.split(":")
            hostname = rest.split("@")[1]
            pathname = path.lstrip("/")
        except ValueError:
            raise ConnectionStringFormattingError(connection_string) from None

        if protocol not in ["postgresql", "postgres"] or not username or not hostname or not pathname:
            raise ConnectionStringFormattingError(connection_string)
        self.url = f"https://{hostname}/sql"
        self.connection_string = connection_string
        register_default_types(types)
        self.transformer = Transformer()
        register_default_adapters(self.transformer)

    def query(
        self,
        query: str,
        params: tuple[Any, ...] = (),
        opts: HTTPQueryOptions | None = None,
    ) -> FullQueryResults | QueryRows:
        """
        Execute a single SQL query against the database.

        Parameters
        ----------
        query : str
            The SQL query to execute. using $1, $2, etc. for parameters.
        params : tuple[Any, ...], optional
            Tuple of parameters to substitute into the query.
        opts : HTTPQueryOptions | None, optional
            Optional query options.

        Returns
        -------
        FullQueryResults | QueryRows
            Either FullQueryResults (if full_results=True) or QueryRows
            (if full_results=False).

        Raises
        ------
        NeonHTTPResponseError
            If the HTTP request fails
        InvalidAuthTokenError
            If auth token is invalid
        ParameterAdaptationError
            If parameters can't be adapted from Python types to Postgres types

        Examples
        --------
        Simple query:

        >>> results = neon.query("SELECT * FROM users WHERE id = $1", (1,))

        Query with options:

        >>> results = neon.query(
        ...     "SELECT * FROM users",
        ...     (),
        ...     HTTPQueryOptions(
        ...         full_results=True,
        ...         array_mode=True,
        ...         fetch_options={"timeout": 30.0}
        ...     )
        ... )

        """
        opts = opts or HTTPQueryOptions()
        processed_params = [self._process_param(p) for p in params]
        paramertized_query = ParameterizedQuery(query, processed_params)

        if opts.query_callback:
            opts.query_callback(paramertized_query)

        body = {
            "query": query,
            "params": processed_params,
        }

        headers: dict[str, str] = {
            "Neon-Connection-String": self.connection_string,
            "Neon-Raw-Text-Output": "true",
            "Neon-Array-Mode": str(opts.array_mode).lower(),
        }

        if opts.auth_token is not None:
            token = opts.auth_token()
            if not token or not isinstance(token, str):
                raise InvalidAuthTokenError(token)
            headers["Authorization"] = f"Bearer {token}"

        with httpx.Client(**opts.fetch_options) as client:
            response = client.post(self.url, json=body, headers=headers)

        if response.status_code != httpx.codes.OK:
            raise NeonHTTPResponseError(response.status_code, response.text)

        json_response = response.json()

        if json_response.get("rows"):
            json_response["rows"] = [self._convert_row(row, json_response["fields"]) for row in json_response["rows"]]

        results = FullQueryResults(**json_response)

        if opts.result_callback:
            opts.result_callback(
                paramertized_query,
                results,
                opts.array_mode,
                opts.full_results,
            )

        if opts.full_results:
            return results
        return results.rows

    def transaction(
        self,
        queries: list[tuple[str, tuple[Any, ...]]],
        opts: NeonTransactionOptions | None = None,
    ) -> list[FullQueryResults] | list[QueryRows]:
        """
        Execute multiple queries in a transaction.

        Parameters
        ----------
        queries : list[tuple[str, tuple[Any, ...]]]
            List of (query, params) tuples to execute in sequence.
        opts : NeonTransactionOptions | None, optional
            Optional transaction options.

        Returns
        -------
        list[FullQueryResults] | list[QueryRows]
            List of either FullQueryResults (if full_results=True) or
            QueryRows (if full_results=False), one for each query.

        Raises
        ------
        NeonHTTPResponseError
            If the HTTP request fails
        InvalidAuthTokenError
            If auth token is invalid
        ParameterAdaptationError
            If parameters can't be adapted from Python types to Postgres types

        Examples
        --------
        >>> results = neon.transaction(
        ...     [
        ...         ("INSERT INTO users (name) VALUES ($1)", ("John",)),
        ...         ("SELECT * FROM users", ()),
        ...     ],
        ...     NeonTransactionOptions(
        ...         isolation_level=IsolationLevel.SERIALIZABLE,
        ...         read_only=False,
        ...     )
        ... )

        """
        opts = opts or NeonTransactionOptions()
        processed_queries = []
        for query, params in queries:
            processed_params = [self._process_param(p) for p in params]
            processed_queries.append(
                {
                    "query": query,
                    "params": processed_params,
                }
            )
        body = {"queries": processed_queries}

        headers: dict[str, str] = {
            "Neon-Connection-String": self.connection_string,
            "Neon-Raw-Text-Output": "true",
            "Neon-Array-Mode": str(opts.array_mode).lower(),
        }
        headers["Neon-Batch-Isolation-Level"] = opts.isolation_level.value
        headers["Neon-Batch-Read-Only"] = str(opts.read_only).lower()
        headers["Neon-Batch-Deferrable"] = str(opts.deferrable).lower()

        if opts.auth_token is not None:
            token = opts.auth_token()
            if not token or not isinstance(token, str):
                raise InvalidAuthTokenError(token)
            headers["Authorization"] = f"Bearer {token}"

        with httpx.Client(**opts.fetch_options) as client:
            response = client.post(
                self.url,
                json=body,
                headers=headers,
            )

        if response.status_code != httpx.codes.OK:
            raise NeonHTTPResponseError(response.status_code, response.text)

        results = response.json()["results"]
        converted_results = []

        for result in results:
            if result.get("rows"):
                result["rows"] = [self._convert_row(row, result["fields"]) for row in result["rows"]]
            converted_result = FullQueryResults(**result)
            converted_results.append(converted_result if opts.full_results else converted_result.rows)

        return converted_results

    def _process_param(self, param: Any) -> Any:
        """Convert Python types to Postgres types."""
        if param is None:
            return None
        # special case for bytes
        # https://www.postgresql.org/docs/current/datatype-binary.html#DATATYPE-BINARY-BYTEA-HEX-FORMAT
        if isinstance(param, bytes):
            return "\\x" + param.hex()
        try:
            if isinstance(param, dict | list):
                param = json.dumps(param)
            return sql.Literal(param).as_string(None)

        except PsycopgError as e:
            raise PostgresAdaptationError(param, str(e)) from None

    def _convert_value(self, value: str | None, type_oid: int) -> Any:
        """Convert a single value to its Python native type."""
        if value is None:
            return None
        # super hacky but monkey patch to force postgres INTERVAL style without ever actually connecting to the database
        # normally in psycopg the style in set in the connection string or in the connection object
        interval_type_oid = 1186
        if type_oid == interval_type_oid:
            psycopg_datetime._get_intervalstyle = lambda _: b"postgres"  # noqa: SLF001
        try:
            loader = self.transformer.get_loader(type_oid, Format.TEXT)
            return loader.load(value.encode())
        except PsycopgError as e:
            raise PythonAdaptationError(value, type_oid, str(e)) from None

    def _convert_row(self, row: dict[str, str] | list[str], fields: list[dict]) -> dict[str, Any] | list[Any]:
        """Convert a row of text format data to Python native types."""
        if isinstance(row, list):
            # Handle array mode
            converted = []
            for i, field in enumerate(fields):
                type_oid = field["dataTypeID"]
                value = row[i]
                converted.append(self._convert_value(value, type_oid))
            return converted
        # Handle object mode
        converted = {}
        for field in fields:
            name = field["name"]
            type_oid = field["dataTypeID"]
            value = row[name]
            converted[name] = self._convert_value(value, type_oid)
        return converted

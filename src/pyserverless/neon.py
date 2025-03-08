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
    NeonHTTPClientError,
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

# monkey patch to force postgres INTERVAL style without ever actually connecting to a database
# normally in psycopg the style is set in the connection object (via an option or in the connection string)
# the type adapter for interval types uses this function to get the style
# which by default would return b"unknown" and raise an error on conversion
psycopg_datetime._get_intervalstyle = lambda _: b"postgres"  # noqa: SLF001


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
        """
        Initialize the Neon client.

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

        """
        self._url, self._connection_string = self._parse_connection_string(connection_string)
        register_default_types(types)
        self._transformer = Transformer()
        register_default_adapters(self._transformer)

    def query(
        self,
        query: str,
        params: tuple[Any, ...] | None = None,
        query_options: HTTPQueryOptions | None = None,
    ) -> FullQueryResults | QueryRows:
        """
        Execute a single SQL query against the database.

        Parameters
        ----------
        query : str
            The SQL query to execute. using $1, $2, etc. for parameters.
        params : tuple[Any, ...] | None, optional
            Tuple of parameters to substitute into the query.
        query_options : HTTPQueryOptions | None, optional
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
        params = params or ()
        query_options = query_options or HTTPQueryOptions()

        processed_params = [self._python_to_pg(p) for p in params]
        parameterized_query = ParameterizedQuery(query, processed_params)

        if query_options.query_callback:
            query_options.query_callback(parameterized_query)

        body = {
            "query": query,
            "params": processed_params,
        }

        headers: dict[str, str] = {
            "Neon-Connection-String": self._connection_string,
            "Neon-Raw-Text-Output": "true",
            "Neon-Array-Mode": str(query_options.array_mode).lower(),
        }

        if query_options.auth_token is not None:
            token = query_options.auth_token()
            if not token or not isinstance(token, str):
                raise InvalidAuthTokenError(token)
            headers["Authorization"] = f"Bearer {token}"

        try:
            with httpx.Client(**query_options.fetch_options) as client:
                response = client.post(
                    self._url,
                    json=body,
                    headers=headers,
                )
        except httpx.RequestError as e:
            raise NeonHTTPClientError from e

        if response.status_code != httpx.codes.OK:
            raise NeonHTTPResponseError(response.status_code, response.text)

        json_response = response.json()

        json_response["rows"] = [self._convert_row(row, json_response["fields"]) for row in json_response["rows"]]

        results = FullQueryResults(**json_response)

        if query_options.result_callback:
            query_options.result_callback(
                parameterized_query,
                results,
                query_options.array_mode,
                query_options.full_results,
            )

        if query_options.full_results:
            return results
        return results.rows

    def transaction(
        self,
        queries: list[tuple[str, tuple[Any, ...] | None]],
        transaction_options: NeonTransactionOptions | None = None,
    ) -> list[FullQueryResults] | list[QueryRows]:
        """
        Execute multiple queries in a transaction.

        Parameters
        ----------
        queries : list[tuple[str, tuple[Any, ...] | None]]
            List of (query, params) tuples to execute in sequence.
        transaction_options : NeonTransactionOptions | None, optional
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
        transaction_options = transaction_options or NeonTransactionOptions()

        processed_queries = []
        for query, params in queries:
            checked_params = params or ()
            processed_params = [self._python_to_pg(p) for p in checked_params]

            processed_queries.append(
                {
                    "query": query,
                    "params": processed_params,
                }
            )
        body = {"queries": processed_queries}

        headers: dict[str, str] = {
            "Neon-Connection-String": self._connection_string,
            "Neon-Raw-Text-Output": "true",
            "Neon-Array-Mode": str(transaction_options.array_mode).lower(),
            "Neon-Batch-Isolation-Level": transaction_options.isolation_level.value,
            "Neon-Batch-Read-Only": str(transaction_options.read_only).lower(),
            "Neon-Batch-Deferrable": str(transaction_options.deferrable).lower(),
        }

        if transaction_options.auth_token is not None:
            token = transaction_options.auth_token()
            if not token or not isinstance(token, str):
                raise InvalidAuthTokenError(token)
            headers["Authorization"] = f"Bearer {token}"

        try:
            with httpx.Client(**transaction_options.fetch_options) as client:
                response = client.post(
                    self._url,
                    json=body,
                    headers=headers,
                )
        except httpx.RequestError as e:
            raise NeonHTTPClientError from e

        if response.status_code != httpx.codes.OK:
            raise NeonHTTPResponseError(response.status_code, response.text)

        results = response.json()["results"]
        converted_results = []

        for result in results:
            result["rows"] = [self._convert_row(row, result["fields"]) for row in result["rows"]]
            converted_result = FullQueryResults(**result)
            converted_results.append(converted_result if transaction_options.full_results else converted_result.rows)

        return converted_results

    def _python_to_pg(self, param: Any) -> str:
        """Convert a single Python value to its Postgres representation."""
        # special case for bytes, same as TS client
        # https://www.postgresql.org/docs/current/datatype-binary.html#DATATYPE-BINARY-BYTEA-HEX-FORMAT
        if isinstance(param, bytes):
            return "\\x" + param.hex()
        try:
            if isinstance(param, dict | list):
                param = json.dumps(param)
            return sql.Literal(param).as_string(None)

        except PsycopgError as e:
            raise PostgresAdaptationError(param) from e

    def _pg_to_python(self, value: str | None, type_oid: int) -> Any:
        """Convert a single Postgres value to its Python native type."""
        if value is None:
            return None
        try:
            loader = self._transformer.get_loader(type_oid, Format.TEXT)
            return loader.load(value.encode())
        except (PsycopgError, ValueError) as e:
            raise PythonAdaptationError(value, type_oid) from e

    def _convert_row(self, row: dict[str, str] | list[str], fields: list[dict]) -> dict[str, Any] | list[Any]:
        """Convert a row of text format data to Python native types."""
        if isinstance(row, list):
            # array mode, from arrayMode=true
            converted = []
            for i, field in enumerate(fields):
                type_oid = field["dataTypeID"]
                value = row[i]
                converted.append(self._pg_to_python(value, type_oid))
            return converted
        # object mode, from arrayMode=false
        converted = {}
        for field in fields:
            name = field["name"]
            type_oid = field["dataTypeID"]
            value = row[name]
            converted[name] = self._pg_to_python(value, type_oid)
        return converted

    def _parse_connection_string(self, connection_string: str | None = None) -> tuple[str, str]:
        """Parse and validate a PostgreSQL connection string."""
        if connection_string is None:
            load_dotenv()
            connection_string = os.getenv("DATABASE_URL")
        if connection_string is None:
            raise ConnectionStringMissingError

        parsed = urlparse(connection_string)
        if parsed.scheme not in ("postgresql", "postgres"):
            raise ConnectionStringFormattingError(connection_string)
        if not parsed.username or not parsed.hostname or not parsed.path:
            raise ConnectionStringFormattingError(connection_string)

        pathname = parsed.path.lstrip("/")
        if not pathname:
            raise ConnectionStringFormattingError(connection_string)

        api_url = f"https://{parsed.hostname}/sql"
        return api_url, connection_string

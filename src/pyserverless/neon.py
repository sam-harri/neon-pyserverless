"""Serverless client for Neon database queries over HTTP."""

import os
from typing import Any, Callable, TypeVar
from urllib.parse import urlparse

import httpx
import psycopg.types.datetime as psycopg_datetime
from psycopg import Error as PsycopgError
from psycopg.adapt import PyFormat, Transformer
from psycopg.postgres import register_default_adapters, register_default_types, types
from psycopg.pq import Format
from psycopg.types.json import Jsonb

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
    QueryRows,
)

# monkey patch to force postgres INTERVAL style without ever actually connecting to a database
# normally in psycopg the style is set in the connection object (via an option or in the connection string)
# the type adapter for interval types uses this function to get the style
# which by default would return b"unknown" and raise an error on conversion
psycopg_datetime._get_intervalstyle = lambda _: b"postgres"  # noqa: SLF001

T = TypeVar("T")


class Neon:
    """
    Sync client for executing queries against a Neon database over HTTP.

    Examples
    --------
    Creating a client instance using an explicit connection string:

    >>> neon = Neon("postgresql://user:pass@hostname/dbname")

    ### Executing a simple query: ###

    >>> results = neon.query("SELECT * FROM users")
    >>> for row in results:
    ...     print(row)

    Creating a client instance using the DATABASE_URL environment variable:

    >>> neon = Neon()
    >>> results = neon.query("SELECT COUNT(*) FROM users")
    >>> print(results[0]["count"])  # Prints the total number of users

    ### Using Parameterized Queries ###

    Parameterized queries prevent SQL injection by using placeholders:

    >>> user_id = 42
    >>> user_data = neon.query("SELECT * FROM users WHERE id = $1", (user_id,))
    >>> print(user_data)

    Inserting new data with parameters:

    >>> neon.query(
    ...     "INSERT INTO users (name, email) VALUES ($1, $2)",
    ...     ("Alice", "alice@example.com"),
    ... )

    ### Executing Multiple Queries in a Transaction ###

    Transactions allow executing multiple queries atomically:

    >>> transaction_results = neon.transaction([
    ...     ("INSERT INTO orders (user_id, total) VALUES ($1, $2)", (42, 99.99)),
    ...     ("UPDATE users SET last_order = NOW() WHERE id = $1", (42,)),
    ...     "SELECT * FROM users WHERE id = 42"
    ... ])

    >>> print(transaction_results[2])  # Prints the user data after the update

    ### Customizing Query Execution with Options ###

    Using `HTTPQueryOptions` to retrieve full query metadata:

    >>> from pyserverless.models import HTTPQueryOptions
    >>> options = HTTPQueryOptions(full_results=True)
    >>> results = neon.query("SELECT * FROM users LIMIT 5", query_options=options)
    >>> print(results.fields)  # Print column metadata

    or when using transactions:

    >>> options = NeonTransactionOptions(
    ...     read_only=True,
    ...     isolation_level=IsolationLevel.SERIALIZABLE,
    ...     deferrable=True,
    ... )
    >>> results = neon.transaction([
    ...     ("SELECT * FROM users LIMIT 5", ()),
    ...     ("SELECT * FROM orders LIMIT 5 WHERE user_id = $1", (42,)),
    ... ], transaction_options=options)
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
            The SQL query to execute, using $1, $2, etc., for parameters.
        params : tuple[Any, ...] | None, optional
            Tuple of parameters to substitute into the query.
        query_options : HTTPQueryOptions | None, optional
            Query options.

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
        """
        params = params or ()
        query_options = query_options or HTTPQueryOptions()

        processed_params = [self._python_to_pg(p) for p in params]
        if query_options.query_callback:
            query_options.query_callback(query, processed_params)

        body = {
            "query": query,
            "params": processed_params,
        }

        headers = self._build_headers(query_options)

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
                query,
                processed_params,
                results,
                query_options.array_mode,
                query_options.full_results,
            )

        if query_options.full_results:
            return results
        return results.rows

    def transaction(
        self,
        queries: list[tuple[str, tuple[Any, ...]] | str],
        transaction_options: NeonTransactionOptions | None = None,
    ) -> list[FullQueryResults] | list[QueryRows]:
        """
        Execute multiple queries in a transaction.

        Parameters
        ----------
        queries : list[tuple[str, tuple[Any, ...]] | str]
            A list of queries to execute in sequence. Each element can either be:
            - A tuple (query, params), where params is a tuple of values to substitute
                into the query.
            - A plain query string (equivalent to (query, ())), if no parameters are needed.
        transaction_options : NeonTransactionOptions, optional
            transaction options.

        Returns
        -------
        list[FullQueryResults] | list[QueryRows]
            A list of results, one for each query. If the `full_results` flag in the options is True,
            each result is a FullQueryResults object; otherwise, it is a list of query rows.

        Raises
        ------
        NeonHTTPResponseError
            If the HTTP request fails.
        InvalidAuthTokenError
            If the authentication token is invalid.
        ParameterAdaptationError
            If parameters cannot be adapted from Python types to PostgreSQL types.
        """
        transaction_options = transaction_options or NeonTransactionOptions()

        # Unparameterized queries are strings, so wrap them as (query, ()).
        queries = [(item, ()) if isinstance(item, str) else item for item in queries]

        processed_queries = [
            {
                "query": query,
                "params": [self._python_to_pg(p) for p in params],
            }
            for query, params in queries
        ]
        body = {"queries": processed_queries}

        headers = self._build_headers(transaction_options)
        headers.update(
            {
                "Neon-Batch-Isolation-Level": transaction_options.isolation_level.value,
                "Neon-Batch-Read-Only": str(transaction_options.read_only).lower(),
                "Neon-Batch-Deferrable": str(transaction_options.deferrable).lower(),
            }
        )

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

    def _build_headers(self, options: HTTPQueryOptions) -> dict[str, str]:
        """Build headers for HTTP request."""
        headers: dict[str, str] = {
            "Neon-Connection-String": self._connection_string,
            "Neon-Raw-Text-Output": "true",
            "Neon-Array-Mode": str(options.array_mode).lower(),
        }

        if options.auth_token is not None:
            token = options.auth_token()
            if not token or not isinstance(token, str):
                raise InvalidAuthTokenError(token)
            headers["Authorization"] = f"Bearer {token}"

        return headers

    def _python_to_pg(self, param: Any) -> Any:
        """Convert a single Python value to its Postgres representation."""
        if param is None:
            return None
        if isinstance(param, dict):
            param = Jsonb(param)
        if isinstance(param, bytes):
            param = "\\x" + param.hex()

        try:
            dumper = self._transformer.get_dumper(param, PyFormat.TEXT)
            result = dumper.dump(param)
            if isinstance(result, bytes):
                result = result.decode("utf-8")
        except PsycopgError as e:
            raise PostgresAdaptationError(param) from e
        return result

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

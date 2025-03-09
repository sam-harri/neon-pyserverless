"""Errors for Neon Python Serverless driver."""

from typing import Any


class NeonPyServerlessError(Exception):
    """Base exception class for Neon Python Serverless driver."""


class ConnectionStringMissingError(NeonPyServerlessError):
    """Raised when no connection string is provided and DATABASE_URL environment variable is not found."""

    def __init__(self) -> None:
        message = (
            "No database connection string was provided to `neon()`"
            "and the DATABASE_URL environment variable was not found.\n\n"
        )
        super().__init__(message)


class ConnectionStringFormattingError(NeonPyServerlessError):
    """Raised when a connection string error occurs."""

    def __init__(self, connection_string: str) -> None:
        message = (
            f"Database connection string provided to `neon()` is not a valid URL. \n"
            f"Connection string provided: {connection_string}\n"
            f"The connection string must be in the format of: \n"
            f"""
postgresql://alex:AbC123dEf@ep-cool-darkness-a1b2c3d4-pooler.us-east-2.aws.neon.tech/dbname?sslmode=require
             ^    ^         ^                         ^                              ^
       role -|    |         |- hostname                                              |- database
                  |
                  |- password\n\n"""
            f"For more information, see https://neon.tech/docs/connect/connect-from-any-app"
        )
        super().__init__(message)


class NeonHTTPResponseError(NeonPyServerlessError):
    """Raised when an HTTP request to Neon fails."""

    def __init__(self, status_code: int, response_text: str) -> None:
        message = f"HTTP Error {status_code}\n\nError Details:\n{response_text}\n\n"
        super().__init__(message)


class NeonHTTPClientError(NeonPyServerlessError):
    """Raised when the HTTP client fails."""


class InvalidAuthTokenError(NeonPyServerlessError):
    """Raised when authentication token retrieval fails."""

    def __init__(
        self,
        token: Any,
    ) -> None:
        token_type = type(token).__name__
        token_value = str(token)
        message = (
            "Invalid authentication token received from token callback.\n"
            "Token was either None or not a string.\n"
            f"Token type: {token_type}\n"
            f"Token value: {token_value}\n\n"
        )
        super().__init__(message)


class PostgresAdaptationError(NeonPyServerlessError):
    """Raised when a parameter cannot be properly adapted for PostgreSQL."""

    def __init__(self, param: Any) -> None:
        param_type = type(param).__name__
        param_value = str(param)
        message = (
            f"Failed to adapt parameter for PostgreSQL.\n\n"
            f"Parameter Details:\n"
            f"Type: {param_type}\n"
            f"Value: {param_value}\n"
        )
        super().__init__(message)


class PythonAdaptationError(NeonPyServerlessError):
    """Raised when a value cannot be converted from PostgreSQL to Python."""

    def __init__(self, value: Any, type_oid: int) -> None:
        value_type = type(value).__name__
        value_str = str(value)
        message = (
            f"Failed to convert PostgreSQL value to Python.\n\n"
            f"Value Details:\n"
            f"Type: {value_type}\n"
            f"Type OID: {type_oid}\n"
            f"Value: {value_str}\n"
        )
        super().__init__(message)


class TransactionConfigurationError(NeonPyServerlessError):
    """Raised when transaction configuration options are invalid."""

    def __init__(self, isolation_level: str, read_only: bool) -> None:  # noqa: FBT001
        message = (
            f"Invalid transaction configuration.\n\n"
            f"For a deferrable transaction, you must use the SERIALIZABLE isolation level and read only mode.\n\n"
            f"Attempted Configuration for deferrable transaction:\n"
            f"- Isolation Level: {isolation_level}\n"
            f"- Read Only: {read_only}\n"
        )
        super().__init__(message)

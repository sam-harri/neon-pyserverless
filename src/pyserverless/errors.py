"""
Errors for Neon Python Serverless driver.

All errors in this module inherit from `NeonPyServerlessError`,
so you can catch all Neon Python Serverless driver errors
by catching this one exception.

Examples
--------
>>> try:
...     neon = Neon()
...     results = neon.query("SELECT * FROM users")
... except NeonPyServerlessError as e:
...     print(e)

Or if you want to catch specific errors:

>>> try:
...     neon = Neon()
...     results = neon.query("SELECT * FROM users")
... except NeonHTTPResponseError as e:
...     print(e)
... except InvalidAuthTokenError as e:
...     print(e)
... except ParameterAdaptationError as e:
...     print(e)

"""

from typing import Any


class NeonPyServerlessError(Exception):
    """Base exception class for Neon Python Serverless driver."""


class ConnectionStringMissingError(NeonPyServerlessError):
    """Raised when no connection string is provided and DATABASE_URL environment variable is not found."""

    def __init__(self) -> None:
        message = (
            "No database connection string was provided to `neon()`"
            "and the DATABASE_URL environment variable was not found.\n\n"
            "You must either:\n"
            "1. Pass a connection string directly to the Neon constructor:\n"
            "   neon = Neon('postgresql://user:pass@hostname/dbname')\n\n"
            "2. Set the DATABASE_URL environment variable:\n"
            "   - In your .env file: DATABASE_URL=postgresql://user:pass@hostname/dbname\n"
            "   - Or export it in your shell: export DATABASE_URL=postgresql://user:pass@hostname/dbname\n\n"
            "For more information on connection strings, see https://neon.tech/docs/connect/connect-from-any-app"
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
       role -|    |         |- hostname               |- pooler option               |- database
                  |
                  |- password\n\n"""
            f"For more information, see https://neon.tech/docs/connect/connect-from-any-app"
        )
        super().__init__(message)


class NeonHTTPResponseError(NeonPyServerlessError):
    """Raised when an HTTP request to Neon's API fails."""

    def __init__(self, status_code: int, response_text: str) -> None:
        status_explanations = {
            401: "Authentication failed. Please check your connection string is valid and has the correct credentials.",
            403: "Permission denied. Please ensure your connection string has the necessary permissions.",
            404: "API endpoint not found. This might indicate an issue with the hostname in your connection string.",
            429: "Too many requests. You may have exceeded the rate limit for your database.",
            500: "Internal server error occurred in Neon's API.",
            502: "Bad gateway. There might be temporary issues with Neon's API.",
            503: "Service unavailable. Neon's API might be down or undergoing maintenance.",
            504: "Gateway timeout. The request to Neon's API timed out.",
        }

        explanation = status_explanations.get(
            status_code,
            "An unexpected error occurred while communicating with Neon's API.",
        )

        message = (
            f"HTTP Error {status_code}\n\n"
            f"Error Details:\n"
            f"{response_text}\n\n"
            f"Explanation:\n"
            f"{explanation}\n\n"
            f"Troubleshooting Steps:\n"
            "1. Verify your connection string is correct\n"
            "2. Check if your database is online and accessible\n"
            "3. Ensure your query syntax is valid\n"
            "4. If the issue persists, check Neon's status page: https://status.neon.tech \n\n"
            "For more help, visit: https://neon.tech/docs/connect/connection-errors"
        )
        super().__init__(message)


class InvalidAuthTokenError(NeonPyServerlessError):
    """Raised when authentication token retrieval fails."""

    def __init__(
        self,
        token: Any,
    ) -> None:
        message = (
            "Invalid authentication token received.\n\n"
            f"Token value: {token}\n\n"
            "Error Details:\n"
            "The auth token callback returned an invalid value. "
            "Token must be a non-empty string.\n\n"
            "Possible causes:\n"
            "1. The auth token callback returned None\n"
            "2. The auth token callback returned an empty string\n"
            "3. The auth token callback returned a non-string value\n\n"
        )

        super().__init__(message)


class PostgresAdaptationError(NeonPyServerlessError):
    """Raised when a parameter cannot be properly adapted for PostgreSQL."""

    def __init__(self, param: Any, error_details: str) -> None:
        param_type = type(param).__name__
        param_value = str(param)
        message = (
            f"Failed to adapt parameter for PostgreSQL.\n\n"
            f"Parameter Details:\n"
            f"Type: {param_type}\n"
            f"Value: {param_value}\n"
            f"Error: {error_details}\n\n"
        )
        super().__init__(message)


class PythonAdaptationError(NeonPyServerlessError):
    """Raised when a value cannot be properly converted from PostgreSQL to Python."""

    def __init__(self, value: Any, type_oid: int, error_details: str) -> None:
        value_type = type(value).__name__
        value_str = str(value)
        message = (
            f"Failed to convert PostgreSQL value to Python.\n\n"
            f"Value Details:\n"
            f"Type: {value_type}\n"
            f"Type OID: {type_oid}\n"
            f"Value: {value_str}\n"
            f"Error: {error_details}\n\n"
        )
        super().__init__(message)


class TransactionConfigurationError(NeonPyServerlessError):
    """
    Raised when transaction configuration options are invalid.

    Parameters
    ----------
    isolation_level : str
        The isolation level that was attempted
    read_only : bool
        The read_only setting that was attempted
    deferrable : bool
        The deferrable setting that was attempted

    """

    def __init__(self, isolation_level: str, read_only: bool, deferrable: bool) -> None:  # noqa: FBT001
        message = (
            f"Invalid transaction configuration.\n\n"
            f"Attempted Configuration:\n"
            f"- Isolation Level: {isolation_level}\n"
            f"- Read Only: {read_only}\n"
            f"- Deferrable: {deferrable}\n\n"
            f"Error Details:\n"
            "DEFERRABLE transactions require both:\n"
            "1. SERIALIZABLE isolation level\n"
            "2. READ ONLY mode\n\n"
            "Valid Configurations:\n"
            "- Non-deferrable transactions can use any isolation level and read mode\n"
            "- Deferrable transactions must be SERIALIZABLE and READ ONLY\n\n"
            "For more information on transaction modes, see:\n"
            "https://www.postgresql.org/docs/current/sql-set-transaction.html"
        )
        super().__init__(message)

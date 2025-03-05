"""Data models for Neon database client."""

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pyserverless.errors import TransactionConfigurationError

# A FieldDef represents metadata for a column
FieldDef = dict[str, Any]

# QueryRows is either a list of tuples (when arrayMode is True) or a list of dicts (when False)
QueryRows = list[list[str]] | list[dict[str, str]]


@dataclass(frozen=True)
class FullQueryResults:
    """
    Results of query with fullResults set to True.

    Parameters
    ----------
    rows : QueryRows
        The actual query results, either as arrays or objects.
    fields : list[FieldDef]
        Metadata about the columns in the results.
    rowCount : int
        Number of rows returned by the query.
    rowAsArray : bool
        Whether rows are returned as arrays (True) or objects (False).
    command : str
        The SQL command that was executed (e.g., "SELECT", "INSERT").

    """

    rows: QueryRows
    fields: list[FieldDef]
    rowCount: int  # noqa: N815
    rowAsArray: bool  # noqa: N815
    command: str


@dataclass(frozen=True)
class ParameterizedQuery:
    """
    Represents a query with placeholders and its associated parameters.

    Parameters
    ----------
    query : str
        The SQL query string with placeholders.
    params : list[Any]
        The parameters to be substituted into the query.

    """

    query: str
    params: list[Any]


# Postgres transaction isolation level: see https://www.postgresql.org/docs/current/transaction-iso.html
class IsolationLevel(Enum):
    """
    Postgres transaction isolation level.

    See https://www.postgresql.org/docs/current/transaction-iso.html for details.

    Attributes
    ----------
    READ_UNCOMMITTED : str
        Lowest isolation level, allows dirty reads.
    READ_COMMITTED : str
        Default isolation level, prevents dirty reads.
    REPEATABLE_READ : str
        Prevents non-repeatable reads.
    SERIALIZABLE : str
        Highest isolation level, prevents phantom reads.

    Notes
    -----
    - READ_UNCOMMITTED is actually just READ_COMMITTED in Postgres since
      dirty reads are not supported.

    """

    READ_UNCOMMITTED = "ReadUncommitted"
    READ_COMMITTED = "ReadCommitted"
    REPEATABLE_READ = "RepeatableRead"
    SERIALIZABLE = "Serializable"


@dataclass(frozen=True)
class HTTPQueryOptions:
    """
    Options for HTTP query execution.

    Parameters
    ----------
    array_mode : bool, default=False
        Whether to return results as arrays or objects.
    full_results : bool, default=False
        Whether to return full result metadata.
    fetch_options : dict[str, Any], default={}
        Options to pass to httpx.Client.
    auth_token : Callable[[], str] | None, default=None
        Function that returns an auth token.
    query_callback : Callable[[ParameterizedQuery], None] | None, default=None
        Callback invoked before query execution.
    result_callback : Callable[[ParameterizedQuery, FullQueryResults, bool, bool], None] | None, default=None
        Callback invoked after query execution.

    """

    array_mode: bool = False
    full_results: bool = False
    fetch_options: dict[str, Any] = field(default_factory=dict)
    auth_token: Callable[[], str] | None = None
    query_callback: Callable[[ParameterizedQuery], None] = None
    result_callback: Callable[
        [ParameterizedQuery, FullQueryResults, bool, bool],
        None,
    ] = None


@dataclass(frozen=True)
class NeonTransactionOptions(HTTPQueryOptions):
    """
    Options for transaction execution.

    Parameters
    ----------
    isolation_level : IsolationLevel, default=IsolationLevel.READ_UNCOMMITTED
        Transaction isolation level.
    read_only : bool, default=False
        Whether transaction is read-only.
    deferrable : bool, default=False
        Whether transaction is deferrable. Only applicable when isolation_level
        is SERIALIZABLE and read_only is True.
    array_mode : bool, default=False
        Whether to return results as arrays.
    full_results : bool, default=False
        Whether to return full result metadata.
    fetch_options : dict[str, Any], default={}
        Options to pass to httpx.Client.
    auth_token : Callable[[], str] | None, default=None
        Function that returns an auth token.
    query_callback : Callable[[ParameterizedQuery], None] | None, default=None
        Callback invoked before query execution.
    result_callback : Callable[[ParameterizedQuery, FullQueryResults, bool, bool], None] | None, default=None
        Callback invoked after query execution.

    """

    read_only: bool = False
    deferrable: bool = False
    isolation_level: IsolationLevel = IsolationLevel.READ_UNCOMMITTED

    def __post_init__(self) -> None:
        """Validate transaction configuration."""
        deferable_requirements = self.isolation_level == IsolationLevel.SERIALIZABLE and self.read_only
        if self.deferrable and not deferable_requirements:
            raise TransactionConfigurationError(
                self.isolation_level,
                self.read_only,
                self.deferrable,
            )

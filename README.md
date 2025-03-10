# Neon Python Serverless Client

A lightweight, serverless client for executing queries against a Neon database over HTTP. This package simplifies querying your Neon PostgreSQL database from Python without managing a persistent connection.

# Usage

## Initialize the client

Either pass in a connection string
```python
from pyserverless import Neon

neon = Neon("postgresql://neondb_owner:password@host/neondb?sslmode=require")
```

or if the `DATABASE_URL` environment variable is set:

```python
neon = Neon()
```

## Execute a query

Without any parameters:
```python
results = neon.query("SELECT * FROM users")
```

With parameters:
```python
results = neon.query("SELECT * FROM users WHERE id = $1", (1,))
```

And with options:
```python
results = neon.query(
    "SELECT * FROM users",
    (),
    HTTPQueryOptions(
        full_results=True,
        array_mode=True,
        fetch_options={"timeout": 30.0}
    ),
)
```

## Execute a transaction

Without any options:
```python
results = neon.transaction(
    [
        ("INSERT INTO users (name) VALUES ($1)", ("John",)),
        ("SELECT * FROM users"),
    ],
)
```

With options:
```python
results = neon.transaction(
    [
        ("SELECT questions, answers FROM questions_and_answers WHERE topic = $1", ("SQL",)),
        ("SELECT topic, COUNT(*) FROM questions_and_answers GROUP BY topic"),
    ],
    NeonTransactionOptions(
        isolation_level=IsolationLevel.SERIALIZABLE,
        read_only=True,
        deferrable=True,
    ),
)
```
## Callbacks
You can pass in callbacks to the query and transaction methods which are called right before the query is executed, and right after the results are returned, respectively.

```python
def query_callback(query: str, params: list[Any]) -> None:
    logger.info(f"Executing query: {query}")

def result_callback(query: str, params: list[Any], results: FullQueryResults | QueryRows, array_mode: bool, full_results: bool):
    logger.info(f"Query {query} executed with {results.rowCount} rows, array_mode: {array_mode}, full_results: {full_results}")

results = neon.query(
    "SELECT * FROM users",
    (),
    HTTPQueryOptions(
        query_callback=query_callback,
        result_callback=result_callback,
    ),
)
```

## Custom Type Conversion

Under the hood, the client uses psycopg to convert Python types to Postgres types and vice versa. 
For now, it is possible to modify the Neon.transformer object and register custom adapters.
A cleaner and more supported way to do this will be added in the future.

# API Reference

### `Neon(connection_string: str | None = None)`

- **Parameters:**
  - `connection_string`: A PostgreSQL connection string in the format `postgresql://user:pass@hostname/dbname`. If omitted, the client looks for the `DATABASE_URL` environment variable.
- **Raises:**
  - `ConnectionStringMissingError`: If no connection string is provided and `DATABASE_URL` is not set.
  - `ConnectionStringFormattingError`: If the provided connection string is not correctly formatted.

### `query(query: str, params: tuple[Any, ...] = None, query_options: HTTPQueryOptions = None) -> FullQueryResults | QueryRows`

- **Parameters:**
  - `query`: The SQL query string with placeholders (`$1`, `$2`, etc.).
  - `params`: A tuple of parameters to bind to the query.
  - `query_options`: Optional `HTTPQueryOptions` to control behavior such as full results, array mode, or request options.
- **Returns:**
  - Either a `FullQueryResults` object (if `full_results=True`) or a list of rows.
- **Raises:**
  - `NeonHTTPResponseError`: If the HTTP request fails.
  - `InvalidAuthTokenError`: If the auth token callback returns an invalid value.
  - `PostgresAdaptationError`: If a parameter cannot be adapted to a PostgreSQL type.
  - `PythonAdaptationError`: If a PostgreSQL value cannot be converted to a Python type.

### `transaction(queries: list[tuple[str, tuple[Any, ...]] | str], transaction_options: NeonTransactionOptions = None) -> list[FullQueryResults] | list[QueryRows]`

- **Parameters:**
  - `queries`: A list of `(query, params)` tuples  or strings to execute in a single transaction.
  - `transaction_options`: Optional `NeonTransactionOptions` controlling transaction isolation, read-only mode, and deferrable settings.
- **Returns:**
  - A list of results corresponding to each query, either as `FullQueryResults` or a list of rows.
- **Raises:**
  - `NeonHTTPResponseError`: If the HTTP request fails.
  - `InvalidAuthTokenError`: If the auth token callback returns an invalid value.
  - `PostgresAdaptationError`: If a parameter cannot be adapted to a PostgreSQL type.
  - `PythonAdaptationError`: If a PostgreSQL value cannot be converted to a Python type.

## Errors

- **`NeonPyServerlessError`**: Base exception class for all errors in this package.
- **`ConnectionStringMissingError`**: Raised when no connection string is provided and `DATABASE_URL` is not set.
- **`ConnectionStringFormattingError`**: Raised for an invalid connection string format.
- **`NeonHTTPResponseError`**: Raised when the HTTP request to the Neon API fails.
- **`InvalidAuthTokenError`**: Raised when the auth token callback returns an invalid value.
- **`PostgresAdaptationError`**: Raised when a parameter cannot be adapted to a PostgreSQL type.
- **`PythonAdaptationError`**: Raised when a PostgreSQL value cannot be converted to a Python type.
- **`TransactionConfigurationError`**: Raised when transaction options (like isolation level, read-only, deferrable) are configured incorrectly.
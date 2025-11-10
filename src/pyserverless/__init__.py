"""
Neon serverless database client.

This library provides a Python client for Neon's serverless Postgres database.
It allows the execution of SQL queries over HTTP with support for transactions,
parameterized queries, and various data types.

Examples
--------
Create a Neon client:

>>> neon = Neon("postgresql://user:pass@hostname/dbname")

or if the DATABASE_URL environment variable is set:

>>> neon = Neon()

Execute a query:

>>> results = neon.query("SELECT * FROM users")

Pass in parameters to the query in the form of a tuple:

>>> results = neon.query("SELECT * FROM users WHERE id = $1", (1,))

And pass in options to the query:

>>> results = neon.query(
...     "SELECT * FROM users",
...     (),
...     HTTPQueryOptions(
...         full_results=True,
...         array_mode=True,
...         fetch_options={"timeout": 30.0}
...     ),
... )

Execute a transaction:

>>> results = neon.transaction(
...     [
...         ("INSERT INTO users (name) VALUES ($1)", ("John",)),
...         ("SELECT * FROM users", ()),
...     ],
... )

Like the query method, the transaction method also accepts options:

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

from pyserverless.models import (
    HTTPQueryOptions,
    IsolationLevel,
    NeonTransactionOptions,
)
from pyserverless.neon import Neon, NeonAsync

__all__ = [
    "HTTPQueryOptions",
    "IsolationLevel",
    "Neon",
    "NeonAsync",
    "NeonTransactionOptions",
]

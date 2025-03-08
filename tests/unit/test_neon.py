import datetime
import ipaddress
import uuid
from decimal import Decimal
from unittest.mock import Mock, patch

import httpx
import pytest

from pyserverless.errors import (
    ConnectionStringFormattingError,
    ConnectionStringMissingError,
    InvalidAuthTokenError,
    NeonHTTPResponseError,
    PostgresAdaptationError,
    PythonAdaptationError,
)
from pyserverless.models import FullQueryResults, HTTPQueryOptions, NeonTransactionOptions
from pyserverless.neon import Neon


@pytest.fixture
def mock_neon_client():
    """Mock Neon client."""
    return Neon("postgresql://user:pass@hostname/dbname")


@pytest.fixture
def mock_response_object_mode():
    """Mock response for query in object mode."""
    response = Mock(spec=httpx.Response)
    response.status_code = httpx.codes.OK
    response.json.return_value = {
        "rows": [
            {"id": "1", "name": "test1", "value": "100", "is_active": "t"},
            {"id": "2", "name": "test2", "value": "200", "is_active": "f"},
        ],
        "fields": [
            {
                "name": "id",
                "dataTypeID": 23,
                "tableID": 24576,
                "columnID": 1,
                "dataTypeSize": 4,
                "dataTypeModifier": -1,
                "format": "text",
            },
            {
                "name": "name",
                "dataTypeID": 1043,
                "tableID": 24576,
                "columnID": 2,
                "dataTypeSize": -1,
                "dataTypeModifier": -1,
                "format": "text",
            },
            {
                "name": "value",
                "dataTypeID": 23,
                "tableID": 24576,
                "columnID": 3,
                "dataTypeSize": 4,
                "dataTypeModifier": -1,
                "format": "text",
            },
            {
                "name": "is_active",
                "dataTypeID": 16,
                "tableID": 24576,
                "columnID": 4,
                "dataTypeSize": 1,
                "dataTypeModifier": -1,
                "format": "text",
            },
        ],
        "rowCount": 2,
        "rowAsArray": False,
        "command": "SELECT",
    }
    return response


@pytest.fixture
def mock_response_array_mode():
    """Mock response for query in array mode."""
    response = Mock(spec=httpx.Response)
    response.status_code = httpx.codes.OK
    response.json.return_value = {
        "rows": [["1", "test1", "100", "t"], ["2", "test2", "200", "f"]],
        "fields": [
            {
                "name": "id",
                "dataTypeID": 23,
                "tableID": 24576,
                "columnID": 1,
                "dataTypeSize": 4,
                "dataTypeModifier": -1,
                "format": "text",
            },
            {
                "name": "name",
                "dataTypeID": 1043,
                "tableID": 24576,
                "columnID": 2,
                "dataTypeSize": -1,
                "dataTypeModifier": -1,
                "format": "text",
            },
            {
                "name": "value",
                "dataTypeID": 23,
                "tableID": 24576,
                "columnID": 3,
                "dataTypeSize": 4,
                "dataTypeModifier": -1,
                "format": "text",
            },
            {
                "name": "is_active",
                "dataTypeID": 16,
                "tableID": 24576,
                "columnID": 4,
                "dataTypeSize": 1,
                "dataTypeModifier": -1,
                "format": "text",
            },
        ],
        "rowCount": 2,
        "rowAsArray": True,
        "command": "SELECT",
    }
    return response


@pytest.fixture
def mock_response_transaction_object_mode():
    """Mock response for transaction in object mode."""
    response = Mock(spec=httpx.Response)
    response.status_code = httpx.codes.OK
    response.json.return_value = {
        "results": [
            {
                "rows": [{"?column?": "1"}],
                "fields": [
                    {
                        "name": "?column?",
                        "dataTypeID": 23,
                        "tableID": 0,
                        "columnID": 0,
                        "dataTypeSize": 4,
                        "dataTypeModifier": -1,
                        "format": "text",
                    }
                ],
                "rowCount": 1,
                "rowAsArray": False,
                "command": "SELECT",
            },
            {
                "rows": [{"?column?": "2"}],
                "fields": [
                    {
                        "name": "?column?",
                        "dataTypeID": 23,
                        "tableID": 0,
                        "columnID": 0,
                        "dataTypeSize": 4,
                        "dataTypeModifier": -1,
                        "format": "text",
                    }
                ],
                "rowCount": 1,
                "rowAsArray": False,
                "command": "SELECT",
            },
        ]
    }
    return response


@pytest.fixture
def mock_response_transaction_array_mode():
    """Mock response for transaction in array mode."""
    response = Mock(spec=httpx.Response)
    response.status_code = httpx.codes.OK
    response.json.return_value = {
        "results": [
            {
                "rows": [["1"]],
                "fields": [
                    {
                        "name": "?column?",
                        "dataTypeID": 23,
                        "tableID": 0,
                        "columnID": 0,
                        "dataTypeSize": 4,
                        "dataTypeModifier": -1,
                        "format": "text",
                    }
                ],
                "rowCount": 1,
                "rowAsArray": True,
                "command": "SELECT",
            },
            {
                "rows": [["2"]],
                "fields": [
                    {
                        "name": "?column?",
                        "dataTypeID": 23,
                        "tableID": 0,
                        "columnID": 0,
                        "dataTypeSize": 4,
                        "dataTypeModifier": -1,
                        "format": "text",
                    }
                ],
                "rowCount": 1,
                "rowAsArray": True,
                "command": "SELECT",
            },
        ]
    }
    return response


class TestNeon:
    @patch("httpx.Client")
    def test_query_object_mode(self, mock_client, mock_neon_client, mock_response_object_mode):
        """Test query in object mode."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_object_mode

        result = mock_neon_client.query(
            "query;",
            (),
            HTTPQueryOptions(full_results=True),
        )

        assert isinstance(result, FullQueryResults)
        assert result.rowCount == 2
        assert result.rowAsArray is False
        assert isinstance(result.rows, list)
        assert all(isinstance(row, dict) for row in result.rows)
        assert result.rows[0]["id"] == 1
        assert result.rows[0]["name"] == "test1"

    @patch("httpx.Client")
    def test_query_array_mode(self, mock_client, mock_neon_client, mock_response_array_mode):
        """Test query in array mode."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_array_mode

        result = mock_neon_client.query(
            "query;",
            (),
            HTTPQueryOptions(full_results=True),
        )

        assert isinstance(result, FullQueryResults)
        assert result.rowCount == 2
        assert result.rowAsArray is True
        assert isinstance(result.rows, list)
        assert all(isinstance(row, list) for row in result.rows)
        assert result.rows[0][0] == 1
        assert result.rows[0][1] == "test1"

    @patch("httpx.Client")
    def test_query_object_mode_rows(self, mock_client, mock_neon_client, mock_response_object_mode):
        """Test query in object mode without full results."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_object_mode

        result = mock_neon_client.query(
            "query;",
            (),
            HTTPQueryOptions(),
        )

        assert isinstance(result, list)
        assert all(isinstance(row, dict) for row in result)
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["name"] == "test1"

    @patch("httpx.Client")
    def test_query_array_mode_rows(self, mock_client, mock_neon_client, mock_response_array_mode):
        """Test query in array mode without full results."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_array_mode

        result = mock_neon_client.query(
            "query;",
            (),
            HTTPQueryOptions(array_mode=True),
        )

        assert isinstance(result, list)
        assert all(isinstance(row, list) for row in result)
        assert len(result) == 2
        assert result[0][0] == 1
        assert result[0][1] == "test1"

    @patch("httpx.Client")
    def test_query_with_auth_token(self, mock_client, mock_neon_client, mock_response_object_mode):
        """Test query with auth token."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_object_mode

        def get_token() -> str:
            return "test-token"

        _ = mock_neon_client.query(
            "query;",
            (),
            HTTPQueryOptions(auth_token=get_token),
        )

        mock_client.return_value.__enter__.return_value.post.assert_called_once()
        call_args = mock_client.return_value.__enter__.return_value.post.call_args
        assert call_args[1]["headers"]["Authorization"] == "Bearer test-token"

    def test_query_with_invalid_auth_token(self, mock_neon_client):
        """Test query with invalid auth token."""

        def get_token() -> None:
            return None

        with pytest.raises(InvalidAuthTokenError):
            mock_neon_client.query(
                "query;",
                (),
                HTTPQueryOptions(auth_token=get_token),
            )

        def get_token() -> int:
            return 1

        with pytest.raises(InvalidAuthTokenError):
            mock_neon_client.query(
                "query;",
                (),
                HTTPQueryOptions(auth_token=get_token),
            )

    @patch("httpx.Client")
    def test_query_http_error(self, mock_client, mock_neon_client):
        """Test query with HTTP error."""
        mock_response = Mock(spec=httpx.Response)
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response

        with pytest.raises(NeonHTTPResponseError):
            mock_neon_client.query("query;", ())

    @patch("httpx.Client")
    def test_query_with_query_callback(self, mock_client, mock_neon_client, mock_response_object_mode):
        """Test query with query callback."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_object_mode

        mock_callback = Mock()

        mock_neon_client.query(
            "SELECT * FROM test_table WHERE value > $1;",
            ("100",),
            HTTPQueryOptions(query_callback=mock_callback),
        )

        mock_callback.assert_called_once()
        callback_arg = mock_callback.call_args[0][0]
        assert callback_arg.query == "SELECT * FROM test_table WHERE value > $1;"
        assert callback_arg.params == ["'100'"]

    @patch("httpx.Client")
    def test_query_with_result_callback(self, mock_client, mock_neon_client, mock_response_object_mode):
        """Test query with result callback."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_object_mode

        mock_callback = Mock()

        mock_neon_client.query(
            "query;",
            (),
            HTTPQueryOptions(result_callback=mock_callback, array_mode=False, full_results=True),
        )

        mock_callback.assert_called_once()
        callback_args = mock_callback.call_args[0]

        assert callback_args[0].query == "query;"
        assert callback_args[0].params == []
        assert isinstance(callback_args[1], FullQueryResults)
        assert callback_args[1].rowCount == 2

        assert callback_args[2] is False  # array_mode
        assert callback_args[3] is True  # full_results

    @patch("httpx.Client")
    def test_query_with_timeout(self, mock_client, mock_neon_client, mock_response_object_mode):
        """Test query with timeout."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_object_mode

        # Test with a reasonable timeout for a slow query
        _ = mock_neon_client.query(
            "slow query;",  # Simulating a slow query
            (),
            HTTPQueryOptions(fetch_options={"timeout": 15.0}),  # 15 second timeout
        )

        mock_client.assert_called_once_with(timeout=15.0)

    @patch("httpx.Client")
    def test_transaction_object_mode(self, mock_client, mock_neon_client, mock_response_transaction_object_mode):
        """Test transaction in object mode."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_transaction_object_mode

        queries = [
            ("query;", ()),
            ("query;", ()),
        ]

        opts = NeonTransactionOptions(
            full_results=True,
        )

        results = mock_neon_client.transaction(queries, opts)

        mock_client.return_value.__enter__.return_value.post.assert_called_once()
        call_args = mock_client.return_value.__enter__.return_value.post.call_args

        assert call_args[1]["json"]["queries"] == [
            {"query": "query;", "params": []},
            {"query": "query;", "params": []},
        ]

        headers = call_args[1]["headers"]
        assert headers["Neon-Batch-Isolation-Level"] == "ReadUncommitted"
        assert headers["Neon-Batch-Read-Only"] == "false"
        assert headers["Neon-Batch-Deferrable"] == "false"

        assert isinstance(results, list)
        assert len(results) == 2
        assert isinstance(results[0], FullQueryResults)
        assert isinstance(results[0].rows, list)
        assert all(isinstance(row, dict) for row in results[0].rows)
        assert results[0].rows[0]["?column?"] == 1
        assert results[0].rowAsArray is False

    @patch("httpx.Client")
    def test_transaction_array_mode(self, mock_client, mock_neon_client, mock_response_transaction_array_mode):
        """Test transaction in array mode."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_transaction_array_mode

        queries = [
            ("SELECT 1", ()),
            ("SELECT 2", ()),
        ]

        opts = NeonTransactionOptions(
            array_mode=True,
            full_results=True,
        )

        results = mock_neon_client.transaction(queries, opts)

        assert isinstance(results, list)
        assert len(results) == 2
        assert isinstance(results[0], FullQueryResults)
        assert isinstance(results[0].rows, list)
        assert all(isinstance(row, list) for row in results[0].rows)
        assert results[0].rows[0][0] == 1
        assert results[0].rowAsArray is True

    @patch("httpx.Client")
    def test_transaction_with_auth_token(self, mock_client, mock_neon_client, mock_response_transaction_object_mode):
        """Test transaction with auth token."""
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response_transaction_object_mode

        def get_token() -> str:
            return "test-token"

        _ = mock_neon_client.transaction(
            [("query;", ())],
            NeonTransactionOptions(auth_token=get_token),
        )

        mock_client.return_value.__enter__.return_value.post.assert_called_once()
        call_args = mock_client.return_value.__enter__.return_value.post.call_args
        assert call_args[1]["headers"]["Authorization"] == "Bearer test-token"

    @pytest.mark.parametrize(
        ("oid", "raw_value", "expected_type", "expected_value"),
        [
            (23, "1", int, 1),
            (20, "9223372036854775807", int, 9223372036854775807),
            (21, "-32768", int, -32768),
            (700, "3.14159", float, 3.14159),
            (701, "2.718281828459045", float, 2.718281828459045),
            (1700, "123456.78", Decimal, Decimal("123456.78")),
            (16, "t", bool, True),
            (16, "f", bool, False),
            (25, "sample text", str, "sample text"),
            (1042, "fixed123  ", str, "fixed123  "),
            (1043, "variable text", str, "variable text"),
            (1082, "2024-02-26", datetime.date, datetime.date(2024, 2, 26)),
            (1083, "14:30:00", datetime.time, datetime.time(14, 30, 0)),
            (1114, "2024-02-26 14:30:00", datetime.datetime, datetime.datetime(2024, 2, 26, 14, 30, 0, tzinfo=None)),  # noqa: DTZ001
            (
                1184,
                "2024-02-27 14:30:00+00",
                datetime.datetime,
                datetime.datetime(2024, 2, 27, 14, 30, 0, tzinfo=datetime.UTC),
            ),
            (1186, "1 day 02:30:00", datetime.timedelta, datetime.timedelta(days=1, hours=2, minutes=30)),
            (17, "\\xdeadbeef", bytes, bytes.fromhex("deadbeef")),
            (114, '{"key": "value", "array": [1, 2, 3]}', dict, {"key": "value", "array": [1, 2, 3]}),
            (3802, '{"key": "value", "array": [1, 2, 3]}', dict, {"key": "value", "array": [1, 2, 3]}),
            (1007, "{1,2,3,4,5}", list, [1, 2, 3, 4, 5]),
            (1009, "{one,two,three}", list, ["one", "two", "three"]),
            (
                2950,
                "123e4567-e89b-12d3-a456-426614174000",
                uuid.UUID,
                uuid.UUID("123e4567-e89b-12d3-a456-426614174000"),
            ),
            (869, "192.168.1.1", ipaddress.IPv4Address, ipaddress.IPv4Address("192.168.1.1")),
            (650, "192.168.1.0/24", ipaddress.IPv4Network, ipaddress.IPv4Network("192.168.1.0/24")),
            (829, "08:00:2b:01:02:03", str, "08:00:2b:01:02:03"),
            (600, "(1,1)", str, "(1,1)"),
            (628, "{1,1,1}", str, "{1,1,1}"),
        ],
    )
    def test_pg_to_python(self, mock_neon_client, oid, raw_value, expected_type, expected_value):
        """Test conversion of PostgreSQL values to Python types."""
        result = mock_neon_client._pg_to_python(raw_value, oid)
        assert isinstance(result, expected_type)
        assert result == expected_value

    @pytest.mark.parametrize(
        ("value", "type_oid"),
        [
            ("not_an_int", 23),  # integer
            ("not_a_uuid", 2950),  # uuid
            ("{invalid_json", 114),  # json
            ("2024-13-45", 1082),  # date
            ("999.999.999.999", 869),  # inet
        ],
    )
    def test_pg_to_python_conversion_errors(self, mock_neon_client, value, type_oid):
        """Test that invalid values raise PythonAdaptationError with appropriate messages."""
        with pytest.raises(PythonAdaptationError):
            mock_neon_client._pg_to_python(value, type_oid)

    @pytest.mark.parametrize(
        "connection_string",
        [
            "postgresql://user:pass@hostname/dbname",
            "postgres://admin:secret@my-db.host.com/mydb",
            "postgresql://user:complex!pass@word@neon.db/app",
            "postgresql://user:pass@ep-cool-darkness-123456.us-east-2.aws.neon.tech/dbname",
            "postgresql://user:pass@ep-quiet-forest-123456-pooler.us-east-2.aws.neon.tech/dbname",
            # missing password is okay (passwordless authenticated role for None Autheorize RLS)
            "postgresql://user@hostname/dbname",
        ],
    )
    def test_valid_connection_strings(self, connection_string):
        """Test that valid connection strings are parsed successfully."""
        neon = Neon(connection_string)
        assert neon._connection_string == connection_string

    @pytest.mark.parametrize(
        "connection_string",
        [
            "invalid://user:pass@hostname/dbname",  # wrong protocol
            "postgresql://@hostname/dbname",  # missing username
            "postgresql://user:pass@/dbname",  # missing hostname
            "postgresql://user:pass@hostname",  # missing database name
            "postgresql://user:pass@hostname/",  # empty database name
            "not_a_url",  # completely invalid URL
            "",  # empty string
        ],
    )
    def test_invalid_connection_strings(self, connection_string):
        """Test that invalid connection strings raise appropriate errors."""
        with pytest.raises(ConnectionStringFormattingError):
            Neon(connection_string)

    def test_connection_string_parsing(self):
        """Test that connection string is parsed into correct URL and stored properly."""
        connection_string = "postgresql://user:pass@hostname/dbname"
        neon = Neon(connection_string)

        assert neon._url == "https://hostname/sql"
        assert neon._connection_string == connection_string

    def test_missing_connection_string(self):
        """Test that missing connection string raises appropriate error."""
        with pytest.raises(ConnectionStringMissingError):
            Neon()

    @patch.dict("os.environ", {"DATABASE_URL": "postgresql://user:pass@hostname/dbname"})
    def test_init_with_env_variable(self):
        """Test that Neon can be initialized with an environment variable."""
        neon = Neon()
        assert neon._connection_string == "postgresql://user:pass@hostname/dbname"

    @pytest.mark.parametrize(
        ("python_value", "expected_pg_string"),
        [
            (42, "42"),
            (-17, " -17"),
            (3.14159, "3.14159"),
            (Decimal("123456.78"), "123456.78"),
            ("hello", "'hello'"),
            ("quote'mark", "'quote''mark'"),
            (True, "true"),
            (False, "false"),
            (None, "NULL"),
            (datetime.date(2024, 2, 26), "'2024-02-26'::date"),
            (datetime.time(14, 30, 0), "'14:30:00'::time"),
            (datetime.datetime(2024, 2, 26, 14, 30, 0, tzinfo=None), "'2024-02-26 14:30:00'::timestamp"),  # noqa: DTZ001
            (
                datetime.datetime(2024, 2, 27, 14, 30, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=-5))),
                "'2024-02-27 14:30:00-05:00'::timestamptz",
            ),
            (
                datetime.datetime(2024, 2, 27, 14, 30, 0, tzinfo=datetime.UTC),
                "'2024-02-27 14:30:00+00:00'::timestamptz",
            ),
            (datetime.timedelta(days=1, hours=2, minutes=30), "'1 day 2:30:00'::interval"),
            (bytes.fromhex("deadbeef"), "\\xdeadbeef"),
            ({"key": "value", "array": [1, 2, 3]}, '\'{"key": "value", "array": [1, 2, 3]}\''),
            ([1, 2, 3], "'[1, 2, 3]'"),
            ([1, 2, 3, 4, 5], "'[1, 2, 3, 4, 5]'"),
            (["one", "two", "three"], '\'["one", "two", "three"]\''),
            (uuid.UUID("123e4567-e89b-12d3-a456-426614174000"), "'123e4567e89b12d3a456426614174000'::uuid"),
            (ipaddress.IPv4Address("192.168.1.1"), "'192.168.1.1'::inet"),
            (ipaddress.IPv4Network("192.168.1.0/24"), "'192.168.1.0/24'::cidr"),
            ("", "''"),
            (" ", "' '"),
            ("\n", "'\n'"),
            ("\\", " E'\\\\'"),
        ],
    )
    def test_python_to_pg(self, mock_neon_client, python_value, expected_pg_string):
        """Test conversion of Python values to their PostgreSQL string representation."""
        result = mock_neon_client._python_to_pg(python_value)
        assert result == expected_pg_string

    @pytest.mark.parametrize(
        "python_value",
        [
            object(),
            lambda x: x,
            type("TestClass", (), {}),
            complex(1, 2),
            {1, 2, 3},
        ],
    )
    def test_python_to_pg_invalid_types(self, mock_neon_client, python_value):
        """Test that invalid Python types raise appropriate errors."""
        with pytest.raises(PostgresAdaptationError):
            mock_neon_client._python_to_pg(python_value)

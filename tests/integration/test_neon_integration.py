import datetime as dt
import ipaddress
import os
import uuid
from decimal import Decimal

import pytest

from pyserverless.neon import Neon

INTEGRATION_DB_URL = os.getenv("INTEGRATION_DATABASE_URL")


@pytest.fixture(scope="module")
def mock_neon_client():
    """Mock Neon client."""
    if not INTEGRATION_DB_URL:
        pytest.fail("INTEGRATION_DATABASE_URL environment variable not set, failing integration tests.")
    return Neon(INTEGRATION_DB_URL)


class TestNeonIntegration:
    @pytest.mark.parametrize(
        ("col_definition", "value"),
        [
            ("INTEGER", 42),
            ("INTEGER", None),
            ("REAL", 2.625),
            ("REAL", None),
            ("NUMERIC", Decimal("123456.78")),
            ("NUMERIC", None),
            ("TEXT", "hello world"),
            ("TEXT", None),
            ("BOOLEAN", True),
            ("BOOLEAN", None),
            ("DATE", dt.datetime.now().date()),
            ("DATE", None),
            ("TIME", dt.datetime.now().time()),
            ("TIME", None),
            ("TIMESTAMP", dt.datetime.now()),
            ("TIMESTAMP", None),
            ("TIMESTAMPTZ", dt.datetime.now(dt.UTC)),
            ("TIMESTAMPTZ", None),
            ("INTERVAL", dt.timedelta(days=1, hours=2, minutes=30)),
            ("INTERVAL", None),
            ("BYTEA", bytes.fromhex("deadbeef")),
            ("BYTEA", None),
            ("JSONB", {"key": "value", "array": [1, 2, 3]}),
            ("JSONB", None),
            ("UUID", uuid.UUID("123e4567-e89b-12d3-a456-426614174000")),
            ("UUID", None),
            ("INET", ipaddress.IPv4Address("192.168.1.1")),
            ("INET", None),
            ("CIDR", ipaddress.IPv4Network("192.168.1.0/24")),
            ("CIDR", None),
            ("INTEGER[]", [1, 2, 3, 4, 5]),
            ("INTEGER[]", [1, None, 3, 4, 5]),
            ("TEXT[]", ["a", "b", "c"]),
            ("TEXT[]", ["a", None, "c"]),
            ("BOOLEAN[]", [True, None, False]),
            ("DATE[]", [dt.date(2024, 2, 27), None, dt.date(2024, 2, 28)]),
            ("TEXT[]", []),
        ],
    )
    def test_types(self, mock_neon_client: Neon, col_definition: str, value: any):
        """
        Create a temporary table with a single column of the given type,
        insert the value, select it back, and verify it matches the original value.
        """
        # unique table name based on type being tested
        # Replace non-alphanumeric characters with underscores.
        col_name = "".join(ch if ch.isalnum() else "_" for ch in col_definition)
        table_name = f"test_types_table_{col_name}"
        queries = [
            (f"DROP TABLE IF EXISTS {table_name};"),
            (f"CREATE TEMPORARY TABLE {table_name} (val {col_definition});"),
            (f"INSERT INTO {table_name} (val) VALUES ($1);", (value,)),  # noqa: S608
            (f"SELECT val FROM {table_name} LIMIT 1;"),  # noqa: S608
        ]
        results = mock_neon_client.transaction(queries)
        returned_value = results[-1][0]["val"]
        assert returned_value == value

    def test_integration(self, mock_neon_client):
        """
        Create a temporary table with all supported types, insert a row with all supported types,
        select the row back, and verify it matches the expected value.
        """
        query_row_values = (
            42,
            2.625,
            Decimal("123456.78"),
            "hello world",
            True,
            dt.datetime.now().date(),
            dt.datetime.now().time(),
            dt.datetime.now(),
            dt.datetime.now(dt.UTC),
            dt.timedelta(days=1, hours=2, minutes=30),
            bytes.fromhex("deadbeef"),
            {"key": "value", "array": [1, 2, 3]},
            uuid.UUID("123e4567-e89b-12d3-a456-426614174000"),
            ipaddress.IPv4Address("192.168.1.1"),
            ipaddress.IPv4Network("192.168.1.0/24"),
            [1, 2, 3, 4, 5],
        )

        results = mock_neon_client.transaction(
            [
                ("DROP TABLE IF EXISTS test_integration_table;"),
                (
                    """
                CREATE TEMPORARY TABLE test_integration_table (
                    id SERIAL PRIMARY KEY,
                    int_val INTEGER,
                    float_val REAL,
                    decimal_val NUMERIC,
                    text_val TEXT,
                    bool_val BOOLEAN,
                    date_val DATE,
                    time_val TIME,
                    ts_val TIMESTAMP,
                    tstz_val TIMESTAMPTZ,
                    interval_val INTERVAL,
                    bytea_val BYTEA,
                    json_val JSONB,
                    uuid_val UUID,
                    inet_val INET,
                    cidr_val CIDR,
                    array_int_val INTEGER[]
                );
                """
                ),
                (
                    """
                INSERT INTO test_integration_table (
                    int_val, float_val, decimal_val, text_val, bool_val, date_val, time_val,
                    ts_val, tstz_val, interval_val, bytea_val, json_val, uuid_val, inet_val, cidr_val, array_int_val
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                );
                """,
                    query_row_values,
                ),
                ("SELECT * FROM test_integration_table ORDER BY id ASC LIMIT 1;"),
            ]
        )
        returned_row = results[-1][0]

        assert returned_row["int_val"] == query_row_values[0]
        assert returned_row["float_val"] == query_row_values[1]
        assert returned_row["decimal_val"] == query_row_values[2]
        assert returned_row["text_val"] == query_row_values[3]
        assert returned_row["bool_val"] == query_row_values[4]
        assert returned_row["date_val"] == query_row_values[5]
        assert returned_row["time_val"] == query_row_values[6]
        assert returned_row["ts_val"] == query_row_values[7]
        assert returned_row["tstz_val"] == query_row_values[8]
        assert returned_row["interval_val"] == query_row_values[9]
        assert returned_row["bytea_val"] == query_row_values[10]
        assert returned_row["json_val"] == query_row_values[11]
        assert returned_row["uuid_val"] == query_row_values[12]
        assert returned_row["inet_val"] == query_row_values[13]
        assert returned_row["cidr_val"] == query_row_values[14]
        assert returned_row["array_int_val"] == query_row_values[15]

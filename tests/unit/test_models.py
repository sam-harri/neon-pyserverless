import pytest

from pyserverless.errors import TransactionConfigurationError
from pyserverless.models import IsolationLevel, NeonTransactionOptions


class TestNeonTransactionOptions:
    @pytest.mark.parametrize(
        ("isolation_level", "read_only"),
        [
            (IsolationLevel.READ_COMMITTED, True),
            (IsolationLevel.REPEATABLE_READ, True),
            (IsolationLevel.SERIALIZABLE, False),
        ],
    )
    def test_invalid_deferrable_configurations(self, isolation_level, read_only):
        """Test that invalid deferrable configurations raise appropriate errors."""
        with pytest.raises(TransactionConfigurationError):
            NeonTransactionOptions(isolation_level=isolation_level, read_only=read_only, deferrable=True)

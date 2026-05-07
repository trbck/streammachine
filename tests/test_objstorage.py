"""
Tests for streammachine.objstorage module.

Note: These tests require a running Redis server and the 'redis' package.
They are marked as integration tests and will be skipped if Redis is not available.
"""
import pytest

# Skip all tests in this module if redis is not installed
redis = pytest.importorskip("redis")

import numpy as np

from streammachine.objstorage.redisobjstore import RedisObjectStorage


def check_redis_available():
    """Check if Redis server is available."""
    import asyncio
    try:
        async def _check():
            client = redis.asyncio.Redis(host='localhost', port=6379)
            await client.ping()
            await client.aclose()
            return True
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_check())
        finally:
            loop.close()
    except Exception:
        return False


# Cache the result
_REDIS_AVAILABLE = check_redis_available()


@pytest.mark.skipif(not _REDIS_AVAILABLE, reason="Redis server not available at localhost:6379")
@pytest.mark.integration
class TestRedisObjectStorage:
    """Tests for RedisObjectStorage class - require running Redis server."""

    @pytest.fixture
    async def storage(self):
        """Create a storage instance for testing."""
        storage = RedisObjectStorage()
        yield storage
        try:
            await storage.close()
        except Exception:
            pass

    @pytest.fixture
    def stock(self):
        """Create a test stock object."""
        quotes = np.random.rand(1000, 10)
        return Stock(ticker="AAPL", company_name="Apple Inc.", quotes=quotes)

    @pytest.mark.asyncio
    async def test_store_and_retrieve(self, storage, stock):
        """Test storing and retrieving an object."""
        await storage.store_with_pickle('test:stock:AAPL', stock)
        retrieved = await storage.retrieve_with_pickle('test:stock:AAPL')

        assert retrieved is not None
        assert retrieved.ticker == stock.ticker
        assert retrieved.company_name == stock.company_name
        np.testing.assert_array_equal(retrieved.quotes, stock.quotes)

        # Cleanup
        try:
            await storage.delete_keys('test:stock:*')
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_list_keys(self, storage, stock):
        """Test listing keys."""
        await storage.store_with_pickle('test:stock:MSFT', stock)
        keys = await storage.list_keys('test:stock:*')
        assert 'test:stock:MSFT' in keys

        # Cleanup
        try:
            await storage.delete_keys('test:stock:*')
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_delete_keys(self, storage, stock):
        """Test deleting keys."""
        await storage.store_with_pickle('test:stock:GOOG', stock)
        await storage.delete_keys('test:stock:*')
        keys = await storage.list_keys('test:stock:*')
        assert 'test:stock:GOOG' not in keys

    @pytest.mark.asyncio
    async def test_retrieve_nonexistent(self, storage):
        """Test retrieving a nonexistent key."""
        result = await storage.retrieve_with_pickle('nonexistent:key')
        assert result is None


class Stock:
    """Test class for object storage tests."""
    def __init__(self, ticker, company_name, quotes):
        self.ticker = ticker
        self.company_name = company_name
        self.quotes = quotes

    def __repr__(self):
        return f"Stock(ticker={self.ticker}, company_name={self.company_name}, quotes=Array of shape {self.quotes.shape})"
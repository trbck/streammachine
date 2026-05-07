"""
Pytest configuration and fixtures for StreamMachine tests.
"""
import asyncio
import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from streammachine.storage import Storage


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def storage():
    """Provide a fresh Storage instance for each test."""
    # Reset singleton before each test
    Storage.reset_instance()
    s = Storage()
    s._ensure_manager()
    yield s
    # Cleanup after test
    Storage.reset_instance()


@pytest.fixture
def sample_message_data():
    """Sample message data for testing."""
    return {
        b'key1': b'value1',
        b'key2': b'value2',
        b'sent': b'1234567890.123',
    }
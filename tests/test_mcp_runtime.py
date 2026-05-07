"""Runtime tests for MCP handlers."""

import importlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("mcp")

mcp_fast_module = importlib.import_module("streammachine.mcp_fast")
mcp_server_module = importlib.import_module("streammachine.mcp_server")


def _response_payload(result) -> dict:
    """Extract the JSON payload from an MCP server TextContent list."""
    return json.loads(result[0].text)


@pytest.mark.asyncio
async def test_mcp_server_stream_read_uses_start_argument():
    """Test that stream_read threads the caller-provided start ID into Redis."""
    mock_redis = MagicMock()
    mock_redis._ensure_pool = AsyncMock()
    mock_redis.client = MagicMock()
    mock_redis.client.xrevrange = AsyncMock(return_value=[])

    with patch("streammachine.mcp_server.get_redis", AsyncMock(return_value=mock_redis)):
        result = await mcp_server_module.call_tool("stream_read", {"stream": "orders", "start": "5-0"})

    payload = _response_payload(result)
    assert payload["success"] is True
    mock_redis.client.xrevrange.assert_awaited_once_with("orders", "5-0", "-", count=10)


@pytest.mark.asyncio
async def test_mcp_server_stream_info_accepts_integer_group_count():
    """Test that stream_info accepts the normal XINFO STREAM integer group count."""
    mock_redis = MagicMock()
    mock_redis._ensure_pool = AsyncMock()
    mock_redis.client = MagicMock()
    mock_redis.client.xinfo_stream = AsyncMock(
        return_value={"length": 2, "groups": 3, "last-generated-id": "9-0"}
    )

    with patch("streammachine.mcp_server.get_redis", AsyncMock(return_value=mock_redis)):
        result = await mcp_server_module.call_tool("stream_info", {"stream": "orders"})

    payload = _response_payload(result)
    assert payload["data"]["groups"] == 3


@pytest.mark.asyncio
async def test_mcp_server_stream_list_accepts_byte_type_responses():
    """Test that stream_list recognizes byte-valued TYPE responses."""
    mock_redis = MagicMock()
    mock_redis._ensure_pool = AsyncMock()
    mock_redis.client = MagicMock()
    mock_redis.client.keys = AsyncMock(return_value=[b"orders", b"cache"])
    mock_redis.client.type = AsyncMock(side_effect=[b"stream", b"string"])

    with patch("streammachine.mcp_server.get_redis", AsyncMock(return_value=mock_redis)):
        result = await mcp_server_module.call_tool("stream_list", {"pattern": "*"})

    payload = _response_payload(result)
    assert payload["data"]["streams"] == ["orders"]


@pytest.mark.asyncio
async def test_mcp_server_obj_get_disabled_by_default():
    """Test that unsafe pickle deserialization is blocked unless explicitly enabled."""
    result = await mcp_server_module.call_tool("obj_get", {"key": "model"})
    payload = _response_payload(result)
    assert payload["success"] is False
    assert "disabled by default" in payload["error"]


@pytest.mark.asyncio
async def test_mcp_fast_stream_read_uses_start_argument():
    """Test that FastMCP stream_read honors its start parameter."""
    mock_redis = MagicMock()
    mock_redis._ensure_pool = AsyncMock()
    mock_redis.client = MagicMock()
    mock_redis.client.xrevrange = AsyncMock(return_value=[])

    with patch("streammachine.mcp_fast.get_redis", AsyncMock(return_value=mock_redis)):
        payload = json.loads(await mcp_fast_module.stream_read("orders", start="5-0"))

    assert payload["success"] is True
    mock_redis.client.xrevrange.assert_awaited_once_with("orders", "5-0", "-", count=10)


@pytest.mark.asyncio
async def test_mcp_fast_stream_info_accepts_integer_group_count():
    """Test that FastMCP stream_info accepts integer group counts."""
    mock_redis = MagicMock()
    mock_redis._ensure_pool = AsyncMock()
    mock_redis.client = MagicMock()
    mock_redis.client.xinfo_stream = AsyncMock(
        return_value={"length": 1, "groups": 4, "last-generated-id": "1-0"}
    )

    with patch("streammachine.mcp_fast.get_redis", AsyncMock(return_value=mock_redis)):
        payload = json.loads(await mcp_fast_module.stream_info("orders"))

    assert payload["data"]["groups"] == 4


@pytest.mark.asyncio
async def test_mcp_fast_stream_list_accepts_byte_type_responses():
    """Test that FastMCP stream_list recognizes byte-valued TYPE responses."""
    mock_redis = MagicMock()
    mock_redis._ensure_pool = AsyncMock()
    mock_redis.client = MagicMock()
    mock_redis.client.keys = AsyncMock(return_value=[b"orders", b"cache"])
    mock_redis.client.type = AsyncMock(side_effect=[b"stream", b"string"])

    with patch("streammachine.mcp_fast.get_redis", AsyncMock(return_value=mock_redis)):
        payload = json.loads(await mcp_fast_module.stream_list("*"))

    assert payload["data"]["streams"] == ["orders"]


@pytest.mark.asyncio
async def test_mcp_list_tools_omits_ohlc_when_unavailable():
    """Test that OHLC tools are not listed when FastOHLC is not installed."""
    with patch.object(mcp_server_module, "_HAS_OHLC", False):
        tools = await mcp_server_module.list_tools()
    tool_names = [t.name for t in tools]
    ohlc_tools = ["ohlc_create", "ohlc_update", "ohlc_get_candles", "ohlc_get_completed",
                  "ohlc_flush", "ohlc_clear", "ohlc_stats", "ohlc_list"]
    for tool_name in ohlc_tools:
        assert tool_name not in tool_names, f"OHLC tool {tool_name} should not be listed when _HAS_OHLC is False"


@pytest.mark.asyncio
async def test_mcp_list_tools_includes_ohlc_when_available():
    """Test that OHLC tools are listed when FastOHLC is installed."""
    with patch.object(mcp_server_module, "_HAS_OHLC", True):
        tools = await mcp_server_module.list_tools()
    tool_names = [t.name for t in tools]
    ohlc_tools = ["ohlc_create", "ohlc_update", "ohlc_get_candles", "ohlc_get_completed",
                  "ohlc_flush", "ohlc_clear", "ohlc_stats", "ohlc_list"]
    for tool_name in ohlc_tools:
        assert tool_name in tool_names, f"OHLC tool {tool_name} should be listed when _HAS_OHLC is True"

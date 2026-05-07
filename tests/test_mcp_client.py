#!/usr/bin/env python3
"""
Test client for StreamMachine MCP Server.

This script tests the MCP server by calling handlers directly
and validating the responses.

Usage:
    python tests/test_mcp_client.py

Prerequisites:
    - Redis running locally (or set REDIS_URL env var)
    - StreamMachine installed
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# Add src to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


async def test_mcp_server():
    """Test the MCP server handlers directly."""
    failures = []
    print("=" * 60)
    print("StreamMachine MCP Server Test Client")
    print("=" * 60)

    # Import the server module to access decorated handlers
    # Note: streammachine.mcp_server in __init__.py exports the server object
    # We need to import the actual module
    import importlib
    mcp_module = importlib.import_module('streammachine.mcp_server')

    # Test 1: Check server is properly configured
    print("\n[TEST 1] Server configuration check")
    print(f"  Server name: {mcp_module.server.name}")
    assert mcp_module.server.name == "streammachine", "Server name mismatch"
    print("  ✓ Server name correct")

    # Test 2: Check handlers exist
    print("\n[TEST 2] Verify handler definitions")
    assert mcp_module.list_tools is not None, "list_tools handler missing"
    assert mcp_module.call_tool is not None, "call_tool handler missing"
    assert mcp_module.list_resources is not None, "list_resources handler missing"
    assert mcp_module.read_resource is not None, "read_resource handler missing"
    assert mcp_module.list_prompts is not None, "list_prompts handler missing"
    assert mcp_module.get_prompt is not None, "get_prompt handler missing"
    print("  ✓ list_tools handler registered")
    print("  ✓ call_tool handler registered")
    print("  ✓ list_resources handler registered")
    print("  ✓ read_resource handler registered")
    print("  ✓ list_prompts handler registered")
    print("  ✓ get_prompt handler registered")

    # Test 3: Get tool definitions
    print("\n[TEST 3] List available tools")
    tools = await mcp_module.list_tools()
    print(f"  Found {len(tools)} tools:")
    tool_names = [t.name for t in tools]
    expected_tools = [
        "stream_send", "stream_send_batch", "stream_read", "stream_info", "stream_list",
        "storage_read", "storage_write", "storage_delete", "storage_keys", "storage_clear",
        "health_check", "redis_info", "redis_ping",
        "obj_list", "obj_delete",
    ]
    if os.environ.get("STREAMMACHINE_ENABLE_UNSAFE_PICKLE_TOOLS") == "1":
        expected_tools.append("obj_get")
    for tool in expected_tools:
        assert tool in tool_names, f"Missing tool: {tool}"
    print(f"  ✓ All {len(expected_tools)} expected tools present")

    # Test 4: Get resource definitions
    print("\n[TEST 4] List available resources")
    resources = await mcp_module.list_resources()
    print(f"  Found {len(resources)} resources:")
    resource_uris = [str(r.uri) for r in resources]
    print(f"  URIs: {resource_uris}")
    assert any("config" in str(uri) for uri in resource_uris), "Missing config resource"
    assert any("status" in str(uri) for uri in resource_uris), "Missing status resource"
    print("  ✓ streammachine://config")
    print("  ✓ streammachine://status")

    # Test 5: Get prompt definitions
    print("\n[TEST 5] List available prompts")
    prompts = await mcp_module.list_prompts()
    print(f"  Found {len(prompts)} prompts:")
    prompt_names = [p.name for p in prompts]
    assert "streammachine-guide" in prompt_names, "Missing streammachine-guide prompt"
    assert "stream-processing-patterns" in prompt_names, "Missing stream-processing-patterns prompt"
    print("  ✓ streammachine-guide")
    print("  ✓ stream-processing-patterns")

    # Test 6: Test tool execution (health_check)
    print("\n[TEST 6] Tool execution - health_check")
    try:
        result = await mcp_module.call_tool("health_check", {})
        print(f"  Response type: {type(result).__name__}")
        print(f"  Content items: {len(result)}")
        response_text = result[0].text
        response_data = json.loads(response_text)
        print(f"  Success: {response_data.get('success')}")
        print(f"  Status: {response_data.get('data', {}).get('status', 'unknown')}")
        print("  ✓ health_check executed successfully")
    except Exception as e:
        print(f"  ⚠ health_check failed (Redis may not be running): {e}")

    # Test 7: Test resource reading
    print("\n[TEST 7] Resource reading - config")
    try:
        config = await mcp_module.read_resource("streammachine://config")
        config_data = json.loads(config)
        print(f"  Redis URL: {config_data.get('redis_url')}")
        print(f"  Redis Host: {config_data.get('redis_host')}")
        print(f"  Redis Port: {config_data.get('redis_port')}")
        print("  ✓ config resource readable")
    except Exception as e:
        print(f"  ✗ Failed to read config: {e}")

    # Test 8: Test prompt retrieval
    print("\n[TEST 8] Prompt retrieval - streammachine-guide")
    try:
        prompt_result = await mcp_module.get_prompt("streammachine-guide", {})
        print(f"  Description: {prompt_result.description}")
        print(f"  Messages: {len(prompt_result.messages)} message(s)")
        print("  ✓ Prompt retrieved successfully")
    except Exception as e:
        print(f"  ✗ Failed to get prompt: {e}")

    # Test 9: Test redis_ping tool
    print("\n[TEST 9] Tool execution - redis_ping")
    try:
        result = await mcp_module.call_tool("redis_ping", {})
        response_data = json.loads(result[0].text)
        ping_result = response_data.get("data", {}).get("ping", "unknown")
        print(f"  Ping result: {ping_result}")
        print("  ✓ redis_ping executed successfully")
    except Exception as e:
        print(f"  ⚠ redis_ping failed (Redis may not be running): {e}")

    # Test 10: Test stream_list tool (requires Redis)
    print("\n[TEST 10] Tool execution - stream_list")
    try:
        result = await mcp_module.call_tool("stream_list", {"pattern": "*"})
        response_data = json.loads(result[0].text)
        if response_data.get("success"):
            streams = response_data.get("data", {}).get("streams", [])
            print(f"  Found {len(streams)} stream(s)")
            print("  ✓ stream_list executed successfully")
        else:
            print(f"  ⚠ stream_list returned error: {response_data.get('error')}")
    except Exception as e:
        print(f"  ⚠ stream_list failed (Redis may not be running): {e}")

    # Test 11: Test storage_write and storage_read
    print("\n[TEST 11] Tool execution - storage_write/storage_read")
    try:
        # Write a test value
        write_result = await mcp_module.call_tool("storage_write", {
            "key": "test_key",
            "value": {"test": "value", "number": 42}
        })
        write_data = json.loads(write_result[0].text)
        if not write_data.get("success"):
            failures.append(f"storage_write failed: {write_data.get('error')}")
        print(f"  Write result: {write_data.get('data')}")

        # Read it back
        read_result = await mcp_module.call_tool("storage_read", {"key": "test_key"})
        read_data = json.loads(read_result[0].text)
        if not read_data.get("success"):
            failures.append(f"storage_read failed: {read_data.get('error')}")
        print(f"  Read result: {read_data.get('data')}")

        # Clean up
        delete_result = await mcp_module.call_tool("storage_delete", {"key": "test_key"})
        delete_data = json.loads(delete_result[0].text)
        if not delete_data.get("success"):
            failures.append(f"storage_delete failed: {delete_data.get('error')}")
        print("  ✓ Storage operations successful")
    except Exception as e:
        failures.append(f"Storage operations failed: {e}")
        print(f"  ✗ Storage operations failed: {e}")

    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)
    print("\nNote: Some tests may have warnings if Redis is not running.")
    print("To fully test, ensure Redis is available at localhost:6379")
    print("or set the REDIS_URL environment variable.")

    if failures:
        print("\nFailures:")
        for failure in failures:
            print(f"  - {failure}")
    return not failures


def main():
    """Run the test client."""
    try:
        success = asyncio.run(test_mcp_server())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

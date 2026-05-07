"""
StreamMachine CLI entry point.

Allows running StreamMachine as a module:
    python -m streammachine [command]

Commands:
    mcp         Run the MCP server (default)
    --help      Show help
"""

import sys


def _is_missing_mcp_dependency(exc: ImportError) -> bool:
    """Return True when the import error comes from the optional MCP package."""
    missing_name = getattr(exc, "name", None)
    return missing_name == "mcp" or (missing_name is not None and missing_name.startswith("mcp."))


def main():
    """CLI entry point."""
    args = sys.argv[1:]

    if not args or args[0] == "mcp":
        # Run MCP server
        try:
            from .mcp_server import main as mcp_main
        except ImportError as exc:
            if not _is_missing_mcp_dependency(exc):
                raise
            print(
                "MCP support is not installed. Install with `pip install streammachine[mcp]`.",
                file=sys.stderr,
            )
            raise SystemExit(1) from exc
        mcp_main()
    elif args[0] == "--help" or args[0] == "-h":
        print("StreamMachine CLI")
        print()
        print("Usage:")
        print("    python -m streammachine [command]")
        print()
        print("Commands:")
        print("    mcp         Run the MCP server (default)")
        print("    --help      Show this help")
        print()
        print("MCP Server:")
        print("    The MCP server should be run by an MCP client (like Claude Desktop).")
        print("    Direct terminal usage will result in JSON parsing errors.")
        print()
        print("Claude Desktop config (~/.config/claude/claude_desktop_config.json):")
        print('    {')
        print('      "mcpServers": {')
        print('        "streammachine": {')
        print('          "command": "python",')
        print('          "args": ["-m", "streammachine", "mcp"]')
        print('        }')
        print('      }')
        print('    }')
    else:
        print(f"Unknown command: {args[0]}", file=sys.stderr)
        print("Use --help for usage information.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

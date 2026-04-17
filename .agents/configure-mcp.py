#!/usr/bin/env python3
"""Add MSC MCP server entry to a tool's MCP config file if not already present."""

import json
import os
import sys


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <config-file> <msc-command>", file=sys.stderr)
        sys.exit(1)

    config_file = sys.argv[1]
    msc_cmd = sys.argv[2]

    try:
        with open(config_file) as f:
            config = json.load(f)
    except FileNotFoundError:
        config = {}
    except (json.JSONDecodeError, ValueError) as e:
        print(f"ERROR: Failed to parse {config_file}: {e}", file=sys.stderr)
        sys.exit(1)

    servers = config.setdefault("mcpServers", {})

    if "Multi-Storage Client" in servers:
        print(f"  MCP: Multi-Storage Client already configured in {config_file}")
        return

    servers["Multi-Storage Client"] = {
        "command": msc_cmd,
        "args": ["mcp-server", "start"],
    }

    try:
        os.makedirs(os.path.dirname(config_file) or ".", exist_ok=True)
        with open(config_file, "w") as f:
            json.dump(config, f, indent=2)
            f.write("\n")
    except OSError as e:
        print(f"ERROR: Failed to write {config_file}: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  MCP: added Multi-Storage Client to {config_file}")


if __name__ == "__main__":
    main()

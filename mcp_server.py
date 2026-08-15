#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable

from kb_agents import build_agent_system
from mcp_runtime import McpStdioServer, ToolSpec


BASE_DIR = Path(__file__).resolve().parent
SERVER_NAME = "kb-realestate-weekly-automation"
SERVER_VERSION = "0.3.0"

AGENT_SYSTEM = build_agent_system(BASE_DIR)
TOOL_SPECS = AGENT_SYSTEM.tool_specs()
TOOL_HANDLERS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    tool.name: tool.handler for tool in TOOL_SPECS
}


def list_tools() -> list[dict[str, Any]]:
    """Compatibility helper for clients that inspected the old module directly."""
    return [tool.as_mcp_tool() for tool in TOOL_SPECS]


def main() -> int:
    os.chdir(BASE_DIR)
    return McpStdioServer(
        name=SERVER_NAME,
        version=SERVER_VERSION,
        tools=[
            ToolSpec(
                name=tool.name,
                description=tool.description,
                input_schema=tool.input_schema,
                handler=tool.handler,
            )
            for tool in TOOL_SPECS
        ],
    ).run()


if __name__ == "__main__":
    raise SystemExit(main())

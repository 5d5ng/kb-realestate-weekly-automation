from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


SUPPORTED_PROTOCOL_VERSIONS = {"2024-11-05", "2025-03-26", "2025-06-18"}
ToolHandler = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: ToolHandler

    def as_mcp_tool(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
        }


class McpProtocolError(Exception):
    def __init__(self, code: int, message: str, data: Any | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def tool_result_text(text: str, *, is_error: bool = False) -> dict[str, Any]:
    return {
        "content": [{"type": "text", "text": text}],
        "isError": is_error,
    }


def tool_result_json(payload: Any, *, is_error: bool = False) -> dict[str, Any]:
    text = json.dumps(json_safe(payload), ensure_ascii=False, indent=2)
    return tool_result_text(text, is_error=is_error)


def empty_input_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }


class McpStdioServer:
    def __init__(self, *, name: str, version: str, tools: list[ToolSpec]) -> None:
        self.name = name
        self.version = version
        self.tools = {tool.name: tool for tool in tools}

    def _read_message(self, stdin: Any) -> dict[str, Any] | None:
        while True:
            first_line = stdin.readline()
            if not first_line:
                return None
            if first_line in {b"\r\n", b"\n"}:
                continue
            break

        if first_line.lstrip().startswith(b"{"):
            return json.loads(first_line.decode("utf-8"))

        headers: dict[str, str] = {}
        line = first_line
        while line not in {b"\r\n", b"\n", b""}:
            decoded = line.decode("ascii", errors="replace").strip()
            if ":" in decoded:
                key, value = decoded.split(":", 1)
                headers[key.lower()] = value.strip()
            line = stdin.readline()

        if "content-length" not in headers:
            raise McpProtocolError(-32700, "Missing Content-Length header")

        try:
            content_length = int(headers["content-length"])
        except ValueError as exc:
            raise McpProtocolError(-32700, "Invalid Content-Length header") from exc

        body = stdin.read(content_length)
        if len(body) != content_length:
            raise McpProtocolError(-32700, "Unexpected EOF while reading message body")
        return json.loads(body.decode("utf-8"))

    @staticmethod
    def _send_message(stdout: Any, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        stdout.write(f"Content-Length: {len(body)}\r\n\r\n".encode("ascii"))
        stdout.write(body)
        stdout.flush()

    @staticmethod
    def _response(message_id: Any, result: Any) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": message_id, "result": result}

    @staticmethod
    def _error(message_id: Any, code: int, message: str, data: Any | None = None) -> dict[str, Any]:
        error_payload: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            error_payload["data"] = data
        return {"jsonrpc": "2.0", "id": message_id, "error": error_payload}

    def _handle_initialize(self, message_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        requested = params.get("protocolVersion")
        protocol_version = requested if requested in SUPPORTED_PROTOCOL_VERSIONS else "2024-11-05"
        return self._response(
            message_id,
            {
                "protocolVersion": protocol_version,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": self.name, "version": self.version},
            },
        )

    def _handle_tool_call(self, message_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, dict):
            return self._response(
                message_id,
                tool_result_json({"success": False, "error": "arguments must be an object"}, is_error=True),
            )
        tool = self.tools.get(str(name))
        if tool is None:
            return self._response(
                message_id,
                tool_result_json({"success": False, "error": f"Unknown tool: {name}"}, is_error=True),
            )

        try:
            result = tool.handler(arguments)
        except Exception as exc:
            print(f"[mcp tool error] {name}: {exc}", file=sys.stderr, flush=True)
            result = tool_result_json(
                {"success": False, "tool": name, "error": str(exc)},
                is_error=True,
            )
        return self._response(message_id, result)

    def handle_request(self, message: dict[str, Any]) -> dict[str, Any] | None:
        message_id = message.get("id")
        method = message.get("method")
        params = message.get("params") or {}
        if not isinstance(params, dict):
            return self._error(message_id, -32602, "params must be an object")

        if message_id is None and isinstance(method, str) and method.startswith("notifications/"):
            return None
        if method == "initialize":
            return self._handle_initialize(message_id, params)
        if method == "ping":
            return self._response(message_id, {})
        if method == "tools/list":
            return self._response(
                message_id,
                {"tools": [tool.as_mcp_tool() for tool in self.tools.values()]},
            )
        if method == "tools/call":
            return self._handle_tool_call(message_id, params)
        if method in {"resources/list", "prompts/list"}:
            return self._response(message_id, {method.split("/")[0]: []})
        if message_id is None:
            return None
        return self._error(message_id, -32601, f"Method not found: {method}")

    def run(self) -> int:
        while True:
            try:
                message = self._read_message(sys.stdin.buffer)
                if message is None:
                    return 0
                response = self.handle_request(message)
                if response is not None:
                    self._send_message(sys.stdout.buffer, response)
            except McpProtocolError as exc:
                self._send_message(sys.stdout.buffer, self._error(None, exc.code, exc.message, exc.data))
            except json.JSONDecodeError as exc:
                self._send_message(sys.stdout.buffer, self._error(None, -32700, f"Parse error: {exc}"))
            except KeyboardInterrupt:
                return 0
            except Exception as exc:
                print(f"[mcp server error] {exc}", file=sys.stderr, flush=True)
                self._send_message(sys.stdout.buffer, self._error(None, -32603, f"Internal error: {exc}"))

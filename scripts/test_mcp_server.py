#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import select
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent.parent
SERVER_PATH = BASE_DIR / "mcp_server.py"


class McpTestError(RuntimeError):
    pass


def _read_exact(fd: int, length: int, deadline: float) -> bytes:
    chunks: list[bytes] = []
    remaining_length = length
    while remaining_length > 0:
        remaining_time = deadline - time.monotonic()
        if remaining_time <= 0:
            raise TimeoutError(f"Timed out while reading {length} bytes")
        readable, _, _ = select.select([fd], [], [], remaining_time)
        if not readable:
            continue
        chunk = os.read(fd, remaining_length)
        if not chunk:
            raise EOFError("MCP server closed stdout")
        chunks.append(chunk)
        remaining_length -= len(chunk)
    return b"".join(chunks)


def _readline(fd: int, deadline: float) -> bytes:
    chunks: list[bytes] = []
    while True:
        char = _read_exact(fd, 1, deadline)
        chunks.append(char)
        if char == b"\n":
            return b"".join(chunks)


def read_message(proc: subprocess.Popen[bytes], timeout_sec: float = 30.0) -> dict[str, Any]:
    if proc.stdout is None:
        raise McpTestError("stdout pipe is not available")

    deadline = time.monotonic() + timeout_sec
    fd = proc.stdout.fileno()
    headers: dict[str, str] = {}

    while True:
        line = _readline(fd, deadline)
        if line in {b"\r\n", b"\n"}:
            break
        decoded = line.decode("ascii", errors="replace").strip()
        if ":" not in decoded:
            raise McpTestError(f"Invalid header line: {decoded!r}")
        key, value = decoded.split(":", 1)
        headers[key.lower()] = value.strip()

    if "content-length" not in headers:
        raise McpTestError(f"Missing Content-Length header: {headers}")

    body = _read_exact(fd, int(headers["content-length"]), deadline)
    return json.loads(body.decode("utf-8"))


def send_message(proc: subprocess.Popen[bytes], payload: dict[str, Any]) -> None:
    if proc.stdin is None:
        raise McpTestError("stdin pipe is not available")

    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    proc.stdin.write(f"Content-Length: {len(body)}\r\n\r\n".encode("ascii"))
    proc.stdin.write(body)
    proc.stdin.flush()


def require_ok_response(response: dict[str, Any], expected_id: int) -> dict[str, Any]:
    if response.get("id") != expected_id:
        raise McpTestError(f"Unexpected response id: {response}")
    if "error" in response:
        raise McpTestError(f"MCP response error: {response['error']}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise McpTestError(f"Missing result object: {response}")
    return result


def call_tool(
    proc: subprocess.Popen[bytes],
    request_id: int,
    name: str,
    arguments: dict[str, Any] | None = None,
    *,
    timeout_sec: float = 30.0,
) -> dict[str, Any]:
    send_message(
        proc,
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments or {},
            },
        },
    )
    result = require_ok_response(read_message(proc, timeout_sec=timeout_sec), request_id)
    if result.get("isError"):
        text = result.get("content", [{}])[0].get("text", "")
        raise McpTestError(f"Tool {name} returned an error: {text}")
    return result


def tool_text(result: dict[str, Any]) -> str:
    content = result.get("content")
    if not isinstance(content, list) or not content:
        raise McpTestError(f"Tool result has no content: {result}")
    first = content[0]
    if first.get("type") != "text":
        raise McpTestError(f"Tool result is not text: {result}")
    return str(first.get("text", ""))


def tool_json(result: dict[str, Any]) -> dict[str, Any]:
    text = tool_text(result)
    json_start = text.find("{")
    if json_start < 0:
        raise McpTestError(f"Tool text has no JSON object: {text[:200]}")
    return json.loads(text[json_start:])


def run_test(*, run_pipeline: bool) -> None:
    proc = subprocess.Popen(
        [sys.executable, str(SERVER_PATH)],
        cwd=BASE_DIR,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
    )
    try:
        send_message(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "kb-mcp-smoke-test",
                        "version": "0.1.0",
                    },
                },
            },
        )
        init_result = require_ok_response(read_message(proc), 1)
        if init_result.get("serverInfo", {}).get("name") != "kb-realestate-weekly-automation":
            raise McpTestError(f"Unexpected server info: {init_result}")

        send_message(proc, {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})

        send_message(proc, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
        tools_result = require_ok_response(read_message(proc), 2)
        tool_names = {tool.get("name") for tool in tools_result.get("tools", [])}
        required_tools = {
            "kb_help",
            "kb_realestate_assistant",
            "generate_authoring_package",
            "generate_weekly_report",
            "get_latest_package",
            "get_latest_weekly_report",
            "get_data_snapshot",
            "get_latest_news",
            "get_latest_transactions",
            "check_latest_artifacts",
            "list_artifacts",
            "get_agent_status",
        }
        missing_tools = sorted(required_tools - tool_names)
        if missing_tools:
            raise McpTestError(f"Missing MCP tools: {missing_tools}")

        help_result = call_tool(proc, 3, "kb_help")
        help_text = tool_text(help_result)
        if "KB부동산 MCP 사용법" not in help_text:
            raise McpTestError("kb_help did not return the Korean help text")

        artifacts_result = call_tool(
            proc,
            4,
            "kb_realestate_assistant",
            {"request": "생성된 파일 목록 보여줘"},
        )
        artifacts_payload = tool_json(artifacts_result)
        if not artifacts_payload.get("success"):
            raise McpTestError(f"kb_realestate_assistant list_files did not return success: {artifacts_payload}")

        status_result = call_tool(
            proc,
            5,
            "kb_realestate_assistant",
            {"request": "에이전트 상태 보여줘"},
        )
        status_payload = tool_json(status_result)
        if status_payload.get("agent_count") != 8:
            raise McpTestError(f"unexpected agent status: {status_payload}")
        if status_payload.get("safety_defaults", {}).get("llm_api_enabled"):
            raise McpTestError(f"LLM API safety default is not disabled: {status_payload}")

        if run_pipeline:
            package_result = call_tool(
                proc,
                6,
                "kb_realestate_assistant",
                {
                    "request": "KB부동산 최신 작성 패키지 만들어줘. 빠르게 실행하고 실거래는 생략해.",
                    "news_days": 1,
                    "news_max_articles": 3,
                    "skip_transactions": True,
                    "transaction_limit": 1,
                },
                timeout_sec=180.0,
            )
            package_payload = tool_json(package_result)
            if not package_payload.get("success"):
                raise McpTestError(f"generate_authoring_package failed: {package_payload}")
            if package_payload.get("llm_api_enabled"):
                raise McpTestError(f"LLM API was unexpectedly enabled: {package_payload}")

            latest_result = call_tool(proc, 7, "get_latest_package")
            latest_text = tool_text(latest_result)
            if "Claude/GPT 작성 패키지" not in latest_text:
                raise McpTestError("latest package content did not look like an authoring package")

            quality_result = call_tool(
                proc,
                8,
                "check_latest_artifacts",
                {"require_weekly_report": False},
            )
            quality_payload = tool_json(quality_result)
            if not quality_payload.get("ready_for_llm"):
                raise McpTestError(f"quality agent rejected generated artifacts: {quality_payload}")

            news_result = call_tool(proc, 9, "get_latest_news", {"limit": 2})
            news_payload = tool_json(news_result)
            if not news_payload.get("success") or not isinstance(news_payload.get("items"), list):
                raise McpTestError(f"news agent returned an invalid payload: {news_payload}")

            transactions_result = call_tool(proc, 10, "get_latest_transactions")
            transactions_payload = tool_json(transactions_result)
            if not transactions_payload.get("success"):
                raise McpTestError(f"transaction agent returned an invalid payload: {transactions_payload}")

        print(
            json.dumps(
                {
                    "success": True,
                    "server": init_result.get("serverInfo"),
                    "tool_count": len(tool_names),
                    "pipeline_tested": run_pipeline,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        if proc.stdin is not None:
            proc.stdin.close()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the local MCP stdio server.")
    parser.add_argument(
        "--run-pipeline",
        action="store_true",
        help="Also call generate_authoring_package through MCP. This uses network data sources but no LLM API and no send.",
    )
    args = parser.parse_args()

    run_test(run_pipeline=args.run_pipeline)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

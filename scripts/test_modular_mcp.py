#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from test_mcp_server import call_tool, read_message, require_ok_response, send_message, tool_json


BASE_DIR = Path(__file__).resolve().parent.parent
CONTENT_SERVER = BASE_DIR / "mcp_servers" / "content_package_server.py"
INSTAGRAM_SERVER = BASE_DIR / "mcp_servers" / "instagram_publisher_server.py"


def _start_server(path: Path, env: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, str(path)],
        cwd=BASE_DIR,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
    )


def _initialize(
    proc: subprocess.Popen[bytes],
    *,
    expected_server: str,
    required_tools: set[str],
) -> dict[str, Any]:
    send_message(
        proc,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "modular-mcp-smoke-test", "version": "1.0.0"},
            },
        },
    )
    initialized = require_ok_response(read_message(proc), 1)
    if initialized.get("serverInfo", {}).get("name") != expected_server:
        raise RuntimeError(f"unexpected server info: {initialized}")
    send_message(proc, {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
    send_message(proc, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    tools_result = require_ok_response(read_message(proc), 2)
    tool_names = {tool.get("name") for tool in tools_result.get("tools", [])}
    missing = sorted(required_tools - tool_names)
    if missing:
        raise RuntimeError(f"missing tools from {expected_server}: {missing}")
    return initialized


def _stop_server(proc: subprocess.Popen[bytes]) -> None:
    if proc.stdin is not None:
        proc.stdin.close()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.terminate()
        proc.wait(timeout=5)


def run_test() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as temp_dir:
        env = os.environ.copy()
        env["CONTENT_PACKAGE_DIR"] = str(Path(temp_dir) / "content_packages")
        env["PUBLISHING_PLAN_DIR"] = str(Path(temp_dir) / "publishing_plans")
        env["INSTAGRAM_PUBLISHING_ENABLED"] = "0"

        content_proc = _start_server(CONTENT_SERVER, env)
        try:
            _initialize(
                content_proc,
                expected_server="content-package-core",
                required_tools={
                    "content_help",
                    "create_content_package",
                    "validate_content_package",
                    "get_content_package",
                    "list_content_packages",
                },
            )
            create_result = call_tool(
                content_proc,
                3,
                "create_content_package",
                {
                    "title": "범용 여행 카드뉴스",
                    "content_type": "carousel",
                    "caption": "부동산과 무관한 콘텐츠도 같은 규격을 사용합니다.",
                    "media": [
                        {"type": "image", "source": "https://example.com/01.png"},
                        {"type": "image", "source": "https://example.com/02.png"},
                    ],
                    "targets": ["instagram"],
                    "metadata": {"source_project": "travel-content"},
                },
            )
            content_payload = tool_json(create_result)
            package = content_payload["package"]
            validation_result = call_tool(
                content_proc,
                4,
                "validate_content_package",
                {"package_id": package["package_id"], "target": "instagram"},
            )
            if not tool_json(validation_result)["validation"]["valid"]:
                raise RuntimeError("generic package did not validate for Instagram")
        finally:
            _stop_server(content_proc)

        publisher_proc = _start_server(INSTAGRAM_SERVER, env)
        try:
            _initialize(
                publisher_proc,
                expected_server="instagram-content-publisher",
                required_tools={
                    "instagram_help",
                    "instagram_connection_status",
                    "prepare_instagram_publish",
                    "approve_instagram_publish",
                    "get_instagram_publish_plan",
                    "list_instagram_publish_plans",
                    "publish_instagram_plan",
                },
            )
            prepare_result = call_tool(
                publisher_proc,
                3,
                "prepare_instagram_publish",
                {"package_id": package["package_id"]},
            )
            plan = tool_json(prepare_result)["plan"]
            approval_result = call_tool(
                publisher_proc,
                4,
                "approve_instagram_publish",
                {
                    "plan_id": plan["plan_id"],
                    "content_digest": package["content_digest"],
                    "confirmation": "이 콘텐츠 게시를 승인합니다",
                },
            )
            approved = tool_json(approval_result)["plan"]
            if approved["status"] != "approved":
                raise RuntimeError(f"unexpected approval state: {approved}")
        finally:
            _stop_server(publisher_proc)

    return {
        "success": True,
        "content_server": "content-package-core",
        "publisher_server": "instagram-content-publisher",
        "package_id": package["package_id"],
        "plan_id": plan["plan_id"],
        "external_publish_performed": False,
    }


def main() -> int:
    print(json.dumps(run_test(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

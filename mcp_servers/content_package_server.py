#!/usr/bin/env python3
"""Standalone MCP server for the generic content-package contract."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.example", override=False)

from content_core import ContentPackageStore, build_content_package, validate_content_package
from mcp_runtime import McpStdioServer, ToolSpec, empty_input_schema, tool_result_json, tool_result_text


SERVER_NAME = "content-package-core"
SERVER_VERSION = "1.0.0"


def _store() -> ContentPackageStore:
    return ContentPackageStore()


def tool_content_help(args: dict[str, Any]) -> dict[str, Any]:
    del args
    return tool_result_text(
        "\n".join(
            [
                "# 범용 콘텐츠 패키지 MCP",
                "",
                "주제와 프로젝트에 상관없이 게시용 콘텐츠를 동일한 JSON 계약으로 저장합니다.",
                "",
                "- create_content_package: 캡션과 미디어를 범용 패키지로 저장",
                "- validate_content_package: 대상 채널 규격 검증",
                "- get_content_package: 패키지 전체 조회",
                "- list_content_packages: 최근 패키지 목록 조회",
                "",
                "이 MCP는 외부 게시를 수행하지 않습니다.",
            ]
        )
    )


def tool_create_content_package(args: dict[str, Any]) -> dict[str, Any]:
    payload = build_content_package(
        title=str(args.get("title") or ""),
        content_type=str(args.get("content_type") or ""),
        caption=str(args.get("caption") or ""),
        media=args.get("media") or [],
        targets=args.get("targets") or [],
        metadata=args.get("metadata") or {},
    )
    path = _store().save(payload)
    return tool_result_json({"success": True, "package": payload, "path": str(path)})


def tool_validate_content_package(args: dict[str, Any]) -> dict[str, Any]:
    payload = _store().get(str(args.get("package_id") or ""))
    target = str(args.get("target") or "").strip() or None
    validation = validate_content_package(payload, target=target)
    return tool_result_json(
        {
            "success": validation["valid"],
            "package_id": payload.get("package_id"),
            "validation": validation,
        },
        is_error=not validation["valid"],
    )


def tool_get_content_package(args: dict[str, Any]) -> dict[str, Any]:
    payload = _store().get(str(args.get("package_id") or ""))
    return tool_result_json({"success": True, "package": payload})


def tool_list_content_packages(args: dict[str, Any]) -> dict[str, Any]:
    raw_limit = args.get("limit", 50)
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    return tool_result_json(
        {
            "success": True,
            "content_package_dir": str(_store().root),
            "packages": _store().list(limit=limit),
        }
    )


def _package_id_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "description": "create_content_package가 반환한 pkg_ 형식의 ID",
    }


def build_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="content_help",
            description="범용 콘텐츠 패키지 MCP의 한국어 사용법을 보여줍니다.",
            input_schema=empty_input_schema(),
            handler=tool_content_help,
        ),
        ToolSpec(
            name="create_content_package",
            description="주제와 무관한 게시용 캡션·미디어를 재사용 가능한 콘텐츠 패키지로 저장합니다.",
            input_schema={
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "content_type": {
                        "type": "string",
                        "enum": ["single_image", "carousel", "video", "text"],
                    },
                    "caption": {"type": "string", "default": ""},
                    "media": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "enum": ["image", "video"]},
                                "source": {"type": "string"},
                                "alt_text": {"type": "string", "default": ""},
                            },
                            "required": ["source"],
                            "additionalProperties": False,
                        },
                        "default": [],
                    },
                    "targets": {
                        "type": "array",
                        "items": {"type": "string"},
                        "default": [],
                    },
                    "metadata": {"type": "object", "default": {}},
                },
                "required": ["title", "content_type"],
                "additionalProperties": False,
            },
            handler=tool_create_content_package,
        ),
        ToolSpec(
            name="validate_content_package",
            description="저장된 콘텐츠 패키지를 공통 규격 또는 지정 채널 규격으로 검증합니다.",
            input_schema={
                "type": "object",
                "properties": {
                    "package_id": _package_id_schema(),
                    "target": {"type": "string", "enum": ["instagram"]},
                },
                "required": ["package_id"],
                "additionalProperties": False,
            },
            handler=tool_validate_content_package,
        ),
        ToolSpec(
            name="get_content_package",
            description="pkg_ ID로 저장된 범용 콘텐츠 패키지를 읽습니다.",
            input_schema={
                "type": "object",
                "properties": {"package_id": _package_id_schema()},
                "required": ["package_id"],
                "additionalProperties": False,
            },
            handler=tool_get_content_package,
        ),
        ToolSpec(
            name="list_content_packages",
            description="최근 생성된 범용 콘텐츠 패키지 목록을 보여줍니다.",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 50,
                    }
                },
                "additionalProperties": False,
            },
            handler=tool_list_content_packages,
        ),
    ]


def main() -> int:
    os.chdir(BASE_DIR)
    return McpStdioServer(
        name=SERVER_NAME,
        version=SERVER_VERSION,
        tools=build_tools(),
    ).run()


if __name__ == "__main__":
    raise SystemExit(main())

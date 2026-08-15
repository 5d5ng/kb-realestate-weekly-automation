#!/usr/bin/env python3
"""Standalone approval-gated Instagram publishing MCP server."""

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

from content_core import ContentPackageStore, validate_content_package
from mcp_runtime import McpStdioServer, ToolSpec, empty_input_schema, tool_result_json, tool_result_text
from publishing.instagram import (
    InstagramConfig,
    InstagramPublisher,
    instagram_account_status,
    list_instagram_account_aliases,
    resolve_instagram_account_alias,
)
from publishing.plans import PublishingPlanStore


SERVER_NAME = "instagram-content-publisher"
SERVER_VERSION = "1.0.0"
APPROVAL_CONFIRMATION = "이 콘텐츠 게시를 승인합니다"
PUBLISH_CONFIRMATION = "승인된 콘텐츠를 인스타그램에 게시합니다"


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _content_store() -> ContentPackageStore:
    return ContentPackageStore()


def _plan_store() -> PublishingPlanStore:
    return PublishingPlanStore()


def tool_instagram_help(args: dict[str, Any]) -> dict[str, Any]:
    del args
    return tool_result_text(
        "\n".join(
            [
                "# 인스타그램 게시 MCP",
                "",
                "이 MCP는 범용 content-package/v1을 Instagram에 게시합니다.",
                "",
                "1. prepare_instagram_publish: 게시 계획 생성, 외부 게시 없음",
                "2. approve_instagram_publish: 검토한 콘텐츠 해시 승인, 외부 게시 없음",
                "3. publish_instagram_plan: 승인된 계획만 실제 게시",
                "",
                "계정별 account_alias를 지정하며 게시 계획에 대상 계정이 고정됩니다.",
                "기본값은 게시 비활성화입니다. 전역 및 계정별 게시 스위치가 모두 1일 때만 실제 게시됩니다.",
                "인스타그램 비밀번호는 저장하거나 입력받지 않습니다.",
            ]
        )
    )


def tool_instagram_connection_status(args: dict[str, Any]) -> dict[str, Any]:
    verify = bool(args.get("verify", False))
    requested_alias = str(args.get("account_alias") or "").strip()
    aliases = (
        [resolve_instagram_account_alias(requested_alias)]
        if requested_alias
        else list_instagram_account_aliases()
    )
    accounts: list[dict[str, Any]] = []
    for alias in aliases:
        status = instagram_account_status(alias)
        status["verified"] = False
        if verify and status["configured"]:
            status["account"] = InstagramPublisher(
                InstagramConfig.from_env(alias)
            ).get_account()
            status["verified"] = True
        accounts.append(status)
    configured = all(item["configured"] for item in accounts)
    payload: dict[str, Any] = {
        "success": configured,
        "default_account": resolve_instagram_account_alias(),
        "accounts": accounts,
    }
    return tool_result_json(
        payload,
        is_error=bool(verify and requested_alias and not configured),
    )


def tool_prepare_instagram_publish(args: dict[str, Any]) -> dict[str, Any]:
    package = _content_store().get(str(args.get("package_id") or ""))
    account_alias = resolve_instagram_account_alias(
        str(args.get("account_alias") or "").strip() or None
    )
    account_status = instagram_account_status(account_alias)
    if not account_status["account_id_present"]:
        raise ValueError(f"Instagram account ID is not configured for {account_alias}")
    validation = validate_content_package(package, target="instagram")
    if not validation["valid"]:
        return tool_result_json(
            {
                "success": False,
                "package_id": package.get("package_id"),
                "validation": validation,
            },
            is_error=True,
        )
    plan, path = _plan_store().create(
        package,
        target="instagram",
        destination_account=account_alias,
    )
    return tool_result_json(
        {
            "success": True,
            "external_publish_performed": False,
            "plan": plan,
            "account": account_status,
            "path": str(path),
            "next_step": (
                "캡션과 미디어를 검토한 뒤 approve_instagram_publish에 "
                "plan_id, content_digest, confirmation을 전달하세요."
            ),
        }
    )


def tool_approve_instagram_publish(args: dict[str, Any]) -> dict[str, Any]:
    if str(args.get("confirmation") or "").strip() != APPROVAL_CONFIRMATION:
        raise ValueError(f"confirmation must be exactly: {APPROVAL_CONFIRMATION}")
    plan, path = _plan_store().approve(
        str(args.get("plan_id") or ""),
        expected_digest=str(args.get("content_digest") or ""),
    )
    return tool_result_json(
        {
            "success": True,
            "external_publish_performed": False,
            "plan": plan,
            "path": str(path),
        }
    )


def tool_get_instagram_publish_plan(args: dict[str, Any]) -> dict[str, Any]:
    plan = _plan_store().get(str(args.get("plan_id") or ""))
    package = _content_store().get(str(plan.get("package_id") or ""))
    return tool_result_json({"success": True, "plan": plan, "package": package})


def tool_list_instagram_publish_plans(args: dict[str, Any]) -> dict[str, Any]:
    raw_limit = args.get("limit", 50)
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    account_alias = str(args.get("account_alias") or "").strip().lower()
    plans = _plan_store().list(target="instagram", limit=limit)
    if account_alias:
        plans = [
            plan
            for plan in plans
            if plan.get("destination_account") == account_alias
        ]
    return tool_result_json({"success": True, "plans": plans})


def tool_publish_instagram_plan(args: dict[str, Any]) -> dict[str, Any]:
    if str(args.get("confirmation") or "").strip() != PUBLISH_CONFIRMATION:
        raise ValueError(f"confirmation must be exactly: {PUBLISH_CONFIRMATION}")
    plan_id = str(args.get("plan_id") or "")
    plan = _plan_store().get(plan_id)
    if plan.get("status") == "published":
        return tool_result_json(
            {
                "success": True,
                "skipped": True,
                "detail": "이미 게시된 계획입니다.",
                "plan": plan,
            }
        )
    if plan.get("status") != "approved":
        raise ValueError("publishing plan must be approved before publishing")
    account_alias = str(plan.get("destination_account") or "").strip()
    if not account_alias:
        raise ValueError("publishing plan has no destination account; prepare it again")
    config = InstagramConfig.from_env(account_alias)
    if not config.publishing_enabled:
        raise ValueError(
            "Instagram publishing is disabled for "
            f"{account_alias}; enable the global and account-specific switches"
        )

    package = _content_store().get(str(plan.get("package_id") or ""))
    if package.get("content_digest") != plan.get("content_digest"):
        raise ValueError("content package changed after the publishing plan was created")

    _plan_store().mark_publishing(plan_id)
    try:
        publish_result = InstagramPublisher(config).publish_package(package)
    except Exception as exc:
        failed_plan, _ = _plan_store().mark_failed(plan_id, error=str(exc))
        return tool_result_json(
            {
                "success": False,
                "external_publish_performed": False,
                "error": str(exc),
                "plan": failed_plan,
            },
            is_error=True,
        )

    published_plan, path = _plan_store().mark_published(
        plan_id,
        media_id=publish_result["media_id"],
    )
    return tool_result_json(
        {
            "success": True,
            "external_publish_performed": True,
            "publish_result": publish_result,
            "plan": published_plan,
            "path": str(path),
        }
    )


def _package_id_schema() -> dict[str, Any]:
    return {"type": "string", "description": "content-package/v1의 pkg_ ID"}


def _plan_id_schema() -> dict[str, Any]:
    return {"type": "string", "description": "prepare_instagram_publish가 반환한 plan_ ID"}


def build_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="instagram_help",
            description="인스타그램 게시 MCP의 한국어 사용법과 승인 절차를 보여줍니다.",
            input_schema=empty_input_schema(),
            handler=tool_instagram_help,
        ),
        ToolSpec(
            name="instagram_connection_status",
            description="비밀번호를 노출하지 않고 Instagram Login 환경설정과 선택적 연결 상태를 점검합니다.",
            input_schema={
                "type": "object",
                "properties": {
                    "verify": {
                        "type": "boolean",
                        "description": "true면 읽기 전용 API 호출로 계정을 확인합니다.",
                        "default": False,
                    },
                    "account_alias": {
                        "type": "string",
                        "description": "확인할 계정 별칭. 생략하면 전체 계정을 보여줍니다.",
                    },
                },
                "additionalProperties": False,
            },
            handler=tool_instagram_connection_status,
        ),
        ToolSpec(
            name="prepare_instagram_publish",
            description="범용 콘텐츠 패키지를 검증하고 검토용 게시 계획을 생성합니다. 실제 게시는 하지 않습니다.",
            input_schema={
                "type": "object",
                "properties": {
                    "package_id": _package_id_schema(),
                    "account_alias": {
                        "type": "string",
                        "description": "게시 대상 계정 별칭. 생략하면 기본 계정을 사용합니다.",
                    },
                },
                "required": ["package_id"],
                "additionalProperties": False,
            },
            handler=tool_prepare_instagram_publish,
        ),
        ToolSpec(
            name="approve_instagram_publish",
            description="검토한 콘텐츠 해시를 승인합니다. 실제 게시는 하지 않습니다.",
            input_schema={
                "type": "object",
                "properties": {
                    "plan_id": _plan_id_schema(),
                    "content_digest": {"type": "string"},
                    "confirmation": {
                        "type": "string",
                        "description": f"정확히 '{APPROVAL_CONFIRMATION}'",
                    },
                },
                "required": ["plan_id", "content_digest", "confirmation"],
                "additionalProperties": False,
            },
            handler=tool_approve_instagram_publish,
        ),
        ToolSpec(
            name="get_instagram_publish_plan",
            description="게시 계획과 연결된 콘텐츠 전체를 함께 읽어 검토합니다.",
            input_schema={
                "type": "object",
                "properties": {"plan_id": _plan_id_schema()},
                "required": ["plan_id"],
                "additionalProperties": False,
            },
            handler=tool_get_instagram_publish_plan,
        ),
        ToolSpec(
            name="list_instagram_publish_plans",
            description="최근 인스타그램 게시 계획과 상태를 보여줍니다.",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 50,
                    },
                    "account_alias": {
                        "type": "string",
                        "description": "특정 대상 계정의 계획만 필터링합니다.",
                    },
                },
                "additionalProperties": False,
            },
            handler=tool_list_instagram_publish_plans,
        ),
        ToolSpec(
            name="publish_instagram_plan",
            description="승인 완료된 계획을 Instagram에 실제 게시합니다. 명시적 확인 문구와 환경변수 활성화가 모두 필요합니다.",
            input_schema={
                "type": "object",
                "properties": {
                    "plan_id": _plan_id_schema(),
                    "confirmation": {
                        "type": "string",
                        "description": f"정확히 '{PUBLISH_CONFIRMATION}'",
                    },
                },
                "required": ["plan_id", "confirmation"],
                "additionalProperties": False,
            },
            handler=tool_publish_instagram_plan,
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

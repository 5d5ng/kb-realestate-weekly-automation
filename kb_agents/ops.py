from __future__ import annotations

import json
from typing import Any, Callable

from mcp_runtime import ToolSpec, tool_result_json

from .common import ArtifactRepository
from .contracts import AgentDescriptor


class OpsAgent:
    descriptor = AgentDescriptor(
        key="ops",
        korean_name="운영 에이전트",
        responsibility="에이전트 구성, 실행 상태, 산출물 위치를 점검합니다.",
        capabilities=("산출물 목록", "에이전트 상태 조회", "파이프라인 실행 상태 조회"),
    )

    def __init__(
        self,
        artifacts: ArtifactRepository,
        descriptor_provider: Callable[[], list[AgentDescriptor]],
    ) -> None:
        self.artifacts = artifacts
        self.descriptor_provider = descriptor_provider

    def list_artifacts(self, args: dict[str, Any]) -> dict[str, Any]:
        del args
        return tool_result_json(self.artifacts.list_artifacts())

    def get_agent_status(self, args: dict[str, Any]) -> dict[str, Any]:
        del args
        active_path = self.artifacts.runtime_dir / "pipeline_active.json"
        active_run: dict[str, Any] | None = None
        if active_path.exists():
            try:
                payload = json.loads(active_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    active_run = payload
            except (OSError, json.JSONDecodeError):
                active_run = {"status": "unreadable", "path": str(active_path)}

        descriptors = self.descriptor_provider()
        return tool_result_json(
            {
                "success": True,
                "architecture": "single MCP entrypoint with deterministic internal agents",
                "agent_count": len(descriptors),
                "agents": [descriptor.as_dict() for descriptor in descriptors],
                "active_run": active_run,
                "safety_defaults": {
                    "llm_api_enabled": False,
                    "external_delivery_enabled": False,
                    "skip_transactions": True,
                    "publishing_requires_separate_approval": True,
                },
            }
        )

    def tool_specs(self) -> list[ToolSpec]:
        empty_schema = {"type": "object", "properties": {}, "additionalProperties": False}
        return [
            ToolSpec(
                name="list_artifacts",
                description="운영 에이전트가 최신 산출물, 프롬프트, 아카이브 파일 목록을 보여줍니다.",
                input_schema=empty_schema,
                handler=self.list_artifacts,
            ),
            ToolSpec(
                name="get_agent_status",
                description="운영 에이전트가 분리된 에이전트 목록, 실행 상태, 안전 기본값을 보여줍니다.",
                input_schema=empty_schema,
                handler=self.get_agent_status,
            ),
        ]

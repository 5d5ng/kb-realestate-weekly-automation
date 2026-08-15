from __future__ import annotations

from typing import Any

from mcp_runtime import ToolSpec

from .common import ArtifactRepository
from .contracts import AgentDescriptor


class DataAgent:
    descriptor = AgentDescriptor(
        key="data",
        korean_name="데이터 에이전트",
        responsibility="KB 원본 분석과 정제 데이터 스냅샷을 담당합니다.",
        capabilities=("KB 주간 데이터 분석", "최신 데이터 스냅샷 조회"),
    )

    def __init__(self, artifacts: ArtifactRepository) -> None:
        self.artifacts = artifacts

    def run_analysis(self) -> dict[str, Any]:
        from analyzer import run_analysis

        return run_analysis()

    def get_data_snapshot(self, args: dict[str, Any]) -> dict[str, Any]:
        del args
        return self.artifacts.read_text_result(
            "data_snapshot.json",
            "데이터 스냅샷이 아직 없습니다.",
        )

    def tool_specs(self) -> list[ToolSpec]:
        return [
            ToolSpec(
                name="get_data_snapshot",
                description="데이터 에이전트가 최근 생성된 정제 원본 data_snapshot.json 전체 내용을 읽습니다.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                handler=self.get_data_snapshot,
            )
        ]

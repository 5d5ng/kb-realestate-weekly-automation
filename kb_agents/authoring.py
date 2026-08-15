from __future__ import annotations

from typing import Any

from mcp_runtime import ToolSpec, tool_result_json

from .common import ArtifactRepository, pipeline_input_schema, pipeline_options_from_args
from .contracts import AgentDescriptor
from .runtime import SafePipelineRunner


class AuthoringAgent:
    descriptor = AgentDescriptor(
        key="authoring",
        korean_name="작성 에이전트",
        responsibility="분석·뉴스·실거래를 보고서와 LLM 작성 패키지로 조립합니다.",
        capabilities=("작성 패키지 생성", "주간 보고서 생성", "최신 초안 조회"),
    )

    def __init__(
        self,
        artifacts: ArtifactRepository,
        runner: SafePipelineRunner | None = None,
    ) -> None:
        self.artifacts = artifacts
        self.runner = runner

    def generate_contents(
        self,
        analysis: dict[str, Any],
        news: list[dict[str, Any]],
        transactions: dict[str, Any],
        *,
        llm_overrides: dict[str, Any],
        telegram_news_limit: int,
        output_mode: str | None,
    ) -> dict[str, Any]:
        from reporter import generate_all_contents

        return generate_all_contents(
            analysis,
            news,
            transactions,
            llm_overrides=llm_overrides,
            telegram_news_limit=telegram_news_limit,
            output_mode=output_mode,
        )

    def generate_news_only_contents(
        self,
        news: list[dict[str, Any]],
        *,
        llm_overrides: dict[str, Any],
        telegram_news_limit: int,
    ) -> dict[str, Any]:
        from reporter import generate_news_only_contents

        return generate_news_only_contents(
            news,
            llm_overrides=llm_overrides,
            telegram_news_limit=telegram_news_limit,
        )

    def _run_for_mcp(self, args: dict[str, Any], *, output_mode: str) -> dict[str, Any]:
        if self.runner is None:
            raise RuntimeError("MCP pipeline runner is not configured")
        summary = self.runner.run(
            pipeline_options_from_args(args),
            output_mode=output_mode,
        )
        return tool_result_json(summary, is_error=not summary.get("success"))

    def generate_authoring_package(self, args: dict[str, Any]) -> dict[str, Any]:
        return self._run_for_mcp(args, output_mode="authoring_package")

    def generate_weekly_report(self, args: dict[str, Any]) -> dict[str, Any]:
        return self._run_for_mcp(args, output_mode="both")

    def get_latest_package(self, args: dict[str, Any]) -> dict[str, Any]:
        del args
        return self.artifacts.read_text_result(
            "llm_package.md",
            "작성 패키지가 아직 없습니다.",
        )

    def get_latest_weekly_report(self, args: dict[str, Any]) -> dict[str, Any]:
        del args
        return self.artifacts.read_text_result(
            "weekly_report.md",
            "주간 보고서 초안이 아직 없습니다.",
        )

    def tool_specs(self) -> list[ToolSpec]:
        return [
            ToolSpec(
                name="generate_authoring_package",
                description=(
                    "작성 에이전트가 LLM API와 발송 없이 KB 분석/뉴스를 실행하고 "
                    "Claude/GPT 웹용 reports/llm_package.md를 생성합니다."
                ),
                input_schema=pipeline_input_schema(),
                handler=self.generate_authoring_package,
            ),
            ToolSpec(
                name="generate_weekly_report",
                description=(
                    "작성 에이전트가 LLM API와 발송 없이 작성 패키지, Markdown 보고서, "
                    "데이터 스냅샷을 함께 생성합니다."
                ),
                input_schema=pipeline_input_schema(),
                handler=self.generate_weekly_report,
            ),
            ToolSpec(
                name="get_latest_package",
                description="작성 에이전트가 최근 생성된 Claude/GPT 작성 패키지 전체를 읽습니다.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                handler=self.get_latest_package,
            ),
            ToolSpec(
                name="get_latest_weekly_report",
                description="작성 에이전트가 최근 생성된 Markdown 주간 보고서 초안 전체를 읽습니다.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                handler=self.get_latest_weekly_report,
            ),
        ]

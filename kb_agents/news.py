from __future__ import annotations

from typing import Any

from mcp_runtime import ToolSpec, tool_result_json

from .common import ArtifactRepository, as_int, number_schema
from .contracts import AgentDescriptor


class NewsAgent:
    descriptor = AgentDescriptor(
        key="news",
        korean_name="뉴스 에이전트",
        responsibility="부동산 뉴스를 수집하고 최신 스냅샷의 기사 목록을 제공합니다.",
        capabilities=("뉴스 수집", "최신 뉴스 조회"),
    )

    def __init__(self, artifacts: ArtifactRepository) -> None:
        self.artifacts = artifacts

    def collect(self, *, days: int, max_articles: int) -> list[dict[str, Any]]:
        from news import get_weekly_news

        return get_weekly_news(days=days, max_articles=max_articles)

    def get_latest_news(self, args: dict[str, Any]) -> dict[str, Any]:
        limit = as_int(args, "limit", 10, minimum=1, maximum=30)
        snapshot = self.artifacts.load_snapshot()
        news = snapshot.get("news") or []
        if not isinstance(news, list):
            raise ValueError("data_snapshot.json news must be an array")
        return tool_result_json(
            {
                "success": True,
                "generated_at": snapshot.get("generated_at"),
                "latest_date": snapshot.get("latest_date"),
                "count": len(news),
                "items": news[:limit],
            }
        )

    def tool_specs(self) -> list[ToolSpec]:
        return [
            ToolSpec(
                name="get_latest_news",
                description="뉴스 에이전트가 최신 데이터 스냅샷에서 부동산 기사 목록을 읽습니다.",
                input_schema={
                    "type": "object",
                    "properties": {
                        "limit": number_schema("가져올 최대 기사 수.", 10, 1, 30),
                    },
                    "additionalProperties": False,
                },
                handler=self.get_latest_news,
            )
        ]

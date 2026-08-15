from __future__ import annotations

from typing import Any, Callable

from mcp_runtime import ToolSpec, tool_result_json

from .common import ArtifactRepository
from .contracts import AgentDescriptor


class TransactionAgent:
    descriptor = AgentDescriptor(
        key="transactions",
        korean_name="실거래 에이전트",
        responsibility="선별 지역의 실거래 캐시와 조회 결과를 담당합니다.",
        capabilities=("실거래 캐시 갱신", "실거래 수집", "최신 실거래 조회"),
    )

    def __init__(self, artifacts: ArtifactRepository) -> None:
        self.artifacts = artifacts

    def refresh_cache(
        self,
        analysis: dict[str, Any],
        *,
        limit: int,
        refresh_rent: bool,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        from realestate import refresh_transaction_cache

        return refresh_transaction_cache(
            analysis,
            limit=limit,
            refresh_rent=refresh_rent,
            progress_callback=progress_callback,
        )

    def collect(self, analysis: dict[str, Any], *, limit: int) -> dict[str, Any]:
        from realestate import get_recent_transactions

        return get_recent_transactions(analysis, limit=limit)

    @staticmethod
    def summarize(transactions: dict[str, Any]) -> dict[str, int]:
        bucket_count = 0
        region_count = 0
        trade_count = 0
        for region_mapping in transactions.values():
            if not isinstance(region_mapping, dict):
                continue
            bucket_count += 1
            region_count += len(region_mapping)
            for area_mapping in region_mapping.values():
                if not isinstance(area_mapping, dict):
                    continue
                for area_info in area_mapping.values():
                    if isinstance(area_info, dict):
                        trade_count += len(area_info.get("trades") or [])
        return {
            "bucket_count": bucket_count,
            "region_count": region_count,
            "trade_count": trade_count,
        }

    def get_latest_transactions(self, args: dict[str, Any]) -> dict[str, Any]:
        del args
        snapshot = self.artifacts.load_snapshot()
        transactions = snapshot.get("transactions") or {}
        if not isinstance(transactions, dict):
            raise ValueError("data_snapshot.json transactions must be an object")
        return tool_result_json(
            {
                "success": True,
                "generated_at": snapshot.get("generated_at"),
                "latest_date": snapshot.get("latest_date"),
                "summary": self.summarize(transactions),
                "transactions": transactions,
                "note": (
                    "비어 있으면 빠른 실행에서 실거래 조회를 생략한 결과입니다. "
                    "'실거래 포함해서 새로 만들어줘'라고 요청하세요."
                ),
            }
        )

    def tool_specs(self) -> list[ToolSpec]:
        return [
            ToolSpec(
                name="get_latest_transactions",
                description="실거래 에이전트가 최신 데이터 스냅샷의 실거래 결과와 건수 요약을 읽습니다.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                handler=self.get_latest_transactions,
            )
        ]

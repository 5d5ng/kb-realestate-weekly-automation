from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from mcp_runtime import tool_result_json, tool_result_text

from .contracts import PipelineOptions


KST = ZoneInfo("Asia/Seoul")


def as_int(
    args: dict[str, Any],
    key: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw_value = args.get(key, default)
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if value < minimum or value > maximum:
        raise ValueError(f"{key} must be between {minimum} and {maximum}")
    return value


def as_bool(args: dict[str, Any], key: str, default: bool) -> bool:
    raw_value = args.get(key, default)
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, str):
        lowered = raw_value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(raw_value, int):
        return bool(raw_value)
    raise ValueError(f"{key} must be a boolean")


def pipeline_options_from_args(
    args: dict[str, Any],
    *,
    skip_transactions: bool | None = None,
) -> PipelineOptions:
    return PipelineOptions(
        news_days=as_int(args, "news_days", 1, minimum=1, maximum=30),
        news_max_articles=as_int(args, "news_max_articles", 3, minimum=0, maximum=30),
        transaction_limit=as_int(args, "transaction_limit", 1, minimum=0, maximum=20),
        skip_transactions=(
            as_bool(args, "skip_transactions", True)
            if skip_transactions is None
            else skip_transactions
        ),
    )


def number_schema(description: str, default: int, minimum: int, maximum: int) -> dict[str, Any]:
    return {
        "type": "integer",
        "description": description,
        "default": default,
        "minimum": minimum,
        "maximum": maximum,
    }


def pipeline_input_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "news_days": number_schema("수집할 최근 뉴스 기간(일). 빠른 개인 실행은 1 권장.", 1, 1, 30),
            "news_max_articles": number_schema("수집할 최대 뉴스 기사 수. 빠른 개인 실행은 3 권장.", 3, 0, 30),
            "transaction_limit": number_schema("실거래 조회를 켤 때 지역/타입별 최대 거래 수.", 1, 0, 20),
            "skip_transactions": {
                "type": "boolean",
                "description": "true면 실거래 조회를 생략해 빠르게 작성 패키지를 만든다.",
                "default": True,
            },
        },
        "additionalProperties": False,
    }


class ArtifactRepository:
    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir).resolve()
        self.reports_dir = self.base_dir / "reports"
        self.prompts_dir = self.reports_dir / "prompts"
        self.archive_dir = self.reports_dir / "archive"
        self.runtime_dir = self.reports_dir / "runtime"

    def artifact_path(self, filename: str) -> Path:
        return self.reports_dir / filename

    def read_text_result(self, filename: str, missing_message: str) -> dict[str, Any]:
        path = self.artifact_path(filename)
        if not path.exists():
            return tool_result_json(
                {
                    "success": False,
                    "error": missing_message,
                    "path": str(path),
                    "next_step": "generate_authoring_package 또는 generate_weekly_report 도구를 먼저 실행하세요.",
                },
                is_error=True,
            )
        return tool_result_text(path.read_text(encoding="utf-8"))

    def load_snapshot(self) -> dict[str, Any]:
        path = self.artifact_path("data_snapshot.json")
        if not path.exists():
            raise FileNotFoundError("데이터 스냅샷이 아직 없습니다. 작성 패키지를 먼저 생성하세요.")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("data_snapshot.json root must be an object")
        return payload

    @staticmethod
    def artifact_meta(path: Path) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "path": str(path),
            "exists": path.exists(),
        }
        if not path.exists():
            return payload
        stat = path.stat()
        payload.update(
            {
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, KST).strftime(
                    "%Y-%m-%d %H:%M:%S %Z"
                ),
            }
        )
        return payload

    def list_artifacts(self) -> dict[str, Any]:
        prompt_files = sorted(self.prompts_dir.glob("*_prompt.txt")) if self.prompts_dir.exists() else []
        archive_files = sorted(self.archive_dir.glob("*")) if self.archive_dir.exists() else []
        return {
            "success": True,
            "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "project_dir": str(self.base_dir),
            "latest": {
                "llm_package": self.artifact_meta(self.artifact_path("llm_package.md")),
                "weekly_report": self.artifact_meta(self.artifact_path("weekly_report.md")),
                "data_snapshot": self.artifact_meta(self.artifact_path("data_snapshot.json")),
            },
            "prompt_files": [self.artifact_meta(path) for path in prompt_files],
            "archive": {
                "path": str(self.archive_dir),
                "count": len(archive_files),
                "recent": [self.artifact_meta(path) for path in archive_files[-10:]],
            },
        }

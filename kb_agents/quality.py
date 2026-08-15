from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mcp_runtime import ToolSpec, tool_result_json

from .common import ArtifactRepository, as_bool
from .contracts import AgentDescriptor


class QualityAgent:
    descriptor = AgentDescriptor(
        key="quality",
        korean_name="품질 검수 에이전트",
        responsibility="필수 파일, 데이터 구조, 날짜 일관성을 규칙 기반으로 검사합니다.",
        capabilities=("필수 프롬프트 검증", "최신 산출물 구조 검증"),
    )

    def __init__(self, artifacts: ArtifactRepository) -> None:
        self.artifacts = artifacts

    @staticmethod
    def validate_required_prompt_files(contents: dict[str, Any]) -> dict[str, Any]:
        required_prompt_files = contents.get("required_prompt_files", {}) or {}
        required_prompt_status = dict(contents.get("required_prompt_status", {}) or {})
        missing_tasks: list[str] = []

        for task_name, path in required_prompt_files.items():
            exists = bool(path) and Path(path).exists()
            required_prompt_status[task_name] = required_prompt_status.get(task_name, False) and exists
            if not exists:
                missing_tasks.append(task_name)

        for task_name, ok in required_prompt_status.items():
            if not ok and task_name not in missing_tasks:
                missing_tasks.append(task_name)

        return {
            "ok": not missing_tasks,
            "required_prompt_files": required_prompt_files,
            "required_prompt_status": required_prompt_status,
            "missing_tasks": missing_tasks,
        }

    @staticmethod
    def _check_markdown_file(path: Path, *, required: bool) -> tuple[list[str], list[str]]:
        errors: list[str] = []
        warnings: list[str] = []
        if not path.exists():
            message = f"파일 없음: {path.name}"
            (errors if required else warnings).append(message)
            return errors, warnings
        content = path.read_text(encoding="utf-8").strip()
        if len(content) < 200:
            errors.append(f"내용이 너무 짧음: {path.name} ({len(content)}자)")
        if not content.startswith("#"):
            warnings.append(f"Markdown 제목으로 시작하지 않음: {path.name}")
        return errors, warnings

    def check_latest_artifacts(self, args: dict[str, Any]) -> dict[str, Any]:
        require_weekly_report = as_bool(args, "require_weekly_report", False)
        errors: list[str] = []
        warnings: list[str] = []

        llm_path = self.artifacts.artifact_path("llm_package.md")
        report_path = self.artifacts.artifact_path("weekly_report.md")
        snapshot_path = self.artifacts.artifact_path("data_snapshot.json")

        file_errors, file_warnings = self._check_markdown_file(llm_path, required=True)
        errors.extend(file_errors)
        warnings.extend(file_warnings)
        file_errors, file_warnings = self._check_markdown_file(
            report_path,
            required=require_weekly_report,
        )
        errors.extend(file_errors)
        warnings.extend(file_warnings)

        snapshot: dict[str, Any] = {}
        if not snapshot_path.exists():
            errors.append("파일 없음: data_snapshot.json")
        else:
            try:
                loaded = json.loads(snapshot_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    snapshot = loaded
                else:
                    errors.append("data_snapshot.json root가 객체가 아님")
            except (OSError, json.JSONDecodeError) as exc:
                errors.append(f"data_snapshot.json 파싱 실패: {exc}")

        required_snapshot_keys = {
            "latest_date",
            "source",
            "analysis",
            "transactions",
            "news",
        }
        if snapshot:
            missing_keys = sorted(required_snapshot_keys - set(snapshot))
            if missing_keys:
                errors.append(f"data_snapshot.json 필수 키 누락: {', '.join(missing_keys)}")
            if not isinstance(snapshot.get("news"), list):
                errors.append("data_snapshot.json news가 배열이 아님")
            if not isinstance(snapshot.get("transactions"), dict):
                errors.append("data_snapshot.json transactions가 객체가 아님")

            latest_date = str(snapshot.get("latest_date") or "")
            if latest_date:
                for path in (llm_path, report_path):
                    if path.exists() and latest_date not in path.read_text(encoding="utf-8"):
                        warnings.append(f"{path.name}에서 기준일 {latest_date}을 찾지 못함")

        payload = {
            "success": not errors,
            "ready_for_llm": not errors,
            "external_calls_performed": False,
            "require_weekly_report": require_weekly_report,
            "latest_date": snapshot.get("latest_date"),
            "errors": errors,
            "warnings": warnings,
            "artifacts": {
                "llm_package": self.artifacts.artifact_meta(llm_path),
                "weekly_report": self.artifacts.artifact_meta(report_path),
                "data_snapshot": self.artifacts.artifact_meta(snapshot_path),
            },
        }
        return tool_result_json(payload, is_error=bool(errors))

    def tool_specs(self) -> list[ToolSpec]:
        return [
            ToolSpec(
                name="check_latest_artifacts",
                description="품질 검수 에이전트가 최신 작성 패키지와 데이터 구조를 외부 호출 없이 검사합니다.",
                input_schema={
                    "type": "object",
                    "properties": {
                        "require_weekly_report": {
                            "type": "boolean",
                            "description": "true면 weekly_report.md도 필수 파일로 검사합니다.",
                            "default": False,
                        }
                    },
                    "additionalProperties": False,
                },
                handler=self.check_latest_artifacts,
            )
        ]

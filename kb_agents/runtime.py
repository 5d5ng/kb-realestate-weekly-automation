from __future__ import annotations

import contextlib
import sys
import time
from typing import Any

from .contracts import PipelineOptions


def compact_pipeline_result(result: dict[str, Any]) -> dict[str, Any]:
    analysis_summary = result.get("analysis_summary") or {}
    transaction_summary = result.get("transaction_summary") or {}
    news_summary = result.get("news_summary") or {}
    contents_summary = result.get("contents_summary") or {}
    authoring_files = result.get("authoring_files") or {}

    return {
        "success": bool(result.get("success")),
        "skipped": bool(result.get("skipped", False)),
        "started_at": result.get("started_at"),
        "completed_at": result.get("completed_at"),
        "duration_sec": result.get("duration_sec"),
        "latest_date": analysis_summary.get("latest_date"),
        "send_enabled": result.get("send_enabled"),
        "llm_api_enabled": any(bool(value) for value in (result.get("llm_overrides") or {}).values()),
        "output_mode": result.get("output_mode"),
        "skip_transactions": result.get("skip_transactions"),
        "stage_timings": result.get("stage_timings") or {},
        "analysis_summary": analysis_summary,
        "transaction_summary": {
            "skipped": transaction_summary.get("skipped", False),
            "bucket_count": transaction_summary.get("bucket_count", 0),
            "region_count": transaction_summary.get("region_count", 0),
            "trade_count": transaction_summary.get("trade_count", 0),
        },
        "news_summary": {
            "count": news_summary.get("count", 0),
            "top_titles": news_summary.get("top_titles", []),
        },
        "contents_summary": {
            "output_mode": contents_summary.get("output_mode"),
            "prompt_files": contents_summary.get("prompt_files", []),
            "authoring_files": contents_summary.get("authoring_files", []),
        },
        "authoring_files": authoring_files,
        "prompt_files": result.get("prompt_files") or {},
        "required_prompt_status": result.get("required_prompt_status") or {},
        "error": result.get("error"),
        "failed_stage": result.get("failed_stage"),
    }


class SafePipelineRunner:
    """MCP calls the existing pipeline through one no-send/no-LLM boundary."""

    def run(self, options: PipelineOptions, *, output_mode: str) -> dict[str, Any]:
        from scheduler import DEFAULT_DISABLED_LLM_OVERRIDES, run_pipeline

        started = time.perf_counter()
        progress_events: list[dict[str, Any]] = []

        def on_progress(event: dict[str, Any]) -> None:
            if len(progress_events) < 60:
                progress_events.append(event)
            print(
                f"[mcp pipeline] {event.get('time', '')} "
                f"{event.get('stage', '')} {event.get('status', 'running')}: "
                f"{event.get('message', '')}",
                file=sys.stderr,
                flush=True,
            )

        with contextlib.redirect_stdout(sys.stderr):
            result = run_pipeline(
                send=False,
                trigger="manual",
                news_days=options.news_days,
                news_max_articles=options.news_max_articles,
                transaction_limit=options.transaction_limit,
                skip_transactions=options.skip_transactions,
                refresh_cache=False,
                channel_overrides={},
                send_prompt_files=False,
                llm_overrides=dict(DEFAULT_DISABLED_LLM_OVERRIDES),
                output_mode=output_mode,
                progress_callback=on_progress,
            )

        summary = compact_pipeline_result(result)
        summary["mcp_runtime_sec"] = round(time.perf_counter() - started, 2)
        summary["progress_events"] = progress_events
        return summary

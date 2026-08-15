"""
APScheduler — 매주 금요일 09:00 자동 실행
"""
from __future__ import annotations

import json
import logging
import os
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import fcntl
from apscheduler.schedulers.background import BackgroundScheduler

_logger = logging.getLogger("pipeline")

KST = ZoneInfo("Asia/Seoul")
JOB_ID = "kb_weekly_pipeline"
BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR / "reports" / "runtime"
LOCK_FILE = RUNTIME_DIR / "pipeline.lock"
ACTIVE_STATE_FILE = RUNTIME_DIR / "pipeline_active.json"
OVERRIDE_REQUEST_FILE = RUNTIME_DIR / "pipeline_manual_override.json"
LOCK_POLL_INTERVAL_SEC = 1.0
MANUAL_OVERRIDE_WAIT_SEC = 300
_scheduler: BackgroundScheduler | None = None
DEFAULT_DISABLED_LLM_OVERRIDES = {
    "telegram_report": False,
    "instagram_caption": False,
    "card_news_script": False,
}
SCHEDULED_LLM_OVERRIDES = {
    "telegram_report": True,
    "instagram_caption": True,
    "card_news_script": True,
}


class PipelineBusyError(RuntimeError):
    """이미 다른 실행이 점유 중일 때 발생."""


class PipelineOverrideRequested(RuntimeError):
    """수동 실행 우선권 요청으로 예약 실행을 중단해야 할 때 발생."""


def _now_text() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def _record_stage_timing(stage_timings: dict[str, float], stage: str, started_at: float) -> None:
    stage_timings[stage] = round(time.perf_counter() - started_at, 2)


def _normalize_llm_overrides(llm_overrides: dict[str, Any] | None) -> dict[str, Any]:
    if llm_overrides is None:
        return dict(DEFAULT_DISABLED_LLM_OVERRIDES)
    normalized = dict(DEFAULT_DISABLED_LLM_OVERRIDES)
    normalized.update(llm_overrides)
    return normalized


def _log(message: str) -> None:
    _logger.info(message)


def _emit_progress(
    callback: Callable[[dict[str, Any]], None] | None,
    *,
    stage: str,
    message: str,
    status: str = "running",
    **extra: Any,
) -> None:
    if callback is None:
        return

    payload = {
        "time": _now_text(),
        "stage": stage,
        "status": status,
        "message": message,
    }
    if extra:
        payload["extra"] = extra

    try:
        callback(payload)
    except Exception:
        return


def _ensure_runtime_dir() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def _instance_id() -> str:
    return (
        os.getenv("RAILWAY_REPLICA_ID")
        or os.getenv("HOSTNAME")
        or socket.gethostname()
        or "unknown-instance"
    )


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    _ensure_runtime_dir()
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def _remove_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _create_run_context(trigger: str) -> dict[str, Any]:
    return {
        "trigger": trigger,
        "started_at": _now_text(),
        "instance_id": _instance_id(),
        "pid": os.getpid(),
    }


def _try_acquire_lock() -> Any | None:
    _ensure_runtime_dir()
    lock_handle = LOCK_FILE.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_handle.close()
        return None
    return lock_handle


def _release_lock(lock_handle: Any | None) -> None:
    if not lock_handle:
        return
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    finally:
        lock_handle.close()


def _write_active_state(run_context: dict[str, Any]) -> None:
    _write_json_file(ACTIVE_STATE_FILE, run_context)


def _clear_active_state(run_context: dict[str, Any]) -> None:
    current_state = _read_json_file(ACTIVE_STATE_FILE)
    if not current_state:
        return
    if current_state.get("started_at") == run_context.get("started_at") and current_state.get("pid") == run_context.get("pid"):
        _remove_file(ACTIVE_STATE_FILE)


def _request_manual_override(active_state: dict[str, Any]) -> None:
    payload = {
        "requested_at": _now_text(),
        "requested_by": _instance_id(),
        "target_started_at": active_state.get("started_at"),
        "target_pid": active_state.get("pid"),
        "target_trigger": active_state.get("trigger"),
    }
    _write_json_file(OVERRIDE_REQUEST_FILE, payload)


def _clear_manual_override_request() -> None:
    _remove_file(OVERRIDE_REQUEST_FILE)


def _has_manual_override_request(run_context: dict[str, Any]) -> bool:
    request_payload = _read_json_file(OVERRIDE_REQUEST_FILE)
    if not request_payload:
        return False
    return (
        request_payload.get("target_started_at") == run_context.get("started_at")
        and request_payload.get("target_pid") == run_context.get("pid")
    )


def _check_for_manual_override(run_context: dict[str, Any], stage: str) -> None:
    if run_context.get("trigger") != "scheduled":
        return
    if _has_manual_override_request(run_context):
        raise PipelineOverrideRequested(f"수동 실행 우선권 요청으로 예약 실행을 중단합니다. stage={stage}")


def _acquire_pipeline_lock(trigger: str) -> tuple[Any, dict[str, Any]]:
    run_context = _create_run_context(trigger)
    lock_handle = _try_acquire_lock()
    if lock_handle:
        _write_active_state(run_context)
        if trigger == "manual":
            _clear_manual_override_request()
        return lock_handle, run_context

    active_state = _read_json_file(ACTIVE_STATE_FILE)
    active_trigger = active_state.get("trigger") or "unknown"

    if trigger == "scheduled":
        raise PipelineBusyError(f"다른 실행이 진행 중입니다. active_trigger={active_trigger}")

    if active_trigger == "manual":
        raise PipelineBusyError("다른 수동 실행이 이미 진행 중입니다.")

    _request_manual_override(active_state)
    _log("수동 실행 우선권 요청 등록 | 예약 실행 종료 대기")
    deadline = time.monotonic() + MANUAL_OVERRIDE_WAIT_SEC
    while time.monotonic() < deadline:
        time.sleep(LOCK_POLL_INTERVAL_SEC)
        lock_handle = _try_acquire_lock()
        if lock_handle:
            run_context = _create_run_context(trigger)
            _write_active_state(run_context)
            _clear_manual_override_request()
            return lock_handle, run_context

    raise PipelineBusyError("예약 실행이 종료되지 않아 수동 실행을 시작하지 못했습니다. 잠시 후 다시 시도해주세요.")


def _finish_pipeline_lock(lock_handle: Any | None, run_context: dict[str, Any] | None) -> None:
    if run_context:
        _clear_active_state(run_context)
        if run_context.get("trigger") == "manual":
            _clear_manual_override_request()
    _release_lock(lock_handle)


def _summarize_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    content_regions = analysis.get("content_regions", {})
    return {
        "latest_date": analysis.get("latest_date", ""),
        "seoul_sale_all": len(content_regions.get("seoul_sale_all") or []),
        "seoul_rent_all": len(content_regions.get("seoul_rent_all") or []),
        "capital_sale_top5": len(content_regions.get("capital_sale_top5") or []),
        "capital_rent_top5": len(content_regions.get("capital_rent_top5") or []),
        "non_capital_sale_top5": len(content_regions.get("non_capital_sale_top5") or []),
        "non_capital_rent_top5": len(content_regions.get("non_capital_rent_top5") or []),
        "content_buckets": {
            bucket_name: len(region_items or [])
            for bucket_name, region_items in content_regions.items()
        },
    }


def _summarize_transactions(transactions: dict[str, Any]) -> dict[str, Any]:
    bucket_count = 0
    region_count = 0
    trade_count = 0
    bucket_regions: dict[str, int] = {}

    for bucket_name, region_mapping in transactions.items():
        if not isinstance(region_mapping, dict):
            continue
        bucket_count += 1
        bucket_regions[bucket_name] = len(region_mapping)
        region_count += len(region_mapping)
        for area_mapping in region_mapping.values():
            if not isinstance(area_mapping, dict):
                continue
            for area_info in area_mapping.values():
                trade_count += len((area_info or {}).get("trades", []))

    return {
        "bucket_count": bucket_count,
        "region_count": region_count,
        "trade_count": trade_count,
        "bucket_regions": bucket_regions,
    }


def _summarize_news(news: list[dict[str, Any]], *, max_titles: int = 5) -> dict[str, Any]:
    publisher_counts: dict[str, int] = {}
    for article in news:
        publisher = article.get("publisher", "unknown")
        publisher_counts[publisher] = publisher_counts.get(publisher, 0) + 1

    return {
        "count": len(news),
        "publishers": publisher_counts,
        "top_titles": [article.get("title", "") for article in news[: max(1, max_titles)]],
    }


def _summarize_contents(contents: dict[str, Any]) -> dict[str, Any]:
    return {
        "keys": sorted(contents.keys()),
        "prompt_files": contents.get("prompt_files", {}),
        "prompt_archive_files": contents.get("prompt_archive_files", {}),
        "authoring_files": contents.get("authoring_files", {}),
        "authoring_archive_files": contents.get("authoring_archive_files", {}),
        "output_mode": contents.get("output_mode", ""),
        "required_prompt_files": contents.get("required_prompt_files", {}),
        "required_prompt_status": contents.get("required_prompt_status", {}),
        "generation_plan": contents.get("generation_plan", {}),
        "generation_meta": contents.get("generation_meta", {}),
    }


def _summarize_cache_refresh(summary: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(summary, dict):
        return {}
    return {
        "bucket_count": int(summary.get("bucket_count") or 0),
        "region_count": int(summary.get("region_count") or 0),
        "unique_region_count": int(summary.get("unique_region_count") or 0),
        "unique_complex_count": int(summary.get("unique_complex_count") or 0),
        "unique_area_pair_count": int(summary.get("unique_area_pair_count") or 0),
        "sale_cache_entry_count": int(summary.get("sale_cache_entry_count") or 0),
        "rent_cache_entry_count": int(summary.get("rent_cache_entry_count") or 0),
        "failed_region_count": len(summary.get("failed_regions") or []),
        "bucket_regions": summary.get("bucket_regions") or {},
        "skipped": bool(summary.get("skipped")),
        "detail": summary.get("detail", ""),
    }


def _iter_prompt_file_values(contents: dict[str, Any]) -> list[str]:
    prompt_paths: list[str] = []
    for key in ("prompt_files", "prompt_archive_files", "required_prompt_files"):
        for value in (contents.get(key, {}) or {}).values():
            if isinstance(value, str) and value:
                prompt_paths.append(value)
    return prompt_paths


def _iter_authoring_file_values(contents: dict[str, Any]) -> list[str]:
    authoring_paths: list[str] = []
    for key in ("authoring_files", "authoring_archive_files"):
        for value in (contents.get(key, {}) or {}).values():
            if isinstance(value, str) and value:
                authoring_paths.append(value)
    return authoring_paths


def _validate_required_prompt_files(contents: dict[str, Any]) -> dict[str, Any]:
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
        "required_prompt_files": required_prompt_files,
        "required_prompt_status": required_prompt_status,
        "missing_tasks": missing_tasks,
        "ok": not missing_tasks,
    }


def _collect_artifact_files(analysis: dict[str, Any], contents: dict[str, Any]) -> list[str]:
    artifact_files: list[str] = []

    source_files = analysis.get("source_files", {}) or {}
    for value in source_files.values():
        if isinstance(value, str) and value:
            artifact_files.append(value)

    for image_path in analysis.get("report_images", []) or []:
        if isinstance(image_path, str) and image_path:
            artifact_files.append(image_path)

    artifact_files.extend(_iter_prompt_file_values(contents))
    artifact_files.extend(_iter_authoring_file_values(contents))

    deduped: list[str] = []
    seen: set[str] = set()
    for path in artifact_files:
        if path in seen:
            continue
        seen.add(path)
        deduped.append(path)
    return deduped


def _collect_prompt_artifact_files(contents: dict[str, Any]) -> list[str]:
    artifact_files: list[str] = []
    seen: set[str] = set()
    for value in _iter_prompt_file_values(contents):
        if value not in seen:
            artifact_files.append(value)
            seen.add(value)
    for value in _iter_authoring_file_values(contents):
        if value not in seen:
            artifact_files.append(value)
            seen.add(value)
    return artifact_files


def run_pipeline(
    *,
    send: bool = True,
    trigger: str = "manual",
    news_days: int = 7,
    news_max_articles: int = 12,
    transaction_limit: int = 5,
    skip_transactions: bool = False,
    refresh_cache: bool | None = None,
    channel_overrides: dict[str, bool] | None = None,
    send_prompt_files: bool | None = None,
    llm_overrides: dict[str, Any] | None = None,
    output_mode: str | None = "both",
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """전체 파이프라인 실행 컨트롤러"""
    from kb_agents import build_pipeline_agents

    agents = build_pipeline_agents(BASE_DIR)

    started_at = _now_text()
    started_perf = time.perf_counter()
    current_stage = "analysis"
    lock_handle = None
    run_context: dict[str, Any] | None = None
    contents: dict[str, Any] = {}
    stage_timings: dict[str, float] = {}

    if refresh_cache is None:
        refresh_cache = trigger == "scheduled"
    llm_overrides = _normalize_llm_overrides(llm_overrides)

    _log(f"파이프라인 시작 | trigger={trigger} | send={send}")
    _emit_progress(
        progress_callback,
        stage="init",
        message="파이프라인을 시작했습니다.",
        status="running",
        trigger=trigger,
        send=send,
        refresh_cache=refresh_cache,
        channel_overrides=channel_overrides or {},
        send_prompt_files=send_prompt_files,
        llm_overrides=llm_overrides or {},
    )

    try:
        lock_handle, run_context = _acquire_pipeline_lock(trigger)
        _log(
            "실행 락 획득 "
            f"| trigger={run_context['trigger']} "
            f"| instance={run_context['instance_id']} "
            f"| pid={run_context['pid']}"
        )
        _emit_progress(
            progress_callback,
            stage="lock",
            message="실행 락을 획득했습니다.",
            trigger=run_context["trigger"],
            instance=run_context["instance_id"],
            pid=run_context["pid"],
        )

        _emit_progress(progress_callback, stage="analysis", message="KB 분석을 시작합니다.")
        stage_started = time.perf_counter()
        analysis = agents.data.run_analysis()
        _record_stage_timing(stage_timings, "analysis", stage_started)
        analysis_summary = _summarize_analysis(analysis)
        _log(f"분석 완료 | latest_date={analysis_summary['latest_date']}")
        _emit_progress(
            progress_callback,
            stage="analysis",
            message="KB 분석이 완료되었습니다.",
            latest_date=analysis_summary["latest_date"],
            content_buckets=analysis_summary["content_buckets"],
        )
        _check_for_manual_override(run_context, current_stage)

        current_stage = "cache"
        if refresh_cache:
            stage_started = time.perf_counter()
            _emit_progress(
                progress_callback,
                stage="cache",
                message="선별 지역 캐시 갱신을 시작합니다.",
                transaction_limit=transaction_limit,
                refresh_rent=False,
            )
            try:
                refreshed_cache = agents.transactions.refresh_cache(
                    analysis,
                    limit=transaction_limit,
                    refresh_rent=False,
                    progress_callback=lambda event: _emit_progress(
                        progress_callback,
                        stage="cache",
                        message=event.get("message", "캐시를 갱신하고 있습니다."),
                        status=event.get("status", "running"),
                        **{key: value for key, value in event.items() if key not in {"message", "status"}},
                    ),
                )
            except Exception as exc:
                cache_refresh_summary = {
                    "success": False,
                    "degraded": True,
                    "fallback_to_existing_cache": True,
                    "error": str(exc),
                    "detail": "실거래 캐시 갱신 실패로 기존 캐시를 사용해 계속 진행합니다.",
                }
                _log(f"캐시 갱신 경고 | 기존 캐시로 계속 진행 | error={exc}")
                _emit_progress(
                    progress_callback,
                    stage="cache",
                    message="캐시 갱신 실패로 기존 캐시를 사용해 계속 진행합니다.",
                    status="degraded",
                    error=str(exc),
                )
            else:
                cache_refresh_summary = _summarize_cache_refresh(refreshed_cache)
                _log(
                    "캐시 갱신 완료 "
                    f"| regions={cache_refresh_summary['unique_region_count']} "
                    f"| complexes={cache_refresh_summary['unique_complex_count']} "
                    f"| sale_entries={cache_refresh_summary['sale_cache_entry_count']}"
                )
                _emit_progress(
                    progress_callback,
                    stage="cache",
                    message="선별 지역 캐시 갱신이 완료되었습니다.",
                    status="completed",
                    cache_refresh_summary=cache_refresh_summary,
                )
            _record_stage_timing(stage_timings, "cache", stage_started)
        else:
            cache_refresh_summary = {
                "skipped": True,
                "detail": "수동 실행은 캐시 갱신을 생략하고 DB 우선으로 진행합니다.",
            }
            stage_timings["cache"] = 0.0
            _log("수동 실행으로 캐시 갱신 생략")
            _emit_progress(
                progress_callback,
                stage="cache",
                message="수동 실행은 캐시 갱신을 생략하고 DB 우선으로 진행합니다.",
                status="skipped",
            )
        _check_for_manual_override(run_context, current_stage)

        current_stage = "transactions"
        if skip_transactions or transaction_limit <= 0:
            transactions = {}
            transaction_summary = {
                "skipped": True,
                "detail": "빠른 작성 패키지 실행을 위해 실거래 조회를 생략했습니다.",
                "bucket_count": 0,
                "region_count": 0,
                "trade_count": 0,
                "bucket_regions": {},
            }
            stage_timings["transactions"] = 0.0
            _log("실거래 조회 생략")
            _emit_progress(
                progress_callback,
                stage="transactions",
                message="빠른 실행 옵션으로 실거래 조회를 생략했습니다.",
                status="skipped",
            )
        else:
            _emit_progress(
                progress_callback,
                stage="transactions",
                message="실거래 조회를 시작합니다.",
                transaction_limit=transaction_limit,
            )
            stage_started = time.perf_counter()
            transactions = agents.transactions.collect(analysis, limit=transaction_limit)
            _record_stage_timing(stage_timings, "transactions", stage_started)
            transaction_summary = _summarize_transactions(transactions)
            _log(
                "실거래 조회 완료 "
                f"| buckets={transaction_summary['bucket_count']} "
                f"| regions={transaction_summary['region_count']} "
                f"| trades={transaction_summary['trade_count']}"
            )
            _emit_progress(
                progress_callback,
                stage="transactions",
                message="실거래 조회가 완료되었습니다.",
                bucket_count=transaction_summary["bucket_count"],
                region_count=transaction_summary["region_count"],
                trade_count=transaction_summary["trade_count"],
            )
        _check_for_manual_override(run_context, current_stage)

        current_stage = "news"
        _emit_progress(
            progress_callback,
            stage="news",
            message="뉴스 수집을 시작합니다.",
            news_days=news_days,
            news_max_articles=news_max_articles,
        )
        stage_started = time.perf_counter()
        news = agents.news.collect(days=news_days, max_articles=news_max_articles)
        _record_stage_timing(stage_timings, "news", stage_started)
        news_summary = _summarize_news(news, max_titles=min(max(1, news_max_articles), 10))
        _log(f"뉴스 수집 완료 | count={news_summary['count']}")
        _emit_progress(
            progress_callback,
            stage="news",
            message="뉴스 수집이 완료되었습니다.",
            count=news_summary["count"],
            top_titles=news_summary["top_titles"],
        )
        _check_for_manual_override(run_context, current_stage)

        current_stage = "contents"
        _emit_progress(progress_callback, stage="contents", message="콘텐츠 생성을 시작합니다.")
        stage_started = time.perf_counter()
        contents = agents.authoring.generate_contents(
            analysis,
            news,
            transactions,
            llm_overrides=llm_overrides,
            telegram_news_limit=min(max(1, news_max_articles), 30),
            output_mode=output_mode,
        )
        _record_stage_timing(stage_timings, "contents", stage_started)
        required_prompt_check = agents.quality.validate_required_prompt_files(contents)
        contents["required_prompt_status"] = required_prompt_check["required_prompt_status"]
        contents_summary = _summarize_contents(contents)
        _log(
            "콘텐츠 생성 완료 "
            f"| prompt_files={len(contents_summary['prompt_files'])}"
        )
        _emit_progress(
            progress_callback,
            stage="contents",
            message="콘텐츠 생성이 완료되었습니다.",
            prompt_files=contents_summary["prompt_files"],
            authoring_files=contents_summary["authoring_files"],
            output_mode=contents_summary["output_mode"],
        )
        if trigger == "scheduled" and not required_prompt_check["ok"]:
            missing_text = ", ".join(required_prompt_check["missing_tasks"])
            raise RuntimeError(f"필수 프롬프트 파일 생성 실패: {missing_text}")
        _check_for_manual_override(run_context, current_stage)

        current_stage = "send"
        if send:
            _emit_progress(
                progress_callback,
                stage="send",
                message="선택한 채널로 발송을 시작합니다.",
                channel_overrides=channel_overrides or {},
                send_prompt_files=send_prompt_files,
            )
            send_results = agents.publishing.send(
                contents,
                channel_overrides=channel_overrides,
                send_prompt_files=send_prompt_files,
            )
            _log("채널 발송 완료")
            _emit_progress(
                progress_callback,
                stage="send",
                message="채널 발송이 완료되었습니다.",
                send_results=send_results,
            )
        else:
            send_results = {
                "skipped": True,
                "detail": "dry-run mode: 발송을 건너뛰었습니다.",
            }
            _log("dry-run 모드로 발송 생략")
            _emit_progress(
                progress_callback,
                stage="send",
                message="dry-run 모드로 발송을 건너뛰었습니다.",
                status="skipped",
            )

        duration_sec = round(time.perf_counter() - started_perf, 2)
        result = {
            "success": True,
            "started_at": started_at,
            "completed_at": _now_text(),
            "duration_sec": duration_sec,
            "send_enabled": send,
            "trigger": trigger,
            "refresh_cache": refresh_cache,
            "skip_transactions": skip_transactions,
            "channel_overrides": channel_overrides or {},
            "send_prompt_files": send_prompt_files,
            "llm_overrides": llm_overrides or {},
            "output_mode": contents.get("output_mode", output_mode),
            "analysis_summary": analysis_summary,
            "cache_refresh_summary": cache_refresh_summary,
            "transaction_summary": transaction_summary,
            "news_summary": news_summary,
            "stage_timings": stage_timings,
            "contents_summary": contents_summary,
            "prompt_files": contents.get("prompt_files", {}),
            "prompt_archive_files": contents.get("prompt_archive_files", {}),
            "authoring_files": contents.get("authoring_files", {}),
            "authoring_archive_files": contents.get("authoring_archive_files", {}),
            "required_prompt_files": contents.get("required_prompt_files", {}),
            "required_prompt_status": contents.get("required_prompt_status", {}),
            "artifact_files": _collect_artifact_files(analysis, contents),
            "send_results": send_results,
        }
        _log(f"파이프라인 종료 | success=True | duration={duration_sec}s")
        _emit_progress(
            progress_callback,
            stage="done",
            message="파이프라인이 정상 종료되었습니다.",
            status="completed",
            duration_sec=duration_sec,
        )
        return result
    except PipelineOverrideRequested as exc:
        duration_sec = round(time.perf_counter() - started_perf, 2)
        _log(
            "예약 실행 중단 "
            f"| duration={duration_sec}s "
            f"| reason={exc}"
        )
        _emit_progress(
            progress_callback,
            stage=current_stage,
            message="수동 실행 우선권으로 예약 실행을 중단했습니다.",
            status="skipped",
            duration_sec=duration_sec,
            reason=str(exc),
        )
        return {
            "success": True,
            "skipped": True,
            "started_at": started_at,
            "completed_at": _now_text(),
            "duration_sec": duration_sec,
            "failed_stage": current_stage,
            "reason": str(exc),
            "send_enabled": send,
            "trigger": trigger,
            "refresh_cache": refresh_cache,
            "skip_transactions": skip_transactions,
            "channel_overrides": channel_overrides or {},
            "send_prompt_files": send_prompt_files,
            "llm_overrides": llm_overrides or {},
            "output_mode": output_mode,
            "stage_timings": stage_timings,
            "manual_override": True,
        }
    except PipelineBusyError as exc:
        duration_sec = round(time.perf_counter() - started_perf, 2)
        _log(
            "파이프라인 건너뜀 "
            f"| trigger={trigger} "
            f"| duration={duration_sec}s "
            f"| reason={exc}"
        )
        _emit_progress(
            progress_callback,
            stage=current_stage,
            message="다른 실행이 진행 중이라 이번 요청을 건너뜁니다.",
            status="skipped",
            duration_sec=duration_sec,
            reason=str(exc),
        )
        return {
            "success": True,
            "skipped": True,
            "started_at": started_at,
            "completed_at": _now_text(),
            "duration_sec": duration_sec,
            "failed_stage": current_stage,
            "reason": str(exc),
            "send_enabled": send,
            "trigger": trigger,
            "refresh_cache": refresh_cache,
            "skip_transactions": skip_transactions,
            "channel_overrides": channel_overrides or {},
            "send_prompt_files": send_prompt_files,
            "llm_overrides": llm_overrides or {},
            "output_mode": output_mode,
            "stage_timings": stage_timings,
        }
    except Exception as exc:
        duration_sec = round(time.perf_counter() - started_perf, 2)
        _log(
            "파이프라인 실패 "
            f"| stage={current_stage} "
            f"| duration={duration_sec}s "
            f"| error={exc}"
        )
        _emit_progress(
            progress_callback,
            stage=current_stage,
            message="파이프라인이 실패했습니다.",
            status="failed",
            duration_sec=duration_sec,
            error=str(exc),
        )
        return {
            "success": False,
            "started_at": started_at,
            "completed_at": _now_text(),
            "duration_sec": duration_sec,
            "failed_stage": current_stage,
            "error": str(exc),
            "send_enabled": send,
            "trigger": trigger,
            "refresh_cache": refresh_cache,
            "skip_transactions": skip_transactions,
            "channel_overrides": channel_overrides or {},
            "send_prompt_files": send_prompt_files,
            "llm_overrides": llm_overrides or {},
            "output_mode": output_mode,
            "stage_timings": stage_timings,
            "prompt_files": contents.get("prompt_files", {}),
            "prompt_archive_files": contents.get("prompt_archive_files", {}),
            "authoring_files": contents.get("authoring_files", {}),
            "authoring_archive_files": contents.get("authoring_archive_files", {}),
            "required_prompt_files": contents.get("required_prompt_files", {}),
            "required_prompt_status": contents.get("required_prompt_status", {}),
        }
    finally:
        _finish_pipeline_lock(lock_handle, run_context)


def run_news_only_pipeline(
    *,
    send: bool = True,
    trigger: str = "manual",
    news_days: int = 7,
    news_max_articles: int = 12,
    channel_overrides: dict[str, bool] | None = None,
    send_prompt_files: bool | None = None,
    llm_overrides: dict[str, Any] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    from kb_agents import build_pipeline_agents

    agents = build_pipeline_agents(BASE_DIR)

    started_at = _now_text()
    started_perf = time.perf_counter()
    current_stage = "news"
    lock_handle = None
    run_context: dict[str, Any] | None = None
    contents: dict[str, Any] = {}

    _log(f"뉴스 전용 파이프라인 시작 | trigger={trigger} | send={send}")
    llm_overrides = _normalize_llm_overrides(llm_overrides)
    _emit_progress(
        progress_callback,
        stage="init",
        message="뉴스 전용 파이프라인을 시작합니다.",
        trigger=trigger,
        send=send,
        send_prompt_files=send_prompt_files,
    )

    try:
        lock_handle, run_context = _acquire_pipeline_lock(trigger)
        _emit_progress(
            progress_callback,
            stage="lock",
            message="실행 락을 획득했습니다.",
            trigger=trigger,
            status="completed",
        )

        _emit_progress(
            progress_callback,
            stage="news",
            message="뉴스 수집을 시작합니다.",
            news_days=news_days,
            news_max_articles=news_max_articles,
        )
        news = agents.news.collect(days=news_days, max_articles=news_max_articles)
        news_summary = _summarize_news(news, max_titles=min(max(1, news_max_articles), 10))
        _log(f"뉴스 전용 수집 완료 | count={news_summary['count']}")
        _emit_progress(
            progress_callback,
            stage="news",
            message="뉴스 수집이 완료되었습니다.",
            count=news_summary["count"],
            top_titles=news_summary["top_titles"],
            status="completed",
        )
        _check_for_manual_override(run_context, current_stage)

        current_stage = "contents"
        _emit_progress(progress_callback, stage="contents", message="뉴스 전용 콘텐츠 생성을 시작합니다.")
        contents = agents.authoring.generate_news_only_contents(
            news,
            llm_overrides=llm_overrides,
            telegram_news_limit=min(max(1, news_max_articles), 30),
        )
        required_prompt_check = agents.quality.validate_required_prompt_files(contents)
        contents["required_prompt_status"] = required_prompt_check["required_prompt_status"]
        contents_summary = _summarize_contents(contents)
        _emit_progress(
            progress_callback,
            stage="contents",
            message="뉴스 전용 콘텐츠 생성이 완료되었습니다.",
            prompt_files=contents_summary["prompt_files"],
            status="completed",
        )
        _check_for_manual_override(run_context, current_stage)

        current_stage = "send"
        if send:
            _emit_progress(
                progress_callback,
                stage="send",
                message="뉴스 전용 발송을 시작합니다.",
                channel_overrides=channel_overrides or {},
                send_prompt_files=send_prompt_files,
            )
            send_results = agents.publishing.send(
                contents,
                channel_overrides=channel_overrides,
                send_prompt_files=send_prompt_files,
            )
            _emit_progress(
                progress_callback,
                stage="send",
                message="뉴스 전용 발송이 완료되었습니다.",
                send_results=send_results,
                status="completed",
            )
        else:
            send_results = {
                "skipped": True,
                "detail": "dry-run mode: 발송을 건너뛰었습니다.",
            }
            _emit_progress(
                progress_callback,
                stage="send",
                message="dry-run 모드로 뉴스 전용 발송을 건너뛰었습니다.",
                status="skipped",
            )

        duration_sec = round(time.perf_counter() - started_perf, 2)
        result = {
            "success": True,
            "started_at": started_at,
            "completed_at": _now_text(),
            "duration_sec": duration_sec,
            "send_enabled": send,
            "trigger": trigger,
            "run_mode": "news_only",
            "channel_overrides": channel_overrides or {},
            "send_prompt_files": send_prompt_files,
            "llm_overrides": llm_overrides or {},
            "news_summary": news_summary,
            "contents_summary": contents_summary,
            "prompt_files": contents.get("prompt_files", {}),
            "prompt_archive_files": contents.get("prompt_archive_files", {}),
            "required_prompt_files": contents.get("required_prompt_files", {}),
            "required_prompt_status": contents.get("required_prompt_status", {}),
            "artifact_files": _collect_prompt_artifact_files(contents),
            "send_results": send_results,
        }
        _emit_progress(
            progress_callback,
            stage="done",
            message="뉴스 전용 파이프라인이 정상 종료되었습니다.",
            duration_sec=duration_sec,
            status="completed",
        )
        return result
    except PipelineOverrideRequested as exc:
        duration_sec = round(time.perf_counter() - started_perf, 2)
        _emit_progress(
            progress_callback,
            stage=current_stage,
            message="다른 실행이 진행 중이라 이번 뉴스 전용 요청을 건너뜁니다.",
            status="skipped",
            duration_sec=duration_sec,
            reason=str(exc),
        )
        return {
            "success": True,
            "skipped": True,
            "started_at": started_at,
            "completed_at": _now_text(),
            "duration_sec": duration_sec,
            "failed_stage": current_stage,
            "reason": str(exc),
            "send_enabled": send,
            "trigger": trigger,
            "run_mode": "news_only",
            "channel_overrides": channel_overrides or {},
            "send_prompt_files": send_prompt_files,
            "llm_overrides": llm_overrides or {},
        }
    except Exception as exc:
        duration_sec = round(time.perf_counter() - started_perf, 2)
        _emit_progress(
            progress_callback,
            stage=current_stage,
            message="뉴스 전용 파이프라인이 실패했습니다.",
            status="failed",
            duration_sec=duration_sec,
            error=str(exc),
        )
        return {
            "success": False,
            "started_at": started_at,
            "completed_at": _now_text(),
            "duration_sec": duration_sec,
            "failed_stage": current_stage,
            "error": str(exc),
            "send_enabled": send,
            "trigger": trigger,
            "run_mode": "news_only",
            "channel_overrides": channel_overrides or {},
            "send_prompt_files": send_prompt_files,
            "llm_overrides": llm_overrides or {},
            "prompt_files": contents.get("prompt_files", {}),
            "prompt_archive_files": contents.get("prompt_archive_files", {}),
            "required_prompt_files": contents.get("required_prompt_files", {}),
            "required_prompt_status": contents.get("required_prompt_status", {}),
        }
    finally:
        _finish_pipeline_lock(lock_handle, run_context)


def init_scheduler() -> BackgroundScheduler:
    global _scheduler

    if _scheduler and _scheduler.running:
        _log("스케줄러가 이미 실행 중입니다.")
        return _scheduler

    scheduler = BackgroundScheduler(timezone=str(KST))
    scheduler.add_job(
        run_pipeline,
        kwargs={
            "send": True,
            "trigger": "scheduled",
            "llm_overrides": dict(SCHEDULED_LLM_OVERRIDES),
        },
        trigger="cron",
        id=JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
        day_of_week="fri",
        hour=9,
        minute=0,
    )
    scheduler.start()
    _scheduler = scheduler
    _log("스케줄러 시작 | 매주 금요일 09:00")
    return scheduler

#!/usr/bin/env python3
"""Run the full Friday delivery outside the web process.

This is the launchd entry point. It deliberately uses the same pipeline agents
as the MCP service, but it is allowed to perform the explicitly configured
Telegram delivery and records the complete result for the next status check.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from log_config import setup_logging
from scheduler import DEFAULT_DISABLED_LLM_OVERRIDES, KST, run_pipeline


RUNTIME_DIR = BASE_DIR / "reports" / "runtime"
HISTORY_DIR = RUNTIME_DIR / "history"
DESKTOP_RECEIPT_PATH = RUNTIME_DIR / "last_desktop_weekly_run.json"


def _write_result(result: dict) -> Path:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    completed_at = str(result.get("completed_at") or datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))
    timestamp = completed_at.replace("-", "").replace(" ", "_").replace(":", "")
    history_path = HISTORY_DIR / f"{timestamp}_scheduled.json"
    payload = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    history_path.write_text(payload, encoding="utf-8")
    (RUNTIME_DIR / "last_scheduled_run.json").write_text(payload, encoding="utf-8")
    return history_path


def _delivery_ok(result: dict) -> bool:
    if not result.get("success") or result.get("skipped"):
        return False
    delivery = result.get("send_results") or {}
    telegram = delivery.get("telegram") or {}
    prompts = delivery.get("telegram_prompt_files") or {}
    telegram_ids = [item for item in (telegram.get("message_ids") or []) if isinstance(item, int)]
    prompt_ids = [item for item in (prompts.get("message_ids") or []) if isinstance(item, int)]
    return (
        bool(telegram.get("success"))
        and bool(telegram_ids)
        and bool(prompts.get("success"))
        and bool(prompt_ids)
    )


def _desktop_delivery_succeeded_today() -> dict | None:
    try:
        receipt = json.loads(DESKTOP_RECEIPT_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None

    try:
        completed = datetime.strptime(str(receipt.get("completed_at")), "%Y-%m-%d %H:%M:%S").date()
    except (TypeError, ValueError):
        return None

    telegram = receipt.get("telegram") or {}
    message_ids = [item for item in (telegram.get("message_ids") or []) if isinstance(item, int)]
    if (
        receipt.get("success")
        and completed == datetime.now(KST).date()
        and telegram.get("success")
        and message_ids
    ):
        return receipt
    return None


def main() -> int:
    setup_logging()
    desktop_receipt = _desktop_delivery_succeeded_today()
    if desktop_receipt:
        result = {
            "success": True,
            "skipped": True,
            "trigger": "scheduled-fallback",
            "completed_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "reason": "오늘 Codex desktop Telegram 발송 영수증이 확인되어 중복 발송을 방지했습니다.",
            "desktop_receipt_path": str(DESKTOP_RECEIPT_PATH),
            "telegram_message_ids": (desktop_receipt.get("telegram") or {}).get("message_ids") or [],
            "api_call_count": 0,
        }
        history_path = _write_result(result)
        print(json.dumps({"success": True, "skipped": True, "result_path": str(history_path)}, ensure_ascii=False))
        return 0

    result = run_pipeline(
        send=True,
        trigger="scheduled",
        news_days=7,
        news_max_articles=12,
        transaction_limit=5,
        skip_transactions=False,
        refresh_cache=True,
        channel_overrides={
            "telegram": True,
            "sms": False,
            "instagram": False,
            "kakao": False,
        },
        send_prompt_files=True,
        llm_overrides=dict(DEFAULT_DISABLED_LLM_OVERRIDES),
        output_mode="both",
    )
    result["mode"] = "deterministic-telegram-fallback"
    result["api_call_count"] = 0
    history_path = _write_result(result)
    print(json.dumps({"success": _delivery_ok(result), "result_path": str(history_path)}, ensure_ascii=False))
    return 0 if _delivery_ok(result) else 1


if __name__ == "__main__":
    raise SystemExit(main())

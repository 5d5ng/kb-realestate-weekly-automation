"""시작 배너 — 현재 설정 요약 출력"""
from __future__ import annotations

from typing import Any

BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"
LINE = "=" * 48


def _status_icon(enabled: bool) -> str:
    return f"{GREEN}✓{RESET}" if enabled else f"{DIM}off{RESET}"


def print_banner(
    *,
    mode: str = "수동 실행 전용",
    url: str | None = None,
    debug: bool = False,
) -> None:
    from db_backend import get_database_backend_snapshot
    from reporters.common import get_generation_plan
    from sender import SEND_INSTAGRAM_ENABLED, SEND_SMS_ENABLED, SEND_TELEGRAM_ENABLED

    db = get_database_backend_snapshot()
    plan = get_generation_plan()

    # LLM 요약
    llm_parts: list[str] = []
    for task_name, config in plan.items():
        provider = config.get("provider", "none")
        if provider != "none":
            model = config.get("model", "")
            llm_parts.append(f"{provider}/{model}")
    llm_summary = ", ".join(dict.fromkeys(llm_parts)) if llm_parts else "없음 (템플릿 fallback)"

    # DB 요약
    db_backend = db.get("backend", "sqlite")
    db_detail = f"turso ({db.get('turso_url_masked', '')})" if db_backend == "turso" else "sqlite (local)"

    print(f"\n{BOLD}{LINE}{RESET}")
    print(f"{BOLD}  KB부동산 자동화 로컬 실행기{RESET}")
    print(f"{BOLD}{LINE}{RESET}")
    print(f"  Mode:       {CYAN}{mode}{RESET}" + (f" {YELLOW}[debug]{RESET}" if debug else ""))
    if url:
        print(f"  URL:        {CYAN}{url}{RESET}")
    print(f"  LLM:        {llm_summary}")
    print(f"  DB:         {db_detail}")
    print(f"  Telegram:   {_status_icon(SEND_TELEGRAM_ENABLED)} ", end="")
    print(f"  SMS: {_status_icon(SEND_SMS_ENABLED)} ", end="")
    print(f"  Instagram: {_status_icon(SEND_INSTAGRAM_ENABLED)}")
    print(f"  Log:        logs/pipeline.log")
    print(f"{BOLD}{LINE}{RESET}\n")

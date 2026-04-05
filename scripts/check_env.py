#!/usr/bin/env python3
"""환경변수 사전 점검 스크립트.

3단계로 분류하여 현재 설정 상태를 보여준다.
- 필수: 파이프라인 실행에 반드시 필요
- 발송용: 실제 채널 발송 시 필요
- 선택: 없어도 동작하지만 있으면 유용
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv

load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.example", override=False)

# ANSI colors
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
DIM = "\033[2m"
RESET = "\033[0m"
BOLD = "\033[1m"

TIERS: list[tuple[str, str, list[str]]] = [
    (
        "필수 (파이프라인 실행)",
        "critical",
        [
            "NAVER_CLIENT_ID",
            "NAVER_CLIENT_SECRET",
        ],
    ),
    (
        "LLM (1개 이상 필요)",
        "llm",
        [
            "OPENAI_API_KEY",
            "GEMINI_API_KEY",
            "ANTHROPIC_API_KEY",
        ],
    ),
    (
        "발송 — 텔레그램",
        "delivery",
        [
            "TELEGRAM_BOT_TOKEN",
            "TELEGRAM_CHAT_ID",
        ],
    ),
    (
        "발송 — SMS/알림톡",
        "delivery",
        [
            "SOLAPI_API_KEY",
            "SOLAPI_API_SECRET",
            "SOLAPI_SENDER",
            "SOLAPI_DEFAULT_RECIPIENTS",
        ],
    ),
    (
        "선택 — 데이터베이스",
        "optional",
        [
            "TURSO_DATABASE_URL",
            "TURSO_AUTH_TOKEN",
        ],
    ),
    (
        "선택 — 인스타그램",
        "optional",
        [
            "META_ACCESS_TOKEN",
            "META_INSTAGRAM_ID",
        ],
    ),
]


def _mask(value: str) -> str:
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def _get_status(name: str) -> tuple[str, str]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return "missing", ""
    return "set", value.strip()


def main() -> int:
    print(f"\n{BOLD}========================================{RESET}")
    print(f"{BOLD}  환경변수 점검{RESET}")
    print(f"{BOLD}========================================{RESET}\n")

    has_critical_error = False
    has_llm_key = False

    for tier_label, tier_type, variables in TIERS:
        print(f"  {BOLD}[{tier_label}]{RESET}")
        tier_all_set = True

        for name in variables:
            status, value = _get_status(name)
            if status == "set":
                icon = f"{GREEN}✓{RESET}"
                display = f"{DIM}{_mask(value)}{RESET}"
                if tier_type == "llm":
                    has_llm_key = True
            else:
                icon = f"{RED}✗{RESET}" if tier_type == "critical" else f"{YELLOW}−{RESET}"
                display = f"{DIM}not set{RESET}"
                tier_all_set = False
                if tier_type == "critical":
                    has_critical_error = True

            print(f"    {icon} {name:<35} {display}")

        print()

    if not has_llm_key:
        print(f"  {YELLOW}⚠ LLM API 키가 하나도 설정되지 않았습니다. 템플릿 fallback으로 동작합니다.{RESET}\n")

    if has_critical_error:
        print(f"  {RED}✗ 필수 환경변수가 누락되었습니다. .env 파일을 확인하세요.{RESET}\n")
        return 1

    print(f"  {GREEN}✓ 필수 환경변수가 모두 설정되었습니다.{RESET}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
로컬 수동 테스트용 파이프라인 실행 스크립트.

이 파일은 개발 중에 터미널에서 바로 실행하기 위한 진입점이다.
기본값은 dry-run 이므로 발송 없이 전체 흐름만 점검한다.

실행 전 준비:
1. 프로젝트 루트의 `.env` 파일에 필요한 키를 넣는다.
2. `pip install -r requirements.txt` 로 의존성을 설치한다.
3. 필요하면 `downloads/` 와 `reports/` 폴더 생성 여부를 확인한다.

가장 많이 쓰는 실행 예시:
1. 발송 없이 전체 파이프라인 점검
   python scripts/run_local_pipeline_test.py

2. 최근 1일 뉴스 / 기사 3건 / 실거래 2건으로 빠르게 테스트
   python scripts/run_local_pipeline_test.py --news-days 1 --news-max-articles 3 --transaction-limit 2

3. 실제 텔레그램 / SMS 발송까지 포함해서 테스트
   python scripts/run_local_pipeline_test.py --send

4. 결과를 JSON 파일로도 저장
   python scripts/run_local_pipeline_test.py --output reports/local_test_result.json

주의:
- `--send` 를 주면 텔레그램과 SOLAPI SMS 가 실제 발송된다.
- 처음에는 `--send` 없이 dry-run 으로 확인하는 것을 권장한다.
- 프롬프트 파일은 실행 후 `reports/prompts/` 아래에 저장된다.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scheduler import run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="KB부동산 자동화 파이프라인 로컬 테스트 실행기"
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="실제 발송까지 수행합니다. 지정하지 않으면 dry-run 입니다.",
    )
    parser.add_argument(
        "--news-days",
        type=int,
        default=1,
        help="뉴스 수집 기간(일). 기본값은 1일입니다.",
    )
    parser.add_argument(
        "--news-max-articles",
        type=int,
        default=3,
        help="수집할 최대 뉴스 수. 기본값은 3건입니다.",
    )
    parser.add_argument(
        "--transaction-limit",
        type=int,
        default=2,
        help="지역/타입별 최근 실거래 최대 건수. 기본값은 2건입니다.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="결과 JSON 을 저장할 파일 경로. 예: reports/local_test_result.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    print("[local-test] 실행 시작")
    print(
        "[local-test] 옵션 "
        f"send={args.send}, "
        f"news_days={args.news_days}, "
        f"news_max_articles={args.news_max_articles}, "
        f"transaction_limit={args.transaction_limit}",
        flush=True,
    )

    result = run_pipeline(
        send=args.send,
        news_days=args.news_days,
        news_max_articles=args.news_max_articles,
        transaction_limit=args.transaction_limit,
    )

    result_text = json.dumps(result, ensure_ascii=False, indent=2)
    print(result_text)

    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(result_text + "\n", encoding="utf-8")
        print(f"[local-test] 결과 저장: {output_path}")

    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())

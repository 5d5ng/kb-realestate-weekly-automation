#!/usr/bin/env python3
"""
로컬 전용 웹 실행 스크립트.

이 스크립트는 Flask 앱을 로컬 개발 서버로 띄운다.
브라우저에서 실행 버튼을 눌러 dry-run 또는 실제 발송 테스트를 할 수 있다.

가장 쉬운 사용법:
1. 로컬 웹 실행
   python scripts/run_local_web.py

2. 브라우저 접속
   http://127.0.0.1:5000

3. 화면에서
   - Dry Run 실행
   - 실제 발송 실행
   중 하나를 선택한다.

옵션:
- 기본값은 스케줄러를 켜지 않는다.
- 즉, 로컬에서는 예약 실행 없이 수동 테스트만 하기 편한 모드다.
- 배포 환경처럼 스케줄러까지 같이 보고 싶으면 `--with-scheduler` 를 사용한다.

예시:
1. 로컬 수동 테스트 전용
   python scripts/run_local_web.py

2. 스케줄러까지 포함해서 실행
   python scripts/run_local_web.py --with-scheduler

3. 포트 변경
   python scripts/run_local_web.py --port 5050
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app import app
from scheduler import init_scheduler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KB부동산 자동화 로컬 웹 실행기")
    parser.add_argument("--host", default="127.0.0.1", help="바인딩 호스트. 기본값은 127.0.0.1 입니다.")
    parser.add_argument("--port", type=int, default=5000, help="바인딩 포트. 기본값은 5000 입니다.")
    parser.add_argument(
        "--with-scheduler",
        action="store_true",
        help="로컬에서도 APScheduler 를 함께 실행합니다.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.with_scheduler:
        os.environ["ENABLE_SCHEDULER"] = "1"
        init_scheduler()
        print("[local-web] 스케줄러 포함 모드로 실행합니다.", flush=True)
    else:
        os.environ["ENABLE_SCHEDULER"] = "0"
        print("[local-web] 수동 실행 전용 모드입니다. 스케줄러는 시작하지 않습니다.", flush=True)

    print(f"[local-web] 브라우저 접속: http://{args.host}:{args.port}", flush=True)
    app.run(host=args.host, port=args.port, debug=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

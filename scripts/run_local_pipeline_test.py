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

from log_config import setup_logging
from scheduler import run_pipeline
from startup_banner import print_banner


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
    parser.add_argument(
        "--json",
        action="store_true",
        help="결과를 raw JSON 으로만 출력합니다 (스크립트 연동용).",
    )
    return parser.parse_args()


def _print_rich_result(result: dict) -> None:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    console = Console()

    # 결과 요약 패널
    success = result.get("success", False)
    status_style = "bold green" if success else "bold red"
    status_text = "SUCCESS" if success else "FAILED"
    duration = result.get("duration_sec", "?")

    console.print()
    console.print(
        Panel(
            f"[{status_style}]{status_text}[/]  |  소요시간: {duration}s  |  trigger: {result.get('trigger', '?')}",
            title="파이프라인 결과",
            border_style="green" if success else "red",
        )
    )

    # 분석 요약
    analysis = result.get("analysis_summary")
    if analysis:
        table = Table(title="KB 분석 요약", show_header=True, header_style="bold cyan")
        table.add_column("항목", style="bold")
        table.add_column("값", justify="right")
        table.add_row("기준일", str(analysis.get("latest_date", "")))
        table.add_row("매매 상위", str(analysis.get("sale_top5", 0)))
        table.add_row("매매 하위", str(analysis.get("sale_bottom5", 0)))
        table.add_row("전세 상위", str(analysis.get("rent_top5", 0)))
        table.add_row("전세 하위", str(analysis.get("rent_bottom5", 0)))
        console.print(table)

    # 실거래 요약
    tx = result.get("transaction_summary")
    if tx:
        table = Table(title="실거래 요약", show_header=True, header_style="bold cyan")
        table.add_column("항목", style="bold")
        table.add_column("값", justify="right")
        table.add_row("버킷 수", str(tx.get("bucket_count", 0)))
        table.add_row("지역 수", str(tx.get("region_count", 0)))
        table.add_row("거래 건수", str(tx.get("trade_count", 0)))
        console.print(table)

    # 뉴스 요약
    news = result.get("news_summary")
    if news:
        table = Table(title="뉴스 요약", show_header=True, header_style="bold cyan")
        table.add_column("항목", style="bold")
        table.add_column("값")
        table.add_row("수집 건수", str(news.get("count", 0)))
        publishers = news.get("publishers", {})
        if publishers:
            pub_text = ", ".join(f"{k}({v})" for k, v in publishers.items())
            table.add_row("언론사", pub_text)
        titles = news.get("top_titles", [])
        for i, title in enumerate(titles[:5]):
            table.add_row(f"기사 {i+1}", title)
        console.print(table)

    # 콘텐츠 생성 요약
    contents = result.get("contents_summary")
    if contents:
        meta = contents.get("generation_meta", {})
        if meta:
            table = Table(title="콘텐츠 생성", show_header=True, header_style="bold cyan")
            table.add_column("작업", style="bold")
            table.add_column("프로바이더")
            table.add_column("모델")
            table.add_column("LLM 사용", justify="center")
            for task_name, info in meta.items():
                used = "[green]Y[/]" if info.get("used_llm") else "[dim]N[/]"
                table.add_row(
                    task_name,
                    str(info.get("provider", "")),
                    str(info.get("model", "")),
                    used,
                )
            console.print(table)

    # 발송 결과
    send_results = result.get("send_results")
    if send_results and not send_results.get("skipped"):
        table = Table(title="발송 결과", show_header=True, header_style="bold cyan")
        table.add_column("채널", style="bold")
        table.add_column("결과")
        table.add_column("상세")
        for channel, info in send_results.items():
            if not isinstance(info, dict):
                continue
            ok = info.get("success", False)
            skipped = info.get("skipped", False)
            if skipped:
                style = "yellow"
                status = "SKIP"
            elif ok:
                style = "green"
                status = "OK"
            else:
                style = "red"
                status = "FAIL"
            table.add_row(channel, f"[{style}]{status}[/]", str(info.get("detail", "")))
        console.print(table)

    # 프롬프트 파일 경로
    prompt_files = result.get("contents_summary", {}).get("prompt_files") or {}
    artifact_files = result.get("artifact_files", [])
    if prompt_files or artifact_files:
        console.print("\n[bold]생성된 파일:[/]")
        for name, path in prompt_files.items():
            console.print(f"  [dim]{name}:[/] {path}")
        for path in artifact_files:
            if path not in prompt_files.values():
                console.print(f"  [dim]artifact:[/] {path}")

    console.print()


def main() -> int:
    setup_logging()
    args = parse_args()

    if not args.json:
        mode = "실제 발송" if args.send else "dry-run"
        print_banner(mode=f"CLI {mode}")
        print(
            f"[local-test] send={args.send}, "
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

    if args.json:
        print(result_text)
    else:
        _print_rich_result(result)

    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(result_text + "\n", encoding="utf-8")
        if not args.json:
            print(f"[local-test] 결과 저장: {output_path}")

    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())

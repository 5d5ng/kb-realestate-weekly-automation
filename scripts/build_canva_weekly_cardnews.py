#!/usr/bin/env python3
"""Build a 16-page Canva-import HTML from a reviewed weekly report.

The Markdown report is the editorial source. The snapshot is used only for
factual cross-validation, and the Codex Desktop social-copy file supplies the
caption-friendly cover, insight, and conclusion prose. This renderer performs
no publishing and no LLM API calls.
"""
from __future__ import annotations

import html
import argparse
from pathlib import Path
from typing import Any, Optional

try:
    from scripts.weekly_report_social import CANVA_PAGE_COUNT, load_and_validate_report, load_social_copy
except ModuleNotFoundError:  # Direct ``python scripts/...py`` execution.
    from weekly_report_social import CANVA_PAGE_COUNT, load_and_validate_report, load_social_copy


BASE_DIR = Path(__file__).resolve().parent.parent
REPORT_PATH = BASE_DIR / "reports" / "weekly_report.md"
SNAPSHOT_PATH = BASE_DIR / "reports" / "data_snapshot.json"
OUTPUT_DIR = BASE_DIR / "outputs"

def esc(value: Any) -> str:
    return html.escape(str(value or ""))


def short_region(value: str) -> str:
    replacements = {
        "서울특별시 ": "서울 ",
        "경기도 ": "경기 ",
        "인천광역시 ": "인천 ",
        "부산광역시 ": "부산 ",
        "울산광역시 ": "울산 ",
        "경상북도 ": "경북 ",
        "경상남도 ": "경남 ",
        "전북특별자치도 ": "전북 ",
    }
    result = value
    for source, target in replacements.items():
        result = result.replace(source, target)
    return result


def streak(value: Any) -> str:
    return f"연속 상승 {int(value or 0)}주"


def money(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    return f"{int(value):,}만원"


def representative_trade(region: str, trades: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    for area in ("84", "59"):
        for trade in trades:
            if trade.get("region") == region and trade.get("area_type") == area and trade.get("sale_price") is not None:
                return trade
    return None


def page(label: str, body: str, notes: str) -> str:
    return (
        f'<section class="page" data-document-role="page" '
        f'data-label="{esc(label)}" data-speaker-notes="{esc(notes)}">{body}</section>'
    )


def bucket_page(
    slide_number: int,
    key: str,
    title: str,
    insight: str,
    items: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    latest_date: str,
) -> str:
    metric_rows: list[str] = []
    trade_rows: list[str] = []
    for index, item in enumerate(items, start=1):
        region = str(item.get("region") or "")
        metric_rows.append(
            "<tr>"
            f"<td class='rank'>{index}</td><td>{esc(short_region(region))}</td>"
            f"<td class='value'>{streak(item.get('consecutive_rise_weeks'))}</td>"
            "</tr>"
        )
        trade = representative_trade(region, transactions)
        if trade:
            trade_rows.append(
                "<tr>"
                f"<td>{esc(short_region(region))}</td>"
                f"<td>{esc(trade.get('name'))}</td>"
                f"<td>{esc(trade.get('area_type'))}㎡</td>"
                f"<td>{money(trade.get('sale_price'))}</td>"
                f"<td>{money(trade.get('rent_price'))}</td>"
                "</tr>"
            )
        else:
            trade_rows.append(
                "<tr>"
                f"<td>{esc(short_region(region))}</td><td colspan='4' class='empty'>최근 거래 없음</td>"
                "</tr>"
            )

    body = f"""
    <div class="eyebrow">KB WEEKLY · {slide_number:02d}/16</div>
    <h1>{esc(title)}</h1>
    <p class="insight">{esc(insight)}</p>
    <div class="grid">
      <div class="panel">
        <h2>연속 상승 현황</h2>
        <table class="metrics"><thead><tr><th>#</th><th>지역</th><th>연속 상승</th></tr></thead>
        <tbody>{''.join(metric_rows)}</tbody></table>
      </div>
      <div class="panel trade-panel">
        <h2>지역별 대표 실거래</h2>
        <table class="trades"><thead><tr><th>지역</th><th>단지</th><th>타입</th><th>매매</th><th>전세</th></tr></thead>
        <tbody>{''.join(trade_rows)}</tbody></table>
      </div>
    </div>
    <div class="source">KB부동산 주간시계열 {esc(latest_date)} · weekly_report.md 검증본</div>
    """
    return page(title, body, f"{title}. 지역별 연속 상승 기간과 대표 실거래를 함께 표시했습니다.")


def build(
    report_path: Path,
    social_copy_path: Path,
    snapshot_path: Path = SNAPSHOT_PATH,
    output_path: Optional[Path] = None,
) -> Path:
    report = load_and_validate_report(report_path, snapshot_path)
    social_copy = load_social_copy(social_copy_path, report)
    latest_date = report["latest_date"]
    bucket_map = {item["key"]: item for item in report["buckets"]}
    slides = social_copy["slides"]

    empty_metric = {"region": "상승 지역 없음", "consecutive_rise_weeks": 0}
    cap_sale = next(iter(bucket_map["capital_sale_top5"]["metrics"]), empty_metric)
    cap_rent = next(iter(bucket_map["capital_rent_top5"]["metrics"]), empty_metric)
    noncap_sale = next(iter(bucket_map["non_capital_sale_top5"]["metrics"]), empty_metric)

    pages = [
        page(
            "01 표지",
            f"""
            <div class="cover-mark">KB부동산 주간 브리프</div>
            <div class="date">{esc(latest_date)}</div>
            <h1 class="cover-title">{esc(slides[0]['title'])}</h1>
            <p class="cover-headline">{esc(slides[0]['body'])}</p>
            <div class="cover-stats">
              <div><span>수도권 매매 1위</span><b>{esc(short_region(cap_sale['region']))}</b><em>{streak(cap_sale['consecutive_rise_weeks'])}</em></div>
              <div><span>비수도권 매매 1위</span><b>{esc(short_region(noncap_sale['region']))}</b><em>{streak(noncap_sale['consecutive_rise_weeks'])}</em></div>
            </div>
            <div class="source">KB부동산 주간시계열 · 서울 25개 구 · 수도권/비수도권 상승 상위</div>
            """,
            "날짜를 가장 크게 표시한 표지입니다.",
        )
    ]

    slide_number = 2
    for bucket in report["buckets"]:
        metrics = bucket["metrics"]
        chunks = [metrics[index:index + 5] for index in range(0, len(metrics), 5)] or [[]]
        for chunk_index, chunk in enumerate(chunks, start=1):
            social_slide = slides[slide_number - 1]
            suffix = f" ({chunk_index}/{len(chunks)})" if len(chunks) > 1 else ""
            pages.append(
                bucket_page(
                    slide_number,
                    bucket["key"],
                    f"{bucket['label']}{suffix}",
                    social_slide["body"],
                    chunk,
                    bucket["transactions"],
                    latest_date,
                )
            )
            slide_number += 1

    news_lines = []
    for item in report["news"][:3]:
        news_lines.append(
            f"<li><b>{esc(item.get('publisher') or '언론사')}</b><span>{esc(item.get('title'))}</span></li>"
        )
    pages.append(
        page(
            "16 결론",
            f"""
            <div class="eyebrow">KB WEEKLY · 16/16</div>
            <h1>{esc(slides[15]['title'])}</h1>
            <p class="insight">{esc(slides[15]['body'])}</p>
            <div class="closing-grid">
              <div><span>수도권 매매 상승 1위</span><b>{esc(short_region(cap_sale['region']))} {streak(cap_sale['consecutive_rise_weeks'])}</b></div>
              <div><span>수도권 전세 상승 1위</span><b>{esc(short_region(cap_rent['region']))} {streak(cap_rent['consecutive_rise_weeks'])}</b></div>
              <div><span>비수도권 매매 상승 1위</span><b>{esc(short_region(noncap_sale['region']))} {streak(noncap_sale['consecutive_rise_weeks'])}</b></div>
            </div>
            <h2 class="news-title">함께 볼 시장 뉴스</h2>
            <ul class="news-list">{''.join(news_lines)}</ul>
            <p class="disclaimer">{esc(report['disclaimer'])}</p>
            """,
            "6개 상승 섹션의 결론과 함께 최신 시장 뉴스 3건을 요약했습니다.",
        )
    )
    if len(pages) != CANVA_PAGE_COUNT:
        raise ValueError(f"Canva page assembly produced {len(pages)} pages, expected {CANVA_PAGE_COUNT}")

    css = """
    *{box-sizing:border-box} body{margin:0;background:#dfe5ec;font-family:Pretendard,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#142033}
    .page{width:1080px;height:1080px;margin:28px auto;padding:64px 66px;background:#f7f9fc;position:relative;overflow:hidden;border-top:18px solid #173b61}
    .page:after{content:"";position:absolute;right:0;bottom:0;width:220px;height:18px;background:#d5a33d}
    .eyebrow,.cover-mark{font-size:22px;font-weight:800;letter-spacing:2px;color:#365f87}.date{font-size:94px;font-weight:900;letter-spacing:-4px;margin-top:72px;color:#173b61}
    h1{font-size:50px;line-height:1.08;letter-spacing:-2px;margin:22px 0 12px}.cover-title{font-size:88px;margin:18px 0}.cover-headline{font-size:32px;font-weight:700;color:#7b5715;border-top:3px solid #d5a33d;padding-top:20px}
    .cover-stats{display:grid;grid-template-columns:1fr 1fr;gap:22px;margin-top:44px}.cover-stats div,.closing-grid div{background:white;border:1px solid #dce3ec;padding:25px}.cover-stats span,.closing-grid span{display:block;font-size:19px;color:#637083;font-weight:700}.cover-stats b,.closing-grid b{display:block;font-size:28px;margin-top:12px}.cover-stats em{display:block;font-size:38px;font-style:normal;font-weight:900;color:#173b61;margin-top:9px}
    .insight{font-size:25px;line-height:1.35;color:#56657a;margin:0 0 24px}.grid{display:grid;gap:20px}.panel{background:white;border:1px solid #dce3ec;padding:18px 22px}.panel h2{font-size:23px;color:#173b61;margin:0 0 12px}.trade-panel{padding-bottom:14px}
    table{width:100%;border-collapse:collapse;table-layout:fixed}th{font-size:16px;color:#667085;text-align:left;padding:7px 9px;border-bottom:2px solid #dce3ec}td{font-size:18px;padding:9px;border-bottom:1px solid #e8edf3;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.metrics th:nth-child(1),.metrics td:nth-child(1){width:8%}.metrics th:nth-child(2),.metrics td:nth-child(2){width:57%}.metrics th:nth-child(3),.metrics td:nth-child(3){width:35%}.rank{font-weight:900;color:#d5a33d}.value{font-weight:900;color:#173b61}
    .trades th:nth-child(1),.trades td:nth-child(1){width:24%}.trades th:nth-child(2),.trades td:nth-child(2){width:31%}.trades th:nth-child(3),.trades td:nth-child(3){width:10%}.trades th:nth-child(4),.trades td:nth-child(4),.trades th:nth-child(5),.trades td:nth-child(5){width:17.5%}.trades td{font-size:15px}.empty{color:#8b95a5}.source{position:absolute;left:66px;bottom:35px;font-size:17px;color:#758094}
    .closing-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-top:30px}.closing-grid b{font-size:22px}.news-title{font-size:25px;margin:38px 0 12px;color:#173b61}.news-list{list-style:none;padding:0;margin:0;display:grid;gap:10px}.news-list li{display:grid;grid-template-columns:105px 1fr;gap:12px;background:#fff;border-left:6px solid #d5a33d;padding:12px 16px}.news-list b{font-size:17px}.news-list span{font-size:18px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.disclaimer{font-size:16px;line-height:1.4;color:#6f7b8e;margin-top:22px}
    """
    document = f"<!doctype html><html lang='ko'><head><meta charset='utf-8'><title>KB부동산 주간 카드뉴스 {esc(latest_date)}</title><style>{css}</style></head><body>{''.join(pages)}</body></html>"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    selected_output = (
        output_path.expanduser().resolve()
        if output_path is not None
        else OUTPUT_DIR / f"{latest_date}_kb_weekly_cardnews_codex_desktop_canva_import.html"
    )
    selected_output.parent.mkdir(parents=True, exist_ok=True)
    selected_output.write_text(document, encoding="utf-8")
    return selected_output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="weekly_report.md와 Codex 소셜 카피로 카드뉴스 16장을 생성")
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--social-copy", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    print(build(args.report, args.social_copy, args.snapshot, args.output))

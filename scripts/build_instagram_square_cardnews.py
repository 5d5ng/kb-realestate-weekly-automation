#!/usr/bin/env python3
from __future__ import annotations

import html
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "outputs"
DB_PATH = BASE_DIR / "data" / "cache_store.sqlite3"
EXCEL_PATH = BASE_DIR / "downloads" / "20260720_주간시계열.xlsx"

import sys

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from analyzer import extract_content_regions, parse_excel


REGION_NAME_ALIASES = {
    "서울특별시": "서울시",
    "부산광역시": "부산시",
    "대구광역시": "대구시",
    "인천광역시": "인천시",
    "광주광역시": "광주시",
    "대전광역시": "대전시",
    "울산광역시": "울산시",
    "전북특별자치도": "전라북도",
}


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_sido(name: str) -> str:
    return REGION_NAME_ALIASES.get(name, name)


def split_region(region: str) -> tuple[str, str, str]:
    parts = clean_text(region).split()
    if len(parts) < 2:
        return "", "", ""
    sido = normalize_sido(parts[0])
    sigungu = parts[1]
    extra = " ".join(parts[2:])
    if extra and sigungu.endswith("시") and extra.endswith("구"):
        extra = extra[:-1]
    return sido, sigungu, extra


def money_eok(value: Any) -> str:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return "-"
    if number >= 10000:
        eok_text = f"{number / 10000:.2f}".rstrip("0").rstrip(".")
        return f"{eok_text}억원"
    return f"{number:,}만원"


def streak(value: Any) -> str:
    return f"연속 상승 {int(value or 0)}주"


def cache_key_parts(cache_key: str, kind: str) -> tuple[int, int] | None:
    match = re.match(rf"realestate:{kind}:(\d+):(\d+):", cache_key)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def contract_date(row: dict[str, Any]) -> str:
    year = clean_text(row.get("계약년"))
    month = clean_text(row.get("계약월"))
    day = clean_text(row.get("계약일"))
    if year and month and day:
        return f"{year}.{int(month):02d}.{int(day):02d}"
    ym = clean_text(row.get("계약년월"))
    if len(ym) == 6 and day:
        return f"{ym[:4]}.{ym[4:]}.{int(day):02d}"
    return clean_text(row.get("조회기간")) or "-"


def date_sort_key(value: str) -> tuple[int, int, int]:
    numbers = [int(part) for part in re.findall(r"\d+", value or "")]
    if len(numbers) >= 3:
        return numbers[0], numbers[1], numbers[2]
    if len(numbers) == 2:
        return numbers[0], numbers[1], 0
    return 0, 0, 0


def load_cached_trades(kind: str, selected_regions: list[str], per_region: int = 3) -> dict[str, list[dict[str, Any]]]:
    region_specs = [(region, *split_region(region)) for region in selected_regions]
    results = {region: [] for region in selected_regions}
    seen = {region: set() for region in selected_regions}
    if not DB_PATH.exists():
        return results

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT cache_key, payload_json, updated_at
        FROM kv_cache
        WHERE cache_key LIKE ?
          AND length(payload_json) > 10
        ORDER BY updated_at DESC
        """,
        (f"realestate:{kind}:%",),
    ).fetchall()

    for row in rows:
        ids = cache_key_parts(row["cache_key"], kind)
        if not ids:
            continue
        complex_id, area_id = ids
        complex_row = conn.execute("SELECT * FROM complexes WHERE complex_id = ?", (complex_id,)).fetchone()
        type_row = conn.execute("SELECT * FROM complex_types WHERE area_id = ?", (area_id,)).fetchone()
        if not complex_row or not type_row:
            continue

        for region, sido, sigungu, extra in region_specs:
            if complex_row["sido_name"] != sido or complex_row["sigungu_name"] != sigungu:
                continue
            dong = clean_text(complex_row["dong_name"])
            if extra and extra not in dong and extra not in clean_text(complex_row["address"]):
                continue
            payload = json.loads(row["payload_json"] or "[]")
            if not payload:
                continue
            trade = payload[0]
            price_key = "매매실거래금액" if kind == "trade" else "전세실거래금액"
            price = trade.get(price_key)
            if not price:
                continue
            trade_date = contract_date(trade)
            identity = (
                region,
                clean_text(complex_row["complex_name"]),
                round(float(type_row["exclusive_area"] or 0), 2),
                trade_date,
                int(price),
            )
            if identity in seen[region]:
                continue
            seen[region].add(identity)
            results[region].append(
                {
                    "region": region,
                    "dong": dong,
                    "complex_name": clean_text(complex_row["complex_name"]),
                    "exclusive_area": round(float(type_row["exclusive_area"] or 0), 2),
                    "contract_date": trade_date,
                    "date_sort_key": date_sort_key(trade_date),
                    "floor": clean_text(trade.get("해당층수")),
                    "price": int(price),
                    "price_text": money_eok(price),
                    "kind": "매매" if kind == "trade" else "전세",
                    "cache_updated_at": row["updated_at"],
                }
            )
    conn.close()
    for region, trades in results.items():
        trades.sort(key=lambda item: item["date_sort_key"], reverse=True)
        results[region] = trades[:per_region]
    return results


def first_items(content_regions: dict[str, Any], bucket: str, limit: int = 3) -> list[dict[str, Any]]:
    return list((content_regions.get(bucket) or [])[:limit])


def trade_line(trade: dict[str, Any] | None) -> str:
    if not trade:
        return "캐시 기준 확인 거래 없음"
    floor = f" {trade['floor']}층" if trade.get("floor") else ""
    return (
        f"{trade['dong']} {trade['complex_name']} "
        f"{trade['exclusive_area']:.2f}㎡{floor} · {trade['contract_date']} · {trade['price_text']}"
    )


def page(title: str, subtitle: str, body: str, notes: str, label: str, extra_class: str = "") -> str:
    return f"""
<section class="page {extra_class}" data-document-role="page" data-label="{html.escape(label)}" data-speaker-notes="{html.escape(notes)}">
  <div class="topline">
    <span>KB부동산 주간 통계</span>
    <span>실거래 포함</span>
  </div>
  <h1>{title}</h1>
  <p class="subtitle">{subtitle}</p>
  {body}
</section>
""".strip()


def region_rows(items: list[dict[str, Any]]) -> str:
    rows = []
    for index, item in enumerate(items, start=1):
        rows.append(
            f"""
      <div class="rank-row">
        <b>{index}</b>
        <span>{html.escape(clean_text(item.get("region")))}</span>
        <strong>{streak(item.get("consecutive_rise_weeks"))}</strong>
      </div>
""".rstrip()
        )
    return "\n".join(rows)


def trade_rows(trades: list[dict[str, Any]], *, limit: int = 3) -> str:
    rows = []
    for trade in trades[:limit]:
        rows.append(
            f"""
      <div class="trade-row">
        <span>{html.escape(trade['region'])}</span>
        <b>{html.escape(trade['complex_name'])}</b>
        <em>{trade['exclusive_area']:.2f}㎡</em>
        <strong>{html.escape(trade['price_text'])}</strong>
        <small>{html.escape(trade['contract_date'])}</small>
      </div>
""".rstrip()
        )
    if not rows:
        rows.append('<div class="trade-row empty">표시할 캐시 실거래가 없습니다.</div>')
    return "\n".join(rows)


def build_outputs() -> dict[str, str]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    analysis = parse_excel(EXCEL_PATH)
    analysis["content_regions"] = extract_content_regions(analysis)
    content_regions = analysis["content_regions"]
    latest_date = clean_text(analysis.get("latest_date")) or "2026-07-20"

    focus_regions = []
    for bucket in content_regions:
        for item in content_regions.get(bucket) or []:
            region = clean_text(item.get("region"))
            if region and region not in focus_regions:
                focus_regions.append(region)

    sale_trade_map = load_cached_trades("trade", focus_regions, per_region=3)
    rent_trade_map = load_cached_trades("rent", focus_regions, per_region=2)
    sale_trades = [trade for region in focus_regions for trade in sale_trade_map.get(region, [])]
    rent_trades = [trade for region in focus_regions for trade in rent_trade_map.get(region, [])]

    cap_sale = first_items(content_regions, "capital_sale_top5", 5)
    cap_rent = first_items(content_regions, "capital_rent_top5", 5)
    noncap_sale = first_items(content_regions, "non_capital_sale_top5", 5)
    noncap_rent = first_items(content_regions, "non_capital_rent_top5", 5)

    headline = f"{clean_text(cap_sale[0].get('region'))} 매매 강세, {clean_text(cap_rent[0].get('region'))} 전세 상위권"
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_note = f"KB부동산 주간시계열 {latest_date} · 매매·전세 실거래 포함"

    pages = [
        page(
            "KB부동산 주간 브리프",
            f"{latest_date} 기준 · 정방형 카드뉴스 · 매매·전세 실거래 포함",
            f"""
  <div class="hero-line">{html.escape(headline)}</div>
  <div class="hero-grid">
    <div><span>매매 강세</span><b>{html.escape(clean_text(cap_sale[0].get('region')))}</b><em>{streak(cap_sale[0].get('consecutive_rise_weeks'))}</em></div>
    <div><span>전세 강세</span><b>{html.escape(clean_text(cap_rent[0].get('region')))}</b><em>{streak(cap_rent[0].get('consecutive_rise_weeks'))}</em></div>
  </div>
  <p class="footnote">KB부동산 주간시계열 · 단지별 매매·전세 실거래</p>
""",
            source_note,
            "01 표지",
            "cover",
        ),
    ]

    section_labels = {
        "seoul_sale_all": "서울 25개 구 매매 상승 현황",
        "seoul_rent_all": "서울 25개 구 전세 상승 현황",
        "capital_sale_top5": "수도권 매매 상승 상위 5",
        "capital_rent_top5": "수도권 전세 상승 상위 5",
        "non_capital_sale_top5": "비수도권 매매 상승 상위 5",
        "non_capital_rent_top5": "비수도권 전세 상승 상위 5",
    }
    slide_number = 2
    for bucket_name, title in section_labels.items():
        items = content_regions.get(bucket_name) or []
        chunks = [items[index:index + 5] for index in range(0, len(items), 5)] or [[]]
        selected_trades = rent_trades if "rent" in bucket_name else sale_trades
        for chunk_index, chunk in enumerate(chunks, start=1):
            regions = [clean_text(item.get("region")) for item in chunk]
            suffix = f" ({chunk_index}/{len(chunks)})" if len(chunks) > 1 else ""
            pages.append(page(
                f"{title}{suffix}",
                "지역별 연속 상승 기간과 최근 실거래를 함께 확인합니다.",
                f'<div class="rank-list">{region_rows(chunk)}</div>'
                f'<div class="trade-list">{trade_rows([trade for trade in selected_trades if trade["region"] in regions], limit=5)}</div>',
                source_note,
                f"{slide_number:02d} {title}{suffix}",
            ))
            slide_number += 1

    pages.append(page(
        "이번 주 한 줄 결론",
        "서울 모든 구와 수도권·비수도권 상승 지역의 연속 상승 기간을 실거래와 함께 정리했습니다.",
        f"""
  <div class="closing">
    <b>{html.escape(headline)}</b>
    <p>KB부동산 주간시계열과 단지별 매매·전세 실거래를 기준으로 정리한 {html.escape(latest_date)} 주간 브리프입니다.</p>
  </div>
  <p class="source">출처: KB부동산 주간시계열 {html.escape(latest_date)}, KB 실거래 로컬 캐시</p>
""",
        source_note,
        "16 결론 및 면책",
        "closing-page",
    ))

    css = """
* { box-sizing: border-box; }
body { margin: 0; background: #d9dee6; font-family: Inter, Pretendard, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #172033; }
.page { width: 1080px; height: 1080px; padding: 46px 58px; margin: 28px auto; background: #f7f8fb; position: relative; overflow: hidden; border: 1px solid #dbe2ea; }
.page::before { content: ""; position: absolute; left: 0; top: 0; width: 18px; height: 100%; background: #234a6f; }
.page::after { content: ""; position: absolute; right: 72px; bottom: 54px; width: 180px; height: 4px; background: #d7a64a; }
.topline { display: flex; justify-content: space-between; font-size: 24px; font-weight: 700; color: #5b6676; letter-spacing: 0; }
h1 { margin: 26px 0 12px; font-size: 44px; line-height: 1.12; letter-spacing: 0; color: #142033; max-width: 920px; }
.subtitle { margin: 0 0 18px; font-size: 23px; line-height: 1.3; color: #59657a; max-width: 920px; }
.hero-line { margin-top: 64px; padding: 34px 0; border-top: 3px solid #172033; border-bottom: 3px solid #172033; font-size: 46px; line-height: 1.25; font-weight: 800; color: #172033; }
.hero-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 26px; margin-top: 46px; }
.hero-grid div { background: #ffffff; border: 1px solid #dce3ec; padding: 34px; min-height: 210px; }
.hero-grid span { display: block; font-size: 24px; color: #667085; font-weight: 700; }
.hero-grid b { display: block; margin-top: 22px; font-size: 35px; line-height: 1.18; }
.hero-grid em { display: block; margin-top: 22px; font-size: 42px; font-style: normal; color: #234a6f; font-weight: 900; }
.rank-list, .trade-list, .point-list { display: grid; gap: 5px; }
.trade-list { margin-top: 10px; }
.rank-row { display: grid; grid-template-columns: 48px 1fr 210px; align-items: center; gap: 12px; min-height: 54px; padding: 7px 16px; background: #fff; border: 1px solid #dce3ec; }
.rank-row b { width: 36px; height: 36px; border-radius: 50%; background: #234a6f; color: white; display: grid; place-items: center; font-size: 20px; }
.rank-row span { font-size: 23px; font-weight: 800; color: #172033; }
.rank-row strong { font-size: 21px; color: #1e496f; }
.rank-row em { font-size: 27px; color: #9b6d1e; font-style: normal; font-weight: 800; }
.caution .rank-row b { background: #7b3f39; }
.trade-row { display: grid; grid-template-columns: 155px 1fr 95px 135px 105px; align-items: center; gap: 10px; min-height: 54px; padding: 7px 14px; background: #fff; border-left: 6px solid #234a6f; border-top: 1px solid #dce3ec; border-right: 1px solid #dce3ec; border-bottom: 1px solid #dce3ec; }
.trade-row span { font-size: 17px; color: #5b6676; font-weight: 800; }
.trade-row b { font-size: 18px; color: #172033; line-height: 1.12; }
.trade-row em { font-size: 16px; color: #5b6676; font-style: normal; }
.trade-row strong { font-size: 18px; color: #1e496f; }
.trade-row small { font-size: 15px; color: #7a8494; }
.trade-row.empty { display: block; font-size: 30px; color: #667085; }
.split { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
.split h2 { margin: 0 0 18px; font-size: 30px; color: #234a6f; }
.split .rank-row { grid-template-columns: 54px 1fr; min-height: 96px; }
.split .rank-row strong, .split .rank-row em { display: none; }
.split .rank-row span { font-size: 26px; }
.point-list div { display: grid; grid-template-columns: 70px 1fr; gap: 24px; align-items: start; background: #fff; border: 1px solid #dce3ec; padding: 30px; min-height: 120px; }
.point-list b { width: 54px; height: 54px; border-radius: 50%; background: #d7a64a; color: #172033; display: grid; place-items: center; font-size: 28px; }
.point-list span { font-size: 34px; line-height: 1.28; font-weight: 750; }
.closing { margin-top: 46px; padding: 42px; background: #172033; color: #fff; }
.closing b { display: block; font-size: 44px; line-height: 1.22; }
.closing p { margin: 28px 0 0; font-size: 28px; line-height: 1.4; color: #dbe2ea; }
.source, .footnote { position: absolute; left: 72px; bottom: 54px; max-width: 720px; margin: 0; font-size: 21px; color: #6b7280; line-height: 1.35; }
"""

    html_doc = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KB부동산 인스타그램 정방형 카드뉴스 - {html.escape(latest_date)}</title>
  <style>{css}</style>
</head>
<body>
{chr(10).join(pages)}
</body>
</html>
"""

    post_caption_lines = [
        f"{latest_date} KB부동산 주간 브리프",
        "",
        f"{headline}.",
        "",
        f"수도권 매매 상승 상위 지역은 {clean_text(cap_sale[0].get('region'))}, {clean_text(cap_sale[1].get('region'))}, {clean_text(cap_sale[2].get('region'))}이며 각각의 연속 상승 기간을 확인했습니다.",
        f"수도권 전세 상승 상위 지역은 {clean_text(cap_rent[0].get('region'))}, {clean_text(cap_rent[1].get('region'))}, {clean_text(cap_rent[2].get('region'))}입니다.",
        "",
        f"매매 실거래: {trade_line(sale_trades[0] if sale_trades else None)}",
        f"전세 실거래: {trade_line(rent_trades[0] if rent_trades else None)}",
        "",
        f"비수도권 매매 상위 지역은 {clean_text(noncap_sale[0].get('region'))}, 전세 상위 지역은 {clean_text(noncap_rent[0].get('region'))}입니다.",
        "",
        f"기준: KB부동산 주간시계열 {latest_date}",
        "실거래: KB 실거래 조회 데이터",
        "",
        "#KB부동산 #부동산시장 #아파트실거래가 #부동산카드뉴스 #주간부동산 #아파트매매 #전세시장",
    ]
    alt_text_lines = []
    page_titles = ["표지"]
    for bucket_name, title in section_labels.items():
        section_items = content_regions.get(bucket_name) or []
        page_titles.extend([title] * max(1, (len(section_items) + 4) // 5))
    page_titles.append("결론 및 면책")
    for index, page_title in enumerate(page_titles, start=1):
        alt_text_lines.append(f"{index}. {page_title}:")
        if index == 1:
            alt_text_lines.append(f"   {headline}.")
        else:
            alt_text_lines.append(f"   {page_title}의 지역별 연속 상승 기간과 실거래를 정리한 카드입니다.")

    caption_lines = [
        f"# KB부동산 인스타그램 게시 캡션 - {latest_date}",
        "",
        "## 복사용 최종 캡션",
        "",
        *post_caption_lines,
        "",
        "## 페이지별 대체 텍스트",
        "",
        *alt_text_lines,
    ]
    caption_body = "\n".join(post_caption_lines)
    alt_text_body = "\n".join(alt_text_lines)
    caption_html = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KB부동산 인스타그램 게시 캡션 - {html.escape(latest_date)}</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: #d9dee6; font-family: Pretendard, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #172033; }}
    .caption-page {{ width: 1080px; min-height: 1350px; margin: 28px auto; padding: 72px; background: #f7f8fb; border-left: 18px solid #234a6f; }}
    .eyebrow {{ font-size: 24px; font-weight: 800; color: #234a6f; }}
    h1 {{ margin: 40px 0 44px; font-size: 58px; line-height: 1.15; }}
    h2 {{ margin: 0 0 28px; font-size: 34px; color: #234a6f; }}
    .copy {{ white-space: pre-wrap; font-size: 28px; line-height: 1.58; background: #fff; border: 1px solid #dce3ec; padding: 38px; }}
    .alt {{ white-space: pre-wrap; font-size: 24px; line-height: 1.5; }}
  </style>
</head>
<body>
  <section class="caption-page" data-document-role="page" data-label="게시용 최종 캡션">
    <div class="eyebrow">INSTAGRAM READY COPY</div>
    <h1>KB부동산 게시용 최종 캡션</h1>
    <div class="copy">{html.escape(caption_body)}</div>
  </section>
  <section class="caption-page" data-document-role="page" data-label="페이지별 대체 텍스트">
    <div class="eyebrow">ACCESSIBILITY COPY</div>
    <h1>페이지별 대체 텍스트</h1>
    <div class="alt">{html.escape(alt_text_body)}</div>
  </section>
</body>
</html>
"""

    data = {
        "generated_at": generated_at,
        "latest_date": latest_date,
        "source_excel": str(EXCEL_PATH.relative_to(BASE_DIR)),
        "source_cache": str(DB_PATH.relative_to(BASE_DIR)),
        "focus_regions": focus_regions,
        "content_regions": content_regions,
        "sale_trades": sale_trades,
        "rent_trades": rent_trades,
        "notes": "실거래는 로컬 KB 실거래 캐시에 저장된 최근 조회 결과 중 카드뉴스 핵심 지역만 선별했습니다.",
    }

    stem = f"{latest_date}_kb_realestate_instagram_square_transactions"
    html_path = OUTPUT_DIR / f"{stem}.html"
    data_path = OUTPUT_DIR / f"{stem}_data.json"
    caption_path = OUTPUT_DIR / f"{stem}_captions_script.md"
    caption_html_path = OUTPUT_DIR / f"{stem}_caption_canva.html"
    html_path.write_text(html_doc, encoding="utf-8")
    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    caption_path.write_text("\n".join(caption_lines).rstrip() + "\n", encoding="utf-8")
    caption_html_path.write_text(caption_html, encoding="utf-8")
    return {
        "html": str(html_path),
        "data": str(data_path),
        "captions_script": str(caption_path),
        "caption_canva_html": str(caption_html_path),
        "latest_date": latest_date,
        "sale_trade_count": str(len(sale_trades)),
        "rent_trade_count": str(len(rent_trades)),
    }


if __name__ == "__main__":
    print(json.dumps(build_outputs(), ensure_ascii=False, indent=2))

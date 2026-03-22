from __future__ import annotations

from .common import (
    BUCKET_LABELS,
    build_context,
    clean_text,
    format_news_item,
    format_region_bucket,
    format_trade_item,
    generate_with_llm,
)


def _format_transaction_highlights(
    transactions: dict | None,
    *,
    max_buckets: int = 3,
    max_regions_per_bucket: int = 1,
    max_trades_per_region: int = 1,
) -> list[str]:
    if not transactions:
        return ["- 실거래 정보 없음"]

    lines: list[str] = []

    def _append_region_lines(region_name: str, area_mapping: dict) -> None:
        for area_key, area_info in list(area_mapping.items())[:2]:
            trades = (area_info or {}).get("trades", [])[:max_trades_per_region]
            if not trades:
                continue

            sale_text = format_trade_item(trades[0])
            related_rents = trades[0].get("related_rent_trades") or []
            if related_rents:
                rent_text = format_trade_item(related_rents[0])
                lines.append(f"- {region_name} {area_key}타입: {sale_text} | 최근 전세 {rent_text}")
            else:
                lines.append(f"- {region_name} {area_key}타입: {sale_text}")

    if all(isinstance(value, dict) and all(str(key).isdigit() for key in value.keys()) for value in transactions.values()):
        for region_name, area_mapping in list(transactions.items())[:max_regions_per_bucket]:
            _append_region_lines(region_name, area_mapping)
        return lines or ["- 실거래 정보 없음"]

    for bucket_name, region_mapping in list(transactions.items())[:max_buckets]:
        bucket_label = BUCKET_LABELS.get(bucket_name, bucket_name)
        if not isinstance(region_mapping, dict) or not region_mapping:
            continue
        lines.append(f"- {bucket_label}")
        for region_name, area_mapping in list(region_mapping.items())[:max_regions_per_bucket]:
            _append_region_lines(region_name, area_mapping)

    return lines or ["- 실거래 정보 없음"]


def fallback_telegram_report(analysis: dict, news: list[dict], transactions: dict | None = None) -> str:
    latest_date = analysis.get("latest_date", "")
    sale = analysis.get("sale", {})
    rent = analysis.get("rent", {})
    news_lines = news[:3]
    transaction_lines = _format_transaction_highlights(transactions)

    lines = [
        f"[KB부동산 주간 리포트] ({latest_date})",
        "",
        "1. 매매 흐름",
        f"- 상승 상위: {format_region_bucket(sale.get('top5', []), 3)}",
        f"- 하락 하위: {format_region_bucket(sale.get('bottom5', []), 3)}",
        "",
        "2. 전세 흐름",
        f"- 상승 상위: {format_region_bucket(rent.get('top5', []), 3)}",
        f"- 하락 하위: {format_region_bucket(rent.get('bottom5', []), 3)}",
        "",
        "3. 실거래 체크",
    ]

    lines.extend(transaction_lines)
    lines.extend(["", "4. 주요 뉴스"])

    if news_lines:
        lines.extend(f"- {format_news_item(article)}" for article in news_lines)
    else:
        lines.append("- 주요 뉴스 없음")

    lines.extend(
        [
            "",
            "5. 한줄 요약",
            "- 상위 지역 중심으로 매매 강세가 이어지는 가운데, 실거래와 전세 흐름도 함께 확인할 필요가 있습니다.",
        ]
    )
    return "\n".join(lines)


def generate_telegram_report(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
) -> str:
    fallback = fallback_telegram_report(analysis, news, transactions)
    prompt = (
        "아래 데이터를 기반으로 텔레그램용 한국어 주간 부동산 리포트를 작성해줘.\n"
        "- 문체는 전문적이되 이해하기 쉽게\n"
        "- 반드시 데이터에 있는 내용만 사용\n"
        "- 구조는 제목, 매매 흐름, 전세 흐름, 실거래 체크, 주요 뉴스, 한줄 시사점 순서\n"
        "- 실거래 체크에서는 최근 거래 단지명, 면적, 가격, 최근 전세 흐름을 짧게 요약\n"
        "- 마크다운 친화적으로 작성\n\n"
        f"{build_context(analysis, news, transactions)}"
    )
    system = "너는 한국 부동산 시장 콘텐츠 에디터다. 없는 수치나 사실을 만들지 말고 제공된 데이터만 사용해라."
    return generate_with_llm("telegram_report", system, prompt, fallback_text=fallback)

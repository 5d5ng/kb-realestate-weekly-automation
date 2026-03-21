from __future__ import annotations

from .common import build_context, format_news_item, format_region_bucket, generate_with_llm


def fallback_telegram_report(analysis: dict, news: list[dict]) -> str:
    latest_date = analysis.get("latest_date", "")
    sale = analysis.get("sale", {})
    rent = analysis.get("rent", {})
    news_lines = news[:3]

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
        "3. 주요 뉴스",
    ]

    if news_lines:
        lines.extend(f"- {format_news_item(article)}" for article in news_lines)
    else:
        lines.append("- 주요 뉴스 없음")

    lines.extend(
        [
            "",
            "4. 한줄 요약",
            "- 매매와 전세 모두 지역별 온도차가 큰 장세입니다.",
        ]
    )
    return "\n".join(lines)


def generate_telegram_report(analysis: dict, news: list[dict]) -> str:
    fallback = fallback_telegram_report(analysis, news)
    prompt = (
        "아래 데이터를 기반으로 텔레그램용 한국어 주간 부동산 리포트를 작성해줘.\n"
        "- 문체는 전문적이되 이해하기 쉽게\n"
        "- 반드시 데이터에 있는 내용만 사용\n"
        "- 구조는 제목, 매매 흐름, 전세 흐름, 주요 뉴스, 한줄 시사점 순서\n"
        "- 마크다운 친화적으로 작성\n\n"
        f"{build_context(analysis, news)}"
    )
    system = "너는 한국 부동산 시장 콘텐츠 에디터다. 없는 수치나 사실을 만들지 말고 제공된 데이터만 사용해라."
    return generate_with_llm("telegram_report", system, prompt, fallback_text=fallback)

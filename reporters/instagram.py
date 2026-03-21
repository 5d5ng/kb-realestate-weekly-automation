from __future__ import annotations

from .common import build_context, format_region_bucket, generate_with_llm


def fallback_instagram_caption(analysis: dict, news: list[dict]) -> str:
    latest_date = analysis.get("latest_date", "")
    sale_top = analysis.get("sale", {}).get("top5", [])[:3]
    rent_top = analysis.get("rent", {}).get("top5", [])[:3]
    top_news = news[:2]

    lines = [
        f"{latest_date} KB부동산 주간 체크",
        "",
        f"매매 상위 지역: {format_region_bucket(sale_top, 3)}",
        f"전세 상위 지역: {format_region_bucket(rent_top, 3)}",
        "",
        "이번 주 주요 뉴스",
    ]

    if top_news:
        lines.extend(f"- {article.get('title', '')}" for article in top_news)
    else:
        lines.append("- 주요 뉴스 없음")

    lines.extend(
        [
            "",
            "#KB부동산 #부동산 #아파트 #주간부동산 #전세 #매매 #재건축 #재개발",
        ]
    )
    return "\n".join(lines)


def build_instagram_caption_prompt(analysis: dict, news: list[dict]) -> tuple[str, str]:
    prompt = (
        "아래 데이터를 기반으로 인스타그램 캡션을 한국어로 작성해줘.\n"
        "- 첫 줄은 눈에 띄는 훅\n"
        "- 중간은 3~5줄 요약\n"
        "- 마지막은 해시태그 6~10개\n"
        "- 데이터에 있는 정보만 활용\n\n"
        f"{build_context(analysis, news)}"
    )
    system = "너는 부동산 인스타그램 콘텐츠 에디터다. 짧고 읽기 쉽게 작성하고 과장하지 말아라."
    return system, prompt


def generate_instagram_caption(analysis: dict, news: list[dict]) -> str:
    fallback = fallback_instagram_caption(analysis, news)
    system, prompt = build_instagram_caption_prompt(analysis, news)
    return generate_with_llm("instagram_caption", system, prompt, fallback_text=fallback)

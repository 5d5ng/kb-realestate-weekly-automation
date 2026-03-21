from __future__ import annotations

from .common import build_context, format_region_bucket, generate_with_llm


def fallback_card_news_script(analysis: dict) -> str:
    latest_date = analysis.get("latest_date", "")
    sale = analysis.get("sale", {})
    rent = analysis.get("rent", {})

    slides = [
        f"[슬라이드 1]\n제목: {latest_date} KB부동산 주간시장 요약",
        f"[슬라이드 2]\n매매 상승 상위 3: {format_region_bucket(sale.get('top5', []), 3)}",
        f"[슬라이드 3]\n매매 하위 3: {format_region_bucket(sale.get('bottom5', []), 3)}",
        f"[슬라이드 4]\n전세 상승 상위 3: {format_region_bucket(rent.get('top5', []), 3)}",
        f"[슬라이드 5]\n전세 하위 3: {format_region_bucket(rent.get('bottom5', []), 3)}",
        "[슬라이드 6]\n마무리: 지역별 온도차가 뚜렷한 시장이 이어지고 있습니다.",
    ]
    return "\n\n".join(slides)


def build_card_news_prompt(analysis: dict) -> tuple[str, str]:
    prompt = (
        "아래 데이터를 기반으로 인스타그램 카드뉴스 스크립트를 한국어로 작성해줘.\n"
        "- 6장 구성\n"
        "- 각 장은 [슬라이드 n] 형식으로 시작\n"
        "- 각 장마다 제목 1줄 + 본문 2~3줄 수준\n"
        "- 매매/전세 흐름이 균형 있게 들어가야 함\n\n"
        f"{build_context(analysis, [])}"
    )
    system = "너는 카드뉴스 기획자다. 장표형 텍스트를 명확하게 작성하고 제공된 데이터만 사용해라."
    return system, prompt


def generate_card_news_script(analysis: dict) -> str:
    fallback = fallback_card_news_script(analysis)
    system, prompt = build_card_news_prompt(analysis)
    return generate_with_llm("card_news_script", system, prompt, fallback_text=fallback)

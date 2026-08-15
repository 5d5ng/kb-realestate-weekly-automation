from __future__ import annotations

from .common import (
    BUCKET_LABELS,
    CONTENT_BUCKET_ORDER,
    build_context,
    format_bucket_metric_table,
    format_bucket_transaction_table,
    generate_with_llm,
)


def fallback_card_news_script(
    analysis: dict,
    transactions: dict | None = None,
) -> str:
    latest_date = analysis.get("latest_date", "")
    content_regions = analysis.get("content_regions", {}) or {}
    normalized_transactions = transactions if isinstance(transactions, dict) else {}

    slides = [f"[슬라이드 1]\n제목: {latest_date} KB부동산 주간시장 요약\n본문: 서울 25개 구와 수도권·비수도권 상승 지역의 연속 상승 기간을 실거래와 함께 확인합니다."]
    slide_index = 2
    for bucket_name in CONTENT_BUCKET_ORDER:
        bucket_items = content_regions.get(bucket_name) or []
        chunks = [bucket_items[index:index + 5] for index in range(0, len(bucket_items), 5)] or [[]]
        for chunk_number, chunk in enumerate(chunks, start=1):
            suffix = f" ({chunk_number}/{len(chunks)})" if len(chunks) > 1 else ""
            slides.append(
                "\n".join(
                    [
                        f"[슬라이드 {slide_index}]",
                        f"제목: {BUCKET_LABELS.get(bucket_name, bucket_name)}{suffix}",
                        "본문: 연속 상승 기간과 해당 지역 실거래를 한 장 안에서 함께 정리합니다.",
                        "",
                        "연속 상승 표",
                        format_bucket_metric_table(chunk),
                        "",
                        "실거래 표",
                        format_bucket_transaction_table(chunk, normalized_transactions.get(bucket_name)),
                    ]
                ).strip()
            )
            slide_index += 1
    slides.append("[슬라이드 16]\n제목: 마무리\n본문: 연속 상승 기간과 실거래, 이번 주 주요 뉴스를 함께 확인하실 필요가 있습니다.")
    return "\n\n".join(slides)


def build_card_news_prompt(
    analysis: dict,
    transactions: dict | None = None,
) -> tuple[str, str]:
    prompt = (
        "아래 데이터를 기반으로 인스타그램 카드뉴스 스크립트를 한국어로 작성해줘.\n"
        "- 모든 문장은 존댓말로 작성\n"
        "- 16장 구성\n"
        "- 각 장은 [슬라이드 n] 형식으로 시작\n"
        "- 서울 매매 5장, 서울 전세 5장, 수도권 매매·전세 각 1장, 비수도권 매매·전세 각 1장을 제공된 순서대로 배정할 것\n"
        "- 각 지역 슬라이드는 제목 1줄, 해설 1~2줄, 연속 상승 표, 해당 섹션 실거래 표 순서로 작성할 것\n"
        "- 각 슬라이드 실거래 표는 같은 장의 5개 지역을 모두 포함할 것\n"
        "- 별도의 독립 실거래가 장표를 만들지 말 것\n\n"
        f"{build_context(analysis, [], transactions)}"
    )
    system = "너는 카드뉴스 기획자다. 모든 문장을 존댓말로 유지하고, 6개 상승 섹션의 연속 상승 기간과 해당 지역 실거래 표가 같은 슬라이드 안에서 연결되도록 작성하며, 제공된 데이터만 사용해라."
    return system, prompt


def generate_card_news_script(
    analysis: dict,
    transactions: dict | None = None,
) -> str:
    fallback = fallback_card_news_script(analysis, transactions)
    system, prompt = build_card_news_prompt(analysis, transactions)
    return generate_with_llm("card_news_script", system, prompt, fallback_text=fallback)

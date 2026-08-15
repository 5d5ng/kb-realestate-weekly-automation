from __future__ import annotations

from .common import (
    BUCKET_LABELS,
    CONTENT_BUCKET_ORDER,
    build_context,
    format_bucket_metric_table,
    format_bucket_transaction_table,
    generate_with_llm,
)


def fallback_instagram_caption(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
) -> str:
    latest_date = analysis.get("latest_date", "")
    content_regions = analysis.get("content_regions", {}) or {}
    normalized_transactions = transactions if isinstance(transactions, dict) else {}
    top_news = news[:2]

    lines = [
        f"{latest_date} KB부동산 주간 체크",
        "",
        "이번 주에는 6개 상승 섹션의 연속 상승 기간과 해당 지역 실거래를 같은 흐름으로 정리해 드립니다.",
        "",
    ]

    for bucket_name in CONTENT_BUCKET_ORDER:
        bucket_items = content_regions.get(bucket_name) or []
        lines.extend(
            [
                f"[{BUCKET_LABELS.get(bucket_name, bucket_name)}]",
                format_bucket_metric_table(bucket_items),
                "",
                "해당 섹션 실거래 표",
                format_bucket_transaction_table(bucket_items, normalized_transactions.get(bucket_name)),
                "",
            ]
        )

    lines.append("[주요 뉴스]")
    if top_news:
        for article in top_news:
            lines.append(f"- {article.get('title', '')}")
            if article.get("url"):
                lines.append(f"  링크: {article.get('url')}")
            lines.append("")
    else:
        lines.append("- 주요 뉴스 없음")

    lines.extend(
        [
            "",
            "#KB부동산 #부동산 #아파트 #주간부동산 #전세 #매매 #재건축 #재개발",
        ]
    )
    return "\n".join(lines)


def build_instagram_caption_prompt(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
) -> tuple[str, str]:
    prompt = (
        "아래 데이터를 기반으로 인스타그램 캡션을 한국어로 작성해줘.\n"
        "- 모든 문장은 존댓말로 작성\n"
        "- 첫 줄은 눈에 띄는 훅으로 시작\n"
        "- 6개 상승 섹션을 제공된 순서대로 모두 반영\n"
        "- 각 섹션은 연속 상승 표 다음에 해당 섹션 실거래 표를 바로 붙일 것\n"
        "- 섹션 사이에는 반드시 한 줄 공백을 둘 것\n"
        "- 별도의 독립 실거래 섹션을 만들지 말 것\n"
        "- 마지막에는 해시태그 6~10개를 붙일 것\n"
        "- 데이터에 있는 정보만 활용\n\n"
        f"{build_context(analysis, news, transactions)}"
    )
    system = "너는 부동산 인스타그램 콘텐츠 에디터다. 모든 문장을 존댓말로 유지하고, 6개 상승 섹션의 연속 상승 기간과 해당 지역 실거래 표가 자연스럽게 이어지는 캡션을 작성하며, 제공된 데이터 밖의 사실은 쓰지 마라."
    return system, prompt


def generate_instagram_caption(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
) -> str:
    fallback = fallback_instagram_caption(analysis, news, transactions)
    system, prompt = build_instagram_caption_prompt(analysis, news, transactions)
    return generate_with_llm("instagram_caption", system, prompt, fallback_text=fallback)

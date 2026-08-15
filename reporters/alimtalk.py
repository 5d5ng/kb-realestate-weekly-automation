from __future__ import annotations

from .common import (
    BUCKET_LABELS,
    CONTENT_BUCKET_ORDER,
    build_context,
    format_bucket_metric_table,
    format_bucket_transaction_table,
    generate_with_llm,
)


def fallback_alimtalk_message(
    analysis: dict,
    transactions: dict | None = None,
) -> str:
    latest_date = analysis.get("latest_date", "")
    content_regions = analysis.get("content_regions", {}) or {}
    normalized_transactions = transactions if isinstance(transactions, dict) else {}

    lines = [
        f"[KB부동산 주간요약] {latest_date}",
        "이번 주에는 6개 상승 섹션의 연속 상승 흐름과 해당 지역 실거래를 함께 정리해 드립니다.",
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
    lines.append("상세 리포트는 텔레그램 채널과 블로그 초안에서 함께 확인하실 수 있습니다.")
    return "\n".join(lines)


def build_alimtalk_prompt(
    analysis: dict,
    transactions: dict | None = None,
) -> tuple[str, str]:
    prompt = (
        "아래 데이터를 기반으로 카카오 알림톡용 짧은 요약 메시지를 한국어로 작성해줘.\n"
        "- 모든 문장은 존댓말로 작성\n"
        "- 이번 버전은 6개 상승 섹션을 모두 제공된 순서대로 반영\n"
        "- 각 섹션은 연속 상승 표 다음에 해당 섹션 실거래 표를 바로 붙일 것\n"
        "- 섹션 사이에는 반드시 한 줄 공백을 둘 것\n"
        "- 별도의 독립 실거래 섹션을 만들지 말 것\n"
        "- 과장 없이 간결하게 정리하되 데이터는 누락하지 말 것\n\n"
        f"{build_context(analysis, [], transactions)}"
    )
    system = "너는 짧고 정확한 금융·부동산 알림 메시지를 쓰는 에디터다. 모든 문장을 존댓말로 유지하고, 6개 상승 섹션의 연속 상승 기간과 해당 지역 실거래 표가 같은 흐름으로 이어지게 작성해라."
    return system, prompt


def generate_alimtalk_message(
    analysis: dict,
    transactions: dict | None = None,
) -> str:
    fallback = fallback_alimtalk_message(analysis, transactions)
    system, prompt = build_alimtalk_prompt(analysis, transactions)
    return generate_with_llm("alimtalk_message", system, prompt, fallback_text=fallback)

from __future__ import annotations

from .common import build_context, format_region_bucket, generate_with_llm


def fallback_alimtalk_message(analysis: dict) -> str:
    latest_date = analysis.get("latest_date", "")
    sale_top = analysis.get("sale", {}).get("top5", [])[:2]
    rent_top = analysis.get("rent", {}).get("top5", [])[:2]

    return "\n".join(
        [
            f"[KB부동산 주간요약] {latest_date}",
            f"매매 강세: {format_region_bucket(sale_top, 2)}",
            f"전세 강세: {format_region_bucket(rent_top, 2)}",
            "상세 리포트는 텔레그램 채널에서 확인하세요.",
        ]
    )


def generate_alimtalk_message(analysis: dict) -> str:
    fallback = fallback_alimtalk_message(analysis)
    prompt = (
        "아래 데이터를 기반으로 카카오 알림톡용 짧은 요약 메시지를 한국어로 작성해줘.\n"
        "- 4~6줄 내외\n"
        "- 핵심 지역만 짧게\n"
        "- 과장 없이 간결하게\n\n"
        f"{build_context(analysis, [])}"
    )
    system = "너는 짧고 정확한 금융/부동산 알림 메시지를 쓰는 에디터다."
    return generate_with_llm("alimtalk_message", system, prompt, fallback_text=fallback)

from __future__ import annotations

import re

from .common import (
    BUCKET_LABELS,
    CONTENT_BUCKET_ORDER,
    build_context,
    format_bucket_metric_table,
    format_bucket_transaction_table,
    format_news_item,
    generate_with_llm,
    protect_article_urls,
    restore_article_urls,
)

MAX_TELEGRAM_NEWS_ITEMS = 30
TARGET_NEWS_PUBLISHERS = ("한국경제", "매일경제", "서울경제", "조선일보", "중앙일보", "동아일보")
INLINE_NEWS_PATTERN = re.compile(
    rf"(?P<title>.*?)(?P<publisher>{'|'.join(TARGET_NEWS_PUBLISHERS)})\s+(?P<page>[A-Z]?\d+)\s+(?P<date>\d{{4}}-\d{{2}}-\d{{2}})\s+(?P<url>https?://\S+)",
    flags=re.DOTALL,
)

SECTION_TITLES = (
    "매매 흐름",
    "전세 흐름",
    "실거래 체크",
    "주요 뉴스",
    "한줄 시사점",
    "한줄 요약",
    "한 줄 정리",
    "한줄 정리",
)


def _format_inline_news_block(block: str) -> str:
    flattened = re.sub(r"\s+", " ", str(block or "")).strip()
    if not flattened:
        return block.strip()

    matches = list(INLINE_NEWS_PATTERN.finditer(flattened))
    if not matches:
        return re.sub(r"(https?://\S+)\s+(?=[가-힣A-Z0-9\"“])", r"\1\n\n", block.strip())

    items: list[str] = []
    for index, match in enumerate(matches, start=1):
        title = re.sub(r"^(?:-|\d+\.)\s*", "", match.group("title")).strip(" -")
        publisher = match.group("publisher").strip()
        page = match.group("page").strip()
        issue_date = match.group("date").strip()
        url = match.group("url").strip()
        page_text = f" {page}" if page else ""
        items.append(
            f"{index}. {title}\n"
            f"  출처: {publisher}{page_text} | {issue_date}\n"
            f"  링크: {url}"
        )

    return "\n\n".join(items).strip()


def _normalize_news_section_layout(text: str) -> str:
    match = re.search(r"(\[주요 뉴스\]\n)(.*?)(?=\n\[[^\]]+\]\n|\Z)", text, flags=re.DOTALL)
    if not match:
        return text

    header = match.group(1)
    block = match.group(2).strip()
    if not block:
        return text

    if block.count("출처:") >= 2 and block.count("링크:") >= 2:
        normalized_block = re.sub(r"(링크:\s+https?://\S+)\s+(?=\d+\.\s)", r"\1\n\n", block)
        normalized_block = re.sub(r"(링크:\s+https?://\S+)\s+(?=[가-힣A-Z0-9\"“])", r"\1\n\n", normalized_block)
    else:
        normalized_block = _format_inline_news_block(block)

    return text[: match.start()] + header + normalized_block + text[match.end() :]


def _normalize_telegram_newsletter(text: str) -> str:
    normalized = str(text or "").strip()
    if not normalized:
        return normalized

    normalized = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1\n링크: \2", normalized)
    normalized = normalized.replace("**", "")
    normalized = normalized.replace("__", "")
    normalized = normalized.replace("`", "")

    normalized = re.sub(r"^\s{0,3}#{1,6}\s*", "", normalized, flags=re.MULTILINE)
    normalized = re.sub(r"\s+\*\s+", "\n- ", normalized)
    normalized = re.sub(r"^\s*[*•]\s+", "- ", normalized, flags=re.MULTILINE)
    normalized = re.sub(r"\n- ([^\n]+?) \| 링크: (https?://\S+)", r"\n  출처: \1\n  링크: \2", normalized)
    normalized = re.sub(r"(?<!\n)\s+출처:\s+", "\n  출처: ", normalized)
    normalized = re.sub(r"(?<!\n)\s+링크:\s+", "\n  링크: ", normalized)
    normalized = re.sub(r"(링크:\s+https?://\S+)\s+(?=[가-힣A-Z0-9\"“])", r"\1\n\n", normalized)
    normalized = re.sub(r"(?<!^)\s(?=\d+\.\s)", "\n", normalized)
    normalized = re.sub(
        r"^(\d+\.\s.*?)(?:\s+출처:\s+)(.*?)(?:\s+링크:\s+)(https?://\S+)$",
        r"\1\n  출처: \2\n  링크: \3",
        normalized,
        flags=re.MULTILINE,
    )

    for title in SECTION_TITLES:
        normalized = re.sub(
            rf"\s*(?:\[\s*)?{re.escape(title)}(?:\s*\])?\s*",
            lambda _m, section=title: f"\n\n[{section}]\n",
            normalized,
            count=1,
        )

    normalized = _normalize_news_section_layout(normalized)
    normalized = re.sub(r"(https?://\S+)\n(\[[^\]]+\])", r"\1\n\n\2", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    normalized = re.sub(r"[ \t]+\n", "\n", normalized)
    return normalized.strip()



def fallback_telegram_report(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
    *,
    max_news_items: int = MAX_TELEGRAM_NEWS_ITEMS,
) -> str:
    latest_date = analysis.get("latest_date", "")
    content_regions = analysis.get("content_regions", {}) or {}
    normalized_transactions = transactions if isinstance(transactions, dict) else {}
    effective_news_limit = max(0, min(int(max_news_items), MAX_TELEGRAM_NEWS_ITEMS))
    news_lines = news[:effective_news_limit]

    lines = [
        f"[KB부동산 주간 리포트] ({latest_date})",
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

    if news_lines:
        for article in news_lines:
            lines.append(f"- {format_news_item(article)}")
            lines.append("")
    else:
        lines.append("- 주요 뉴스가 없습니다.")

    lines.extend(
        [
            "",
            "[한줄 시사점]",
            "- 서울 모든 구와 수도권·비수도권 상위 지역의 연속 상승 기간을 실거래와 함께 확인할 필요가 있습니다.",
        ]
    )
    return "\n".join(lines)


def fallback_news_only_telegram_report(
    news: list[dict],
    *,
    max_news_items: int = MAX_TELEGRAM_NEWS_ITEMS,
) -> str:
    effective_news_limit = max(0, min(int(max_news_items), MAX_TELEGRAM_NEWS_ITEMS))
    news_lines = news[:effective_news_limit]
    lines = [
        "[부동산 뉴스 브리핑]",
        "",
        f"- 수집 기사 수: {len(news_lines)}건",
        "",
        "1. 주요 뉴스",
    ]

    if news_lines:
        lines.extend(f"- {format_news_item(article)}" for article in news_lines)
    else:
        lines.append("- 주요 뉴스가 없습니다.")

    lines.extend(
        [
            "",
            "2. 한줄 정리",
            "- 오늘은 뉴스만 빠르게 정리해 드리는 별도 발송입니다.",
        ]
    )
    return "\n".join(lines)


def build_telegram_report_prompt(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
    *,
    max_news_items: int = MAX_TELEGRAM_NEWS_ITEMS,
) -> tuple[str, str]:
    effective_news_limit = max(0, min(int(max_news_items), MAX_TELEGRAM_NEWS_ITEMS))
    prompt = (
        "아래 데이터를 기반으로 텔레그램용 한국어 주간 부동산 리포트를 작성해줘.\n"
        "- 문체는 전문적이되 이해하기 쉽게, 모든 문장은 존댓말로 작성\n"
        "- 반드시 데이터에 있는 내용만 사용\n"
        "- 구조는 제목, 6개 상승 섹션 상세, 주요 뉴스, 한줄 시사점 순서로 작성\n"
        "- 6개 상승 섹션은 제공된 순서를 그대로 유지할 것\n"
        "- 각 버킷에서는 수치 표를 먼저 보여주고, 바로 아래에 같은 버킷의 실거래 표를 붙일 것\n"
        "- 별도의 독립 실거래 섹션을 만들지 말 것\n"
        f"- 주요 뉴스는 최대 {effective_news_limit}건까지만 반영\n"
        "- 일반 텍스트 뉴스레터 형식으로 작성\n"
        "- Markdown 문법(#, ##, *, **, [], ()) 사용 금지. 단, `|` 구분 텍스트 표는 허용\n"
        "- 섹션 제목은 [수도권 매매 상승 상위 5], [주요 뉴스], [한줄 시사점]처럼 한 줄로 작성\n"
        "- 기사 1건은 제목 1줄, 출처/날짜 1줄, 링크 1줄 정도로 가독성 있게 배치\n"
        "- 각 버킷 블록과 다음 버킷 블록 사이에는 반드시 한 줄 공백을 둘 것\n"
        "- 링크 URL은 절대 수정하거나 단축하지 말고 원문 그대로 출력할 것\n\n"
        f"{build_context(analysis, news[:effective_news_limit], transactions)}"
    )
    system = "너는 한국 부동산 시장 콘텐츠 에디터다. 텔레그램 일반 텍스트 뉴스레터처럼 읽기 좋게 작성하고, 모든 문장은 존댓말로 유지하며, 없는 수치나 사실을 만들지 말고 제공된 데이터만 사용해라. 기사 링크 URL은 어떤 경우에도 변경하지 말고 반드시 원문 그대로 출력해라."
    return system, prompt


def build_news_only_telegram_prompt(
    news: list[dict],
    *,
    max_news_items: int = MAX_TELEGRAM_NEWS_ITEMS,
) -> tuple[str, str]:
    effective_news_limit = max(0, min(int(max_news_items), MAX_TELEGRAM_NEWS_ITEMS))
    news_context = "\n".join(f"- {format_news_item(article)}" for article in news[:effective_news_limit]) or "- 주요 뉴스 없음"
    prompt = (
        "아래 데이터를 기반으로 텔레그램용 한국어 부동산 뉴스 브리핑을 작성해줘.\n"
        "- 구조는 제목, 주요 뉴스, 한줄 정리 순서\n"
        f"- 주요 뉴스는 최대 {effective_news_limit}건까지만 반영\n"
        "- 기사 제목, 언론사, 링크를 빠짐없이 반영\n"
        "- 모든 문장은 존댓말로 작성할 것\n"
        "- 일반 텍스트 뉴스레터 형식으로 작성\n"
        "- Markdown 문법(#, ##, *, **, [], ()) 사용 금지\n"
        "- 기사 1건은 제목 1줄, 출처/날짜 1줄, 링크 1줄로 정리\n"
        "- 여러 뉴스 항목을 연속으로 소개할 때는 각 항목 사이에 반드시 한 줄 공백을 둘 것\n"
        "- 제공된 기사만 사용하고 과장하지 말 것\n"
        "- 링크 URL은 절대 수정하거나 단축하지 말고 원문 그대로 출력할 것\n\n"
        f"[주요 뉴스]\n{news_context}"
    )
    system = "너는 한국 부동산 뉴스 브리핑 에디터다. 제공된 기사만 사용해 텔레그램 일반 텍스트 뉴스레터처럼 요약을 작성하되, 모든 문장은 존댓말로 유지해라. 기사 링크 URL은 어떤 경우에도 변경하지 말고 반드시 원문 그대로 출력해라."
    return system, prompt


def generate_telegram_report(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
    *,
    max_news_items: int = MAX_TELEGRAM_NEWS_ITEMS,
) -> str:
    effective_limit = max(0, min(int(max_news_items), MAX_TELEGRAM_NEWS_ITEMS))
    capped_news = news[:effective_limit]
    fallback = fallback_telegram_report(analysis, news, transactions, max_news_items=max_news_items)
    system, prompt = build_telegram_report_prompt(
        analysis,
        news,
        transactions,
        max_news_items=max_news_items,
    )
    protected_prompt, original_urls = protect_article_urls(prompt, capped_news)
    generated = generate_with_llm("telegram_report", system, protected_prompt, fallback_text=fallback)
    generated = restore_article_urls(generated, original_urls)
    return _normalize_telegram_newsletter(generated)


def generate_news_only_telegram_report(
    news: list[dict],
    *,
    max_news_items: int = MAX_TELEGRAM_NEWS_ITEMS,
) -> str:
    effective_limit = max(0, min(int(max_news_items), MAX_TELEGRAM_NEWS_ITEMS))
    capped_news = news[:effective_limit]
    fallback = fallback_news_only_telegram_report(news, max_news_items=max_news_items)
    system, prompt = build_news_only_telegram_prompt(news, max_news_items=max_news_items)
    protected_prompt, original_urls = protect_article_urls(prompt, capped_news)
    generated = generate_with_llm("telegram_report", system, protected_prompt, fallback_text=fallback)
    generated = restore_article_urls(generated, original_urls)
    return _normalize_telegram_newsletter(generated)

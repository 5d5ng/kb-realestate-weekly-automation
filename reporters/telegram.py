from __future__ import annotations

import re

from .common import (
    BUCKET_LABELS,
    build_context,
    format_news_item,
    format_region_bucket,
    format_trade_item,
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


def fallback_telegram_report(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
    *,
    max_news_items: int = MAX_TELEGRAM_NEWS_ITEMS,
) -> str:
    latest_date = analysis.get("latest_date", "")
    sale = analysis.get("sale", {})
    rent = analysis.get("rent", {})
    effective_news_limit = max(0, min(int(max_news_items), MAX_TELEGRAM_NEWS_ITEMS))
    news_lines = news[:effective_news_limit]
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
        lines.append("- 주요 뉴스 없음")

    lines.extend(
        [
            "",
            "2. 한줄 정리",
            "- 오늘은 뉴스만 빠르게 정리한 별도 발송입니다.",
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
        "- 문체는 전문적이되 이해하기 쉽게\n"
        "- 반드시 데이터에 있는 내용만 사용\n"
        "- 구조는 제목, 매매 흐름, 전세 흐름, 실거래 체크, 주요 뉴스, 한줄 시사점 순서\n"
        "- 실거래 체크에서는 최근 거래 단지명, 면적, 가격, 최근 전세 흐름을 짧게 요약\n"
        f"- 주요 뉴스는 최대 {effective_news_limit}건까지만 반영\n"
        "- 일반 텍스트 뉴스레터 형식으로 작성\n"
        "- Markdown 문법(#, ##, *, **, [], ()) 사용 금지\n"
        "- 섹션 제목은 [매매 흐름], [전세 흐름], [실거래 체크], [주요 뉴스], [한줄 시사점]처럼 한 줄로 작성\n"
        "- 기사 1건은 제목 1줄, 출처/날짜 1줄, 링크 1줄 정도로 가독성 있게 배치\n"
        "- 링크 URL은 절대 수정하거나 단축하지 말고 원문 그대로 출력할 것\n\n"
        f"{build_context(analysis, news[:effective_news_limit], transactions)}"
    )
    system = "너는 한국 부동산 시장 콘텐츠 에디터다. 텔레그램 일반 텍스트 뉴스레터처럼 읽기 좋게 작성하고, 없는 수치나 사실을 만들지 말고 제공된 데이터만 사용해라. 기사 링크 URL은 어떤 경우에도 변경하지 말고 반드시 원문 그대로 출력해라."
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
        "- 일반 텍스트 뉴스레터 형식으로 작성\n"
        "- Markdown 문법(#, ##, *, **, [], ()) 사용 금지\n"
        "- 기사 1건은 제목 1줄, 출처/날짜 1줄, 링크 1줄로 정리\n"
        "- 제공된 기사만 사용하고 과장하지 말 것\n"
        "- 링크 URL은 절대 수정하거나 단축하지 말고 원문 그대로 출력할 것\n\n"
        f"[주요 뉴스]\n{news_context}"
    )
    system = "너는 한국 부동산 뉴스 브리핑 에디터다. 제공된 기사만 사용해 텔레그램 일반 텍스트 뉴스레터처럼 요약을 작성해라. 기사 링크 URL은 어떤 경우에도 변경하지 말고 반드시 원문 그대로 출력해라."
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

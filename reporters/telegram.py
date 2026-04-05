from __future__ import annotations

import re

from .common import (
    BUCKET_LABELS,
    build_context,
    format_news_item,
    format_region_bucket,
    generate_with_llm,
    protect_article_urls,
    restore_article_urls,
)


def _price_to_eok(value) -> str:
    """Convert 만원 numeric value to 억 display string."""
    if not isinstance(value, (int, float)) or value <= 0:
        return ""
    eok = value / 10000
    if eok == int(eok):
        return f"{int(eok)}억"
    return f"{eok:.1f}억"


def _format_trade_eok(trade: dict) -> str:
    """Format a single trade as '단지명 면적 가격(억)'."""
    name = (trade.get("name") or "").strip()
    area = trade.get("area")
    price = trade.get("price")
    area_str = f"{float(area):.0f}㎡" if isinstance(area, (int, float)) else ""
    price_str = _price_to_eok(price)
    parts = [p for p in [name, area_str, price_str] if p]
    return " ".join(parts)


def _format_direct_transaction_section(transactions: dict | None) -> str:
    """Format transaction data directly without LLM, structured by bucket → region → area."""
    if not transactions:
        return "[실거래 체크]\n- 실거래 정보 없음"

    lines = ["[실거래 체크]"]

    rank_counter = 0

    def _append_region(region_name: str, area_mapping: dict) -> None:
        nonlocal rank_counter
        rank_counter += 1
        lines.append(f"\n{rank_counter}. {region_name}")
        for area_key, area_info in list(area_mapping.items())[:2]:
            trades = (area_info or {}).get("trades", [])
            if not trades:
                continue
            for trade in trades:
                sale_str = _format_trade_eok(trade)
                households = trade.get("complex_households") or trade.get("households")
                rental = trade.get("rental_households")
                if isinstance(households, (int, float)) and households > 0:
                    hh_str = f" {int(households):,}세대"
                    if isinstance(rental, (int, float)) and rental > 0:
                        hh_str += f"(임대{int(rental):,})"
                else:
                    hh_str = ""
                related_rents = (trade.get("related_rent_trades") or [])[:1]
                if related_rents:
                    rent_price = _price_to_eok(related_rents[0].get("price"))
                    if rent_price:
                        lines.append(f"  {area_key}타입: {sale_str}{hh_str} (전세 {rent_price})")
                    else:
                        lines.append(f"  {area_key}타입: {sale_str}{hh_str}")
                else:
                    lines.append(f"  {area_key}타입: {sale_str}{hh_str}")

    # Check if flat (region → area) or bucketed (bucket → region → area)
    is_flat = all(
        isinstance(v, dict) and all(str(k).isdigit() for k in v.keys())
        for v in transactions.values()
    )

    if is_flat:
        for region_name, area_mapping in list(transactions.items())[:5]:
            _append_region(region_name, area_mapping)
    else:
        for bucket_name, region_mapping in list(transactions.items())[:4]:
            if not isinstance(region_mapping, dict) or not region_mapping:
                continue
            bucket_label = BUCKET_LABELS.get(bucket_name, bucket_name)
            lines.append(f"\n< {bucket_label} >")
            rank_counter = 0
            for region_name, area_mapping in list(region_mapping.items())[:2]:
                _append_region(region_name, area_mapping)

    return "\n".join(lines)

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
    sale = analysis.get("sale", {})
    rent = analysis.get("rent", {})
    effective_news_limit = max(0, min(int(max_news_items), MAX_TELEGRAM_NEWS_ITEMS))
    news_lines = news[:effective_news_limit]

    lines = [
        f"[KB부동산 주간 리포트] ({latest_date})",
        "",
        "[매매 흐름]",
        f"- 상승 상위: {format_region_bucket(sale.get('top5', []), 3)}",
        f"- 하락 하위: {format_region_bucket(sale.get('bottom5', []), 3)}",
        "",
        "[전세 흐름]",
        f"- 상승 상위: {format_region_bucket(rent.get('top5', []), 3)}",
        f"- 하락 하위: {format_region_bucket(rent.get('bottom5', []), 3)}",
        "",
        _format_direct_transaction_section(transactions),
        "",
        "[주요 뉴스]",
    ]

    if news_lines:
        lines.extend(f"- {format_news_item(article)}" for article in news_lines)
    else:
        lines.append("- 주요 뉴스 없음")

    lines.extend(
        [
            "",
            "[한줄 시사점]",
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
    # Transactions are formatted directly — LLM only handles analysis + news
    prompt = (
        "아래 데이터를 기반으로 텔레그램용 한국어 주간 부동산 리포트를 작성해줘.\n"
        "- 문체는 전문적이되 이해하기 쉽게\n"
        "- 반드시 데이터에 있는 내용만 사용\n"
        "- 구조는 제목, 매매 흐름, 전세 흐름, 주요 뉴스, 한줄 시사점 순서 (총 4개 섹션)\n"
        "- 실거래 체크 섹션은 별도 처리되므로 작성하지 말 것\n"
        f"- 주요 뉴스는 최대 {effective_news_limit}건까지만 반영\n"
        "- 일반 텍스트 뉴스레터 형식으로 작성\n"
        "- Markdown 문법(#, ##, *, **, [], ()) 사용 금지\n"
        "- 섹션 제목은 [매매 흐름], [전세 흐름], [주요 뉴스], [한줄 시사점]처럼 한 줄로 작성\n"
        "- 기사 1건은 제목 1줄, 출처/날짜 1줄, 링크 1줄 정도로 가독성 있게 배치\n"
        "- 링크 URL은 절대 수정하거나 단축하지 말고 원문 그대로 출력할 것\n\n"
        f"{build_context(analysis, news[:effective_news_limit])}"
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


def _splice_transaction_section(llm_text: str, transaction_section: str) -> str:
    """Insert the raw transaction section between 전세 흐름 and 주요 뉴스 in the LLM output."""
    # Try to find [주요 뉴스] section to insert before it
    news_match = re.search(r"\n(\[주요 뉴스\])", llm_text)
    if news_match:
        insert_pos = news_match.start()
        return llm_text[:insert_pos] + "\n\n" + transaction_section + "\n" + llm_text[insert_pos:]

    # Fallback: append at the end
    return llm_text + "\n\n" + transaction_section


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
    normalized = _normalize_telegram_newsletter(generated)

    # Splice in the raw transaction section
    transaction_section = _format_direct_transaction_section(transactions)
    return _splice_transaction_section(normalized, transaction_section)


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

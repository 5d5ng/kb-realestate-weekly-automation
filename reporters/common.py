from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
PROMPT_OUTPUT_DIR = BASE_DIR / "reports" / "prompts"
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.example", override=False)

TIMEOUT_SEC = 60

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

DEFAULT_TASK_MODELS = {
    "telegram_report": {"provider": "openai", "model": "gpt-5-mini", "max_tokens": 1400},
    "alimtalk_message": {"provider": "none", "model": "", "max_tokens": 0},
    "instagram_caption": {"provider": "gemini", "model": "gemini-2.5-flash-lite", "max_tokens": 700},
    "card_news_script": {"provider": "openai", "model": "gpt-5-mini", "max_tokens": 1000},
    "naver_blog_post": {"provider": "none", "model": "", "max_tokens": 0},
}

BUCKET_LABELS = {
    "capital_sale_top5": "수도권 매매 상승 상위 5",
    "capital_sale_bottom5": "수도권 매매 하위 5",
    "capital_rent_top5": "수도권 전세 상승 상위 5",
    "capital_rent_bottom5": "수도권 전세 하위 5",
    "non_capital_sale_top5": "비수도권 매매 상승 상위 5",
    "non_capital_sale_bottom5": "비수도권 매매 하위 5",
    "non_capital_rent_top5": "비수도권 전세 상승 상위 5",
    "non_capital_rent_bottom5": "비수도권 전세 하위 5",
}

SESSION = requests.Session()
SESSION.headers.update(
    {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0",
    }
)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def format_number(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.3f}"
    return clean_text(value)


def format_region_item(item: dict[str, Any]) -> str:
    region = item.get("region", "")
    current = format_number(item.get("current", ""))
    delta = item.get("delta")
    delta_text = ""
    if isinstance(delta, (int, float)):
        delta_text = f", 전주대비 {delta:+.3f}"
    return f"{region}({current}{delta_text})"


def format_region_bucket(items: list[dict[str, Any]], limit: int = 5) -> str:
    if not items:
        return "없음"
    return ", ".join(format_region_item(item) for item in items[:limit])


def format_news_item(article: dict[str, Any]) -> str:
    publisher = article.get("publisher", "언론사")
    issue_date = article.get("issue_date") or article.get("published_at", "")
    page = article.get("page", "")
    title = article.get("title", "")
    url = article.get("url") or article.get("resolved_url") or article.get("originallink") or article.get("link") or ""
    keywords = article.get("matched_keywords", [])
    keyword_text = f" | 키워드: {', '.join(keywords)}" if keywords else ""
    page_text = f" {page}" if page else ""
    url_text = f"\n  링크: {url}" if url else ""
    return f"{publisher}{page_text} {issue_date} - {title}{keyword_text}{url_text}"


def format_news_bucket(news: list[dict[str, Any]], limit: int = 5) -> str:
    if not news:
        return "없음"
    return "\n".join(f"- {format_news_item(article)}" for article in news[:limit])


def format_price(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{int(round(value)):,}만원"
    return clean_text(value)


def format_area(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{float(value):.2f}㎡"
    return clean_text(value)


def format_trade_item(trade: dict[str, Any]) -> str:
    parts: list[str] = []
    contract_date = clean_text(trade.get("contract_date") or trade.get("date"))
    complex_name = clean_text(trade.get("name"))
    area = format_area(trade.get("area"))
    price = format_price(trade.get("price"))
    households = trade.get("households")

    if contract_date:
        parts.append(contract_date)
    if complex_name:
        parts.append(complex_name)
    if area:
        parts.append(f"전용 {area}")
    if price:
        parts.append(price)
    if isinstance(households, (int, float)) and households > 0:
        parts.append(f"{int(households):,}세대")

    return " / ".join(parts) if parts else "실거래 정보 없음"


def _format_sale_trade_with_related_rent(
    trade: dict[str, Any],
    *,
    max_related_rents: int = 1,
) -> str:
    sale_text = format_trade_item(trade)
    related_rents = (trade.get("related_rent_trades") or [])[:max_related_rents]
    if not related_rents:
        return f"{sale_text} -> 최근 전세 없음"

    rent_text = " / ".join(format_trade_item(rent_trade) for rent_trade in related_rents)
    return f"{sale_text} -> 최근 전세 {rent_text}"


def _is_area_mapping(value: Any) -> bool:
    return isinstance(value, dict) and bool(value) and all(str(key).isdigit() for key in value.keys())


def _format_region_transactions(
    region_name: str,
    area_mapping: dict[str, Any],
    *,
    max_area_types: int = 2,
    max_trades_per_area: int = 2,
) -> list[str]:
    lines = [region_name]
    for area_key, area_info in list(area_mapping.items())[:max_area_types]:
        trades = (area_info or {}).get("trades", [])[:max_trades_per_area]
        rent_trades = (area_info or {}).get("rent_trades", [])[:max_trades_per_area]
        if not trades:
            lines.append(f"{area_key}타입: 최근 거래 없음")
        else:
            trade_summary = " | ".join(_format_sale_trade_with_related_rent(trade) for trade in trades)
            lines.append(f"{area_key}타입 매매: {trade_summary}")

        if rent_trades:
            rent_summary = " | ".join(format_trade_item(trade) for trade in rent_trades)
            lines.append(f"{area_key}타입 전세 모음: {rent_summary}")
        else:
            lines.append(f"{area_key}타입 전세 모음: 최근 거래 없음")
    return lines


def format_transactions_context(
    transactions: dict[str, Any] | None,
    *,
    max_buckets: int = 4,
    max_regions_per_bucket: int = 2,
    max_trades_per_area: int = 2,
) -> str:
    if not transactions:
        return "없음"

    lines: list[str] = []

    if all(_is_area_mapping(value) for value in transactions.values()):
        for region_name, area_mapping in list(transactions.items())[:max_regions_per_bucket]:
            lines.extend(
                _format_region_transactions(
                    region_name,
                    area_mapping,
                    max_trades_per_area=max_trades_per_area,
                )
            )
            lines.append("")
        return "\n".join(lines).strip()

    for bucket_name, region_mapping in list(transactions.items())[:max_buckets]:
        lines.append(f"[{BUCKET_LABELS.get(bucket_name, bucket_name)}]")
        if not isinstance(region_mapping, dict) or not region_mapping:
            lines.append("거래 정보 없음")
            lines.append("")
            continue

        for region_name, area_mapping in list(region_mapping.items())[:max_regions_per_bucket]:
            lines.extend(
                _format_region_transactions(
                    region_name,
                    area_mapping,
                    max_trades_per_area=max_trades_per_area,
                )
            )
            lines.append("")

    return "\n".join(lines).strip()


def build_context(analysis: dict, news: list[dict[str, Any]]) -> str:
    latest_date = analysis.get("latest_date", "")
    sale = analysis.get("sale", {})
    rent = analysis.get("rent", {})
    return (
        f"[분석 기준일]\n{latest_date}\n\n"
        f"[매매 상위]\n{format_region_bucket(sale.get('top5', []))}\n\n"
        f"[매매 하위]\n{format_region_bucket(sale.get('bottom5', []))}\n\n"
        f"[전세 상위]\n{format_region_bucket(rent.get('top5', []))}\n\n"
        f"[전세 하위]\n{format_region_bucket(rent.get('bottom5', []))}\n\n"
        f"[주요 뉴스]\n{format_news_bucket(news)}"
    )


def resolve_task_config(task_name: str) -> dict[str, Any]:
    default = DEFAULT_TASK_MODELS.get(task_name, {"provider": "none", "model": "", "max_tokens": 0})
    env_prefix = f"REPORTER_{task_name.upper()}"

    provider = clean_text(os.getenv(f"{env_prefix}_PROVIDER", default["provider"])).lower()
    model = clean_text(os.getenv(f"{env_prefix}_MODEL", default["model"]))
    max_tokens = int(os.getenv(f"{env_prefix}_MAX_TOKENS", str(default["max_tokens"])))

    return {
        "provider": provider,
        "model": model,
        "max_tokens": max_tokens,
    }


def get_generation_plan() -> dict[str, dict[str, Any]]:
    return {
        task_name: resolve_task_config(task_name)
        for task_name in DEFAULT_TASK_MODELS
    }


def save_prompt_file(
    task_name: str,
    system_prompt: str,
    user_prompt: str,
    *,
    fallback_text: str | None = None,
) -> str:
    PROMPT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    config = resolve_task_config(task_name)
    sections = [
        f"[task]\n{task_name}",
        f"[generated_at]\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"[provider]\n{config['provider']}",
        f"[model]\n{config['model'] or '(none)'}",
        f"[max_tokens]\n{config['max_tokens']}",
        f"[system_prompt]\n{system_prompt.strip()}",
        f"[user_prompt]\n{user_prompt.strip()}",
    ]
    if fallback_text:
        sections.append(f"[fallback_preview]\n{fallback_text.strip()}")

    output_path = PROMPT_OUTPUT_DIR / f"{task_name}_prompt.txt"
    output_path.write_text("\n\n".join(sections).strip() + "\n", encoding="utf-8")
    return str(output_path)


def _extract_openai_text(payload: dict[str, Any]) -> str:
    text = clean_text(payload.get("output_text"))
    if text:
        return text

    parts: list[str] = []
    for item in payload.get("output", []):
        for content in item.get("content", []):
            piece = clean_text(content.get("text"))
            if piece:
                parts.append(piece)
    return "\n".join(parts).strip()


def _extract_gemini_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for candidate in payload.get("candidates", []):
        content = candidate.get("content", {})
        for part in content.get("parts", []):
            piece = clean_text(part.get("text"))
            if piece:
                parts.append(piece)
    return "\n".join(parts).strip()


def _extract_anthropic_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for item in payload.get("content", []):
        piece = clean_text(item.get("text"))
        if piece:
            parts.append(piece)
    return "\n".join(parts).strip()


def _call_openai(model: str, system_prompt: str, user_prompt: str, *, max_tokens: int) -> str | None:
    if not OPENAI_API_KEY or not model:
        return None

    try:
        response = SESSION.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": model,
                "temperature": 0.4,
                "max_output_tokens": max_tokens,
                "input": [
                    {
                        "role": "system",
                        "content": [{"type": "input_text", "text": system_prompt}],
                    },
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": user_prompt}],
                    },
                ],
            },
            timeout=TIMEOUT_SEC,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None

    return _extract_openai_text(response.json()) or None


def _call_gemini(model: str, system_prompt: str, user_prompt: str, *, max_tokens: int) -> str | None:
    if not GEMINI_API_KEY or not model:
        return None

    try:
        response = SESSION.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            params={"key": GEMINI_API_KEY},
            json={
                "system_instruction": {"parts": [{"text": system_prompt}]},
                "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                "generationConfig": {
                    "temperature": 0.4,
                    "maxOutputTokens": max_tokens,
                },
            },
            timeout=TIMEOUT_SEC,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None

    return _extract_gemini_text(response.json()) or None


def _call_anthropic(model: str, system_prompt: str, user_prompt: str, *, max_tokens: int) -> str | None:
    if not ANTHROPIC_API_KEY or not model:
        return None

    try:
        response = SESSION.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": model,
                "max_tokens": max_tokens,
                "temperature": 0.4,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            },
            timeout=TIMEOUT_SEC,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None

    return _extract_anthropic_text(response.json()) or None


def generate_with_llm(
    task_name: str,
    system_prompt: str,
    user_prompt: str,
    *,
    fallback_text: str,
) -> str:
    config = resolve_task_config(task_name)
    provider = config["provider"]
    model = config["model"]
    max_tokens = config["max_tokens"]

    if provider == "none" or max_tokens <= 0:
        return fallback_text
    if provider == "openai":
        return _call_openai(model, system_prompt, user_prompt, max_tokens=max_tokens) or fallback_text
    if provider in {"gemini", "google"}:
        return _call_gemini(model, system_prompt, user_prompt, max_tokens=max_tokens) or fallback_text
    if provider == "anthropic":
        return _call_anthropic(model, system_prompt, user_prompt, max_tokens=max_tokens) or fallback_text
    return fallback_text

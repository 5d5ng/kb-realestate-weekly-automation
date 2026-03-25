from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
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

DEFAULT_TASK_MODELS = {
    "telegram_report": {"provider": "openai", "model": "gpt-5-mini", "max_tokens": 4000},
    "alimtalk_message": {"provider": "none", "model": "", "max_tokens": 0},
    "instagram_caption": {"provider": "gemini", "model": "gemini-2.5-flash-lite", "max_tokens": 700},
    "card_news_script": {"provider": "openai", "model": "gpt-5-mini", "max_tokens": 1000},
    "naver_blog_post": {"provider": "none", "model": "", "max_tokens": 0},
}

BACKUP_TASK_MODELS = {
    "telegram_report": {"provider": "gemini", "model": "gemini-2.5-flash-lite", "max_tokens": 5000},
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
GENERATION_OVERRIDE_VAR: ContextVar[dict[str, Any]] = ContextVar("generation_override", default={})
GENERATION_META_VAR: ContextVar[dict[str, Any]] = ContextVar("generation_meta", default={})


def _env_text(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = str(value).strip()
    if normalized == "":
        return default
    return normalized


def _mask_secret(value: str | None, *, keep_prefix: int = 4, keep_suffix: int = 4) -> str | None:
    if not value:
        return None
    if len(value) <= keep_prefix + keep_suffix:
        return "*" * len(value)
    return f"{value[:keep_prefix]}...{value[-keep_suffix:]}"


def _provider_key_status(provider: str) -> dict[str, Any]:
    provider = clean_text(provider).lower()
    if provider == "openai":
        key = _env_text("OPENAI_API_KEY")
        return {"required": True, "present": bool(key), "masked": _mask_secret(key)}
    if provider in {"gemini", "google"}:
        key = _env_text("GEMINI_API_KEY")
        return {"required": True, "present": bool(key), "masked": _mask_secret(key)}
    if provider == "anthropic":
        key = _env_text("ANTHROPIC_API_KEY")
        return {"required": True, "present": bool(key), "masked": _mask_secret(key)}
    return {"required": False, "present": True, "masked": None}


def _coerce_task_override(task_name: str, override: Any, base: dict[str, Any]) -> dict[str, Any]:
    if override is None:
        return base
    if isinstance(override, bool):
        if override:
            return base
        return {"provider": "none", "model": "", "max_tokens": 0}
    if isinstance(override, dict):
        enabled = override.get("enabled")
        if enabled is False:
            return {"provider": "none", "model": "", "max_tokens": 0}
        merged = dict(base)
        for key in ("provider", "model", "max_tokens", "allow_backup"):
            if key in override and override[key] is not None:
                merged[key] = override[key]
        return merged
    return base


@contextmanager
def generation_override_context(overrides: dict[str, Any] | None):
    token = GENERATION_OVERRIDE_VAR.set(overrides or {})
    try:
        yield
    finally:
        GENERATION_OVERRIDE_VAR.reset(token)


@contextmanager
def generation_meta_context():
    token = GENERATION_META_VAR.set({})
    try:
        yield
    finally:
        GENERATION_META_VAR.reset(token)


def _record_generation_meta(task_name: str, **metadata: Any) -> None:
    current = dict(GENERATION_META_VAR.get({}))
    current[task_name] = metadata
    GENERATION_META_VAR.set(current)


def get_generation_meta() -> dict[str, Any]:
    return dict(GENERATION_META_VAR.get({}))


def get_llm_config_snapshot(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    plan = get_generation_plan(overrides)
    tasks: dict[str, Any] = {}
    for task_name, config in plan.items():
        provider = config["provider"]
        key_status = _provider_key_status(provider)
        tasks[task_name] = {
            "provider": provider,
            "model": config["model"],
            "max_tokens": config["max_tokens"],
            "api_key_required": key_status["required"],
            "api_key_present": key_status["present"],
            "api_key_masked": key_status["masked"],
            "ready": (not key_status["required"]) or key_status["present"] or provider == "none",
        }

    return {
        "providers": {
            "openai": _provider_key_status("openai"),
            "gemini": _provider_key_status("gemini"),
            "anthropic": _provider_key_status("anthropic"),
        },
        "tasks": tasks,
    }


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


def _make_url_placeholder(index: int) -> str:
    return f"URL_REF_{index}"


def protect_article_urls(prompt: str, articles: list[dict[str, Any]]) -> tuple[str, list[str]]:
    """Replace article URLs in prompt with numbered placeholders.

    Returns the protected prompt and a list of original URLs indexed from 0.
    The LLM will preserve placeholders verbatim, preventing URL corruption.
    """
    original_urls: list[str] = []
    protected = prompt
    for i, article in enumerate(articles):
        url = (
            article.get("url")
            or article.get("resolved_url")
            or article.get("originallink")
            or article.get("link")
            or ""
        )
        original_urls.append(url)
        if url:
            placeholder = _make_url_placeholder(i + 1)
            protected = protected.replace(url, placeholder)
    return protected, original_urls


def restore_article_urls(text: str, original_urls: list[str]) -> str:
    """Replace numbered URL placeholders back with original URLs."""
    result = text
    for i, url in enumerate(original_urls):
        if url:
            placeholder = _make_url_placeholder(i + 1)
            result = result.replace(placeholder, url)
    return result


def filter_hallucinated_news_articles(text: str, allowed_urls: list[str]) -> str:
    """Remove news article blocks whose link URL is not in the allowed set.

    After LLM generation and URL restoration, any article block containing a
    ``링크:`` line with a URL that is not in *allowed_urls* is considered
    hallucinated and is stripped from the output.  Remaining articles are
    renumbered sequentially.
    """
    import re

    allowed_set = {url for url in (allowed_urls or []) if url}
    if not allowed_set:
        return text

    news_match = re.search(
        r"(\[주요 뉴스\]\n)(.*?)(?=\n\[[^\]]+\]|\Z)",
        text,
        flags=re.DOTALL,
    )
    if not news_match:
        return text

    header = news_match.group(1)
    block = news_match.group(2)

    # Split into per-article chunks on any blank line.
    raw_articles = re.split(r"\n{2,}", block)

    filtered: list[str] = []
    new_index = 1
    for chunk in raw_articles:
        chunk = chunk.strip()
        if not chunk:
            continue
        url_match = re.search(r"링크:\s+(https?://\S+)", chunk)
        if url_match:
            url = url_match.group(1).strip()
            if url not in allowed_set:
                # Hallucinated article – discard.
                continue
        # Renumber the leading index.
        renumbered = re.sub(r"^\d+\.", f"{new_index}.", chunk, count=1)
        filtered.append(renumbered)
        new_index += 1

    new_block = "\n\n".join(filtered)
    return text[: news_match.start()] + header + new_block + text[news_match.end() :]


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


def build_context(
    analysis: dict,
    news: list[dict[str, Any]],
    transactions: dict[str, Any] | None = None,
) -> str:
    latest_date = analysis.get("latest_date", "")
    sale = analysis.get("sale", {})
    rent = analysis.get("rent", {})
    sections = [
        f"[분석 기준일]\n{latest_date}\n\n"
        f"[매매 상위]\n{format_region_bucket(sale.get('top5', []))}\n\n"
        f"[매매 하위]\n{format_region_bucket(sale.get('bottom5', []))}\n\n"
        f"[전세 상위]\n{format_region_bucket(rent.get('top5', []))}\n\n"
        f"[전세 하위]\n{format_region_bucket(rent.get('bottom5', []))}"
    ]
    if transactions is not None:
        sections.append(f"[실거래 요약]\n{format_transactions_context(transactions)}")
    sections.append(f"[주요 뉴스]\n{format_news_bucket(news, limit=len(news))}")
    return "\n\n".join(sections)


def resolve_task_config(task_name: str, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    default = DEFAULT_TASK_MODELS.get(task_name, {"provider": "none", "model": "", "max_tokens": 0})
    env_prefix = f"REPORTER_{task_name.upper()}"

    provider = clean_text(os.getenv(f"{env_prefix}_PROVIDER", default["provider"])).lower()
    model = clean_text(os.getenv(f"{env_prefix}_MODEL", default["model"]))
    max_tokens = int(os.getenv(f"{env_prefix}_MAX_TOKENS", str(default["max_tokens"])))

    config = {
        "provider": provider,
        "model": model,
        "max_tokens": max_tokens,
        "allow_backup": True,
    }
    active_overrides = overrides if overrides is not None else GENERATION_OVERRIDE_VAR.get({})
    return _coerce_task_override(task_name, active_overrides.get(task_name), config)


def get_generation_plan(overrides: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    return {
        task_name: resolve_task_config(task_name, overrides)
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


def save_llm_raw_response(task_name: str, raw_text: str) -> str:
    """LLM이 반환한 원문(post-processing 전)을 파일로 저장."""
    PROMPT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROMPT_OUTPUT_DIR / f"{task_name}_raw_response.txt"
    sections = [
        f"[task]\n{task_name}",
        f"[saved_at]\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"[raw_llm_output]\n{raw_text.strip()}",
    ]
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
    api_key = _env_text("OPENAI_API_KEY")
    if not api_key or not model:
        return None

    try:
        response = SESSION.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": model,
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
    api_key = _env_text("GEMINI_API_KEY")
    if not api_key or not model:
        return None

    try:
        response = SESSION.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            params={"key": api_key},
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
    api_key = _env_text("ANTHROPIC_API_KEY")
    if not api_key or not model:
        return None

    try:
        response = SESSION.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
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
    allow_backup = bool(config.get("allow_backup", True))
    if task_name == "telegram_report":
        provider_floor = 5000 if provider in {"gemini", "google"} else 4000
        max_tokens = max(int(max_tokens or 0), provider_floor)

    if provider == "none" or max_tokens <= 0:
        _record_generation_meta(
            task_name,
            provider=provider,
            model=model,
            max_tokens=max_tokens,
            used_llm=False,
            fallback_used=True,
            reason="disabled",
        )
        return fallback_text

    generated_text: str | None = None
    if provider == "openai":
        generated_text = _call_openai(model, system_prompt, user_prompt, max_tokens=max_tokens)
    elif provider in {"gemini", "google"}:
        generated_text = _call_gemini(model, system_prompt, user_prompt, max_tokens=max_tokens)
    elif provider == "anthropic":
        generated_text = _call_anthropic(model, system_prompt, user_prompt, max_tokens=max_tokens)
    else:
        _record_generation_meta(
            task_name,
            provider=provider,
            model=model,
            max_tokens=max_tokens,
            used_llm=False,
            fallback_used=True,
            reason="unknown_provider",
        )
        return fallback_text

    if generated_text:
        save_llm_raw_response(task_name, generated_text)
        _record_generation_meta(
            task_name,
            provider=provider,
            model=model,
            max_tokens=max_tokens,
            used_llm=True,
            fallback_used=False,
            reason="llm_success",
            backup_used=False,
        )
        return generated_text

    backup = BACKUP_TASK_MODELS.get(task_name)
    if allow_backup and backup and backup.get("provider") != provider:
        backup_provider = str(backup.get("provider") or "").lower()
        backup_model = str(backup.get("model") or "")
        backup_max_tokens = int(backup.get("max_tokens") or max_tokens or 0)
        backup_text: str | None = None
        if backup_provider == "openai":
            backup_text = _call_openai(backup_model, system_prompt, user_prompt, max_tokens=backup_max_tokens)
        elif backup_provider in {"gemini", "google"}:
            backup_text = _call_gemini(backup_model, system_prompt, user_prompt, max_tokens=backup_max_tokens)
        elif backup_provider == "anthropic":
            backup_text = _call_anthropic(backup_model, system_prompt, user_prompt, max_tokens=backup_max_tokens)

        if backup_text:
            save_llm_raw_response(task_name, backup_text)
            _record_generation_meta(
                task_name,
                provider=backup_provider,
                model=backup_model,
                max_tokens=backup_max_tokens,
                used_llm=True,
                fallback_used=False,
                reason="backup_llm_success",
                backup_used=True,
                primary_provider=provider,
                primary_model=model,
            )
            return backup_text

    _record_generation_meta(
        task_name,
        provider=provider,
        model=model,
        max_tokens=max_tokens,
        used_llm=False,
        fallback_used=True,
        reason="llm_failed_or_empty",
        backup_used=False,
    )
    return fallback_text

#!/usr/bin/env python3
"""Parse a KB weekly Markdown report and validate derived social copy.

``weekly_report.md`` is the reviewable editorial source. The JSON snapshot is
used only to detect factual drift before card-news media or a publish package
is produced. This module performs no network, LLM, upload, or publish action.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable, Optional


SOCIAL_COPY_SCHEMA = "kb-social-copy/v2"
BUCKETS = (
    ("seoul_sale_all", "서울 25개 구 매매 상승 현황"),
    ("seoul_rent_all", "서울 25개 구 전세 상승 현황"),
    ("capital_sale_top5", "수도권 매매 상승 상위 5"),
    ("capital_rent_top5", "수도권 전세 상승 상위 5"),
    ("non_capital_sale_top5", "비수도권 매매 상승 상위 5"),
    ("non_capital_rent_top5", "비수도권 전세 상승 상위 5"),
)
CANVA_PAGE_COUNT = 16


def report_sha256(text: str) -> str:
    return "sha256:%s" % hashlib.sha256(text.encode("utf-8")).hexdigest()


def _section(text: str, heading: str, level: int) -> str:
    marks = "#" * level
    match = re.search(
        r"^%s\s+%s\s*$\n(.*?)(?=^#{1,%d}\s+|\Z)"
        % (re.escape(marks), re.escape(heading), level),
        text,
        re.MULTILINE | re.DOTALL,
    )
    if not match:
        raise ValueError("weekly report is missing section: %s" % heading)
    return match.group(1).strip()


def _table_blocks(section: str) -> list[str]:
    return re.findall(r"(?:^\|.*\|\s*$\n?){2,}", section, re.MULTILINE)


def _table_rows(block: str) -> tuple[list[str], list[dict[str, str]]]:
    raw_rows = []
    for line in block.splitlines():
        if not line.strip():
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if cells:
            raw_rows.append(cells)
    if len(raw_rows) < 2:
        raise ValueError("invalid Markdown table")
    header = raw_rows[0]
    data_rows = raw_rows[1:]
    if data_rows and all(re.fullmatch(r":?-{3,}:?", cell) for cell in data_rows[0]):
        data_rows = data_rows[1:]
    rows = []
    for cells in data_rows:
        if len(cells) != len(header):
            raise ValueError("Markdown table column count does not match its header")
        rows.append(dict(zip(header, cells)))
    return header, rows


def _number(value: str) -> float:
    return float(value.replace(",", "").replace("%", "").strip())


def _money(value: str) -> Optional[int]:
    if "없음" in value or value.strip() in {"", "-"}:
        return None
    return int(value.replace(",", "").replace("만원", "").strip())


def _paragraph_before_first_table(section: str) -> str:
    lines = []
    for line in section.splitlines():
        if line.lstrip().startswith("|"):
            break
        clean = line.strip()
        if clean and clean != "해당 버킷 실거래 표입니다.":
            lines.append(clean)
    return " ".join(lines)


def _parse_news(section: str) -> list[dict[str, str]]:
    pattern = re.compile(
        r"^\d+\.\s+\*\*(.*?)\*\*\s*$\n"
        r"\s+-\s+출처:\s*(.*?)\s*\|\s*(.*?)\s*$\n"
        r"\s+-\s+요약:\s*(.*?)\s*$\n"
        r"\s+-\s+링크:\s*(https://\S+)\s*$",
        re.MULTILINE,
    )
    return [
        {"title": title.strip(), "publisher": publisher.strip(),
         "date": date.strip(), "summary": summary.strip(), "url": url.strip()}
        for title, publisher, date, summary, url in pattern.findall(section)
    ]


def parse_weekly_report(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    title_match = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
    if not title_match:
        raise ValueError("weekly report has no Markdown H1 title")
    date_match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", title_match.group(1))
    if not date_match:
        raise ValueError("weekly report H1 has no YYYY-MM-DD reference date")

    parsed_buckets = []
    for key, label in BUCKETS:
        bucket_section = _section(text, label, 3)
        tables = _table_blocks(bucket_section)
        if len(tables) != 2:
            raise ValueError("%s must contain one metric table and one transaction table" % label)
        metric_header, metric_rows = _table_rows(tables[0])
        trade_header, trade_rows = _table_rows(tables[1])
        if metric_header != ["순위", "지역", "연속 상승"]:
            raise ValueError("unexpected metric table header in %s" % label)
        if trade_header != ["지역", "타입", "단지명", "계약일", "매매가", "전세참고"]:
            raise ValueError("unexpected transaction table header in %s" % label)
        metrics = [
            {
                "rank": int(row["순위"]),
                "region": row["지역"],
                "consecutive_rise_weeks": int(_number(row["연속 상승"].replace("주", ""))),
            }
            for row in metric_rows
        ]
        expected_count = 25 if key.startswith("seoul_") else len(metrics)
        if key.startswith("seoul_") and len(metrics) != expected_count:
            raise ValueError("%s must contain all 25 Seoul districts" % label)
        if not key.startswith("seoul_") and len(metrics) > 5:
            raise ValueError("%s must contain no more than five ranked regions" % label)
        if [item["rank"] for item in metrics] != list(range(1, len(metrics) + 1)):
            raise ValueError("%s ranks must be sequential" % label)
        trades = [
            {
                "region": row["지역"],
                "area_type": row["타입"].replace("타입", "").strip(),
                "name": row["단지명"],
                "contract_date": row["계약일"],
                "sale_price": _money(row["매매가"]),
                "rent_price": _money(row["전세참고"]),
            }
            for row in trade_rows
        ]
        metric_regions = {item["region"] for item in metrics}
        if {item["region"] for item in trades} != metric_regions:
            raise ValueError("%s transaction regions must match its five metric regions" % label)
        parsed_buckets.append({
            "key": key,
            "label": label,
            "insight": _paragraph_before_first_table(bucket_section),
            "metrics": metrics,
            "transactions": trades,
        })

    news = _parse_news(_section(text, "이번 주 주요 뉴스", 2))
    if len(news) < 3:
        raise ValueError("weekly report must contain at least three grounded news items")
    disclaimer = _section(text, "면책 문구", 2)
    if not disclaimer:
        raise ValueError("weekly report disclaimer is empty")

    return {
        "title": title_match.group(1).strip(),
        "latest_date": date_match.group(1),
        "report_sha256": report_sha256(text),
        "report_path": str(path.resolve()),
        "summary": _section(text, "이번 주 핵심 요약", 2),
        "buckets": parsed_buckets,
        "news": news,
        "conclusion": _section(text, "한 줄 정리", 2),
        "disclaimer": disclaimer,
        "body": text.strip(),
    }


def _representative_snapshot_trade(payload: dict[str, Any], area_type: str) -> dict[str, Any]:
    area = payload.get(area_type) or {}
    trades = area.get("trades") or []
    if not trades:
        return {}
    trade = trades[0]
    rents = trade.get("related_rent_trades") or []
    if not rents:
        rents = area.get("rent_trades") or []
    return {
        "name": str(trade.get("name") or ""),
        "contract_date": str(trade.get("contract_date") or ""),
        "sale_price": trade.get("price"),
        "rent_price": rents[0].get("price") if rents else None,
    }


def validate_report_against_snapshot(report: dict[str, Any], snapshot: dict[str, Any]) -> None:
    errors = []
    if report["latest_date"] != str(snapshot.get("latest_date") or ""):
        errors.append("reference date differs from data_snapshot.json")
    snapshot_regions = ((snapshot.get("analysis") or {}).get("content_regions") or {})
    snapshot_transactions = snapshot.get("transactions") or {}
    for bucket in report["buckets"]:
        key = bucket["key"]
        expected_metrics = snapshot_regions.get(key) or []
        if key.startswith("seoul_") and len(expected_metrics) != 25:
            errors.append("snapshot section %s does not contain all 25 Seoul districts" % key)
            continue
        if not key.startswith("seoul_") and len(expected_metrics) > 5:
            errors.append("snapshot section %s contains more than five regions" % key)
            continue
        if len(bucket["metrics"]) != len(expected_metrics):
            errors.append("%s region count differs from snapshot" % key)
            continue
        for actual, expected in zip(bucket["metrics"], expected_metrics):
            if actual["region"] != str(expected.get("region") or ""):
                errors.append("%s region order differs from snapshot" % key)
            if actual["consecutive_rise_weeks"] != int(expected.get("consecutive_rise_weeks") or 0):
                errors.append("%s %s consecutive rise weeks differ from snapshot" % (key, actual["region"]))
        bucket_transactions = snapshot_transactions.get(key) or {}
        for actual in bucket["transactions"]:
            expected = _representative_snapshot_trade(
                bucket_transactions.get(actual["region"]) or {}, actual["area_type"]
            )
            if actual["sale_price"] is None:
                if expected:
                    errors.append("%s %s %s missing trade differs from snapshot" % (
                        key, actual["region"], actual["area_type"],
                    ))
                continue
            for field in ("name", "contract_date", "sale_price", "rent_price"):
                if actual[field] != expected.get(field):
                    errors.append("%s %s %s %s differs from snapshot" % (
                        key, actual["region"], actual["area_type"], field,
                    ))
    snapshot_news = {
        (str(item.get("title") or ""), str(item.get("resolved_url") or item.get("url") or item.get("link") or ""))
        for item in snapshot.get("news") or []
    }
    for item in report["news"]:
        if (item["title"], item["url"]) not in snapshot_news:
            errors.append("news item is not grounded in snapshot: %s" % item["title"])
    if errors:
        raise ValueError("weekly report validation failed: " + "; ".join(dict.fromkeys(errors)))


def load_and_validate_report(report_path: Path, snapshot_path: Path) -> dict[str, Any]:
    report = parse_weekly_report(report_path)
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    validate_report_against_snapshot(report, snapshot)
    return report


def _copy_text(copy: dict[str, Any]) -> str:
    slides = copy.get("slides") or []
    values: Iterable[str] = [str(copy.get("caption") or "")]
    values = list(values) + [
        "%s\n%s" % (str(item.get("title") or ""), str(item.get("body") or ""))
        for item in slides if isinstance(item, dict)
    ]
    return "\n".join(values)


def validate_social_copy(copy: dict[str, Any], report: dict[str, Any]) -> None:
    errors = []
    if copy.get("schema_version") != SOCIAL_COPY_SCHEMA:
        errors.append("schema_version must be %s" % SOCIAL_COPY_SCHEMA)
    if copy.get("source_report_sha256") != report["report_sha256"]:
        errors.append("source_report_sha256 does not match weekly_report.md")
    if str(copy.get("latest_date") or "") != report["latest_date"]:
        errors.append("social copy reference date does not match weekly_report.md")
    caption = str(copy.get("caption") or "").strip()
    if not caption or len(caption) > 2200:
        errors.append("caption must contain 1 to 2200 characters")
    slides = copy.get("slides") or []
    if not isinstance(slides, list) or len(slides) != CANVA_PAGE_COUNT:
        errors.append("social copy must contain exactly %s slides" % CANVA_PAGE_COUNT)
    else:
        positions = [item.get("position") for item in slides if isinstance(item, dict)]
        if positions != list(range(1, CANVA_PAGE_COUNT + 1)):
            errors.append("slide positions must be 1 through %s in order" % CANVA_PAGE_COUNT)
        for item in slides:
            if not isinstance(item, dict) or not str(item.get("title") or "").strip() or not str(item.get("body") or "").strip():
                errors.append("every slide requires a non-empty title and body")
                break

    report_numbers = {
        token.replace(",", "")
        for token in re.findall(r"(?<![A-Za-z가-힣])[-+]?\d[\d,]*(?:\.\d+)?", report["body"])
    }
    report_numbers.update(str(number) for number in range(1, CANVA_PAGE_COUNT + 1))
    copy_numbers = {
        token.replace(",", "")
        for token in re.findall(r"(?<![A-Za-z가-힣])[-+]?\d[\d,]*(?:\.\d+)?", _copy_text(copy))
    }
    unknown_numbers = sorted(copy_numbers - report_numbers)
    if unknown_numbers:
        errors.append("social copy contains numbers not grounded in the report: %s" % ", ".join(unknown_numbers))
    if errors:
        raise ValueError("invalid kb-social-copy: " + "; ".join(errors))


def load_social_copy(path: Path, report: dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("kb-social-copy root must be a JSON object")
    validate_social_copy(payload, report)
    return payload

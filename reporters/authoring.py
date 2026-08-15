from __future__ import annotations

import json
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .blog import KB_DATAHUB_URL
from .common import (
    BUCKET_LABELS,
    CONTENT_BUCKET_ORDER,
    build_context,
    clean_text,
    format_bucket_metric_table,
    format_bucket_transaction_table,
)

BASE_DIR = Path(__file__).resolve().parent.parent
AUTHORING_OUTPUT_DIR = BASE_DIR / "reports"
AUTHORING_ARCHIVE_DIR = AUTHORING_OUTPUT_DIR / "archive"
OUTPUT_MODES = {"authoring_package", "draft_only", "both"}


def _subject_phrase(value: str) -> str:
    text = clean_text(value)
    if not text:
        return text
    last = text[-1]
    if "가" <= last <= "힣":
        has_final_consonant = (ord(last) - ord("가")) % 28 != 0
        return f"{text}{'이' if has_final_consonant else '가'}"
    return f"{text}이"


def normalize_output_mode(output_mode: str | None) -> str:
    normalized = clean_text(output_mode or "both").lower()
    return normalized if normalized in OUTPUT_MODES else "both"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(_json_safe(item) for item in value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def _table_cell(value: Any) -> str:
    text = clean_text(value)
    return text.replace("|", "/") if text else "-"


def _latest_date_text(analysis: dict[str, Any]) -> str:
    return clean_text(analysis.get("latest_date")) or datetime.now().strftime("%Y-%m-%d")


def _archive_copy(path: Path, latest_date: str) -> str:
    AUTHORING_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    normalized_date = clean_text(latest_date).replace("/", "-") or "unknown-date"
    archive_path = AUTHORING_ARCHIVE_DIR / f"{timestamp}_{normalized_date}_{path.name}"
    shutil.copyfile(path, archive_path)
    return str(archive_path)


def _write_text_artifact(filename: str, content: str, latest_date: str) -> dict[str, str]:
    AUTHORING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = AUTHORING_OUTPUT_DIR / filename
    path.write_text(content.rstrip() + "\n", encoding="utf-8")
    return {"latest": str(path), "archive": _archive_copy(path, latest_date)}


def _write_json_artifact(filename: str, payload: dict[str, Any], latest_date: str) -> dict[str, str]:
    AUTHORING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = AUTHORING_OUTPUT_DIR / filename
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"latest": str(path), "archive": _archive_copy(path, latest_date)}


def build_data_snapshot(
    analysis: dict[str, Any],
    news: list[dict[str, Any]],
    transactions: dict[str, Any] | None,
    *,
    generation_plan: dict[str, Any] | None = None,
    generation_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_date": _latest_date_text(analysis),
        "source": {
            "kb_datahub_url": KB_DATAHUB_URL,
            "source_files": analysis.get("source_files", {}) or {},
            "report_images": analysis.get("report_images", []) or [],
        },
        "analysis": {
            "latest_date": analysis.get("latest_date", ""),
            "sale": analysis.get("sale", {}) or {},
            "rent": analysis.get("rent", {}) or {},
            "content_regions": analysis.get("content_regions", {}) or {},
        },
        "transactions": transactions or {},
        "news": news,
        "generation_plan": generation_plan or {},
        "generation_meta": generation_meta or {},
    }


def _top_region(content_regions: dict[str, Any], bucket_name: str) -> str:
    bucket = content_regions.get(bucket_name) or []
    if not bucket:
        return "확인 지역 없음"
    item = bucket[0] if isinstance(bucket[0], dict) else {"region": bucket[0]}
    region = clean_text(item.get("region"))
    weeks = int(item.get("consecutive_rise_weeks") or 0)
    return f"{region}(연속 상승 {weeks}주)" if region else "확인 지역 없음"


def _bucket_commentary(bucket_name: str, items: list[dict[str, Any]] | None) -> str:
    items = items or []
    label = BUCKET_LABELS.get(bucket_name, bucket_name)
    if not items:
        return f"{label}은 이번 실행에서 확인된 지역이 없습니다."

    first = items[0]
    last = items[-1]
    first_region = clean_text(first.get("region"))
    last_region = clean_text(last.get("region"))
    first_weeks = int(first.get("consecutive_rise_weeks") or 0)
    if bucket_name.startswith("seoul_"):
        return f"{label}은 서울 25개 구를 모두 포함하며, {_subject_phrase(first_region)} 연속 상승 {first_weeks}주로 가장 길게 이어지고 있습니다. {last_region}까지 빠짐없이 확인합니다."
    return f"{label}에서는 {_subject_phrase(first_region)} 연속 상승 {first_weeks}주이며, {last_region}까지 최신 주간 상승 상위권에 포함됐습니다."


def _format_news_markdown(news: list[dict[str, Any]], *, limit: int = 10) -> str:
    if not news:
        return "- 수집된 주요 뉴스가 없습니다."

    lines: list[str] = []
    for index, article in enumerate(news[:limit], start=1):
        title = _table_cell(article.get("title"))
        publisher = _table_cell(article.get("publisher") or "언론사")
        issue_date = _table_cell(article.get("issue_date") or article.get("published_at"))
        url = clean_text(article.get("url") or article.get("resolved_url") or article.get("originallink") or article.get("link"))
        description = clean_text(article.get("description") or article.get("content"))
        if len(description) > 180:
            description = description[:177].rstrip() + "..."

        lines.append(f"{index}. **{title}**")
        lines.append(f"   - 출처: {publisher} | {issue_date}")
        if description:
            lines.append(f"   - 요약: {description}")
        if url:
            lines.append(f"   - 링크: {url}")
    return "\n".join(lines)


def build_weekly_markdown_report(
    analysis: dict[str, Any],
    news: list[dict[str, Any]],
    transactions: dict[str, Any] | None,
) -> str:
    latest_date = _latest_date_text(analysis)
    content_regions = analysis.get("content_regions", {}) or {}
    normalized_transactions = transactions if isinstance(transactions, dict) else {}

    lines = [
        f"# {latest_date} KB부동산 주간 동향 정리",
        "",
        "이번 주 KB 주간 시계열에서 서울 모든 구와 수도권·비수도권 상승 지역을 확인했습니다. 아래 내용은 연속 상승 기간, 최근 실거래, 수집 뉴스를 같은 섹션 안에서 연결해 읽을 수 있도록 정리한 초안입니다.",
        "",
        "## 이번 주 핵심 요약",
        "",
        f"- 서울 매매 최장 연속 상승 지역: {_top_region(content_regions, 'seoul_sale_all')}",
        f"- 서울 전세 최장 연속 상승 지역: {_top_region(content_regions, 'seoul_rent_all')}",
        f"- 수도권 매매 강세 대표 지역: {_top_region(content_regions, 'capital_sale_top5')}",
        f"- 비수도권 매매 강세 대표 지역: {_top_region(content_regions, 'non_capital_sale_top5')}",
        f"- 수도권 전세 강세 대표 지역: {_top_region(content_regions, 'capital_rent_top5')}",
        f"- 비수도권 전세 강세 대표 지역: {_top_region(content_regions, 'non_capital_rent_top5')}",
        "",
        "## 6개 상승 섹션 상세",
        "",
    ]

    for bucket_name in CONTENT_BUCKET_ORDER:
        bucket_items = content_regions.get(bucket_name) or []
        bucket_label = BUCKET_LABELS.get(bucket_name, bucket_name)
        lines.extend(
            [
                f"### {bucket_label}",
                "",
                _bucket_commentary(bucket_name, bucket_items),
                "",
                format_bucket_metric_table(bucket_items),
                "",
                "해당 섹션 실거래 표입니다.",
                "",
                format_bucket_transaction_table(bucket_items, normalized_transactions.get(bucket_name)),
                "",
            ]
        )

    lines.extend(
        [
            "## 이번 주 주요 뉴스",
            "",
            _format_news_markdown(news),
            "",
            "## 한 줄 정리",
            "",
            "연속 상승 기간과 실제 거래 흐름을 함께 확인하면 지역별 상승의 지속성을 더 입체적으로 볼 수 있습니다.",
            "",
            "## 참고 링크",
            "",
            f"- KB부동산 데이터허브: {KB_DATAHUB_URL}",
        ]
    )

    for article in news[:10]:
        title = clean_text(article.get("title"))
        url = clean_text(article.get("url") or article.get("resolved_url") or article.get("originallink") or article.get("link"))
        if title and url:
            lines.append(f"- {title}: {url}")

    lines.extend(
        [
            "",
            "## 면책 문구",
            "",
            "이 글은 KB 주간 통계, 수집 뉴스, 확인된 실거래 요약을 바탕으로 작성한 참고용 브리핑입니다. 투자 판단과 최종 의사결정은 추가 확인과 본인 책임하에 진행하시기 바랍니다.",
        ]
    )
    return "\n".join(lines).strip()


def build_llm_authoring_package(
    analysis: dict[str, Any],
    news: list[dict[str, Any]],
    transactions: dict[str, Any] | None,
    data_snapshot: dict[str, Any],
) -> str:
    latest_date = _latest_date_text(analysis)
    report_images = analysis.get("report_images", []) or []
    image_text = "\n".join(f"- {path}" for path in report_images[:12]) if report_images else "- 첨부 이미지 없음"
    data_json = json.dumps(_json_safe(data_snapshot), ensure_ascii=False, indent=2)

    return f"""
# Claude/GPT 작성 패키지 - {latest_date} KB부동산 주간 리포트

아래 전체 내용을 Claude 또는 ChatGPT 웹에 붙여넣고, 네이버 블로그용 Markdown 보고서와 텔레그램 요약문을 작성하도록 요청하세요. 제공된 데이터 밖의 수치, 단지명, 정책 사실은 만들지 않는 것이 핵심입니다.

## 작성 요청

당신은 한국 부동산 시장 주간 브리핑을 작성하는 에디터입니다. 아래 KB 통계, 실거래 데이터, 뉴스, 보도자료 이미지 후보를 바탕으로 다음 산출물을 작성해 주세요.

- 네이버 블로그용 Markdown 보고서 1개
- 텔레그램에 바로 보낼 수 있는 짧은 요약문 1개
- 제목은 과장하지 말고 기준일과 핵심 흐름이 드러나게 작성
- 모든 문장은 존댓말로 작성
- 6개 상승 섹션을 제공된 순서대로 모두 포함
- 각 섹션은 연속 상승 표 다음에 같은 섹션의 실거래 표를 바로 배치
- 뉴스는 제공된 기사만 사용하고 링크 URL은 수정하지 않기
- 확인되지 않은 인과관계는 단정하지 않기
- 마지막에 참고 링크와 면책 문구 포함

## 빠른 컨텍스트

{build_context(analysis, news, transactions)}

## 보도자료 이미지 후보

{image_text}

## 구조화 데이터 JSON

```json
{data_json}
```
""".strip()


def generate_authoring_artifacts(
    analysis: dict[str, Any],
    news: list[dict[str, Any]],
    transactions: dict[str, Any] | None,
    *,
    output_mode: str | None = "both",
    generation_plan: dict[str, Any] | None = None,
    generation_meta: dict[str, Any] | None = None,
    card_news_script: str | None = None,
) -> dict[str, Any]:
    mode = normalize_output_mode(output_mode)
    latest_date = _latest_date_text(analysis)
    data_snapshot = build_data_snapshot(
        analysis,
        news,
        transactions,
        generation_plan=generation_plan,
        generation_meta=generation_meta,
    )

    latest_files: dict[str, str] = {}
    archive_files: dict[str, str] = {}

    data_paths = _write_json_artifact("data_snapshot.json", data_snapshot, latest_date)
    latest_files["data_snapshot"] = data_paths["latest"]
    archive_files["data_snapshot"] = data_paths["archive"]

    if clean_text(card_news_script):
        card_paths = _write_text_artifact("card_news_script.md", str(card_news_script), latest_date)
        latest_files["card_news_script"] = card_paths["latest"]
        archive_files["card_news_script"] = card_paths["archive"]

    if mode in {"authoring_package", "both"}:
        package = build_llm_authoring_package(analysis, news, transactions, data_snapshot)
        package_paths = _write_text_artifact("llm_package.md", package, latest_date)
        latest_files["llm_package"] = package_paths["latest"]
        archive_files["llm_package"] = package_paths["archive"]

    if mode in {"draft_only", "both"}:
        report = build_weekly_markdown_report(analysis, news, transactions)
        report_paths = _write_text_artifact("weekly_report.md", report, latest_date)
        latest_files["weekly_report"] = report_paths["latest"]
        archive_files["weekly_report"] = report_paths["archive"]

    return {
        "output_mode": mode,
        "authoring_files": latest_files,
        "authoring_archive_files": archive_files,
    }

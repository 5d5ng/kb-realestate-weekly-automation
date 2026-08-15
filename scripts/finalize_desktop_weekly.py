#!/usr/bin/env python3
"""Publish verified Codex desktop artifacts to Telegram and write a receipt.

The script is deliberately limited to Telegram. Canva has already been created
by the desktop task, while Naver Blog and Instagram remain unpublished.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from reporters.telegram import fallback_telegram_report
from scheduler import KST
from sender import send_telegram, send_telegram_document


RUNTIME_DIR = BASE_DIR / "reports" / "runtime"
HISTORY_DIR = RUNTIME_DIR / "history"
LAST_RECEIPT_PATH = RUNTIME_DIR / "last_desktop_weekly_run.json"
SNAPSHOT_PATH = BASE_DIR / "reports" / "data_snapshot.json"
CANVA_PAGE_COUNT = 16


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Codex desktop KB 주간 작업의 Telegram 최종 발송")
    parser.add_argument("--canva-design-id", required=True)
    parser.add_argument("--canva-title", required=True)
    parser.add_argument("--canva-edit-url", required=True)
    parser.add_argument("--canva-view-url", default="")
    parser.add_argument("--canva-page-count", type=int, required=True)
    parser.add_argument("--blog-path", required=True)
    parser.add_argument("--cardnews-path", required=True)
    return parser.parse_args()


def _resolve_file(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = BASE_DIR / path
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {resolved}")
    return resolved


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _message_ids(result: dict[str, Any]) -> list[int]:
    return [item for item in (result.get("message_ids") or []) if isinstance(item, int)]


def _validate_canva_page_count(page_count: int) -> None:
    if page_count != CANVA_PAGE_COUNT:
        raise ValueError(f"Canva 페이지 수가 {CANVA_PAGE_COUNT}이 아닙니다: {page_count}")


def _successful_receipt(receipt: dict[str, Any], artifact_digest: str) -> bool:
    telegram = receipt.get("telegram") or {}
    return bool(
        receipt.get("success")
        and receipt.get("artifact_digest") == artifact_digest
        and telegram.get("success")
        and _message_ids(telegram)
    )


def _artifact_digest(
    *,
    latest_date: str,
    canva_design_id: str,
    blog_path: Path,
    cardnews_path: Path,
) -> str:
    canonical = {
        "latest_date": latest_date,
        "canva_design_id": canva_design_id,
        "blog_sha256": _sha256(blog_path),
        "cardnews_sha256": _sha256(cardnews_path),
    }
    return hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def _write_receipt(payload: dict[str, Any]) -> Path:
    now = datetime.now(KST)
    history_path = HISTORY_DIR / f"{now.strftime('%Y%m%d_%H%M%S')}_desktop_weekly.json"
    _write_json(history_path, payload)
    _write_json(LAST_RECEIPT_PATH, payload)
    return history_path


def main() -> int:
    args = _parse_args()
    started_at = datetime.now(KST)
    blog_path: Path | None = None
    cardnews_path: Path | None = None
    try:
        _validate_canva_page_count(args.canva_page_count)
        if not args.canva_edit_url.startswith("https://www.canva.com/"):
            raise ValueError("Canva 편집 URL이 올바르지 않습니다.")
        if args.canva_view_url and not args.canva_view_url.startswith("https://www.canva.com/"):
            raise ValueError("Canva 보기 URL이 올바르지 않습니다.")

        snapshot = _read_json(SNAPSHOT_PATH)
        if not snapshot:
            raise RuntimeError(f"데이터 스냅샷을 읽을 수 없습니다: {SNAPSHOT_PATH}")
        latest_date = str(snapshot.get("latest_date") or snapshot.get("analysis", {}).get("latest_date") or "")
        if not latest_date:
            raise RuntimeError("KB 기준일이 비어 있습니다.")

        blog_path = _resolve_file(args.blog_path)
        cardnews_path = _resolve_file(args.cardnews_path)
        if latest_date not in blog_path.name or latest_date not in cardnews_path.name:
            raise ValueError("KB 기준일과 데스크톱 산출물 파일명이 일치하지 않습니다.")
        artifact_digest = _artifact_digest(
            latest_date=latest_date,
            canva_design_id=args.canva_design_id,
            blog_path=blog_path,
            cardnews_path=cardnews_path,
        )

        previous = _read_json(LAST_RECEIPT_PATH)
        if _successful_receipt(previous, artifact_digest):
            duplicate_receipt = dict(previous)
            duplicate_receipt["duplicate_prevented"] = True
            duplicate_receipt["duplicate_checked_at"] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            print(json.dumps(duplicate_receipt, ensure_ascii=False))
            return 0

        report = fallback_telegram_report(
            snapshot.get("analysis") or {},
            snapshot.get("news") or [],
            snapshot.get("transactions") or {},
            max_news_items=12,
        )
        canva_url = args.canva_view_url or args.canva_edit_url
        report = (
            f"{report.rstrip()}\n\n"
            f"[검토용 결과물]\n"
            f"- Canva 프로젝트: {canva_url}\n"
            f"- 네이버 블로그: 초안 파일로 첨부\n"
            f"- Instagram·네이버 공개 업로드: 미실행"
        )

        telegram = send_telegram(report, enabled=True)
        blog_document = send_telegram_document(
            blog_path,
            caption=f"{latest_date} KB부동산 네이버 블로그 초안",
            enabled=True,
        )
        cardnews_document = send_telegram_document(
            cardnews_path,
            caption=f"{latest_date} KB부동산 Canva 편집 원본 HTML",
            enabled=True,
        )
        success = bool(
            telegram.get("success")
            and _message_ids(telegram)
            and blog_document.get("success")
            and isinstance(blog_document.get("message_id"), int)
            and cardnews_document.get("success")
            and isinstance(cardnews_document.get("message_id"), int)
        )
        completed_at = datetime.now(KST)
        receipt = {
            "success": success,
            "mode": "codex-desktop",
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": completed_at.strftime("%Y-%m-%d %H:%M:%S"),
            "kb_latest_date": latest_date,
            "api_call_count": 0,
            "artifact_digest": artifact_digest,
            "telegram": telegram,
            "telegram_documents": {
                "naver_blog_draft": blog_document,
                "canva_import_html": cardnews_document,
            },
            "canva": {
                "design_id": args.canva_design_id,
                "title": args.canva_title,
                "page_count": args.canva_page_count,
                "edit_url": args.canva_edit_url,
                "view_url": args.canva_view_url,
            },
            "artifacts": {
                "naver_blog_draft": str(blog_path),
                "canva_import_html": str(cardnews_path),
            },
            "publishing": {
                "telegram_sent": success,
                "sms_sent": False,
                "kakao_sent": False,
                "instagram_uploaded": False,
                "naver_published": False,
            },
        }
        history_path = _write_receipt(receipt)
        receipt["history_path"] = str(history_path)
        print(json.dumps(receipt, ensure_ascii=False))
        return 0 if success else 1
    except Exception as exc:
        receipt = {
            "success": False,
            "mode": "codex-desktop",
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "api_call_count": 0,
            "error": str(exc),
            "artifacts": {
                "naver_blog_draft": str(blog_path) if blog_path else args.blog_path,
                "canva_import_html": str(cardnews_path) if cardnews_path else args.cardnews_path,
            },
            "publishing": {
                "telegram_sent": False,
                "sms_sent": False,
                "kakao_sent": False,
                "instagram_uploaded": False,
                "naver_published": False,
            },
        }
        history_path = _write_receipt(receipt)
        receipt["history_path"] = str(history_path)
        print(json.dumps(receipt, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

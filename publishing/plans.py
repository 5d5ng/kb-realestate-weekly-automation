from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PLAN_SCHEMA = "publishing-plan/v1"
PLAN_STATUSES = {"pending_review", "approved", "publishing", "published", "failed"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _plan_id(
    target: str,
    package_id: str,
    content_digest: str,
    destination_account: str = "",
) -> str:
    destination = str(destination_account or "").strip().lower()
    identity = (
        f"{target}:{destination}:{package_id}:{content_digest}"
        if destination
        else f"{target}:{package_id}:{content_digest}"
    )
    raw = identity.encode("utf-8")
    return f"plan_{hashlib.sha256(raw).hexdigest()[:16]}"


class PublishingPlanStore:
    def __init__(self, root: str | Path | None = None) -> None:
        configured = root or os.getenv("PUBLISHING_PLAN_DIR") or "reports/publishing_plans"
        self.root = Path(configured).expanduser().resolve()

    def _ensure_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, plan_id: str) -> Path:
        normalized = str(plan_id or "").strip()
        if not normalized.startswith("plan_") or not normalized.replace("_", "").isalnum():
            raise ValueError("invalid plan_id")
        return self.root / f"{normalized}.json"

    def _write(self, payload: dict[str, Any]) -> Path:
        self._ensure_root()
        path = self.path_for(str(payload.get("plan_id") or ""))
        temp_path = path.with_suffix(".json.tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(path)
        return path

    def create(
        self,
        package: dict[str, Any],
        *,
        target: str,
        destination_account: str = "",
    ) -> tuple[dict[str, Any], Path]:
        normalized_target = str(target or "").strip().lower()
        normalized_destination = str(destination_account or "").strip().lower()
        package_id = str(package.get("package_id") or "").strip()
        content_digest = str(package.get("content_digest") or "").strip()
        if not normalized_target:
            raise ValueError("target is required")
        if not package_id or not content_digest:
            raise ValueError("package_id and content_digest are required")

        plan_id = _plan_id(
            normalized_target,
            package_id,
            content_digest,
            normalized_destination,
        )
        path = self.path_for(plan_id)
        if path.exists():
            return self.get(plan_id), path

        payload = {
            "schema_version": PLAN_SCHEMA,
            "plan_id": plan_id,
            "target": normalized_target,
            "destination_account": normalized_destination or None,
            "package_id": package_id,
            "content_digest": content_digest,
            "title": package.get("title"),
            "media_count": len(package.get("media") or []),
            "status": "pending_review",
            "created_at": _now_iso(),
            "approved_at": None,
            "published_at": None,
            "published_media_id": None,
            "last_error": None,
        }
        return payload, self._write(payload)

    def get(self, plan_id: str) -> dict[str, Any]:
        path = self.path_for(plan_id)
        if not path.exists():
            raise FileNotFoundError(f"publishing plan not found: {plan_id}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid publishing plan: {plan_id}")
        if payload.get("schema_version") != PLAN_SCHEMA:
            raise ValueError(f"unsupported publishing plan schema: {plan_id}")
        if payload.get("status") not in PLAN_STATUSES:
            raise ValueError(f"invalid publishing plan status: {plan_id}")
        return payload

    def approve(self, plan_id: str, *, expected_digest: str) -> tuple[dict[str, Any], Path]:
        payload = self.get(plan_id)
        if payload.get("status") == "published":
            return payload, self.path_for(plan_id)
        if str(expected_digest or "").strip() != payload.get("content_digest"):
            raise ValueError("expected_digest does not match the reviewed content")
        payload.update(
            {
                "status": "approved",
                "approved_at": _now_iso(),
                "last_error": None,
            }
        )
        return payload, self._write(payload)

    def mark_publishing(self, plan_id: str) -> tuple[dict[str, Any], Path]:
        payload = self.get(plan_id)
        if payload.get("status") != "approved":
            raise ValueError("publishing plan must be approved before publishing")
        payload.update({"status": "publishing", "last_error": None})
        return payload, self._write(payload)

    def mark_published(self, plan_id: str, *, media_id: str) -> tuple[dict[str, Any], Path]:
        payload = self.get(plan_id)
        payload.update(
            {
                "status": "published",
                "published_at": _now_iso(),
                "published_media_id": str(media_id),
                "last_error": None,
            }
        )
        return payload, self._write(payload)

    def mark_failed(self, plan_id: str, *, error: str) -> tuple[dict[str, Any], Path]:
        payload = self.get(plan_id)
        payload.update({"status": "failed", "last_error": str(error)})
        return payload, self._write(payload)

    def list(self, *, target: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        if not self.root.exists():
            return []
        normalized_target = str(target or "").strip().lower()
        paths = sorted(
            self.root.glob("plan_*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        items: list[dict[str, Any]] = []
        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if normalized_target and payload.get("target") != normalized_target:
                continue
            items.append(payload)
            if len(items) >= max(1, min(limit, 200)):
                break
        return items

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse


CONTENT_PACKAGE_SCHEMA = "content-package/v1"
CONTENT_TYPES = {"single_image", "carousel", "video", "text"}
MEDIA_TYPES = {"image", "video"}


class ContentPackageError(ValueError):
    pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _normalized_media(media: Any) -> list[dict[str, Any]]:
    if media is None:
        return []
    if not isinstance(media, list):
        raise ContentPackageError("media must be an array")

    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(media, start=1):
        if not isinstance(item, dict):
            raise ContentPackageError(f"media[{index}] must be an object")
        media_type = _clean_text(item.get("type", "image")).lower()
        if media_type not in MEDIA_TYPES:
            raise ContentPackageError(f"media[{index}].type must be image or video")
        source = _clean_text(item.get("source"))
        if not source:
            raise ContentPackageError(f"media[{index}].source is required")
        normalized.append(
            {
                "position": index,
                "type": media_type,
                "source": source,
                "alt_text": _clean_text(item.get("alt_text")),
            }
        )
    return normalized


def _canonical_content(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": payload.get("schema_version"),
        "title": payload.get("title"),
        "content_type": payload.get("content_type"),
        "caption": payload.get("caption"),
        "media": payload.get("media"),
        "targets": payload.get("targets"),
        "metadata": payload.get("metadata"),
    }


def content_digest(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        _canonical_content(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def build_content_package(
    *,
    title: str,
    content_type: str,
    caption: str = "",
    media: list[dict[str, Any]] | None = None,
    targets: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_type = _clean_text(content_type).lower()
    if normalized_type not in CONTENT_TYPES:
        raise ContentPackageError(f"content_type must be one of {sorted(CONTENT_TYPES)}")
    normalized_title = _clean_text(title)
    if not normalized_title:
        raise ContentPackageError("title is required")
    normalized_targets = [
        _clean_text(target).lower()
        for target in (targets or [])
        if _clean_text(target)
    ]
    normalized_metadata = metadata or {}
    if not isinstance(normalized_metadata, dict):
        raise ContentPackageError("metadata must be an object")

    payload = {
        "schema_version": CONTENT_PACKAGE_SCHEMA,
        "title": normalized_title,
        "content_type": normalized_type,
        "caption": _clean_text(caption),
        "media": _normalized_media(media),
        "targets": list(dict.fromkeys(normalized_targets)),
        "metadata": normalized_metadata,
    }
    digest = content_digest(payload)
    payload.update(
        {
            "package_id": f"pkg_{digest[:16]}",
            "content_digest": digest,
            "created_at": _now_iso(),
        }
    )
    validation = validate_content_package(payload)
    if not validation["valid"]:
        raise ContentPackageError("; ".join(validation["errors"]))
    return payload


def _is_https_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme == "https" and bool(parsed.netloc)


def validate_content_package(
    payload: dict[str, Any],
    *,
    target: str | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    if not isinstance(payload, dict):
        return {"valid": False, "errors": ["package must be an object"], "warnings": []}
    if payload.get("schema_version") != CONTENT_PACKAGE_SCHEMA:
        errors.append(f"schema_version must be {CONTENT_PACKAGE_SCHEMA}")
    if not _clean_text(payload.get("title")):
        errors.append("title is required")
    content_type = _clean_text(payload.get("content_type")).lower()
    if content_type not in CONTENT_TYPES:
        errors.append(f"content_type must be one of {sorted(CONTENT_TYPES)}")

    media = payload.get("media")
    if not isinstance(media, list):
        errors.append("media must be an array")
        media = []
    if content_type == "single_image" and len(media) != 1:
        errors.append("single_image content requires exactly one media item")
    if content_type == "carousel" and len(media) < 2:
        errors.append("carousel content requires at least two media items")
    if content_type == "video" and len(media) != 1:
        errors.append("video content requires exactly one media item")
    if content_type == "text" and media:
        warnings.append("text content usually does not need media")

    for index, item in enumerate(media, start=1):
        if not isinstance(item, dict):
            errors.append(f"media[{index}] must be an object")
            continue
        if item.get("type") not in MEDIA_TYPES:
            errors.append(f"media[{index}].type is invalid")
        if not _clean_text(item.get("source")):
            errors.append(f"media[{index}].source is required")

    normalized_target = _clean_text(target).lower()
    if normalized_target == "instagram":
        if content_type not in {"single_image", "carousel", "video"}:
            errors.append("Instagram supports single_image, carousel, or video packages")
        if content_type == "carousel" and len(media) > 10:
            errors.append("Instagram carousel supports at most 10 media items")
        for index, item in enumerate(media, start=1):
            if isinstance(item, dict) and not _is_https_url(_clean_text(item.get("source"))):
                errors.append(f"media[{index}].source must be a public HTTPS URL for Instagram")
        caption = _clean_text(payload.get("caption"))
        if len(caption) > 2200:
            errors.append("Instagram caption must be 2200 characters or fewer")

    expected_digest = content_digest(payload)
    stored_digest = _clean_text(payload.get("content_digest"))
    if stored_digest and stored_digest != expected_digest:
        errors.append("content_digest does not match the package content")
    expected_id = f"pkg_{expected_digest[:16]}"
    stored_id = _clean_text(payload.get("package_id"))
    if stored_id and stored_id != expected_id:
        errors.append("package_id does not match the package content")

    return {
        "valid": not errors,
        "target": normalized_target or None,
        "errors": errors,
        "warnings": warnings,
        "content_digest": expected_digest,
    }

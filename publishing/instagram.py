from __future__ import annotations

import os
import re
import time
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from content_core import validate_content_package


class InstagramPublishingError(RuntimeError):
    pass


ACCOUNT_REGISTRY_SCHEMA = "instagram-accounts/v1"
DEFAULT_ACCOUNT_REGISTRY = "config/instagram_accounts.json"


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_account_alias(value: str) -> str:
    alias = str(value or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", alias):
        raise InstagramPublishingError(
            "Instagram account alias must use lowercase letters, numbers, hyphens, or underscores"
        )
    return alias


def _account_env_prefix(alias: str) -> str:
    return f"INSTAGRAM_{re.sub(r'[^A-Z0-9]', '_', alias.upper())}"


def _registry_path() -> Path:
    configured = os.getenv("INSTAGRAM_ACCOUNT_REGISTRY", DEFAULT_ACCOUNT_REGISTRY)
    path = Path(configured).expanduser()
    if not path.is_absolute():
        path = Path(__file__).resolve().parent.parent / path
    return path.resolve()


def _load_account_registry() -> dict[str, Any]:
    path = _registry_path()
    if not path.exists():
        return {
            "schema_version": ACCOUNT_REGISTRY_SCHEMA,
            "default_account": "default",
            "accounts": {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise InstagramPublishingError(f"invalid Instagram account registry: {path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != ACCOUNT_REGISTRY_SCHEMA:
        raise InstagramPublishingError(f"unsupported Instagram account registry: {path}")
    accounts = payload.get("accounts")
    if not isinstance(accounts, dict):
        raise InstagramPublishingError("Instagram account registry accounts must be an object")
    return payload


def list_instagram_account_aliases() -> list[str]:
    registry = _load_account_registry()
    aliases = [_normalize_account_alias(alias) for alias in registry["accounts"]]
    if aliases:
        return sorted(set(aliases))
    return ["default"]


def resolve_instagram_account_alias(account_alias: str | None = None) -> str:
    if account_alias:
        return _normalize_account_alias(account_alias)
    registry = _load_account_registry()
    configured = registry.get("default_account") or "default"
    return _normalize_account_alias(str(configured))


def _instagram_account_runtime(account_alias: str | None = None) -> dict[str, Any]:
    alias = resolve_instagram_account_alias(account_alias)
    registry = _load_account_registry()
    account = registry["accounts"].get(alias)
    if account is not None and not isinstance(account, dict):
        raise InstagramPublishingError(f"invalid Instagram account entry: {alias}")

    is_legacy = account is None and not registry["accounts"] and alias == "default"
    prefix = _account_env_prefix(alias)
    token_env = str((account or {}).get("token_env") or f"{prefix}_ACCESS_TOKEN")
    enabled_env = str(
        (account or {}).get("publishing_enabled_env") or f"{prefix}_PUBLISHING_ENABLED"
    )
    account_id = (
        os.getenv(f"{prefix}_ACCOUNT_ID")
        or str((account or {}).get("instagram_user_id") or "")
    ).strip()
    access_token = os.getenv(token_env, "").strip()
    account_enabled = _env_flag(enabled_env, False)

    if is_legacy:
        account_id = (
            os.getenv("INSTAGRAM_ACCOUNT_ID")
            or os.getenv("META_INSTAGRAM_ID")
            or ""
        ).strip()
        access_token = (
            os.getenv("INSTAGRAM_ACCESS_TOKEN")
            or os.getenv("META_ACCESS_TOKEN")
            or ""
        ).strip()
        account_enabled = _env_flag("INSTAGRAM_PUBLISHING_ENABLED", False)

    return {
        "account_alias": alias,
        "username": str((account or {}).get("username") or alias),
        "content_profile": str((account or {}).get("content_profile") or ""),
        "configured": bool(account_id and access_token),
        "access_token_present": bool(access_token),
        "account_id_present": bool(account_id),
        "account_id": account_id or None,
        "token_env": token_env,
        "publishing_enabled_env": enabled_env,
        "master_publishing_enabled": _env_flag("INSTAGRAM_PUBLISHING_ENABLED", False),
        "account_publishing_enabled": account_enabled,
        "publishing_enabled": (
            _env_flag("INSTAGRAM_PUBLISHING_ENABLED", False) and account_enabled
            if not is_legacy
            else account_enabled
        ),
        "api_version": os.getenv("INSTAGRAM_GRAPH_API_VERSION", "v25.0").strip(),
        "base_url": os.getenv(
            "INSTAGRAM_GRAPH_API_BASE_URL",
            "https://graph.instagram.com",
        ).strip(),
        "_access_token": access_token,
    }


def instagram_account_status(account_alias: str | None = None) -> dict[str, Any]:
    status = _instagram_account_runtime(account_alias)
    status.pop("_access_token", None)
    return status


@dataclass(frozen=True)
class InstagramConfig:
    access_token: str
    account_id: str
    api_version: str
    base_url: str = "https://graph.instagram.com"
    account_alias: str = "default"
    content_profile: str = ""
    publishing_enabled: bool = False
    timeout_sec: float = 30.0
    poll_interval_sec: float = 2.0
    poll_attempts: int = 45

    @classmethod
    def from_env(cls, account_alias: str | None = None) -> "InstagramConfig":
        status = _instagram_account_runtime(account_alias)
        access_token = str(status["_access_token"])
        account_id = str(status["account_id"] or "")
        api_version = str(status["api_version"])
        base_url = str(status["base_url"])
        if not access_token:
            raise InstagramPublishingError(
                f"{status['token_env']} is not configured for {status['account_alias']}"
            )
        if not account_id:
            raise InstagramPublishingError(
                f"Instagram account ID is not configured for {status['account_alias']}"
            )
        if not re.fullmatch(r"v\d+\.\d+", api_version):
            raise InstagramPublishingError("INSTAGRAM_GRAPH_API_VERSION must look like v25.0")
        if not base_url.startswith("https://"):
            raise InstagramPublishingError("INSTAGRAM_GRAPH_API_BASE_URL must use HTTPS")
        return cls(
            access_token=access_token,
            account_id=account_id,
            api_version=api_version,
            base_url=base_url.rstrip("/"),
            account_alias=str(status["account_alias"]),
            content_profile=str(status["content_profile"]),
            publishing_enabled=bool(status["publishing_enabled"]),
        )


class InstagramPublisher:
    def __init__(
        self,
        config: InstagramConfig,
        *,
        session: requests.Session | None = None,
        sleep: Any = time.sleep,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.sleep = sleep

    def _url(self, path: str) -> str:
        return f"{self.config.base_url}/{self.config.api_version}/{path.lstrip('/')}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.config.access_token}"}
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = self.session.request(
                    method,
                    self._url(path),
                    headers=headers,
                    data=data,
                    params=params,
                    timeout=self.config.timeout_sec,
                )
                payload = response.json()
                if response.status_code < 400:
                    if not isinstance(payload, dict):
                        raise InstagramPublishingError("Instagram API returned a non-object response")
                    return payload

                error_payload = payload.get("error") if isinstance(payload, dict) else None
                error_message = (
                    error_payload.get("message")
                    if isinstance(error_payload, dict)
                    else response.text
                )
                error = InstagramPublishingError(
                    f"Instagram API {response.status_code}: {error_message}"
                )
                if response.status_code not in {429, 500, 502, 503, 504}:
                    raise error
                last_error = error
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
            if attempt < 2:
                self.sleep(2**attempt)
        raise InstagramPublishingError(str(last_error or "Instagram API request failed"))

    def get_account(self) -> dict[str, Any]:
        return self._request(
            "GET",
            self.config.account_id,
            params={"fields": "id,username"},
        )

    def create_media_container(
        self,
        *,
        source: str,
        media_type: str,
        is_carousel_item: bool,
        caption: str = "",
    ) -> str:
        data: dict[str, Any] = {}
        if media_type == "image":
            data["image_url"] = source
        elif media_type == "video":
            data.update({"video_url": source, "media_type": "VIDEO"})
        else:
            raise InstagramPublishingError(f"unsupported Instagram media type: {media_type}")
        if is_carousel_item:
            data["is_carousel_item"] = "true"
        if caption:
            data["caption"] = caption
        payload = self._request(
            "POST",
            f"{self.config.account_id}/media",
            data=data,
        )
        container_id = str(payload.get("id") or "").strip()
        if not container_id:
            raise InstagramPublishingError("Instagram did not return a media container ID")
        return container_id

    def create_carousel_container(self, *, child_ids: list[str], caption: str) -> str:
        payload = self._request(
            "POST",
            f"{self.config.account_id}/media",
            data={
                "media_type": "CAROUSEL",
                "children": ",".join(child_ids),
                "caption": caption,
            },
        )
        container_id = str(payload.get("id") or "").strip()
        if not container_id:
            raise InstagramPublishingError("Instagram did not return a carousel container ID")
        return container_id

    def get_container_status(self, container_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            container_id,
            params={"fields": "status_code,status"},
        )

    def wait_until_ready(self, container_id: str) -> dict[str, Any]:
        last_status: dict[str, Any] = {}
        for _ in range(self.config.poll_attempts):
            last_status = self.get_container_status(container_id)
            status_code = str(last_status.get("status_code") or "").upper()
            if status_code in {"FINISHED", "PUBLISHED"}:
                return last_status
            if status_code in {"ERROR", "EXPIRED"}:
                detail = last_status.get("status") or status_code
                raise InstagramPublishingError(f"Instagram container failed: {detail}")
            self.sleep(self.config.poll_interval_sec)
        raise InstagramPublishingError(
            f"Instagram container did not become ready: {last_status}"
        )

    def publish_container(self, container_id: str) -> str:
        payload = self._request(
            "POST",
            f"{self.config.account_id}/media_publish",
            data={"creation_id": container_id},
        )
        media_id = str(payload.get("id") or "").strip()
        if not media_id:
            raise InstagramPublishingError("Instagram did not return a published media ID")
        return media_id

    def publish_package(self, package: dict[str, Any]) -> dict[str, Any]:
        validation = validate_content_package(package, target="instagram")
        if not validation["valid"]:
            raise InstagramPublishingError("; ".join(validation["errors"]))

        content_type = package["content_type"]
        caption = str(package.get("caption") or "")
        media = package.get("media") or []
        child_ids: list[str] = []

        if content_type == "carousel":
            for item in media:
                child_id = self.create_media_container(
                    source=item["source"],
                    media_type=item["type"],
                    is_carousel_item=True,
                )
                child_ids.append(child_id)
                if item["type"] == "video":
                    self.wait_until_ready(child_id)
            container_id = self.create_carousel_container(
                child_ids=child_ids,
                caption=caption,
            )
        else:
            item = media[0]
            container_id = self.create_media_container(
                source=item["source"],
                media_type=item["type"],
                is_carousel_item=False,
                caption=caption,
            )

        self.wait_until_ready(container_id)
        media_id = self.publish_container(container_id)
        return {
            "success": True,
            "media_id": media_id,
            "container_id": container_id,
            "child_container_ids": child_ids,
        }

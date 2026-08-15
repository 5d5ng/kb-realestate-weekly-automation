from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .contracts import validate_content_package


class ContentPackageStore:
    def __init__(self, root: str | Path | None = None) -> None:
        configured = root or os.getenv("CONTENT_PACKAGE_DIR") or "reports/content_packages"
        self.root = Path(configured).expanduser().resolve()

    def _ensure_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, package_id: str) -> Path:
        normalized = str(package_id or "").strip()
        if not normalized.startswith("pkg_") or not normalized.replace("_", "").isalnum():
            raise ValueError("invalid package_id")
        return self.root / f"{normalized}.json"

    def save(self, payload: dict[str, Any]) -> Path:
        validation = validate_content_package(payload)
        if not validation["valid"]:
            raise ValueError("; ".join(validation["errors"]))
        package_id = str(payload.get("package_id") or "")
        path = self.path_for(package_id)
        self._ensure_root()
        temp_path = path.with_suffix(".json.tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(path)
        return path

    def get(self, package_id: str) -> dict[str, Any]:
        path = self.path_for(package_id)
        if not path.exists():
            raise FileNotFoundError(f"content package not found: {package_id}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid content package: {package_id}")
        return payload

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        if not self.root.exists():
            return []
        files = sorted(
            self.root.glob("pkg_*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        items: list[dict[str, Any]] = []
        for path in files[: max(1, min(limit, 200))]:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            items.append(
                {
                    "package_id": payload.get("package_id"),
                    "title": payload.get("title"),
                    "content_type": payload.get("content_type"),
                    "targets": payload.get("targets") or [],
                    "created_at": payload.get("created_at"),
                    "path": str(path),
                }
            )
        return items

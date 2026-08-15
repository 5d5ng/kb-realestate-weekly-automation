#!/usr/bin/env python3
"""Render the controlled 16-page Canva-import HTML to review JPEG media.

The renderer is deterministic and performs no network call or publication. It
uses the locally installed Chrome headless renderer and ImageMagick, then writes
a SHA-256 manifest that can be attached to content-package/v1.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional, Sequence


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
PAGE_PATTERN = re.compile(
    r"(<section\b[^>]*\bdata-document-role=[\"']page[\"'][^>]*>.*?</section>)",
    re.IGNORECASE | re.DOTALL,
)
STYLE_PATTERN = re.compile(r"(<style\b[^>]*>.*?</style>)", re.IGNORECASE | re.DOTALL)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_document(html_text: str) -> tuple[str, list[str]]:
    styles = "\n".join(STYLE_PATTERN.findall(html_text))
    pages = PAGE_PATTERN.findall(html_text)
    if len(pages) != 16:
        raise ValueError("card-news HTML must contain exactly 16 data-document-role=page sections")
    if not styles:
        raise ValueError("card-news HTML has no style block")
    return styles, pages


def page_document(styles: str, page_html: str) -> str:
    override = """
<style>
html,body{margin:0!important;padding:0!important;width:1080px!important;height:1080px!important;overflow:hidden!important;background:transparent!important}
.page{margin:0!important;width:1080px!important;height:1080px!important}
</style>
""".strip()
    return (
        "<!doctype html><html lang='ko'><head><meta charset='utf-8'>"
        + styles + override + "</head><body>" + page_html + "</body></html>"
    )


def _run(command: Sequence[str]) -> None:
    result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(
            "renderer command failed (%s): %s" % (result.returncode, result.stderr.strip()[-2000:])
        )


def render(
    html_path: Path,
    output_dir: Path,
    *,
    chrome_path: Path = DEFAULT_CHROME,
    magick_path: Optional[str] = None,
    runner: Callable[[Sequence[str]], None] = _run,
) -> dict[str, Any]:
    source = html_path.expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(str(source))
    chrome = chrome_path.expanduser().resolve()
    if not chrome.is_file():
        raise FileNotFoundError("Google Chrome executable not found: %s" % chrome)
    magick = magick_path or shutil.which("magick") or shutil.which("convert")
    if not magick:
        raise FileNotFoundError("ImageMagick executable not found")
    styles, pages = extract_document(source.read_text(encoding="utf-8"))
    destination = output_dir.expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    media = []
    with tempfile.TemporaryDirectory(prefix="kb-cardnews-render-") as temporary:
        temporary_dir = Path(temporary)
        for index, page_html in enumerate(pages, start=1):
            wrapper = temporary_dir / ("page-%02d.html" % index)
            png_path = temporary_dir / ("page-%02d.png" % index)
            jpg_path = destination / ("%02d.jpg" % index)
            wrapper.write_text(page_document(styles, page_html), encoding="utf-8")
            runner([
                str(chrome), "--headless=new", "--disable-gpu", "--hide-scrollbars",
                "--run-all-compositor-stages-before-draw", "--virtual-time-budget=1000",
                "--force-device-scale-factor=1", "--window-size=1080,1080",
                "--screenshot=%s" % png_path, wrapper.as_uri(),
            ])
            runner([str(magick), str(png_path), "-quality", "92", "-strip", str(jpg_path)])
            if not jpg_path.is_file() or jpg_path.stat().st_size == 0:
                raise RuntimeError("renderer did not create media: %s" % jpg_path)
            media.append({
                "position": index,
                "type": "image",
                "source": str(jpg_path),
                "alt_text": "KB부동산 주간 카드뉴스 %s/16" % index,
                "sha256": sha256(jpg_path),
                "size_bytes": jpg_path.stat().st_size,
            })
    manifest = {
        "schema_version": "cardnews-media-manifest/v1",
        "source_html": str(source),
        "source_sha256": sha256(source),
        "page_count": len(media),
        "media": media,
        "external_publish_performed": False,
    }
    manifest_path = destination / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KB 카드뉴스 HTML을 검토용 JPEG 16장으로 렌더링")
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--chrome", type=Path, default=DEFAULT_CHROME)
    parser.add_argument("--magick")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = render(
        args.html,
        args.output_dir,
        chrome_path=args.chrome,
        magick_path=args.magick,
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

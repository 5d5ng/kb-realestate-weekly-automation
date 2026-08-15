from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.render_cardnews_media import extract_document, render


def document(page_count: int = 16) -> str:
    pages = "".join(
        "<section class='page' data-document-role='page'><h1>페이지 %s</h1></section>" % index
        for index in range(1, page_count + 1)
    )
    return "<!doctype html><html><head><style>.page{width:1080px;height:1080px}</style></head><body>%s</body></html>" % pages


class CardnewsMediaRendererTests(unittest.TestCase):
    def test_requires_exactly_sixteen_pages(self):
        with self.assertRaisesRegex(ValueError, "exactly 16"):
            extract_document(document(15))

    def test_builds_sixteen_jpegs_and_sha256_manifest_without_network(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            html_path = base / "cardnews.html"
            html_path.write_text(document(), encoding="utf-8")
            chrome = base / "chrome"
            magick = base / "magick"
            chrome.write_text("fake", encoding="utf-8")
            magick.write_text("fake", encoding="utf-8")
            commands = []

            def runner(command):
                commands.append(list(command))
                screenshot = next(
                    (item.split("=", 1)[1] for item in command if item.startswith("--screenshot=")),
                    None,
                )
                if screenshot:
                    Path(screenshot).write_bytes(b"png")
                else:
                    Path(command[-1]).write_bytes(b"jpeg")

            result = render(
                html_path,
                base / "media",
                chrome_path=chrome,
                magick_path=str(magick),
                runner=runner,
            )
            self.assertEqual(16, result["page_count"])
            self.assertEqual(32, len(commands))
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(16, len(manifest["media"]))
            self.assertTrue(all(Path(item["source"]).is_file() for item in manifest["media"]))
            self.assertFalse(manifest["external_publish_performed"])


if __name__ == "__main__":
    unittest.main()

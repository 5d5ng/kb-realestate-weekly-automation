from __future__ import annotations

import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from content_core import (
    ContentPackageStore,
    build_content_package,
    validate_content_package,
)
from publishing.plans import PublishingPlanStore
from publishing.instagram import InstagramConfig, instagram_account_status
from reporters.authoring import _subject_phrase


class ContentPackageTests(unittest.TestCase):
    def test_generic_carousel_package_validates_for_instagram(self) -> None:
        package = build_content_package(
            title="여행 카드뉴스",
            content_type="carousel",
            caption="여행 기록",
            media=[
                {"type": "image", "source": "https://example.com/01.png"},
                {"type": "image", "source": "https://example.com/02.png"},
            ],
            targets=["instagram"],
            metadata={"source_project": "travel-content"},
        )

        validation = validate_content_package(package, target="instagram")

        self.assertTrue(validation["valid"])
        self.assertTrue(package["package_id"].startswith("pkg_"))
        self.assertEqual(package["media"][0]["position"], 1)

    def test_instagram_rejects_local_media_source(self) -> None:
        package = build_content_package(
            title="로컬 초안",
            content_type="carousel",
            media=[
                {"type": "image", "source": "/tmp/01.png"},
                {"type": "image", "source": "/tmp/02.png"},
            ],
        )

        validation = validate_content_package(package, target="instagram")

        self.assertFalse(validation["valid"])
        self.assertIn("public HTTPS URL", " ".join(validation["errors"]))

    def test_content_tampering_breaks_digest_validation(self) -> None:
        package = build_content_package(
            title="원본",
            content_type="text",
            caption="검토 전",
        )
        package["caption"] = "검토 후 몰래 변경"

        validation = validate_content_package(package)

        self.assertFalse(validation["valid"])
        self.assertIn("content_digest", " ".join(validation["errors"]))


class KoreanParticleTests(unittest.TestCase):
    def test_subject_particle_matches_last_syllable(self) -> None:
        self.assertEqual(_subject_phrase("경기도 수원시 영통구"), "경기도 수원시 영통구가")
        self.assertEqual(_subject_phrase("경기도 이천시"), "경기도 이천시가")
        self.assertEqual(_subject_phrase("서울특별시 중랑구"), "서울특별시 중랑구가")


class PublishingPlanTests(unittest.TestCase):
    def test_plan_requires_matching_digest_for_approval(self) -> None:
        package = build_content_package(
            title="승인 테스트",
            content_type="carousel",
            media=[
                {"type": "image", "source": "https://example.com/01.png"},
                {"type": "image", "source": "https://example.com/02.png"},
            ],
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            content_store = ContentPackageStore(Path(temp_dir) / "packages")
            plan_store = PublishingPlanStore(Path(temp_dir) / "plans")
            content_store.save(package)
            plan, _ = plan_store.create(package, target="instagram")

            with self.assertRaisesRegex(ValueError, "expected_digest"):
                plan_store.approve(plan["plan_id"], expected_digest="wrong")

            approved, _ = plan_store.approve(
                plan["plan_id"],
                expected_digest=package["content_digest"],
            )

        self.assertEqual(approved["status"], "approved")

    def test_destination_account_is_bound_to_plan_identity(self) -> None:
        package = build_content_package(
            title="멀티계정 테스트",
            content_type="carousel",
            media=[
                {"type": "image", "source": "https://example.com/01.png"},
                {"type": "image", "source": "https://example.com/02.png"},
            ],
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            store = PublishingPlanStore(Path(temp_dir) / "plans")
            economy, _ = store.create(
                package,
                target="instagram",
                destination_account="ddony_marble",
            )
            travel, _ = store.create(
                package,
                target="instagram",
                destination_account="travel_account",
            )

        self.assertNotEqual(economy["plan_id"], travel["plan_id"])
        self.assertEqual(economy["destination_account"], "ddony_marble")


class InstagramAccountConfigTests(unittest.TestCase):
    def test_named_account_never_falls_back_to_legacy_token(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "INSTAGRAM_ACCOUNT_REGISTRY": "config/instagram_accounts.json",
                "META_ACCESS_TOKEN": "legacy-token-must-not-be-used",
                "META_INSTAGRAM_ID": "legacy-id",
                "INSTAGRAM_DDONY_MARBLE_ACCESS_TOKEN": "",
            },
            clear=False,
        ):
            status = instagram_account_status("ddony_marble")

        self.assertFalse(status["access_token_present"])
        self.assertEqual(status["account_id"], "17841475556425581")

    def test_account_publish_requires_both_switches(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "INSTAGRAM_ACCOUNT_REGISTRY": "config/instagram_accounts.json",
                "INSTAGRAM_PUBLISHING_ENABLED": "1",
                "INSTAGRAM_DDONY_MARBLE_PUBLISHING_ENABLED": "0",
                "INSTAGRAM_DDONY_MARBLE_ACCESS_TOKEN": "test-token",
            },
            clear=False,
        ):
            config = InstagramConfig.from_env("ddony_marble")

        self.assertFalse(config.publishing_enabled)


if __name__ == "__main__":
    unittest.main()

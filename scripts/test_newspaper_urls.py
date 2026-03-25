#!/usr/bin/env python3
"""
신문보기 URL 유효성 테스트.

네이버 신문보기 페이지에서 추출된 /article/newspaper/ 형식 URL이
실제로 접근 가능한지 확인하고,
표준 URL로 변환 후 어떻게 달라지는지 비교한다.

실행:
    python scripts/test_newspaper_urls.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urlparse, urlencode, urljoin

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import requests

TIMEOUT = 15
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
})

# 신문보기 URL → 표준 URL 변환 패턴
_NEWSPAPER_URL_RE = re.compile(
    r"https?://n\.news\.naver\.com/article/newspaper/(\d+)/(\d+)"
)


def normalize_newspaper_url(url: str) -> str:
    """
    /article/newspaper/{press}/{id}?date=... 형태를
    /mnews/article/{press}/{id} 표준 URL로 변환한다.
    """
    m = _NEWSPAPER_URL_RE.match(url)
    if not m:
        return url
    press, article_id = m.group(1), m.group(2)
    return f"https://n.news.naver.com/mnews/article/{press}/{article_id}"


def check_url(url: str, label: str) -> dict:
    try:
        resp = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
        return {
            "label": label,
            "url": url,
            "status": resp.status_code,
            "final_url": resp.url,
            "redirected": resp.url != url,
            "ok": resp.status_code == 200,
        }
    except requests.RequestException as e:
        return {
            "label": label,
            "url": url,
            "status": None,
            "final_url": None,
            "redirected": False,
            "ok": False,
            "error": str(e),
        }


def fetch_sample_newspaper_urls(press_code: str = "023", date: str = "20260325", limit: int = 5) -> list[str]:
    """
    실제 네이버 신문보기 페이지에서 기사 링크 샘플 수집.
    """
    page_url = f"https://media.naver.com/press/{press_code}/newspaper?date={date}"
    print(f"\n[fetch] 신문보기 페이지: {page_url}")
    try:
        resp = SESSION.get(page_url, timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  오류: {e}")
        return []

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(resp.text, "html.parser")
    hrefs = []
    for a in soup.select(".newspaper_article_lst a[href]"):
        href = a.get("href", "").strip()
        if href and _NEWSPAPER_URL_RE.match(href):
            hrefs.append(href)
        if len(hrefs) >= limit:
            break

    print(f"  수집된 링크 {len(hrefs)}건")
    return hrefs


def run_test():
    # 1) 실제 페이지에서 샘플 URL 수집
    sample_urls = fetch_sample_newspaper_urls(press_code="023", date="20260325", limit=5)

    if not sample_urls:
        # 페이지 수집 실패 시 고정 샘플 사용
        sample_urls = [
            "https://n.news.naver.com/article/newspaper/023/0003966593?date=202603250",
        ]
        print("  → 고정 샘플로 대체")

    print("\n" + "=" * 60)
    print("URL 유효성 비교 테스트")
    print("=" * 60)

    for original_url in sample_urls:
        normalized_url = normalize_newspaper_url(original_url)
        print(f"\n원본:  {original_url}")
        print(f"변환:  {normalized_url}")

        r_orig = check_url(original_url, "신문보기 URL")
        r_norm = check_url(normalized_url, "표준 URL")

        for r in [r_orig, r_norm]:
            status = r.get("status", "ERR")
            final = r.get("final_url") or "-"
            redirected = "→ 리다이렉트" if r.get("redirected") else ""
            ok_mark = "✓" if r.get("ok") else "✗"
            print(f"  [{ok_mark}] {r['label']:15s} HTTP {status}  {redirected}")
            if r.get("redirected"):
                print(f"       최종 URL: {final}")
            if r.get("error"):
                print(f"       오류: {r['error']}")

    print("\n" + "=" * 60)
    print("테스트 완료")


if __name__ == "__main__":
    run_test()

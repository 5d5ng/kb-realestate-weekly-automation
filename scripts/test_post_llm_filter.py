"""LLM 없이 post-processing 파이프라인(URL 복원 → 정규화 → 필터) 단독 테스트.

실행:
    python scripts/test_post_llm_filter.py

통과 조건:
- 실제 URL만 출력에 남을 것
- 조작(11자리) URL 기사는 전부 제거될 것
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reporters.common import filter_hallucinated_news_articles, protect_article_urls, restore_article_urls
from reporters.telegram import _normalize_telegram_newsletter

# ---------------------------------------------------------------------------
# 실제 수집된 기사 (URL은 사용자 메시지에서 확인된 정상 URL)
# ---------------------------------------------------------------------------
REAL_ARTICLES = [
    {"title": "동북선 개통 예정, 번동·장위뉴타운 재개발 탄력 기대", "publisher": "한국경제", "issue_date": "2026-03-25", "url": "https://n.news.naver.com/mnews/article/015/0005266364"},
    {"title": "재건축 유언대용신탁, 현금청산 논란 발생", "publisher": "매일경제", "issue_date": "2026-03-25", "url": "https://n.news.naver.com/mnews/article/009/0005655371"},
    {"title": "GS건설, 정비사업 수주 5조 원 눈앞…수의계약 강점", "publisher": "조선일보", "issue_date": "2026-03-25", "url": "https://n.news.naver.com/mnews/article/023/0003966593"},
    {"title": "창신·숭인동 재개발 사업, 20년 만에 속도 붙어", "publisher": "매일경제", "issue_date": "2026-03-25", "url": "https://n.news.naver.com/mnews/article/009/0005655370"},
    {"title": "한미글로벌, 올림픽선수촌 재건축 사업 수주", "publisher": "매일경제", "issue_date": "2026-03-25", "url": "https://n.news.naver.com/mnews/article/009/0005655373"},
]

# ---------------------------------------------------------------------------
# LLM이 뱉었다고 가정한 텍스트
# - URL_REF_N 플레이스홀더: protect 단계에서 치환됐다가 restore 단계에서 복원되는 정상 URL
# - 11자리 article ID URL: LLM이 지어낸 조작 URL (필터가 제거해야 함)
# ---------------------------------------------------------------------------
def _make_fake_llm_response(protected_prompt: str) -> str:
    """프롬프트에서 플레이스홀더를 그대로 유지하고, 조작 기사를 추가한 LLM 응답 시뮬레이션."""
    return (
        "부동산 뉴스 브리핑\n\n"
        "[주요 뉴스]\n"
        "동북선 개통 예정, 번동·장위뉴타운 재개발 탄력 기대\n"
        "  출처: 한국경제 | 2026-03-25\n"
        "  링크: URL_REF_1\n\n"
        "재건축 유언대용신탁, 현금청산 논란 발생\n"
        "  출처: 매일경제 | 2026-03-25\n"
        "  링크: URL_REF_2\n\n"
        "GS건설, 정비사업 수주 5조 원 눈앞\n"
        "  출처: 조선일보 | 2026-03-25\n"
        "  링크: URL_REF_3\n\n"
        # --- 아래는 LLM이 지어낸 조작 기사들 ---
        "집값 안정을 위한 필요충분조건 논의\n"
        "  출처: 서울경제 | 2026-03-25\n"
        "  링크: https://n.news.naver.com/mnews/article/015/00052663640\n\n"
        "농지 투기 및 집값 띄우기 부동산 범죄 1493명 단속\n"
        "  출처: 한국경제 | 2026-03-25\n"
        "  링크: https://n.news.naver.com/mnews/article/015/00052663641\n\n"
        "원자재 대출 후 아파트 매입 사례 발생\n"
        "  출처: 조선일보 | 2026-03-25\n"
        "  링크: https://n.news.naver.com/mnews/article/009/00056553710\n\n"
        "[한줄 정리]\n"
        "재개발·재건축 사업 활기 속 보유세 인상 논의."
    )


def run_test() -> None:
    # 1) 프롬프트에서 URL을 플레이스홀더로 치환
    dummy_prompt = "\n".join(
        f"- {a['title']} | {a['url']}" for a in REAL_ARTICLES
    )
    protected_prompt, original_urls = protect_article_urls(dummy_prompt, REAL_ARTICLES)

    print("=== [1] 원본 URL 목록 ===")
    for i, u in enumerate(original_urls, 1):
        print(f"  URL_REF_{i}: {u}")

    # 2) LLM 응답 시뮬레이션 (플레이스홀더 + 조작 URL 포함)
    fake_llm_output = _make_fake_llm_response(protected_prompt)

    # 3) URL 복원
    restored = restore_article_urls(fake_llm_output, original_urls)

    # 4) 텔레그램 정규화
    normalized = _normalize_telegram_newsletter(restored)

    # 5) 할루시네이션 필터
    filtered = filter_hallucinated_news_articles(normalized, original_urls)

    print("\n=== [2] 필터 후 최종 출력 ===")
    print(filtered)

    # ---------------------------------------------------------------------------
    # 검증
    # ---------------------------------------------------------------------------
    fabricated = [
        "00052663640",
        "00052663641",
        "00056553710",
    ]
    real = [
        "0005266364",
        "0005655371",
        "0003966593",
    ]

    errors: list[str] = []
    for fid in fabricated:
        if fid in filtered:
            errors.append(f"FAIL: 조작 URL({fid})이 출력에 남아 있음")

    for rid in real:
        if rid not in filtered:
            errors.append(f"FAIL: 정상 URL({rid})이 출력에서 사라짐")

    print("\n=== [3] 검증 결과 ===")
    if errors:
        for e in errors:
            print(" ", e)
        sys.exit(1)
    else:
        print("  PASS: 조작 URL 전부 제거, 정상 URL 전부 유지")


if __name__ == "__main__":
    run_test()

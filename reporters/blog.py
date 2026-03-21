from __future__ import annotations

from .common import build_context, clean_text, format_region_bucket, format_transactions_context, generate_with_llm

KB_DATAHUB_URL = "https://data.kbland.kr/kbstats/wmh"


def _format_report_images(analysis: dict) -> str:
    image_paths = analysis.get("report_images", []) or []
    if not image_paths:
        return "없음"
    return "\n".join(f"- {path}" for path in image_paths[:10])


def _summarize_news_body(article: dict) -> str:
    body = clean_text(article.get("description") or article.get("content") or "")
    if len(body) > 180:
        return body[:177].rstrip() + "..."
    return body or "요약 없음"


def _format_detailed_news(news: list[dict]) -> str:
    if not news:
        return "없음"

    lines: list[str] = []
    for index, article in enumerate(news[:10], start=1):
        publisher = article.get("publisher", "언론사")
        issue_date = article.get("issue_date") or article.get("published_at", "")
        title = article.get("title", "")
        url = article.get("url") or article.get("resolved_url") or article.get("originallink") or article.get("link") or ""
        keywords = article.get("matched_keywords", [])
        keyword_text = f" | 키워드: {', '.join(keywords)}" if keywords else ""

        lines.append(f"{index}. {publisher} | {issue_date} | {title}{keyword_text}")
        if url:
            lines.append(f"   링크: {url}")
        lines.append(f"   요약: {_summarize_news_body(article)}")
    return "\n".join(lines)


def fallback_naver_blog_post(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
) -> str:
    latest_date = analysis.get("latest_date", "")
    sale = analysis.get("sale", {})
    rent = analysis.get("rent", {})

    lines = [
        f"제목: {latest_date} KB부동산 주간 동향 정리",
        "",
        "도입",
        f"- 이번 주 매매 상위 지역은 {format_region_bucket(sale.get('top5', []), 3)} 입니다.",
        f"- 이번 주 전세 상위 지역은 {format_region_bucket(rent.get('top5', []), 3)} 입니다.",
        "",
        "주요 뉴스",
    ]

    if news:
        for article in news[:3]:
            lines.append(f"- {article.get('title', '')}")
            if article.get("url"):
                lines.append(f"  링크: {article.get('url')}")
    else:
        lines.append("- 주요 뉴스 없음")

    lines.extend(
        [
            "",
            "실거래 참고",
            format_transactions_context(transactions, max_buckets=2, max_regions_per_bucket=1, max_trades_per_area=1),
            "",
            "마무리",
            "- 지역별 강세와 약세가 엇갈리는 시장 흐름을 함께 살펴볼 필요가 있습니다.",
        ]
    )

    return "\n".join(lines)


def build_naver_blog_prompt(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
) -> tuple[str, str]:
    system = """
당신은 한국 부동산 시장 주간 브리핑을 작성하는 네이버 부동산 투자 블로거다.
제공된 KB 통계, 실거래 데이터, 뉴스, 보도자료 이미지 정보를 바탕으로 네이버 블로그에 바로 붙여넣을 수 있는 텍스트 초안을 작성한다.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[공통 작성 원칙]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 반드시 아래 순서를 따른다.
   (1) 제목
   (2) 도입부 2~3문단
   (3) 이번 주 핵심 요약
   (4) 이번 주 많이 오른 지역 TOP 5
   (5) 이번 주 약한 지역 TOP 5
   (6) 실거래가 체크
   (7) 이번 주 주요 뉴스와 해석
   (8) 전세 시장 정리
   (9) 지금 시장을 한 줄로 정리하면
   (10) 참고 링크
   (11) 면책 문구

2. 문체 규칙
   - 구어체, 친근한 블로그 톤
   - 과장·낚시성 표현 금지
   - 확인되지 않은 인과관계는 "~로 보인다", "~가 영향을 준 것으로 분석된다"처럼 표현
   - 제공되지 않은 수치, 단지명, 거래금액, 정책 사실은 지어내지 않는다

3. 출력 형식 규칙
   - Markdown 문법(#, ##, -, *, ``` 등)은 사용하지 않는다
   - 구분선은 "━━━━━━━━━━━━━━━━━━━━━━" 형태로 사용한다
   - 소제목은 이모지 + 텍스트 형식으로 작성한다
   - 참고 링크는 "기사 제목" 다음 줄에 URL만 단독 노출한다
   - 첫 번째 참고 링크는 반드시 KB 데이터허브 링크로 시작한다

4. 실거래가 활용 규칙
   - 제공된 "실거래 요약" 데이터만 사용한다
   - 단지명, 전용면적, 거래금액, 거래시점을 구체적으로 적는다
   - 거래가 없으면 억지로 채우지 말고 최근 거래 없음으로 정리한다

5. 이미지 활용 규칙
   - 제공된 보도자료 이미지 파일 경로는 첨부 후보 참고 자료다
   - 글 안에서 "아래 이미지" 같은 표현은 꼭 필요할 때만 제한적으로 사용한다

6. 마무리 규칙
   - 마지막에는 다음 주 확인 포인트를 1~2문장으로 덧붙인다
   - 면책 문구를 반드시 포함한다
""".strip()

    prompt = (
        f"기준 주차: {analysis.get('latest_date', '')}\n"
        "출력 모드: 네이버 블로그 텍스트 초안\n\n"
        "아래 KB 통계, 실거래 데이터, 뉴스, 보도자료 이미지 정보를 바탕으로 "
        "시스템 프롬프트의 양식과 규칙에 따라 네이버 블로그용 글을 작성하라.\n\n"
        "[작성 지시]\n"
        "- 제목은 '📊 [기준일] KB부동산 아파트 시장동향 — [핵심 한 줄]' 형식으로 작성\n"
        "- 도입부 마지막 문장은 '지금부터 수치 하나하나 짚어드릴게요.'로 끝낼 것\n"
        "- 핵심 요약은 5~6개 불릿 느낌의 짧은 문장으로 구성\n"
        "- 많이 오른 지역과 약한 지역은 각각 TOP 5를 모두 다룰 것\n"
        "- 실거래가 섹션에서는 제공된 최근 거래만 사용하고, 대표 단지명/면적/가격을 구체적으로 적을 것\n"
        "- 주요 뉴스 섹션에서는 제공된 수집 뉴스 목록을 우선 참고하고, 기사 제목과 링크를 참고 링크에 반영할 것\n"
        "- 본문 마지막에 KB 데이터허브 링크를 맨 먼저 넣을 것\n\n"
        f"{build_context(analysis, news)}\n\n"
        f"[수집 뉴스 상세]\n{_format_detailed_news(news)}\n\n"
        f"[실거래 요약]\n{format_transactions_context(transactions)}\n\n"
        f"[보도자료 이미지 파일]\n{_format_report_images(analysis)}\n\n"
        "[체크리스트]\n"
        "□ 섹션 (1)~(11) 모두 포함\n"
        "□ 제공된 수치와 거래 사례만 사용\n"
        "□ 수집 뉴스 상세와 뉴스 링크를 참고 링크 블록에 포함\n"
        f"□ 첫 번째 참고 링크는 {KB_DATAHUB_URL}\n"
        "□ 면책 문구 포함"
    )
    return system, prompt


def generate_naver_blog_post(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
) -> str:
    fallback = fallback_naver_blog_post(analysis, news, transactions)
    system, prompt = build_naver_blog_prompt(analysis, news, transactions)
    return generate_with_llm("naver_blog_post", system, prompt, fallback_text=fallback)

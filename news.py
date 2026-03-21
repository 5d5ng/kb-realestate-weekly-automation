"""
네이버 뉴스 API 수집 + 키워드 중요도 필터링

- 네이버 검색 API 뉴스 수집
- 네이버 뉴스 / 일반 기사 페이지 본문 추출
- 지정 언론사 필터링
- 부동산 키워드 가중치 필터링
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.example", override=False)

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

NAVER_NEWS_API_URL = "https://openapi.naver.com/v1/search/news.json"
TIMEOUT_SEC = 20
DEFAULT_DISPLAY = 20
DEFAULT_MAX_ARTICLES = 12
DEFAULT_LOOKBACK_DAYS = 7
KST = timezone(timedelta(hours=9))

DEFAULT_QUERIES = (
    "부동산",
    "아파트",
    "재건축",
    "재개발",
    "금리 부동산",
    "DSR 부동산",
)

TARGET_PUBLISHERS = (
    "매일경제",
    "한국경제",
    "서울경제",
    "조선일보",
    "중앙일보",
    "동아일보",
)

PRESS_CODE_BY_PUBLISHER = {
    "매일경제": "009",
    "서울경제": "011",
    "한국경제": "015",
    "동아일보": "020",
    "조선일보": "023",
    "중앙일보": "025",
}

PUBLISHER_BY_PRESS_CODE = {code: publisher for publisher, code in PRESS_CODE_BY_PUBLISHER.items()}

PUBLISHER_DOMAIN_MAP = {
    "mk.co.kr": "매일경제",
    "hankyung.com": "한국경제",
    "sedaily.com": "서울경제",
    "chosun.com": "조선일보",
    "joongang.co.kr": "중앙일보",
    "donga.com": "동아일보",
}

HIGH_PRIORITY_KEYWORDS = {
    "신고가": 5,
    "급등": 5,
    "재개발": 4,
    "재건축": 4,
    "규제": 4,
    "금리": 3,
    "DSR": 3,
}

EXCLUDED_KEYWORDS = (
    "분양광고",
    "PR",
    "제공",
)

REAL_ESTATE_CONTEXT_KEYWORDS = (
    "부동산",
    "아파트",
    "주택",
    "전세",
    "월세",
    "매매",
    "청약",
    "집값",
    "실거래",
    "재건축",
    "재개발",
    "정비사업",
    "분양",
)

NAVER_ARTICLE_BODY_SELECTORS = (
    "#dic_area",
    "#articeBody",
    "article#dic_area",
    ".go_trans._article_content",
)

GENERIC_ARTICLE_BODY_SELECTORS = (
    "[itemprop='articleBody']",
    ".article_view",
    ".article-body",
    ".article_body",
    "#articleBody",
    "#article_body",
    "#article_txt",
    "article",
)

SESSION = requests.Session()
SESSION.headers.update(
    {
        "accept": "application/json, text/html, */*",
        "user-agent": "Mozilla/5.0",
    }
)


def _iter_recent_dates(days: int = DEFAULT_LOOKBACK_DAYS, today: datetime | None = None) -> list[datetime]:
    base = today.astimezone(KST) if today else datetime.now(KST)
    return [base - timedelta(days=offset) for offset in range(days)]


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _strip_html(text: str) -> str:
    if not text:
        return ""
    return _clean_text(BeautifulSoup(unescape(text), "html.parser").get_text(" ", strip=True))


def _parse_pub_date(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST)


def _normalize_publisher_name(name: str) -> str:
    normalized = _clean_text(name)
    normalized = normalized.replace("(주)", "").replace("주식회사", "")
    normalized = normalized.replace("㈜", "").replace("신문", "")
    return normalized.strip()


def _is_allowed_publisher(name: str) -> bool:
    normalized = _normalize_publisher_name(name)
    return normalized in TARGET_PUBLISHERS


def _infer_publisher_from_url(url: str) -> str:
    hostname = urlparse(url).netloc.lower()
    for domain, publisher in PUBLISHER_DOMAIN_MAP.items():
        if hostname.endswith(domain):
            return publisher
    return ""


def _is_naver_news_url(url: str) -> bool:
    hostname = urlparse(url).netloc.lower()
    return hostname.endswith("news.naver.com") or hostname.endswith("n.news.naver.com")


def _extract_naver_sid(url: str) -> str:
    if not url:
        return ""
    query = parse_qs(urlparse(url).query)
    return (query.get("sid") or query.get("sid1") or [""])[0]


def _extract_press_code(value: str) -> str:
    text = _clean_text(value)
    if not text:
        raise ValueError("언론사 코드 또는 신문보기 URL이 비어 있습니다.")

    if re.fullmatch(r"\d{3}", text):
        return text

    match = re.search(r"/press/(\d{3})/", text)
    if match:
        return match.group(1)

    raise ValueError(f"언론사 코드를 추출할 수 없습니다: {value}")


def _build_newspaper_url(press_code: str, date_ymd: str) -> str:
    return f"https://media.naver.com/press/{press_code}/newspaper?date={date_ymd}"


def _extract_meta_content(soup: BeautifulSoup, key: str, value: str) -> str:
    tag = soup.find("meta", attrs={key: value})
    if not tag:
        return ""
    return _clean_text(tag.get("content"))


def _node_text(soup: BeautifulSoup, selector: str) -> str:
    node = soup.select_one(selector)
    if not node:
        return ""
    return _clean_text(node.get_text(" ", strip=True))


def _extract_publisher(soup: BeautifulSoup, resolved_url: str) -> str:
    selectors = (
        "a.media_end_head_top_logo img[alt]",
        ".media_end_head_top_logo img[alt]",
        ".press_logo img[alt]",
    )
    for selector in selectors:
        node = soup.select_one(selector)
        if node and node.get("alt"):
            return _normalize_publisher_name(node["alt"])

    for key, value in (
        ("property", "og:article:author"),
        ("property", "article:author"),
        ("name", "author"),
    ):
        meta_value = _extract_meta_content(soup, key, value)
        if meta_value:
            return _normalize_publisher_name(meta_value)

    return _normalize_publisher_name(_infer_publisher_from_url(resolved_url))


def _extract_article_body(soup: BeautifulSoup, *, is_naver_news: bool) -> str:
    selectors = NAVER_ARTICLE_BODY_SELECTORS if is_naver_news else GENERIC_ARTICLE_BODY_SELECTORS

    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue

        for child in node.select("script, style, iframe, button"):
            child.decompose()

        text = _clean_text(node.get_text("\n", strip=True))
        if len(text) >= 120:
            return text

    for script in soup.find_all("script", type="application/ld+json"):
        script_text = script.get_text(" ", strip=True)
        if "articleBody" not in script_text:
            continue
        match = re.search(r'"articleBody"\s*:\s*"(.+?)"', script_text)
        if match:
            return _clean_text(unescape(match.group(1)))

    return ""


def _infer_section(article: dict[str, Any]) -> str:
    naver_sid = article.get("naver_sid")
    if naver_sid and naver_sid != "101":
        return ""

    if naver_sid == "101":
        title_and_description = " ".join(
            filter(
                None,
                [
                    article.get("title", ""),
                    article.get("description", ""),
                ],
            )
        )
        if any(keyword in title_and_description for keyword in REAL_ESTATE_CONTEXT_KEYWORDS):
            return "부동산"

    title_and_description = " ".join(
        filter(
            None,
            [
                article.get("title", ""),
                article.get("description", ""),
            ],
        )
    )
    if any(keyword in title_and_description for keyword in REAL_ESTATE_CONTEXT_KEYWORDS):
        return "부동산"

    content = article.get("content", "")
    matches = [keyword for keyword in REAL_ESTATE_CONTEXT_KEYWORDS if keyword in content]
    if len(matches) >= 2:
        return "부동산"
    return ""


def _extract_article_details(url: str) -> dict[str, Any]:
    if not url:
        return {}

    try:
        response = SESSION.get(url, timeout=TIMEOUT_SEC)
        response.raise_for_status()
    except requests.RequestException:
        return {}

    soup = BeautifulSoup(response.text, "html.parser")
    resolved_url = response.url
    is_naver_news = _is_naver_news_url(resolved_url)

    title = (
        _extract_meta_content(soup, "property", "og:title")
        or _node_text(soup, "h2.media_end_head_headline")
        or _node_text(soup, "title")
    )

    published_at = (
        _extract_meta_content(soup, "property", "article:published_time")
        or _extract_meta_content(soup, "property", "og:article:published_time")
    )

    description = (
        _extract_meta_content(soup, "property", "og:description")
        or _extract_meta_content(soup, "name", "description")
    )

    content = _extract_article_body(soup, is_naver_news=is_naver_news) or description
    publisher = _extract_publisher(soup, resolved_url)

    article = {
        "resolved_url": resolved_url,
        "title": title,
        "publisher": publisher,
        "content": content,
        "description": description,
        "published_at": published_at,
        "naver_sid": _extract_naver_sid(resolved_url),
    }
    article["section"] = _infer_section(article)
    return article


def _is_real_estate_candidate(article: dict[str, Any]) -> bool:
    title_and_description = " ".join(
        filter(
            None,
            [
                article.get("title", ""),
                article.get("description", ""),
            ],
        )
    )
    return any(keyword in title_and_description for keyword in REAL_ESTATE_CONTEXT_KEYWORDS)


def _parse_newspaper_issue(press_code: str, issue_date: datetime) -> list[dict[str, Any]]:
    issue_date_str = issue_date.strftime("%Y%m%d")
    response = SESSION.get(
        _build_newspaper_url(press_code, issue_date_str),
        timeout=TIMEOUT_SEC,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    publisher = PUBLISHER_BY_PRESS_CODE.get(press_code, "")
    articles: list[dict[str, Any]] = []

    for wrapper in soup.select(".newspaper_wrp"):
        page_node = wrapper.select_one(".page_notation em")
        page = _clean_text(page_node.get_text(" ", strip=True) if page_node else "")

        for link in wrapper.select(".newspaper_article_lst a[href]"):
            href = _clean_text(link.get("href"))
            title_node = link.select_one("strong")
            description_node = link.select_one("p")

            title = _clean_text(title_node.get_text(" ", strip=True) if title_node else link.get_text(" ", strip=True))
            description = _clean_text(description_node.get_text(" ", strip=True) if description_node else "")

            article = {
                "press_code": press_code,
                "publisher": publisher,
                "query": publisher,
                "page": page,
                "title": title,
                "description": description,
                "link": href,
                "originallink": href,
                "resolved_url": href,
                "is_naver_news": _is_naver_news_url(href),
                "naver_sid": _extract_naver_sid(href),
                "issue_date": issue_date.strftime("%Y-%m-%d"),
                "published_at": issue_date.isoformat(),
                "_published_dt": issue_date,
                "source_type": "newspaper",
            }

            if _is_real_estate_candidate(article):
                articles.append(article)

    return articles


def fetch_news_from_newspaper(
    press_source: str,
    *,
    days: int = DEFAULT_LOOKBACK_DAYS,
) -> list[dict[str, Any]]:
    """
    네이버 언론사 신문보기 페이지에서 최근 `days`일 후보 기사를 수집한다.

    `press_source`는 3자리 언론사 코드 또는
    `https://media.naver.com/press/{code}/newspaper` 형태 URL을 받을 수 있다.
    """
    press_code = _extract_press_code(press_source)
    raw_articles: list[dict[str, Any]] = []
    for issue_date in _iter_recent_dates(days=days):
        raw_articles.extend(_parse_newspaper_issue(press_code, issue_date))
    return _dedupe_articles(raw_articles)


def _contains_excluded_keyword(article: dict[str, Any]) -> list[str]:
    haystack = " ".join(
        filter(
            None,
            [
                article.get("title", ""),
                article.get("description", ""),
                article.get("content", ""),
            ],
        )
    ).upper()
    return [keyword for keyword in EXCLUDED_KEYWORDS if keyword.upper() in haystack]


def _is_real_estate_article(article: dict[str, Any]) -> bool:
    naver_sid = article.get("naver_sid")
    if naver_sid and naver_sid != "101":
        return False

    if article.get("section") == "부동산":
        return True

    title_and_description = " ".join(
        filter(
            None,
            [
                article.get("title", ""),
                article.get("description", ""),
            ],
        )
    )
    if any(keyword in title_and_description for keyword in REAL_ESTATE_CONTEXT_KEYWORDS):
        return True

    content = article.get("content", "")
    matches = [keyword for keyword in REAL_ESTATE_CONTEXT_KEYWORDS if keyword in content]
    return len(matches) >= 2


def _score_article(article: dict[str, Any]) -> tuple[int, list[str]]:
    title = article.get("title", "").upper()
    description = article.get("description", "").upper()
    content = article.get("content", "").upper()

    score = 0
    matched_keywords: list[str] = []

    for keyword, weight in HIGH_PRIORITY_KEYWORDS.items():
        keyword_upper = keyword.upper()
        keyword_score = 0
        if keyword_upper in title:
            keyword_score += weight * 3
        if keyword_upper in description:
            keyword_score += weight * 2
        if keyword_upper in content:
            keyword_score += weight
        if keyword_score:
            score += keyword_score
            matched_keywords.append(keyword)

    if article.get("section") == "부동산":
        score += 2

    return score, matched_keywords


def _dedupe_articles(articles: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()

    for article in articles:
        identity = article.get("resolved_url") or article.get("originallink") or article.get("link") or article.get("title")
        identity = _clean_text(identity)
        if not identity or identity in seen:
            continue
        seen.add(identity)
        deduped.append(article)

    return deduped


def _build_news_headers() -> dict[str, str]:
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        raise RuntimeError("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수를 설정해야 합니다.")

    return {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }


def fetch_news(
    query: str,
    display: int = DEFAULT_DISPLAY,
    *,
    start: int = 1,
    sort: str = "date",
) -> list[dict[str, Any]]:
    """네이버 뉴스 검색 API로 뉴스 검색 결과를 수집한다."""
    headers = _build_news_headers()
    response = SESSION.get(
        NAVER_NEWS_API_URL,
        params={
            "query": query,
            "display": display,
            "start": start,
            "sort": sort,
        },
        headers=headers,
        timeout=TIMEOUT_SEC,
    )
    response.raise_for_status()
    payload = response.json()

    articles: list[dict[str, Any]] = []
    for item in payload.get("items", []):
        published_dt = _parse_pub_date(item.get("pubDate", ""))
        articles.append(
            {
                "query": query,
                "title": _strip_html(item.get("title", "")),
                "description": _strip_html(item.get("description", "")),
                "link": _clean_text(item.get("link")),
                "originallink": _clean_text(item.get("originallink")),
                "url": _clean_text(item.get("originallink")) or _clean_text(item.get("link")),
                "published_at": published_dt.isoformat() if published_dt else "",
                "_published_dt": published_dt,
            }
        )

    return articles


def filter_by_claude(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    현재는 Claude 대신 로컬 키워드 가중치 필터를 사용한다.

    - 대상 언론사만 통과
    - 제외 키워드 제거
    - 우선순위 키워드 점수 부여
    """
    weekly_cutoff = datetime.now(KST) - timedelta(days=7)
    filtered: list[dict[str, Any]] = []

    for article in articles:
        published_dt = article.get("_published_dt")
        if published_dt and published_dt < weekly_cutoff:
            continue

        if not article.get("is_naver_news"):
            continue

        naver_sid = article.get("naver_sid", "")
        if naver_sid and naver_sid != "101":
            continue

        if not _is_allowed_publisher(article.get("publisher", "")):
            continue

        excluded_keywords = _contains_excluded_keyword(article)
        if excluded_keywords:
            continue

        if not _is_real_estate_article(article):
            continue

        priority_score, matched_keywords = _score_article(article)
        article["priority_score"] = priority_score
        article["matched_keywords"] = matched_keywords
        filtered.append(article)

    return sorted(
        _dedupe_articles(filtered),
        key=lambda item: (
            item.get("priority_score", 0),
            item.get("published_at", ""),
        ),
        reverse=True,
    )


def get_weekly_news(
    press_sources: Iterable[str] | None = None,
    *,
    days: int = DEFAULT_LOOKBACK_DAYS,
    max_articles: int = DEFAULT_MAX_ARTICLES,
) -> list[dict[str, Any]]:
    """
    네이버 언론사 신문보기 페이지에서 최근 일주일 부동산 관련 기사를 수집하고 정제한다.

    기본 대상:
    - 매일경제(009)
    - 서울경제(011)
    - 한국경제(015)
    - 동아일보(020)
    - 조선일보(023)
    - 중앙일보(025)
    """
    sources = tuple(press_sources or PRESS_CODE_BY_PUBLISHER.values())

    raw_articles: list[dict[str, Any]] = []
    for press_source in sources:
        raw_articles.extend(fetch_news_from_newspaper(press_source, days=days))

    deduped_raw = _dedupe_articles(raw_articles)
    enriched_articles: list[dict[str, Any]] = []

    for article in deduped_raw:
        article_url = article.get("link") or article.get("originallink")
        details = _extract_article_details(article_url)

        publisher = details.get("publisher") or _infer_publisher_from_url(article.get("originallink", ""))
        content = details.get("content") or article.get("description", "")

        enriched = {
            **article,
            "title": details.get("title") or article.get("title", ""),
            "description": details.get("description") or article.get("description", ""),
            "content": content,
            "publisher": publisher,
            "section": details.get("section") or "",
            "resolved_url": details.get("resolved_url") or article_url,
            "url": details.get("resolved_url") or article_url,
            "is_naver_news": _is_naver_news_url(details.get("resolved_url") or article_url or ""),
            "naver_sid": details.get("naver_sid") or _extract_naver_sid(details.get("resolved_url") or article_url or ""),
        }

        if details.get("published_at") and not enriched.get("published_at"):
            enriched["published_at"] = details["published_at"]

        enriched_articles.append(enriched)

    return filter_by_claude(enriched_articles)[:max_articles]

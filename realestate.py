"""
KB부동산 실거래가 연동

- analyzer.py 결과 지역명 수신
- 지역명 -> KB 지역 스코프(시도/시군구/법정동) 변환
- KB 단지 목록 조회
- 지역별 84타입 / 59타입 최근 실거래 조회
"""
from __future__ import annotations

import re
import time
from datetime import date
from functools import lru_cache
from typing import Any

import requests

TIMEOUT_SEC = 30
DEFAULT_LIMIT = 5
DEFAULT_AREA_TYPES = (84, 59)
MAX_REGIONS_PER_BUCKET = 5
REQUEST_RETRY_COUNT = 3
REQUEST_RETRY_DELAY_SEC = 1.0
REGION_RETRY_COUNT = 2
REGION_RETRY_DELAY_SEC = 2.0
CONTENT_REGION_BUCKETS = (
    "capital_sale_top5",
    "capital_sale_bottom5",
    "capital_rent_top5",
    "capital_rent_bottom5",
    "non_capital_sale_top5",
    "non_capital_sale_bottom5",
    "non_capital_rent_top5",
    "non_capital_rent_bottom5",
)

LAND_COMPLEX_API_URL = "https://api.kbland.kr/land-complex"
LAND_PRICE_API_URL = "https://api.kbland.kr/land-price"

COMMON_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "origin": "https://kbland.kr",
    "referer": "https://kbland.kr/",
    "user-agent": "Mozilla/5.0",
}

KB_SIDO_ALIASES = {
    "서울": "서울시",
    "서울시": "서울시",
    "서울특별시": "서울시",
    "부산": "부산시",
    "부산시": "부산시",
    "부산광역시": "부산시",
    "대구": "대구시",
    "대구시": "대구시",
    "대구광역시": "대구시",
    "인천": "인천시",
    "인천시": "인천시",
    "인천광역시": "인천시",
    "광주": "광주시",
    "광주시": "광주시",
    "광주광역시": "광주시",
    "대전": "대전시",
    "대전시": "대전시",
    "대전광역시": "대전시",
    "울산": "울산시",
    "울산시": "울산시",
    "울산광역시": "울산시",
    "세종": "세종시",
    "세종시": "세종시",
    "세종특별자치시": "세종시",
    "경기": "경기도",
    "경기도": "경기도",
    "강원": "강원도",
    "강원도": "강원도",
    "강원특별자치도": "강원도",
    "충북": "충청북도",
    "충청북도": "충청북도",
    "충남": "충청남도",
    "충청남도": "충청남도",
    "전북": "전라북도",
    "전라북도": "전라북도",
    "전북특별자치도": "전라북도",
    "전남": "전라남도",
    "전라남도": "전라남도",
    "경북": "경상북도",
    "경상북도": "경상북도",
    "경남": "경상남도",
    "경상남도": "경상남도",
    "제주": "제주도",
    "제주도": "제주도",
    "제주특별자치도": "제주도",
}

SESSION = requests.Session()
SESSION.headers.update(COMMON_HEADERS)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _normalize_area(value: Any) -> int | float | None:
    text = _clean_text(value)
    if not text:
        return None

    digits = re.sub(r"[^\d.]", "", text)
    if not digits:
        return None

    number = float(digits)
    if number.is_integer():
        return int(number)

    rounded = round(number, 2)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def _normalize_int(value: Any) -> int | None:
    text = _clean_text(value)
    if not text:
        return None

    digits = re.sub(r"[^\d-]", "", text)
    if not digits or digits == "-":
        return None
    return int(digits)


def _request_json(base_url: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(1, REQUEST_RETRY_COUNT + 1):
        try:
            response = SESSION.get(f"{base_url}{path}", params=params, timeout=TIMEOUT_SEC)
            response.raise_for_status()

            payload = response.json()
            header = payload.get("dataHeader", {})
            if header.get("resultCode") != "10000":
                raise RuntimeError(f"KB API 오류: {header}")

            body = payload.get("dataBody", {})
            result_code = body.get("resultCode")
            if result_code not in (None, 11000, "11000", 31210, "31210", 33210, "33210"):
                raise RuntimeError(f"KB API dataBody 오류: {body}")
            return body
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= REQUEST_RETRY_COUNT:
                break
            time.sleep(REQUEST_RETRY_DELAY_SEC * attempt)

    raise RuntimeError(f"KB API 요청 실패: {base_url}{path}") from last_error


@lru_cache(maxsize=1)
def _get_sido_list() -> tuple[dict[str, Any], ...]:
    body = _request_json(LAND_COMPLEX_API_URL, "/map/siDoAreaNameList")
    return tuple(body.get("data", []))


@lru_cache(maxsize=64)
def _get_sigungu_list(sido_name: str) -> tuple[dict[str, Any], ...]:
    body = _request_json(
        LAND_COMPLEX_API_URL,
        "/map/siGunGuAreaNameList",
        params={"시도명": sido_name},
    )
    return tuple(body.get("data", []))


@lru_cache(maxsize=256)
def _get_dong_list(sido_name: str, sigungu_name: str) -> tuple[dict[str, Any], ...]:
    body = _request_json(
        LAND_COMPLEX_API_URL,
        "/map/stutDongAreaNameList",
        params={"시도명": sido_name, "시군구명": sigungu_name},
    )
    return tuple(body.get("data", []))


@lru_cache(maxsize=1024)
def _get_complexes_by_dong_code(dong_code: str) -> tuple[dict[str, Any], ...]:
    body = _request_json(
        LAND_COMPLEX_API_URL,
        "/complexComm/hscmList",
        params={"법정동코드": dong_code},
    )
    return tuple(body.get("data", []))


@lru_cache(maxsize=2048)
def _get_complex_types(complex_id: int) -> tuple[dict[str, Any], ...]:
    body = _request_json(
        LAND_COMPLEX_API_URL,
        "/complex/typInfo",
        params={"단지기본일련번호": complex_id},
    )
    return tuple(body.get("data", []))


def _normalize_region_name(region_name: str) -> str:
    region_name = _clean_text(region_name)
    if not region_name:
        raise ValueError("지역명이 비어 있습니다.")

    tokens = region_name.split(" ")
    if tokens and tokens[0] in KB_SIDO_ALIASES:
        tokens[0] = KB_SIDO_ALIASES[tokens[0]]

    return " ".join(tokens)


def _resolve_sigungu_from_short_name(short_name: str) -> tuple[str, str]:
    matches: list[tuple[str, str]] = []
    for sido in _get_sido_list():
        kb_sido_name = _clean_text(sido.get("시도명"))
        if not kb_sido_name:
            continue
        for sigungu in _get_sigungu_list(kb_sido_name):
            sigungu_name = _clean_text(sigungu.get("시군구명"))
            if sigungu_name == short_name:
                matches.append((kb_sido_name, sigungu_name))

    if not matches:
        raise ValueError(f"KB 지역 스코프를 찾을 수 없습니다: {short_name}")
    if len(matches) > 1:
        raise ValueError(f"동일한 시군구명이 여러 시도에 존재합니다: {short_name}")
    return matches[0]


def _resolve_region_scope(region_name: str) -> tuple[str, str]:
    normalized = _normalize_region_name(region_name)
    tokens = normalized.split(" ")

    if tokens[0] in {item["시도명"] for item in _get_sido_list()}:
        kb_sido_name = tokens[0]
        if len(tokens) == 1:
            sigungu_list = _get_sigungu_list(kb_sido_name)
            if len(sigungu_list) == 1:
                return kb_sido_name, _clean_text(sigungu_list[0].get("시군구명"))
            raise ValueError(f"시도 단위만으로는 조회할 수 없습니다: {region_name}")
        return kb_sido_name, " ".join(tokens[1:])

    return _resolve_sigungu_from_short_name(normalized)


def _extract_region_names(regions: Any) -> list[str]:
    if isinstance(regions, dict):
        if "sale" in regions or "rent" in regions:
            names: list[str] = []
            for category in ("sale", "rent"):
                section = regions.get(category, {})
                if not isinstance(section, dict):
                    continue
                for bucket in section.values():
                    if not isinstance(bucket, list):
                        continue
                    for item in bucket:
                        if isinstance(item, dict) and item.get("region"):
                            names.append(_clean_text(item["region"]))
            return list(dict.fromkeys(filter(None, names)))

        if all(isinstance(value, list) for value in regions.values()):
            return list(dict.fromkeys(_clean_text(key) for key in regions))

    if isinstance(regions, list):
        names: list[str] = []
        for item in regions:
            if isinstance(item, str):
                names.append(_clean_text(item))
            elif isinstance(item, dict) and item.get("region"):
                names.append(_clean_text(item["region"]))
        return list(dict.fromkeys(filter(None, names)))

    raise ValueError("regions는 지역명 리스트 또는 analyzer.py 결과 dict여야 합니다.")


def _extract_grouped_region_names(regions: Any) -> dict[str, list[str]] | None:
    if not isinstance(regions, dict):
        return None

    grouped_source = regions.get("content_regions") if isinstance(regions.get("content_regions"), dict) else regions
    if not isinstance(grouped_source, dict):
        return None

    matched_bucket_names = [bucket_name for bucket_name in CONTENT_REGION_BUCKETS if bucket_name in grouped_source]
    if not matched_bucket_names:
        return None

    grouped_regions: dict[str, list[str]] = {}
    for bucket_name in matched_bucket_names:
        bucket = grouped_source.get(bucket_name, [])
        if not isinstance(bucket, list):
            grouped_regions[bucket_name] = []
            continue

        region_names: list[str] = []
        for item in bucket:
            if isinstance(item, str):
                region_names.append(_clean_text(item))
            elif isinstance(item, dict) and item.get("region"):
                region_names.append(_clean_text(item["region"]))

        grouped_regions[bucket_name] = list(dict.fromkeys(filter(None, region_names)))[:MAX_REGIONS_PER_BUCKET]

    return grouped_regions


def _iter_region_complexes(region_name: str) -> list[dict[str, Any]]:
    kb_sido_name, sigungu_name = _resolve_region_scope(region_name)
    dongs = _get_dong_list(kb_sido_name, sigungu_name)

    complexes: dict[int, dict[str, Any]] = {}
    for dong in dongs:
        dong_code = _clean_text(dong.get("법정동코드"))
        if not dong_code:
            continue

        for complex_info in _get_complexes_by_dong_code(dong_code):
            if _clean_text(complex_info.get("매물종별구분")) != "01":
                continue
            complex_id = complex_info.get("단지기본일련번호")
            if isinstance(complex_id, int):
                complexes[complex_id] = complex_info

    return list(complexes.values())


def _today_ymd() -> str:
    # 최근 기준은 호출 시점(sysdate)이다.
    return date.today().strftime("%Y%m%d")


def _parse_latest_trade_rows(
    rows: list[dict[str, Any]],
    complex_name: str,
    area_profile: dict[str, Any],
    complex_id: int,
    area_id: int,
) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row in rows:
        year = _clean_text(row.get("계약년"))
        month = _clean_text(row.get("계약월"))
        day = _clean_text(row.get("계약일"))
        price = row.get("매매실거래금액")
        if not price or not year or not month:
            continue

        day_number = int(day or "1")
        parsed.append(
            {
                "date": f"{year}-{int(month):02d}",
                "contract_date": f"{year}-{int(month):02d}-{day_number:02d}",
                "name": complex_name,
                "area": area_profile["exclusive_area"],
                "area_pyeong": area_profile["exclusive_area_pyeong"],
                "supply_area": area_profile["supply_area"],
                "supply_area_pyeong": area_profile["supply_area_pyeong"],
                "contract_area": area_profile["contract_area"],
                "contract_area_pyeong": area_profile["contract_area_pyeong"],
                "price": int(price),
                "floor": _normalize_int(row.get("해당층수")),
                "households": area_profile["households"],
                "room_count": area_profile["room_count"],
                "bathroom_count": area_profile["bathroom_count"],
                "total_trade_count": area_profile["total_trade_count"],
                "total_jeonse_count": area_profile["total_jeonse_count"],
                "total_monthly_rent_count": area_profile["total_monthly_rent_count"],
                "trade_type": _clean_text(row.get("물건거래명")) or "매매",
                "building_info": _clean_text(row.get("실거래동정보")),
                "_complex_id": complex_id,
                "_area_id": area_id,
                "_sort_key": (int(year), int(month), day_number),
            }
        )

    return parsed


def _parse_latest_rent_rows(
    rows: list[dict[str, Any]],
    trade_sample: dict[str, Any],
) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row in rows:
        trade_type = _clean_text(row.get("물건거래명")) or "전세"
        deposit = _normalize_int(row.get("전세실거래금액")) or _normalize_int(row.get("보증금액"))
        monthly_rent = _normalize_int(row.get("월세금액"))
        year = _clean_text(row.get("계약년"))
        month = _clean_text(row.get("계약월"))
        day = _clean_text(row.get("계약일"))

        if trade_type != "전세":
            continue
        if deposit is None or not year or not month:
            continue

        day_number = int(day or "1")
        parsed.append(
            {
                "date": f"{year}-{int(month):02d}",
                "contract_date": f"{year}-{int(month):02d}-{day_number:02d}",
                "name": trade_sample["name"],
                "area": trade_sample["area"],
                "area_pyeong": trade_sample["area_pyeong"],
                "supply_area": trade_sample["supply_area"],
                "supply_area_pyeong": trade_sample["supply_area_pyeong"],
                "contract_area": trade_sample["contract_area"],
                "contract_area_pyeong": trade_sample["contract_area_pyeong"],
                "price": deposit,
                "deposit": deposit,
                "monthly_rent": monthly_rent,
                "floor": _normalize_int(row.get("해당층수")),
                "households": trade_sample["households"],
                "room_count": trade_sample["room_count"],
                "bathroom_count": trade_sample["bathroom_count"],
                "total_jeonse_count": trade_sample.get("total_jeonse_count"),
                "total_monthly_rent_count": trade_sample.get("total_monthly_rent_count"),
                "trade_type": trade_type,
                "building_info": _clean_text(row.get("실거래동정보")),
                "_complex_id": trade_sample.get("_complex_id"),
                "_area_id": trade_sample.get("_area_id"),
                "_sort_key": (int(year), int(month), day_number),
            }
        )

    return parsed


def _match_area_type(type_info: dict[str, Any], target_area_type: int) -> bool:
    area = _normalize_area(type_info.get("전용면적"))
    if area is None:
        return False
    return int(float(area)) == target_area_type


def _pick_area_type(
    complex_id: int,
    target_area_type: int,
) -> tuple[int | None, dict[str, Any] | None]:
    best_type: dict[str, Any] | None = None
    best_score: tuple[int, int, float] | None = None

    for type_info in _get_complex_types(complex_id):
        if not _match_area_type(type_info, target_area_type):
            continue

        trade_count = int(type_info.get("매매건수") or 0)
        if trade_count <= 0:
            continue
        households = int(type_info.get("세대수") or 0)
        supply_area = float(type_info.get("공급면적") or 0)
        score = (trade_count, households, supply_area)

        if best_score is None or score > best_score:
            best_type = type_info
            best_score = score

    if not best_type:
        return None, None

    return best_type.get("면적일련번호"), {
        "target_area_type": target_area_type,
        "exclusive_area": _normalize_area(best_type.get("전용면적")),
        "exclusive_area_pyeong": _normalize_area(best_type.get("전용면적평")),
        "supply_area": _normalize_area(best_type.get("공급면적")),
        "supply_area_pyeong": _normalize_area(best_type.get("공급면적평")),
        "contract_area": _normalize_area(best_type.get("계약면적")),
        "contract_area_pyeong": _normalize_area(best_type.get("계약면적평")),
        "households": int(best_type.get("세대수") or 0),
        "room_count": int(best_type.get("방수") or 0),
        "bathroom_count": int(best_type.get("욕실수") or 0),
        "total_trade_count": int(best_type.get("매매건수") or 0),
        "total_jeonse_count": int(best_type.get("전세건수") or 0),
        "total_monthly_rent_count": int(best_type.get("월세건수") or 0),
    }


def fetch_trade_data(
    complex_id: int,
    area_id: int,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """단지 + 면적 기준 최근 매매 실거래 조회. 조회종료일은 sysdate 기준."""
    body = _request_json(
        LAND_PRICE_API_URL,
        "/price/LatestRealTranPrc",
        params={
            "단지기본일련번호": complex_id,
            "면적일련번호": area_id,
            "거래구분": "1",
            "조회구분": "2",
            "조회시작일": "20000101",
            "조회종료일": _today_ymd(),
            "첫페이지갯수": limit,
            "현재페이지": 1,
            "페이지갯수": limit,
        },
    )
    return body.get("data", []) or []


def fetch_rent_data(
    complex_id: int,
    area_id: int,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """단지 + 면적 기준 최근 전월세 실거래 조회. 조회종료일은 sysdate 기준."""
    body = _request_json(
        LAND_PRICE_API_URL,
        "/price/LatestRealTranPrc",
        params={
            "단지기본일련번호": complex_id,
            "면적일련번호": area_id,
            "거래구분": "2",
            "조회구분": "2",
            "조회시작일": "20000101",
            "조회종료일": _today_ymd(),
            "첫페이지갯수": limit,
            "현재페이지": 1,
            "페이지갯수": limit,
        },
    )
    return body.get("data", []) or []


def _collect_region_transactions(
    region_name: str,
    area_types: tuple[int, ...],
    limit: int,
) -> dict[str, dict[str, Any]]:
    for attempt in range(1, REGION_RETRY_COUNT + 1):
        region_result = {str(area_type): [] for area_type in area_types}
        seen_by_area = {str(area_type): set() for area_type in area_types}

        try:
            complexes = _iter_region_complexes(region_name)
        except RuntimeError:
            complexes = []

        for complex_info in complexes:
            complex_id = complex_info.get("단지기본일련번호")
            if not isinstance(complex_id, int):
                continue

            complex_name = _clean_text(complex_info.get("단지명"))

            for area_type in area_types:
                area_key = str(area_type)
                try:
                    area_id, area_profile = _pick_area_type(complex_id, area_type)
                except RuntimeError:
                    continue
                if not area_id:
                    continue

                try:
                    raw_rows = fetch_trade_data(complex_id, area_id, limit=limit)
                except RuntimeError:
                    continue

                for trade in _parse_latest_trade_rows(raw_rows, complex_name, area_profile or {}, complex_id, area_id):
                    identity = (
                        trade["date"],
                        trade["contract_date"],
                        trade["name"],
                        trade["area"],
                        trade["price"],
                        trade["_sort_key"],
                    )
                    if identity in seen_by_area[area_key]:
                        continue
                    seen_by_area[area_key].add(identity)
                    region_result[area_key].append(trade)

        cleaned_by_area: dict[str, dict[str, Any]] = {}
        for area_key, trades in region_result.items():
            trades.sort(key=lambda item: item["_sort_key"], reverse=True)
            selected_sale_trades: list[dict[str, Any]] = []
            rent_cache: dict[tuple[int, int], list[dict[str, Any]]] = {}
            aggregated_rent_trades: list[dict[str, Any]] = []
            aggregated_rent_seen: set[tuple[Any, ...]] = set()
            for item in trades[:limit]:
                item = dict(item)
                related_rent_trades = _load_related_rent_transactions(item, limit=limit, cache=rent_cache)
                public_related_rent_trades: list[dict[str, Any]] = []

                for rent_trade in related_rent_trades:
                    public_related_rent_trades.append(_strip_internal_trade_fields(rent_trade))
                    identity = (
                        rent_trade["date"],
                        rent_trade["contract_date"],
                        rent_trade["name"],
                        rent_trade["area"],
                        rent_trade["price"],
                        rent_trade["_sort_key"],
                    )
                    if identity in aggregated_rent_seen:
                        continue
                    aggregated_rent_seen.add(identity)
                    aggregated_rent_trades.append(dict(rent_trade))

                item["related_rent_count"] = len(public_related_rent_trades)
                item["related_rent_trades"] = public_related_rent_trades
                selected_sale_trades.append(item)

            aggregated_rent_trades.sort(key=lambda item: item["_sort_key"], reverse=True)
            rent_trades = [
                _strip_internal_trade_fields(item)
                for item in aggregated_rent_trades[:limit]
            ]

            area_trades: list[dict[str, Any]] = []
            for item in selected_sale_trades:
                area_trades.append(_strip_internal_trade_fields(item))

            cleaned_by_area[area_key] = {
                "target_area_type": int(area_key),
                "recent_trade_count": len(area_trades),
                "trades": area_trades,
                "recent_rent_count": len(rent_trades),
                "rent_trades": rent_trades,
            }

        if any(area_info["trades"] for area_info in cleaned_by_area.values()) or attempt >= REGION_RETRY_COUNT:
            return cleaned_by_area

        time.sleep(REGION_RETRY_DELAY_SEC * attempt)

    return {
        str(area_type): {
            "target_area_type": area_type,
            "recent_trade_count": 0,
            "trades": [],
            "recent_rent_count": 0,
            "rent_trades": [],
        }
        for area_type in area_types
    }


def _strip_internal_trade_fields(item: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(item)
    cleaned.pop("_sort_key", None)
    cleaned.pop("_complex_id", None)
    cleaned.pop("_area_id", None)
    return cleaned


def _load_related_rent_transactions(
    trade_sample: dict[str, Any],
    *,
    limit: int,
    cache: dict[tuple[int, int], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    complex_id = trade_sample.get("_complex_id")
    area_id = trade_sample.get("_area_id")
    if not isinstance(complex_id, int) or not isinstance(area_id, int):
        return []

    cache_key = (complex_id, area_id)
    if cache_key not in cache:
        try:
            raw_rows = fetch_rent_data(complex_id, area_id, limit=limit)
        except RuntimeError:
            cache[cache_key] = []
        else:
            parsed = _parse_latest_rent_rows(raw_rows, trade_sample)
            parsed.sort(key=lambda item: item["_sort_key"], reverse=True)
            cache[cache_key] = parsed[:limit]

    return [dict(item) for item in cache[cache_key]]


def get_recent_transactions(
    regions: Any,
    area_types: tuple[int, ...] = DEFAULT_AREA_TYPES,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, Any]:
    """
    analyzer.py 결과 또는 지역명 목록을 받아 최근 매매 실거래를 반환한다.

    `content_regions` 8개 버킷이 포함된 analyzer 결과를 넣으면,
    버킷별 -> 지역별 -> 면적타입별 구조로 반환한다.

    반환 형식:
    {
      "capital_sale_top5": {
        "경기도 과천시": {
          "84": {
            "target_area_type": 84,
            "recent_trade_count": 1,
            "trades": [
              {
                "date": "2026-02",
                "contract_date": "2026-02-21",
                "name": "과천위버필드",
                "area": 84.99,
                "area_pyeong": 25.7,
                "supply_area": 108.12,
                "supply_area_pyeong": 32.7,
                "price": 150000,
                "floor": 12,
                "households": 120,
                "total_trade_count": 13,
                "related_rent_count": 1,
                "related_rent_trades": [
                  {
                    "date": "2026-02",
                    "contract_date": "2026-02-05",
                    "name": "과천위버필드",
                    "area": 84.99,
                    "price": 90000,
                    "trade_type": "전세"
                  }
                ]
              }
            ],
            "recent_rent_count": 1,
            "rent_trades": [
              {
                "date": "2026-02",
                "contract_date": "2026-02-05",
                "name": "과천위버필드",
                "area": 84.99,
                "price": 90000,
                "trade_type": "전세"
              }
            ]
          },
          "59": {
            "target_area_type": 59,
            "recent_trade_count": 0,
            "trades": [],
            "recent_rent_count": 0,
            "rent_trades": []
          }
        }
      }
    }
    """
    grouped_region_names = _extract_grouped_region_names(regions)
    if grouped_region_names is not None:
        region_cache: dict[str, dict[str, list[dict[str, Any]]]] = {}
        grouped_results: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}

        for bucket_name, bucket_region_names in grouped_region_names.items():
            grouped_results[bucket_name] = {}
            for region_name in bucket_region_names:
                if region_name not in region_cache:
                    region_cache[region_name] = _collect_region_transactions(region_name, area_types, limit)
                grouped_results[bucket_name][region_name] = region_cache[region_name]
        return grouped_results

    region_names = _extract_region_names(regions)
    return {region_name: _collect_region_transactions(region_name, area_types, limit) for region_name in region_names}

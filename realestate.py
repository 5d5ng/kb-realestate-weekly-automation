"""
KB부동산 실거래가 연동

- analyzer.py 결과 지역명 수신
- 지역명 -> KB 지역 스코프(시도/시군구/법정동) 변환
- KB 단지 목록 조회
- 지역별 84타입 / 59타입 최근 실거래 조회
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import threading
import time
from datetime import date
from functools import lru_cache
from typing import Any

import requests
import valuation_db

TIMEOUT_SEC = 30
DEFAULT_LIMIT = 5
DEFAULT_AREA_TYPES = (84, 59)
MAX_REGIONS_PER_BUCKET = 5
REQUEST_RETRY_COUNT = 3
REQUEST_RETRY_DELAY_SEC = 1.0
REGION_RETRY_COUNT = 2
REGION_RETRY_DELAY_SEC = 2.0
COMPLEX_CACHE_MAX_AGE_HOURS = 24 * 7
REALTRADE_CACHE_TTL_SEC = 60 * 60 * 24
MAX_REGION_WORKERS = 6
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

_SESSION_LOCAL = threading.local()


def _get_session() -> requests.Session:
    session = getattr(_SESSION_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(COMMON_HEADERS)
        _SESSION_LOCAL.session = session
    return session


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
            response = _get_session().get(f"{base_url}{path}", params=params, timeout=TIMEOUT_SEC)
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
    return _fetch_complexes_by_dong_code(dong_code)


def _fetch_complexes_by_dong_code(dong_code: str) -> tuple[dict[str, Any], ...]:
    body = _request_json(
        LAND_COMPLEX_API_URL,
        "/complexComm/hscmList",
        params={"법정동코드": dong_code},
    )
    return tuple(body.get("data", []))


@lru_cache(maxsize=2048)
def _get_complex_main(complex_id: int) -> dict[str, Any]:
    body = _request_json(
        LAND_COMPLEX_API_URL,
        "/complex/main",
        params={"단지기본일련번호": complex_id},
    )
    return body.get("data", {}) or {}


@lru_cache(maxsize=2048)
def _get_complex_types_remote(complex_id: int) -> tuple[dict[str, Any], ...]:
    return _fetch_complex_types_remote(complex_id)


def _fetch_complex_types_remote(complex_id: int) -> tuple[dict[str, Any], ...]:
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


def _normalize_complex_cache_row(
    complex_info: dict[str, Any],
    *,
    sido_name: str,
    sigungu_name: str,
    dong_name: str,
    dong_code: str,
) -> dict[str, Any] | None:
    complex_id = complex_info.get("단지기본일련번호")
    if not isinstance(complex_id, int):
        return None
    return {
        "complex_id": complex_id,
        "complex_name": _clean_text(complex_info.get("단지명")),
        "sido_name": sido_name,
        "sigungu_name": sigungu_name,
        "dong_name": dong_name,
        "dong_code": dong_code,
        "address": "",
        "households": _normalize_int(complex_info.get("세대수")),
        "completion_year": None,
        "entrance_type": "",
        "raw_json": {"complex_info": complex_info},
    }


def _normalize_type_cache_row(type_info: dict[str, Any]) -> dict[str, Any] | None:
    area_id = type_info.get("면적일련번호")
    if not isinstance(area_id, int):
        return None
    return {
        "area_id": area_id,
        "type_name": _clean_text(type_info.get("타입명") or type_info.get("면적명")),
        "exclusive_area": _normalize_area(type_info.get("전용면적")),
        "exclusive_area_pyeong": _normalize_area(type_info.get("전용면적평")),
        "supply_area": _normalize_area(type_info.get("공급면적")),
        "supply_area_pyeong": _normalize_area(type_info.get("공급면적평")),
        "contract_area": _normalize_area(type_info.get("계약면적")),
        "contract_area_pyeong": _normalize_area(type_info.get("계약면적평")),
        "households": _normalize_int(type_info.get("세대수")),
        "room_count": _normalize_int(type_info.get("방수")),
        "bathroom_count": _normalize_int(type_info.get("욕실수")),
        "total_trade_count": _normalize_int(type_info.get("매매건수")),
        "total_jeonse_count": _normalize_int(type_info.get("전세건수")),
        "total_monthly_rent_count": _normalize_int(type_info.get("월세건수")),
        "raw_json": type_info,
    }


def _get_complex_types(complex_id: int, *, force_refresh: bool = False) -> tuple[dict[str, Any], ...]:
    if not force_refresh:
        cached_types = valuation_db.get_complex_types(complex_id)
        if cached_types:
            cached_raw = [row.get("raw_json") for row in cached_types if isinstance(row.get("raw_json"), dict)]
            if cached_raw:
                return tuple(cached_raw)

    remote_types = _fetch_complex_types_remote(complex_id) if force_refresh else _get_complex_types_remote(complex_id)
    normalized_rows = [row for row in (_normalize_type_cache_row(item) for item in remote_types) if row]
    if normalized_rows:
        valuation_db.upsert_complex_types(complex_id, normalized_rows)
    return remote_types


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


def _iter_region_complexes(region_name: str, *, force_refresh: bool = False) -> list[dict[str, Any]]:
    kb_sido_name, sigungu_name = _resolve_region_scope(region_name)
    if not force_refresh:
        cached_complexes = valuation_db.get_complexes_by_scope(
            kb_sido_name,
            sigungu_name,
            max_age_hours=COMPLEX_CACHE_MAX_AGE_HOURS,
        )
        if cached_complexes:
            cached_raw = [
                row["raw_json"]["complex_info"]
                for row in cached_complexes
                if isinstance(row.get("raw_json"), dict) and isinstance(row["raw_json"].get("complex_info"), dict)
            ]
            if cached_raw:
                return cached_raw

    dongs = _get_dong_list(kb_sido_name, sigungu_name)

    complexes: dict[int, dict[str, Any]] = {}
    cache_rows: list[dict[str, Any]] = []
    for dong in dongs:
        dong_code = _clean_text(dong.get("법정동코드"))
        dong_name = _clean_text(dong.get("법정동명") or dong.get("법정동읍면동명") or dong.get("법정동한글명"))
        if not dong_code:
            continue

        complex_rows = _fetch_complexes_by_dong_code(dong_code) if force_refresh else _get_complexes_by_dong_code(dong_code)
        for complex_info in complex_rows:
            if _clean_text(complex_info.get("매물종별구분")) != "01":
                continue
            complex_id = complex_info.get("단지기본일련번호")
            if isinstance(complex_id, int):
                complexes[complex_id] = complex_info
                cache_row = _normalize_complex_cache_row(
                    complex_info,
                    sido_name=kb_sido_name,
                    sigungu_name=sigungu_name,
                    dong_name=dong_name,
                    dong_code=dong_code,
                )
                if cache_row:
                    cache_rows.append(cache_row)

    if cache_rows:
        deduped_rows = {row["complex_id"]: row for row in cache_rows}
        valuation_db.upsert_complexes(list(deduped_rows.values()))

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
    *,
    force_refresh: bool = False,
) -> tuple[int | None, dict[str, Any] | None]:
    return _pick_area_type_from_types(
        _get_complex_types(complex_id, force_refresh=force_refresh),
        target_area_type,
    )


def _pick_area_type_from_types(
    type_rows: tuple[dict[str, Any], ...] | list[dict[str, Any]],
    target_area_type: int,
) -> tuple[int | None, dict[str, Any] | None]:
    best_type: dict[str, Any] | None = None
    best_score: tuple[int, int, float] | None = None

    for type_info in type_rows:
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
    *,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """단지 + 면적 기준 최근 매매 실거래 조회. 조회종료일은 sysdate 기준."""
    cache_key = f"realestate:trade:{complex_id}:{area_id}:{_today_ymd()}:{limit}"
    if not force_refresh:
        cached = valuation_db.get_json_cache(cache_key)
        if isinstance(cached, list):
            return cached

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
    data = body.get("data", []) or []
    valuation_db.set_json_cache(cache_key, data, ttl_seconds=REALTRADE_CACHE_TTL_SEC)
    return data


def fetch_rent_data(
    complex_id: int,
    area_id: int,
    limit: int = DEFAULT_LIMIT,
    *,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """단지 + 면적 기준 최근 전월세 실거래 조회. 조회종료일은 sysdate 기준."""
    cache_key = f"realestate:rent:{complex_id}:{area_id}:{_today_ymd()}:{limit}"
    if not force_refresh:
        cached = valuation_db.get_json_cache(cache_key)
        if isinstance(cached, list):
            return cached

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
    data = body.get("data", []) or []
    valuation_db.set_json_cache(cache_key, data, ttl_seconds=REALTRADE_CACHE_TTL_SEC)
    return data


def _emit_refresh_progress(
    progress_callback: Any | None,
    *,
    message: str,
    status: str = "running",
    **extra: Any,
) -> None:
    if progress_callback is None:
        return
    payload = {"message": message, "status": status}
    if extra:
        payload.update(extra)
    try:
        progress_callback(payload)
    except Exception:
        return


def refresh_transaction_cache(
    regions: Any,
    area_types: tuple[int, ...] = DEFAULT_AREA_TYPES,
    limit: int = DEFAULT_LIMIT,
    *,
    refresh_rent: bool = False,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    grouped_region_names = _extract_grouped_region_names(regions)
    if grouped_region_names is None:
        grouped_region_names = {"manual": _extract_region_names(regions)}

    refreshed_regions: dict[str, dict[str, Any]] = {}
    refreshed_complex_ids: set[int] = set()
    refreshed_type_complex_ids: set[int] = set()
    refreshed_area_pairs: set[tuple[int, int]] = set()
    summary = {
        "bucket_count": len(grouped_region_names),
        "unique_region_count": 0,
        "region_count": sum(len(region_names) for region_names in grouped_region_names.values()),
        "complex_count": 0,
        "type_complex_count": 0,
        "sale_cache_entry_count": 0,
        "sale_row_count": 0,
        "rent_cache_entry_count": 0,
        "rent_row_count": 0,
        "reused_region_count": 0,
        "failed_regions": [],
        "bucket_regions": {},
    }

    _emit_refresh_progress(
        progress_callback,
        message="캐시 갱신을 시작합니다.",
        bucket_count=summary["bucket_count"],
        region_count=summary["region_count"],
        refresh_rent=refresh_rent,
    )

    for bucket_name, bucket_region_names in grouped_region_names.items():
        bucket_refreshed_regions = 0
        bucket_reused_regions = 0
        _emit_refresh_progress(
            progress_callback,
            message=f"{bucket_name} 캐시 갱신을 시작합니다.",
            bucket_name=bucket_name,
            bucket_region_count=len(bucket_region_names),
        )

        for index, region_name in enumerate(bucket_region_names, start=1):
            if region_name in refreshed_regions:
                bucket_reused_regions += 1
                summary["reused_region_count"] += 1
                _emit_refresh_progress(
                    progress_callback,
                    message=f"{region_name}은 이번 실행에서 이미 갱신했습니다.",
                    bucket_name=bucket_name,
                    region_name=region_name,
                    region_index=index,
                    status="completed",
                    reused=True,
                )
                continue

            _emit_refresh_progress(
                progress_callback,
                message=f"{region_name} 캐시 갱신 중입니다.",
                bucket_name=bucket_name,
                region_name=region_name,
                region_index=index,
            )

            try:
                complexes = _iter_region_complexes(region_name, force_refresh=True)
            except Exception as exc:
                summary["failed_regions"].append({"bucket_name": bucket_name, "region_name": region_name, "error": str(exc)})
                _emit_refresh_progress(
                    progress_callback,
                    message=f"{region_name} 캐시 갱신에 실패했습니다.",
                    bucket_name=bucket_name,
                    region_name=region_name,
                    region_index=index,
                    status="failed",
                    error=str(exc),
                )
                continue

            region_complex_count = 0
            region_type_complex_count = 0
            region_sale_entry_count = 0
            region_sale_row_count = 0
            region_rent_entry_count = 0
            region_rent_row_count = 0

            for complex_info in complexes:
                complex_id = complex_info.get("단지기본일련번호")
                if not isinstance(complex_id, int):
                    continue
                region_complex_count += 1
                refreshed_complex_ids.add(complex_id)

                if complex_id in refreshed_type_complex_ids:
                    type_rows = _get_complex_types(complex_id)
                else:
                    type_rows = _get_complex_types(complex_id, force_refresh=True)
                    refreshed_type_complex_ids.add(complex_id)
                    region_type_complex_count += 1

                for area_type in area_types:
                    area_id, _area_profile = _pick_area_type_from_types(type_rows, area_type)
                    if not area_id:
                        continue

                    area_pair = (complex_id, area_id)
                    if area_pair in refreshed_area_pairs:
                        continue

                    sale_rows = fetch_trade_data(complex_id, area_id, limit=limit, force_refresh=True)
                    region_sale_entry_count += 1
                    region_sale_row_count += len(sale_rows)
                    refreshed_area_pairs.add(area_pair)

                    if refresh_rent:
                        rent_rows = fetch_rent_data(complex_id, area_id, limit=limit, force_refresh=True)
                        region_rent_entry_count += 1
                        region_rent_row_count += len(rent_rows)

            refreshed_regions[region_name] = {
                "complex_count": region_complex_count,
                "type_complex_count": region_type_complex_count,
                "sale_cache_entry_count": region_sale_entry_count,
                "sale_row_count": region_sale_row_count,
                "rent_cache_entry_count": region_rent_entry_count,
                "rent_row_count": region_rent_row_count,
            }
            bucket_refreshed_regions += 1
            summary["unique_region_count"] += 1
            summary["complex_count"] += region_complex_count
            summary["type_complex_count"] += region_type_complex_count
            summary["sale_cache_entry_count"] += region_sale_entry_count
            summary["sale_row_count"] += region_sale_row_count
            summary["rent_cache_entry_count"] += region_rent_entry_count
            summary["rent_row_count"] += region_rent_row_count

            _emit_refresh_progress(
                progress_callback,
                message=f"{region_name} 캐시 갱신이 완료되었습니다.",
                bucket_name=bucket_name,
                region_name=region_name,
                region_index=index,
                status="completed",
                complex_count=region_complex_count,
                sale_cache_entry_count=region_sale_entry_count,
                sale_row_count=region_sale_row_count,
                rent_cache_entry_count=region_rent_entry_count,
                rent_row_count=region_rent_row_count,
            )

        summary["bucket_regions"][bucket_name] = {
            "requested_region_count": len(bucket_region_names),
            "refreshed_region_count": bucket_refreshed_regions,
            "reused_region_count": bucket_reused_regions,
        }

    summary["unique_complex_count"] = len(refreshed_complex_ids)
    summary["unique_area_pair_count"] = len(refreshed_area_pairs)
    _emit_refresh_progress(
        progress_callback,
        message="캐시 갱신이 완료되었습니다.",
        status="completed",
        unique_region_count=summary["unique_region_count"],
        unique_complex_count=summary["unique_complex_count"],
        unique_area_pair_count=summary["unique_area_pair_count"],
        sale_cache_entry_count=summary["sale_cache_entry_count"],
        rent_cache_entry_count=summary["rent_cache_entry_count"],
    )
    return summary


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
        except (RuntimeError, ValueError):
            complexes = []

        for complex_info in complexes:
            complex_id = complex_info.get("단지기본일련번호")
            if not isinstance(complex_id, int):
                continue

            complex_name = _clean_text(complex_info.get("단지명"))
            try:
                type_rows = _get_complex_types(complex_id)
            except RuntimeError:
                continue
            try:
                main_info = _get_complex_main(complex_id)
                complex_households = _normalize_int(main_info.get("총세대수"))
                rental_households = _normalize_int(main_info.get("임대세대수"))
            except RuntimeError:
                main_info = {}
                complex_households = None
                rental_households = None
            if not complex_households:
                complex_households = sum(
                    int(t.get("세대수") or 0) for t in type_rows
                ) or None

            for area_type in area_types:
                area_key = str(area_type)
                area_id, area_profile = _pick_area_type_from_types(type_rows, area_type)
                if not area_id:
                    continue

                try:
                    raw_rows = fetch_trade_data(complex_id, area_id, limit=limit)
                except RuntimeError:
                    continue

                for trade in _parse_latest_trade_rows(raw_rows, complex_name, area_profile or {}, complex_id, area_id):
                    trade["complex_households"] = complex_households
                    trade["rental_households"] = rental_households
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
    def _build_region_cache(region_names: list[str]) -> dict[str, dict[str, Any]]:
        if not region_names:
            return {}

        unique_region_names = list(dict.fromkeys(region_names))
        max_workers = min(MAX_REGION_WORKERS, len(unique_region_names))
        if max_workers <= 1:
            return {
                region_name: _collect_region_transactions(region_name, area_types, limit)
                for region_name in unique_region_names
            }

        region_cache: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_collect_region_transactions, region_name, area_types, limit): region_name
                for region_name in unique_region_names
            }
            for future in as_completed(future_map):
                region_name = future_map[future]
                region_cache[region_name] = future.result()
        return region_cache

    grouped_region_names = _extract_grouped_region_names(regions)
    if grouped_region_names is not None:
        region_cache = _build_region_cache([
            region_name
            for bucket_region_names in grouped_region_names.values()
            for region_name in bucket_region_names
        ])
        grouped_results: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}

        for bucket_name, bucket_region_names in grouped_region_names.items():
            grouped_results[bucket_name] = {}
            for region_name in bucket_region_names:
                grouped_results[bucket_name][region_name] = region_cache[region_name]
        return grouped_results

    region_names = _extract_region_names(regions)
    return _build_region_cache(region_names)

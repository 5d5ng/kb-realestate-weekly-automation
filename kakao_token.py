"""
카카오 OAuth 토큰 수명주기 관리

kv_cache(SQLite)에 저장된 access_token / refresh_token을 조회·갱신한다.
access_token 만료 시 refresh_token으로 자동 갱신하며,
refresh_token까지 만료되면 KakaoTokenError를 발생시킨다.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from valuation_db import get_json_cache, set_json_cache

load_dotenv()
load_dotenv(".env.example", override=False)

log = logging.getLogger(__name__)

KAKAO_TOKEN_URL = "https://kauth.kakao.com/oauth/token"
ACCESS_TOKEN_KEY = "kakao:access_token"
REFRESH_TOKEN_KEY = "kakao:refresh_token"
ACCESS_TOKEN_TTL = 21_000  # ~5h50m (실제 만료 6h, 여유분 차감)
REFRESH_TOKEN_TTL = 5_000_000  # ~58일 (실제 만료 ~60일)


def _env_text(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = str(value).strip()
    return default if normalized == "" else normalized


class KakaoTokenError(RuntimeError):
    """access_token과 refresh_token 모두 유효하지 않을 때 발생"""


def get_valid_access_token() -> str:
    cached = get_json_cache(ACCESS_TOKEN_KEY)
    if cached and cached.get("token"):
        return cached["token"]

    log.info("카카오 access_token 만료 — refresh_token으로 갱신 시도")
    return _refresh_access_token()


def _refresh_access_token() -> str:
    refresh_data = get_json_cache(REFRESH_TOKEN_KEY)
    if not refresh_data or not refresh_data.get("token"):
        raise KakaoTokenError(
            "카카오 refresh_token이 없거나 만료되었습니다. "
            "scripts/kakao_auth.py 를 다시 실행해 주세요."
        )

    client_id = _env_text("KAKAO_REST_API_KEY")
    if not client_id:
        raise KakaoTokenError("KAKAO_REST_API_KEY 환경변수가 설정되지 않았습니다.")

    payload = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_data["token"],
    }
    client_secret = _env_text("KAKAO_CLIENT_SECRET")
    if client_secret:
        payload["client_secret"] = client_secret

    try:
        resp = requests.post(KAKAO_TOKEN_URL, data=payload, timeout=15)
        resp.raise_for_status()
        body = resp.json()
    except Exception as exc:
        raise KakaoTokenError(f"카카오 토큰 갱신 요청 실패: {exc}") from exc

    new_access = body.get("access_token")
    if not new_access:
        raise KakaoTokenError(f"카카오 토큰 갱신 응답에 access_token이 없습니다: {body}")

    now_text = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    set_json_cache(
        ACCESS_TOKEN_KEY,
        {"token": new_access, "obtained_at": now_text},
        ttl_seconds=ACCESS_TOKEN_TTL,
    )
    log.info("카카오 access_token 갱신 완료")

    new_refresh = body.get("refresh_token")
    if new_refresh:
        set_json_cache(
            REFRESH_TOKEN_KEY,
            {"token": new_refresh, "obtained_at": now_text},
            ttl_seconds=REFRESH_TOKEN_TTL,
        )
        log.info("카카오 refresh_token도 함께 갱신됨 (잔여 30일 미만)")

    return new_access


def store_tokens(access_token: str, refresh_token: str) -> None:
    now_text = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    set_json_cache(
        ACCESS_TOKEN_KEY,
        {"token": access_token, "obtained_at": now_text},
        ttl_seconds=ACCESS_TOKEN_TTL,
    )
    set_json_cache(
        REFRESH_TOKEN_KEY,
        {"token": refresh_token, "obtained_at": now_text},
        ttl_seconds=REFRESH_TOKEN_TTL,
    )

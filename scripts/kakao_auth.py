#!/usr/bin/env python3
"""
카카오 OAuth 최초 인가 스크립트 (1회성)

로컬에서 브라우저를 열어 카카오 로그인 → 인가 코드를 받고,
access_token + refresh_token 을 kv_cache(SQLite)에 저장한다.

사전 준비:
1. Kakao Developer 콘솔 → 내 애플리케이션 → 앱 설정
   - REST API 키를 .env의 KAKAO_REST_API_KEY 에 기입
   - (선택) 보안 → Client Secret 활성화 → KAKAO_CLIENT_SECRET 에 기입
2. 카카오 로그인 → Redirect URI 에 http://localhost:9876/callback 추가
3. 동의항목 → "카카오톡 메시지 전송" (talk_message) 동의항목 설정

실행:
    python scripts/kakao_auth.py
"""
from __future__ import annotations

import sys
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.example", override=False)

import os

from kakao_token import KAKAO_TOKEN_URL, store_tokens

REDIRECT_URI = "http://localhost:9876/callback"
AUTHORIZE_URL = "https://kauth.kakao.com/oauth/authorize"


def _env_text(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    normalized = str(value).strip()
    return None if normalized == "" else normalized


def _build_auth_url(client_id: str) -> str:
    return (
        f"{AUTHORIZE_URL}"
        f"?client_id={client_id}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_type=code"
        f"&scope=talk_message"
    )


def _exchange_code(
    client_id: str,
    code: str,
    client_secret: str | None = None,
) -> dict:
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "code": code,
    }
    if client_secret:
        payload["client_secret"] = client_secret

    resp = requests.post(KAKAO_TOKEN_URL, data=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _receive_code_via_server() -> str | None:
    received_code: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            qs = parse_qs(urlparse(self.path).query)
            code = qs.get("code", [None])[0]
            if code:
                received_code.append(code)
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    b"<h2>OK</h2>"
                    b"<p>Authorization code received. You can close this tab.</p>"
                )
            else:
                error = qs.get("error_description", qs.get("error", ["unknown"]))[0]
                self.send_response(400)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(f"<h2>Error</h2><p>{error}</p>".encode())

        def log_message(self, format, *args):
            pass

    server = HTTPServer(("127.0.0.1", 9876), Handler)
    server.timeout = 120
    server.handle_request()
    server.server_close()

    return received_code[0] if received_code else None


def main() -> int:
    client_id = _env_text("KAKAO_REST_API_KEY")
    if not client_id:
        print("[ERROR] KAKAO_REST_API_KEY 가 .env 에 설정되어 있지 않습니다.")
        return 1

    client_secret = _env_text("KAKAO_CLIENT_SECRET")

    auth_url = _build_auth_url(client_id)

    print("=" * 60)
    print("  카카오 OAuth 인가 스크립트")
    print("=" * 60)
    print()
    print("[사전 확인]")
    print(f"  Kakao Developer 콘솔 → Redirect URI 에")
    print(f"  {REDIRECT_URI} 가 등록되어 있어야 합니다.")
    print()
    print("[1단계] 브라우저에서 카카오 로그인 및 동의를 진행합니다.")
    print(f"  URL: {auth_url}")
    print()

    webbrowser.open(auth_url)

    print("[2단계] 인가 코드를 기다리는 중... (최대 2분)")
    code = _receive_code_via_server()

    if not code:
        print()
        print("[FALLBACK] 콜백 수신 실패. 브라우저 주소창의 code= 값을 직접 붙여넣어 주세요.")
        code = input("인가 코드: ").strip()
        if not code:
            print("[ERROR] 인가 코드가 비어 있습니다.")
            return 1

    print()
    print("[3단계] 인가 코드 → 토큰 교환 중...")
    try:
        token_response = _exchange_code(client_id, code, client_secret)
    except Exception as exc:
        print(f"[ERROR] 토큰 교환 실패: {exc}")
        return 1

    access_token = token_response.get("access_token")
    refresh_token = token_response.get("refresh_token")
    if not access_token or not refresh_token:
        print(f"[ERROR] 응답에 토큰이 없습니다: {token_response}")
        return 1

    store_tokens(access_token, refresh_token)

    expires_in = token_response.get("expires_in", "?")
    refresh_expires_in = token_response.get("refresh_token_expires_in", "?")

    print()
    print("[완료] 토큰이 kv_cache (SQLite) 에 저장되었습니다.")
    print(f"  access_token  만료: {expires_in}초")
    print(f"  refresh_token 만료: {refresh_expires_in}초")
    print()
    print("  이제 SEND_KAKAO_ENABLED=1 로 설정하면 파이프라인에서")
    print("  카카오톡 나에게 보내기가 자동 발송됩니다.")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())

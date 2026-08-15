# MCP 모듈형 콘텐츠·게시 아키텍처

## 설계 목표

KB부동산 데이터 수집, 콘텐츠 구성, Canva 편집, Instagram 게시를 하나의 프로세스에 묶지 않는다.
각 MCP는 하나의 책임과 안정적인 JSON 계약만 가진다.

```text
[도메인 MCP]
KB부동산 / 여행 / 상품 / 뉴스 / 기타 프로젝트
        |
        | 도메인별 데이터와 초안
        v
[content-package-core MCP]
content-package/v1 생성·검증·저장
        |
        | package_id + content_digest
        v
[채널 게시 MCP]
Instagram / 향후 Naver Blog / Threads / 기타 채널
        |
        | 게시 계획 -> 사용자 검토 -> 해시 승인
        v
[외부 게시]
명시적 확인 + 채널별 활성화 환경변수
```

## MCP 단위

### 1. `kb-realestate`

- 진입점: `mcp_server.py`
- 책임: KB 데이터 수집, 실거래·뉴스 보강, 주간 보고서와 작성 패키지 생성
- 내부 구조: 하나의 MCP 진입점에서 디렉터가 7개 전문 에이전트에 위임
- 외부 게시 책임 없음
- 기존 한국어 도구명과 등록 경로 유지
- 상세: [KB부동산 MCP 에이전트 구조](./MCP_AGENT_ARCHITECTURE.md)

### 2. `content-package-core`

- 진입점: `mcp_servers/content_package_server.py`
- 책임: 주제와 무관한 게시용 콘텐츠 계약 생성·검증·조회
- 외부 API 호출 없음
- 주요 도구:
  - `create_content_package`
  - `validate_content_package`
  - `get_content_package`
  - `list_content_packages`

### 3. `instagram-content-publisher`

- 진입점: `mcp_servers/instagram_publisher_server.py`
- 책임: Instagram 규격 검증, 게시 계획, 승인 상태, 실제 게시
- KB부동산·Canva·특정 콘텐츠 생성기를 알지 못함
- `config/instagram_accounts.json`의 계정 별칭으로 여러 프로 계정을 분리
- 토큰은 `INSTAGRAM_<ACCOUNT_ALIAS>_ACCESS_TOKEN` 비밀변수로만 주입
- 주요 도구:
  - `instagram_connection_status`
  - `prepare_instagram_publish`
  - `approve_instagram_publish`
  - `publish_instagram_plan`
  - `get_instagram_publish_plan`

## 범용 콘텐츠 계약

`content-package/v1`의 필수 개념:

```json
{
  "schema_version": "content-package/v1",
  "package_id": "pkg_...",
  "content_digest": "sha256...",
  "title": "콘텐츠 제목",
  "content_type": "carousel",
  "caption": "게시 캡션",
  "media": [
    {
      "position": 1,
      "type": "image",
      "source": "https://...",
      "alt_text": "첫 페이지 설명"
    }
  ],
  "targets": ["instagram"],
  "metadata": {
    "source_project": "어떤 프로젝트든 가능"
  }
}
```

`metadata`는 도메인별 확장 영역이다. 게시 MCP는 여기에 KB부동산 필드가 있어도 해석하지 않는다.

## 승인과 게시 안전장치

1. 콘텐츠 패키지를 만들 때 본문과 미디어의 SHA-256 해시를 계산한다.
2. `prepare_instagram_publish`는 해당 해시를 가진 게시 계획만 생성한다.
3. 게시 계획에 `destination_account`를 기록하여 승인 후 대상 계정을 바꿀 수 없다.
4. `approve_instagram_publish`는 검토한 해시와 정확한 승인 문구가 일치해야 한다.
5. 승인 뒤 콘텐츠가 바뀌면 게시를 거부한다.
6. 실제 게시에는 다시 정확한 게시 확인 문구가 필요하다.
7. 전역 및 계정별 게시 스위치가 모두 `1`이 아니면 외부 게시하지 않는다.
8. 이미 게시된 `plan_id`는 다시 게시하지 않는다.

## 콘텐츠 소스 규칙

- 초안 단계에서는 로컬 파일 경로를 콘텐츠 패키지에 넣을 수 있다.
- Instagram 게시 계획을 만들 때는 모든 미디어가 공개 접근 가능한 HTTPS URL이어야 한다.
- 계정 비밀번호는 어떤 MCP에도 전달하거나 저장하지 않는다.
- Access Token은 `.env` 또는 실행 환경의 비밀 변수로만 주입한다.

## 멀티계정 레지스트리

```json
{
  "schema_version": "instagram-accounts/v1",
  "default_account": "ddony_marble",
  "accounts": {
    "ddony_marble": {
      "instagram_user_id": "17841475556425581",
      "content_profile": "economy",
      "token_env": "INSTAGRAM_DDONY_MARBLE_ACCESS_TOKEN",
      "publishing_enabled_env": "INSTAGRAM_DDONY_MARBLE_PUBLISHING_ENABLED"
    }
  }
}
```

새 계정은 레지스트리에 항목을 추가하고 해당 `token_env`만 비밀변수로 주입한다.
콘텐츠 프로젝트는 `account_alias`만 전달하므로 계정 인증 구현에 종속되지 않는다.

## Codex MCP 등록 예시

```toml
[mcp_servers.kb-realestate]
command = "python3"
args = ["/Users/dave/Project/kb-realestate-weekly-automation/mcp_server.py"]
cwd = "/Users/dave/Project/kb-realestate-weekly-automation"

[mcp_servers.content-core]
command = "python3"
args = ["/Users/dave/Project/kb-realestate-weekly-automation/mcp_servers/content_package_server.py"]
cwd = "/Users/dave/Project/kb-realestate-weekly-automation"

[mcp_servers.instagram-publisher]
command = "python3"
args = ["/Users/dave/Project/kb-realestate-weekly-automation/mcp_servers/instagram_publisher_server.py"]
cwd = "/Users/dave/Project/kb-realestate-weekly-automation"
```

## 검증

```bash
make test
make mcp-test
make mcp-test-modular
```

`mcp-test-modular`은 부동산과 무관한 여행 카드뉴스를 생성해 두 MCP 사이의 계약과 승인까지 검증한다.
실제 Instagram 게시는 호출하지 않는다.

# KB부동산 주간 자동화 파이프라인

KB부동산 주간시계열 데이터를 기반으로 지역별 흐름을 분석하고, 실거래/뉴스를 보강한 뒤, 콘텐츠 생성과 발송까지 이어지는 자동화 프로젝트입니다.

실행 방법만 빠르게 보려면 [사용자 매뉴얼](./USER_MANUAL.md)을 먼저 확인하세요.

현재 기준으로 로컬 테스트와 주요 모듈 검증은 가능하며, 운영 기본 발송 채널은 텔레그램입니다. SOLAPI SMS 연동은 구현 및 테스트가 완료됐지만 비용 이슈로 기본 비활성화 상태입니다.

## 현재 구현 범위

- KB부동산 주간시계열 엑셀 자동 다운로드 및 분석
- 서울 25개 구 매매·전세 연속 상승 현황 계산
- 수도권/비수도권 매매·전세 최신 상승 상위 5개 지역 선별
- KB 부동산 실거래 데이터 조회
- 지역별 `84`, `59` 타입 최근 실거래 조회
- 네이버 신문보기 기준 부동산 뉴스 수집
- 지정 언론사/키워드 기반 뉴스 중요도 필터
- 플랫폼별 콘텐츠 생성 구조 분리
- Claude/GPT 웹 붙여넣기용 작성 패키지 생성
- 비용 없는 Markdown 보고서 초안 생성
- 구조화 데이터 스냅샷 JSON 저장
- 개인용 로컬 MCP 서버로 LLM 클라이언트 직접 호출 지원
- 디렉터와 7개 전문 에이전트로 MCP 내부 책임 분리
- 인스타 카드뉴스/캡션/네이버 블로그용 프롬프트 파일 저장
- 텔레그램 실제 발송
- 텔레그램 프롬프트 파일 첨부 발송 옵션
- SOLAPI SMS 실제 발송 확인 완료 (운영 기본값은 비활성화)
- APScheduler 기반 주간 실행 뼈대
- 로컬 CLI 테스트 스크립트
- 로컬 웹 실행 페이지
- 주제 독립적인 `content-package/v1` 콘텐츠 계약
- 콘텐츠 생성과 외부 게시를 분리한 독립 MCP 서버
- 해시 검토·승인을 거쳐 게시하는 Instagram Login 게시 모듈

## 현재 기준 실제 채널 상태

- 텔레그램: 구현 및 실발송 확인 완료
- SOLAPI: SMS 기준 구현 및 실발송 확인 완료, 운영 기본값은 비활성화
- 카카오 알림톡: 아직 미구현, 현재는 SMS로 대체 운영
- 인스타그램 업로드: 독립 Publisher MCP 구현 완료, 계정 OAuth 연결 전·기본 비활성화
- 네이버 블로그 게시: 아직 미구현
- 프롬프트 파일 저장: 구현 완료

## 프로젝트 구조

```text
app.py                         Flask 웹 진입점
analyzer.py                    KB 주간시계열 분석
realestate.py                  KB 실거래 조회
news.py                        네이버 뉴스/신문보기 수집 및 필터링
reporter.py                    콘텐츠 생성 오케스트레이터
sender.py                      텔레그램 발송 / SMS 선택 발송
scheduler.py                   파이프라인 실행 컨트롤러 + APScheduler
mcp_server.py                  개인용 로컬 MCP stdio 서버
kb_agents/                     디렉터·데이터·뉴스·실거래·작성·품질·게시·운영 에이전트

mcp_runtime/                   공통 MCP stdio/JSON-RPC 런타임
mcp_servers/content_package_server.py
                               범용 콘텐츠 패키지 MCP
mcp_servers/instagram_publisher_server.py
                               승인형 Instagram 게시 MCP
content_core/                  content-package/v1 계약과 저장소
publishing/                    게시 계획·승인·Instagram 어댑터

reporters/common.py            공통 프롬프트/LLM/파일 저장 유틸
reporters/telegram.py          텔레그램 리포트 생성
reporters/alimtalk.py          짧은 메시지 생성
reporters/instagram.py         인스타 캡션 생성
reporters/cardnews.py          카드뉴스 스크립트 생성
reporters/blog.py              네이버 블로그 프롬프트 생성

scripts/run_local_pipeline_test.py   로컬 CLI 테스트
scripts/run_local_web.py             로컬 웹 테스트 실행기
scripts/test_mcp_server.py           MCP 서버 smoke/integration 테스트

reports/llm_package.md         Claude/GPT 웹 작성용 패키지
reports/weekly_report.md       비용 없는 Markdown 보고서 초안
reports/data_snapshot.json     정제 데이터 스냅샷
reports/prompts/               저장된 플랫폼별 프롬프트 파일
reports/                       테스트 결과 파일
downloads/                     다운로드된 KB 파일
```

네이버·Instagram 검수 흐름에서는 `reports/weekly_report.md`가 사람이 수정하는
단일 편집 원본입니다. `reports/data_snapshot.json`은 숫자·지역·실거래 교차검증에만
사용하며, 기존 `reports/card_news_script.md`는 Telegram 등 레거시 호환 산출물로만
유지합니다. Codex Desktop 소셜 카피, 카드뉴스 HTML/JPEG, 게시 패키지는 모두
검수한 `weekly_report.md`의 SHA-256에 묶입니다.

## 데이터 흐름

전체 흐름은 아래 순서이며, `scheduler.py`는 각 단계를 `kb_agents/`의 전문 에이전트에 위임합니다.

1. `analyzer.py`
   - KB 주간시계열 파일 다운로드
   - 매매/전세 시트 파싱
   - 전체 주간 이력에서 지역별 연속 상승 주수 계산
   - 서울 전체와 수도권/비수도권 상승 상위로 구성된 6개 콘텐츠 섹션 생성

2. `realestate.py`
   - `analysis` 결과를 받아 지역명 해석
   - KB 실거래 데이터 조회
   - 지역별 `84`, `59` 타입 최근 거래 정리

3. `news.py`
   - 네이버 언론사 신문보기에서 최근 기사 수집
   - 대상 언론사 필터
   - 제외 키워드 제거
   - 우선순위 키워드 점수화

4. `reporter.py`
   - 텔레그램/문자/인스타/카드뉴스용 콘텐츠 생성
   - Claude/GPT 웹 작성 패키지, Markdown 보고서 초안, 데이터 스냅샷 저장
   - 인스타 카드뉴스/캡션/네이버 블로그용 프롬프트 파일 저장

5. `sender.py`
   - 텔레그램 발송
   - SOLAPI SMS 발송
   - 인스타 게시 책임은 별도 Publisher MCP로 분리

6. `scheduler.py`
   - Data → Transaction → News → Authoring → Quality → Publishing 에이전트를 하나의 파이프라인으로 조정

## 환경 변수

기본 템플릿은 [`.env.example`](./.env.example) 에 있습니다.

시작 전:

```bash
cp .env.example .env
```

### 최소 테스트용

뉴스 수집과 발송 없이 로컬 dry-run 만 볼 경우:

- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`

### 콘텐츠 생성 실제 LLM 호출까지 테스트할 경우

- `OPENAI_API_KEY`
- `GEMINI_API_KEY`
- `ANTHROPIC_API_KEY` 선택

키가 없어도 템플릿 fallback 으로 동작합니다.
현재 기본 실행은 LLM API 호출을 끄고 작성 패키지와 Markdown 초안을 생성합니다.
웹 UI 또는 실행 옵션에서 LLM 사용을 명시적으로 켰을 때만 API 호출이 발생합니다.

### 발송 테스트용

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `SEND_TELEGRAM_PROMPT_FILES_ENABLED` 선택, 기본 `1`
- `SOLAPI_API_KEY`
- `SOLAPI_API_SECRET`
- `SOLAPI_SENDER`
- `SOLAPI_DEFAULT_RECIPIENTS`

### Instagram 게시 MCP

- `INSTAGRAM_ACCOUNT_REGISTRY` 기본 `config/instagram_accounts.json`
- `INSTAGRAM_PUBLISHING_ENABLED` 전역 게시 스위치, 기본 `0`
- `INSTAGRAM_<ACCOUNT_ALIAS>_PUBLISHING_ENABLED` 계정별 게시 스위치, 기본 `0`
- `INSTAGRAM_<ACCOUNT_ALIAS>_ACCESS_TOKEN` 계정별 토큰
- `INSTAGRAM_GRAPH_API_VERSION` 기본 `v25.0`

계정 ID, 콘텐츠 주제, 토큰 환경변수명은 공개 레지스트리에 두고 실제 토큰은
`.env` 또는 배포 비밀변수에만 둡니다. 게시 계획에는 대상 계정이 고정됩니다.
계정 비밀번호는 입력하거나 저장하지 않습니다. 모듈 구성과 승인 절차는
[MCP 모듈형 아키텍처](./docs/MCP_MODULAR_ARCHITECTURE.md)를 확인하세요.

## 설치

권장 방식은 `make setup` 입니다. `.venv` 생성, 의존성 설치, 기본 폴더 생성, `.env` 템플릿 복사까지 한 번에 처리합니다.

```bash
make setup
```

직접 설치할 경우:

```bash
pip install -r requirements.txt
```

## 로컬 테스트 방법

### 빠른 시작

```bash
make setup                 # 최초 1회
make check-env             # 환경변수 점검
make dry-run               # CLI dry-run, 발송 없음
make mcp-test              # MCP 서버 연결/도구 목록 확인
make test                  # 범용 콘텐츠 계약·승인 단위 테스트
make mcp-test-modular      # content-core → publisher MCP 통합 드라이런
make mcp-test-run          # MCP 경유 작성 패키지 생성까지 확인
make mcp-tunnel-init       # ChatGPT 웹용 Secure MCP Tunnel 프로필 최초 생성 + 실행
make mcp-tunnel            # 생성된 터널 프로필 실행
make web                   # 웹 UI 실행
```

`make web` 실행 후 브라우저에서 아래 주소로 접속합니다.

```text
http://127.0.0.1:5050
```

> macOS에서는 AirPlay/ControlCenter가 `localhost:5000`을 잡는 경우가 있습니다. 그래서 로컬 웹 기본 포트는 `5050`을 권장합니다.

---

### 0. 로컬 MCP 테스트 (`mcp_server.py`)

Claude Desktop, Cursor, Codex 같은 로컬 MCP 클라이언트에서 이 프로젝트를 직접 호출하기 위한 stdio MCP 서버입니다. 사용자는 MCP 하나만 등록하고, 내부 디렉터가 데이터·뉴스·실거래·작성·품질·운영 에이전트에 작업을 위임합니다. 이 에이전트들은 별도 LLM API가 아니라 Python 모듈이므로 에이전트 분리 자체에는 API 비용이 들지 않습니다.

상세 구조는 [KB부동산 MCP 에이전트 구조](./docs/MCP_AGENT_ARCHITECTURE.md)를 확인하세요.

#### MCP 클라이언트 등록 예시

```json
{
  "mcpServers": {
    "kb-realestate": {
      "command": "python3",
      "args": [
        "/Users/dave/Project/kb-realestate-weekly-automation/mcp_server.py"
      ],
      "cwd": "/Users/dave/Project/kb-realestate-weekly-automation"
    }
  }
}
```

#### 로컬 검증

```bash
make mcp-test
make mcp-test-run
```

MCP 연결 후에는 영어 도구명을 외우지 않고 한국어로 요청하면 됩니다.

```text
KB부동산 최신 작성 패키지 만들어줘. 빠르게 실행하고 실거래는 생략해.
최신 뉴스 보여줘.
최신 실거래 내역 보여줘.
생성 결과 품질 검사해줘.
에이전트 상태 보여줘.
```

도구 선택이 애매할 때는 `kb_realestate_assistant`를 직접 지목합니다.

```text
kb-realestate의 kb_realestate_assistant로 "이번 주 KB부동산 블로그 글 쓸 자료 만들어줘" 요청을 처리해줘.
```

ChatGPT 웹은 로컬 stdio MCP 서버에 직접 붙는 방식이 아닙니다. ChatGPT 웹에서 쓰려면 Secure MCP Tunnel 또는 원격 MCP 배포 구성이 필요합니다. 자세한 절차는 [사용자 매뉴얼](./USER_MANUAL.md)을 확인하세요.

터널 준비가 끝난 뒤에는 아래 명령을 씁니다.

```bash
export CONTROL_PLANE_API_KEY="sk-..."
export OPENAI_MCP_TUNNEL_ID="tunnel_..."
make mcp-tunnel-init
```

---

### 1. CLI 테스트 (`run_local_pipeline_test.py`)

발송 없이 전체 파이프라인 흐름을 확인하는 CLI 도구입니다.

#### 옵션

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--send` | off | 실제 텔레그램/SMS 발송 수행 |
| `--send-prompt-files` | on | `--send` 사용 시 생성된 프롬프트 파일을 텔레그램 문서로 첨부 |
| `--no-send-prompt-files` | off | `--send` 사용 시 프롬프트 파일 첨부만 비활성화 |
| `--news-days` | `1` | 뉴스 수집 기간 (일) |
| `--news-max-articles` | `3` | 수집할 최대 뉴스 수 |
| `--transaction-limit` | `2` | 지역/타입별 최근 실거래 최대 건수 |
| `--skip-transactions` | off | 실거래 조회를 생략하고 작성 패키지/초안 생성 속도 우선 |
| `--output-mode` | `both` | `authoring_package`, `draft_only`, `both` 중 작성 산출물 선택 |
| `--output` | - | 결과 JSON 저장 경로 |
| `--json` | off | raw JSON 만 출력 (스크립트 연동용) |

#### 예시

**기본 dry-run** — 발송 없이 전체 흐름 점검:

```bash
make dry-run
```

직접 실행:

```bash
.venv/bin/python scripts/run_local_pipeline_test.py
```

**파라미터 지정 테스트** — 뉴스 3일치, 기사 5건, 실거래 3건:

```bash
.venv/bin/python scripts/run_local_pipeline_test.py \
  --news-days 3 \
  --news-max-articles 5 \
  --transaction-limit 3
```

**가벼운 빠른 테스트** — 최근 1일 뉴스, 기사 3건, 실거래 2건:

```bash
make fast-test
```

**결과 JSON 파일 저장** — 실행 결과를 파일로 남기기:

```bash
.venv/bin/python scripts/run_local_pipeline_test.py \
  --output reports/local_test_result.json
```

**스크립트 연동용 JSON 출력** — 다른 스크립트에서 파이프로 받을 때:

```bash
.venv/bin/python scripts/run_local_pipeline_test.py --json
```

**실제 발송 테스트** — 텔레그램/SMS가 실제로 발송됩니다:

```bash
make send
```

직접 실행:

```bash
.venv/bin/python scripts/run_local_pipeline_test.py --send
```

**실제 발송 + 프롬프트 파일 첨부** — 기본값입니다. 텔레그램 리포트 전송 뒤 생성된 프롬프트 파일도 문서로 보냅니다:

```bash
.venv/bin/python scripts/run_local_pipeline_test.py --send
```

프롬프트 파일 첨부만 끄려면:

```bash
.venv/bin/python scripts/run_local_pipeline_test.py --send --no-send-prompt-files
```

> 처음에는 반드시 `--send` 없이 dry-run 으로 확인한 뒤 발송하세요.

---

### 2. 웹 UI 테스트 (`run_local_web.py`)

브라우저에서 버튼 클릭으로 Dry Run / 실제 발송을 테스트할 수 있는 로컬 웹 서버입니다.

#### 옵션

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--host` | `127.0.0.1` | 바인딩 호스트 |
| `--port` | `5000` | 스크립트 자체 기본 포트. 로컬에서는 `make web`으로 `5050` 실행 권장 |
| `--with-scheduler` | off | APScheduler 예약 실행도 함께 활성화 |

#### 예시

**기본 실행** — 웹 서버 시작 후 브라우저 접속:

```bash
make web
# 브라우저에서 http://127.0.0.1:5050 접속
```

**포트 변경** — 5000번이 사용 중일 때:

```bash
.venv/bin/python scripts/run_local_web.py --port 5050
# 브라우저에서 http://127.0.0.1:5050 접속
```

**스케줄러 포함 실행** — 배포 환경처럼 APScheduler 까지 같이 테스트:

```bash
make web-scheduler
```

#### 웹 화면에서 할 수 있는 것

- Dry Run 실행
- 실제 발송 실행
- 뉴스 기간 / 기사 수 / 실거래 건수 입력
- 실행 결과 JSON 바로 확인
- 생성된 프롬프트를 유형별로 열기
- 생성된 프롬프트를 유형별로 즉시 복사

## API 엔드포인트

### `GET /`

로컬 실행 페이지

### `GET /health`

헬스체크

응답 예시:

```json
{
  "status": "ok"
}
```

### `POST /run`

수동 파이프라인 실행

요청 예시:

```json
{
  "send": false,
  "news_days": 1,
  "news_max_articles": 3,
  "transaction_limit": 2,
  "skip_transactions": true
}
```

응답에는 아래 요약이 포함됩니다.

- `analysis_summary`
- `transaction_summary`
- `news_summary`
- `contents_summary`
- `send_results`

## 생성 결과 확인 위치

### 프롬프트 파일

실행 후 LLM에 넣은 프롬프트 원문이 아래 최신 파일로 저장됩니다.

- `reports/prompts/telegram_report_prompt.txt`
- `reports/prompts/naver_blog_post_prompt.txt`
- `reports/prompts/instagram_caption_prompt.txt`
- `reports/prompts/card_news_script_prompt.txt`
- `reports/prompts/alimtalk_message_prompt.txt`

매 실행본은 덮어쓰기 방지를 위해 아카이브에도 남습니다.

```text
reports/prompts/archive/
```

예:

```text
reports/prompts/archive/2026-04-30_234011_2026-04-27_telegram_report_prompt.txt
```

프롬프트 파일은 "모델에 넣은 입력값"입니다. 실제 생성된 최종 글은 웹 실행 결과 JSON, 터미널 실행 결과, 또는 실행 아티팩트 zip에서 확인합니다.

### 실행 아티팩트

웹에서 실행하면 다운로드 가능한 실행 묶음이 아래에 생성됩니다.

```text
reports/exports/
```

### 실거래 테스트 리포트

- `reports/realestate_test_report.txt`

## 현재 모델 라우팅

현재 기본 설정은 다음과 같습니다.

- 텔레그램 리포트: `gemini / gemini-2.5-flash-lite`
- 알림 메시지: `none` 또는 템플릿
- 인스타 캡션: `gemini / gemini-2.5-flash-lite`
- 카드뉴스 스크립트: `gemini / gemini-2.5-flash-lite`
- 네이버 블로그 프롬프트: `none`

실제 설정값은 `.env` 의 `REPORTER_{TASK}_PROVIDER`, `REPORTER_{TASK}_MODEL`, `REPORTER_{TASK}_MAX_TOKENS` 로 변경할 수 있습니다. `provider=none` 이면 LLM 호출 없이 템플릿 fallback 문구를 사용합니다.

## 현재 제한 사항

- `scheduler.py` 는 실제 실행 컨트롤러로 동작하지만, 아직 운영 배포 전 최종 통합 검증은 더 필요합니다.
- `app.py` 의 로컬 웹 실행기는 편의 기능 위주입니다.
- 카카오 알림톡은 아직 붙지 않았고, 현재는 SMS로 대체되어 있습니다.
- 인스타 업로드는 계정 조건 충족 후 구현 예정입니다.
- 네이버 블로그는 프롬프트 저장까지만 구현되어 있습니다.

## 권장 사용 순서

1. `.env` 설정
2. `make setup`
3. `make check-env`
4. `make dry-run`
5. `reports/prompts/` 와 터미널 결과 확인
6. 필요 시 `make web`
7. 마지막에 `make send` 또는 `--send` 로 실제 발송 테스트

터미널만 사용할 때는 아래 순서로 충분합니다.

```bash
cd /Users/dave/Project/kb-realestate-weekly-automation
make check-env
make dry-run
```

실제 발송은 마지막에만 실행합니다.

```bash
make send
```

웹으로 확인할 때는 아래처럼 실행합니다.

```bash
make web
# http://127.0.0.1:5050
```

## Railway 배포 메모

- 권장 시작 커맨드: `gunicorn app:app --bind 0.0.0.0:$PORT`
- 헬스체크 경로: `/health`
- 운영 권장 인스턴스 수: `1`
- 현재 실행 우선순위 정책: `수동 실행 > 예약 실행`
- 예약 실행 기본 시각: `매주 금요일 09:00`
- `ENABLE_SCHEDULER=1` 이면 예약 실행 활성화, `0` 이면 웹/API 수동 실행만 사용
- Railway에 등록할 환경변수는 로컬 `.env` 기준으로 동일하게 옮기면 됩니다.

## 다음 개발 우선순위

- `app.py` 와 `scheduler.py` 최종 통합 검증
- 카카오 알림톡 실제 연동
- Instagram Login OAuth 계정 연결과 제한된 테스트 게시
- Canva 내보내기 결과를 공개 HTTPS 미디어 URL로 전달하는 저장소 어댑터
- 네이버 블로그 실제 게시 기능
- Railway 배포 및 운영 안정화

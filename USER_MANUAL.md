# KB부동산 주간 자동화 사용자 매뉴얼

이 문서는 매번 실행 방법을 다시 찾지 않기 위한 운영용 메모입니다.

## 1. 핵심 개념

이 프로젝트는 KB부동산 주간 데이터를 받아서 아래 파일을 만듭니다.

- `reports/llm_package.md`: Claude/GPT 웹에 그대로 붙여넣는 작성 패키지
- `reports/weekly_report.md`: LLM API 비용 없이 만든 Markdown 보고서 초안
- `reports/data_snapshot.json`: 정제된 원본 데이터 JSON
- `reports/prompts/*.txt`: 텔레그램, 블로그, 인스타, 카드뉴스용 프롬프트 원문

네이버·Instagram 콘텐츠를 만들 때는 `reports/weekly_report.md` 하나만 검토·수정하면
됩니다. Codex Desktop이 이 보고서에서 캡션과 카드뉴스 문구를 만들고,
`reports/data_snapshot.json`은 게시 전 사실 검증에만 사용합니다. 기존
`reports/card_news_script.md`는 현재 Telegram 호환을 위해 남아 있지만 새 소셜 작성
흐름의 입력은 아닙니다.

기본 실행은 LLM API를 쓰지 않습니다. 웹 화면에서 LLM 사용 체크박스를 켜거나 API payload에서 LLM을 명시적으로 켠 경우에만 OpenAI/Gemini/Anthropic API 비용이 발생합니다.

## 2. 처음 1회 설정

```bash
cd /Users/dave/Project/kb-realestate-weekly-automation
make setup
make check-env
```

`.env`에 필요한 값을 넣습니다.

- 뉴스 수집: `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
- 텔레그램 발송: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- LLM API 사용 시에만: `OPENAI_API_KEY`, `GEMINI_API_KEY`, `ANTHROPIC_API_KEY`

## 3. 가장 안전한 실행

발송 없이 파일만 만들려면:

```bash
make dry-run
```

빠른 테스트:

```bash
make fast-test
```

외부 LLM 작성 패키지만 만들려면:

```bash
.venv/bin/python scripts/run_local_pipeline_test.py --output-mode authoring_package
```

LLM 에이전트 호출용으로 빠르게 만들려면:

```bash
.venv/bin/python scripts/run_local_pipeline_test.py --output-mode authoring_package --skip-transactions
```

## 4. 웹 화면으로 실행

```bash
make web
```

브라우저에서 접속합니다.

```text
http://127.0.0.1:5050
```

추천 순서:

1. `사전 점검 실행`
2. `Dry Run` 실행
3. 실행 완료 후 `작성 패키지 / 프롬프트`에서 `Claude/GPT 작성 패키지` 열기
4. 필요하면 `생성 파일 ZIP 다운로드`
5. 결과가 괜찮을 때만 실제 발송 실행

## 5. 실제 발송

CLI에서 실제 발송:

```bash
make send
```

웹에서는 `실제 발송` 카드에서 채널을 체크하고 실행합니다.

주의:

- 텔레그램, SMS, 카카오 등 체크한 채널만 발송됩니다.
- SMS/SOLAPI는 비용이 발생할 수 있습니다.
- 처음에는 반드시 `Dry Run`으로 결과를 확인한 뒤 발송합니다.

## 6. 생성 파일 보는 법

가장 많이 볼 파일:

```text
reports/llm_package.md
reports/weekly_report.md
reports/data_snapshot.json
```

아카이브:

```text
reports/archive/
reports/prompts/archive/
```

웹 실행 ZIP:

```text
reports/exports/
```

## 7. Claude/GPT 웹에서 쓰는 법

비용 없이 앱에서 자료만 만들고, 최종 글은 Claude/GPT 웹에서 작성하려면:

1. `make dry-run` 또는 웹 `Dry Run` 실행
2. `reports/llm_package.md` 열기
3. 전체 내용을 Claude/GPT 웹에 붙여넣기
4. 결과를 보고 네이버 블로그, 텔레그램, 인스타 등에 맞게 최종 편집

이 방식은 이 프로젝트의 OpenAI/Gemini/Anthropic API 비용이 들지 않습니다. 단, Claude/GPT 웹 서비스 자체의 구독 또는 사용량 정책은 별개입니다.

## 8. LLM으로 프로젝트를 구동할 수 있나?

가능합니다. 단, 조건이 있습니다.

로컬 MCP 클라이언트로 연결한 경우:

- Claude Desktop, Cursor, Codex 같은 로컬 MCP 클라이언트에서 `mcp_server.py`를 등록하면 LLM이 이 프로젝트를 직접 호출할 수 있습니다.
- 이 방식은 보통 `make web`을 따로 켜둘 필요가 없습니다. MCP 클라이언트가 `python3 mcp_server.py`를 직접 실행합니다.
- 기본 MCP 도구는 발송과 LLM API 호출을 하지 않습니다. 내부의 8개 에이전트는 별도 유료 LLM이 아니라 책임별 Python 모듈입니다.

로컬에서 `make web`으로 서버를 켜둔 경우:

- Codex, Claude Code처럼 같은 컴퓨터의 터미널이나 브라우저에 접근할 수 있는 LLM 에이전트는 실행할 수 있습니다.
- 예: 로컬 API `POST http://127.0.0.1:5050/run/start` 호출 또는 웹 화면 조작

일반 Claude/GPT 웹 채팅만 켜둔 경우:

- 보통은 내 컴퓨터의 `localhost`에 직접 접근할 수 없습니다.
- 이 경우 `reports/llm_package.md`를 붙여넣어 글을 작성시키는 방식으로 사용합니다.
- ChatGPT 웹에서 MCP 앱으로 붙이려면 원칙적으로 원격 MCP 서버가 필요합니다.
- OpenAI 문서 기준으로 ChatGPT는 로컬 MCP 서버에 직접 연결하지 않고, 개발자 PC/사설망 MCP 서버는 Secure MCP Tunnel 같은 방식으로 연결해야 합니다.

외부에서 언제든 호출하고 싶다면:

- Railway 같은 곳에 배포하거나 터널을 열어야 합니다.
- 현재 실행 API에는 별도 인증이 없으므로 공개 URL로 열면 위험합니다.
- 공개 운영하려면 먼저 간단한 관리자 토큰/API 키 인증을 추가하는 것이 좋습니다.

## 9. 로컬 MCP로 개인 연결

이 방식은 "나 혼자 로컬에서 LLM에게 이 프로젝트를 호출시키는" 1차 구성입니다.

### 등록 설정

MCP 클라이언트 설정에 아래 서버를 추가합니다.

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

가상환경을 명시하고 싶으면 `command`를 아래처럼 바꿉니다.

```json
"command": "/Users/dave/Project/kb-realestate-weekly-automation/.venv/bin/python"
```

### 노출된 MCP 도구

- `kb_help`: 한국어 사용법 보기
- `kb_realestate_assistant`: 디렉터가 한국어 요청을 받아 알맞은 에이전트에 자동 위임
- `generate_authoring_package`: LLM API/발송 없이 `reports/llm_package.md` 생성
- `generate_weekly_report`: LLM API/발송 없이 작성 패키지, Markdown 초안, 데이터 스냅샷 생성
- `get_latest_package`: 최신 `reports/llm_package.md` 읽기
- `get_latest_weekly_report`: 최신 `reports/weekly_report.md` 읽기
- `get_data_snapshot`: 최신 `reports/data_snapshot.json` 읽기
- `get_latest_news`: 뉴스 에이전트가 최신 기사 목록 읽기
- `get_latest_transactions`: 실거래 에이전트가 최신 거래 결과 읽기
- `check_latest_artifacts`: 품질 에이전트가 산출물 구조와 기준일 검사
- `list_artifacts`: 현재 생성 파일 목록 확인
- `get_agent_status`: 에이전트 구성, 실행 상태, 안전 기본값 확인

### 내부 에이전트

- 디렉터: 한국어 요청 분류와 위임
- 데이터: KB 분석과 데이터 스냅샷
- 뉴스: 뉴스 수집과 조회
- 실거래: 거래 캐시, 수집, 조회
- 작성: 패키지와 보고서 생성
- 품질: 파일과 데이터 구조 검수
- 게시: 실제 발송 경계. MCP 기본 호출에서는 비활성
- 운영: 에이전트와 산출물 상태 확인

사용자는 이 8개를 따로 등록하지 않습니다. `mcp_server.py` 하나만 등록하면 됩니다.
상세 개발 구조는 [KB부동산 MCP 에이전트 구조](./docs/MCP_AGENT_ARCHITECTURE.md)를 확인하세요.

### 로컬에서 MCP 동작 확인

서버 연결만 확인:

```bash
make mcp-test
```

MCP를 통해 실제 작성 패키지 생성까지 확인:

```bash
make mcp-test-run
```

`mcp-test-run`은 네트워크 데이터 수집은 하지만, 기본값으로 LLM API 호출과 발송은 하지 않습니다.

### LLM에 요청하는 문장 예시

MCP 연결 후에는 영어 도구명을 외우지 말고 이렇게 요청합니다.

### 범용 콘텐츠·게시 MCP

KB부동산 생성 기능과 외부 게시 기능은 독립 MCP로 분리되어 있습니다.

- `content-package-core`: 부동산, 여행, 상품 등 어떤 콘텐츠든 동일한 패키지로 저장
- `instagram-content-publisher`: 게시 계획, 사용자 승인, 실제 Instagram 게시
- 기존 `kb-realestate`: KB 데이터와 보고서 생성만 담당

설계와 등록 예시는 [MCP 모듈형 콘텐츠·게시 아키텍처](./docs/MCP_MODULAR_ARCHITECTURE.md)를 확인하세요.

```text
KB부동산 도구 사용법 보여줘.
```

```text
KB부동산 최신 작성 패키지 만들어줘. 빠르게 실행하고 실거래는 생략해.
```

```text
이번 주 KB부동산 블로그 글 쓸 자료 만들어줘.
```

```text
최신 작성 패키지 읽고 네이버 블로그 글로 작성해줘.
```

```text
최신 뉴스 보여줘.
최신 실거래 내역 보여줘.
생성 결과 품질 검사해줘.
에이전트 상태 보여줘.
```

LLM이 도구를 잘 못 고르면 아래처럼 라우터 도구를 직접 지목합니다.

```text
kb-realestate의 kb_realestate_assistant를 사용해서 "KB부동산 최신 작성 패키지 만들어줘. 빠르게 실행하고 실거래는 생략해." 요청을 처리해줘.
```

생성된 패키지를 직접 읽게 할 때:

```text
kb-realestate의 get_latest_package로 최신 작성 패키지를 읽고, 네이버 블로그용 보고서와 텔레그램 요약문을 작성해줘.
```

### ChatGPT 웹에서 쓰고 싶을 때

ChatGPT 웹은 로컬 `stdio` MCP 서버를 직접 등록하는 방식이 아닙니다. ChatGPT 웹에 붙이려면 다음 단계가 필요합니다.

- Secure MCP Tunnel로 이 로컬 MCP 서버를 ChatGPT가 접근 가능한 MCP 엔드포인트로 연결
- 또는 Railway/서버에 원격 MCP 서버로 배포
- ChatGPT 플랜/워크스페이스에서 custom MCP app 또는 developer mode 사용 가능 여부 확인

이 프로젝트에는 터널 실행용 보조 스크립트가 있습니다.

필요한 준비물:

- `tunnel-client` 바이너리
- OpenAI Platform tunnel settings에서 만든 `tunnel_id`
- Tunnels Read + Use 권한이 있는 `CONTROL_PLANE_API_KEY`

환경변수 설정:

```bash
export CONTROL_PLANE_API_KEY="sk-..."
export OPENAI_MCP_TUNNEL_ID="tunnel_..."
export OPENAI_MCP_TUNNEL_PROFILE="kb-realestate-local"
```

최초 1회 프로필 생성 및 실행:

```bash
make mcp-tunnel-init
```

다음부터 실행:

```bash
make mcp-tunnel
```

`make mcp-tunnel`을 켜둔 상태에서 ChatGPT의 developer-mode app 생성 화면에서 Connection을 Tunnel로 선택하고 해당 tunnel을 고릅니다.

관련 OpenAI 문서:

- https://help.openai.com/en/articles/12584461-developer-mode-and-mcp-apps-in-chatgpt
- https://help.openai.com/en/articles/11487775-connectors-in-chatgpt
- https://developers.openai.com/api/docs/guides/secure-mcp-tunnels

## 10. 로컬 API 호출 예시

웹 서버가 켜져 있을 때 dry-run을 시작합니다.

```bash
curl -X POST http://127.0.0.1:5050/run/start \
  -H 'Content-Type: application/json' \
  -d '{
    "send": false,
    "run_mode": "full",
    "news_days": 1,
    "news_max_articles": 5,
    "transaction_limit": 2,
    "skip_transactions": true,
    "output_mode": "both",
    "llm_telegram_report": false,
    "llm_instagram_caption": false,
    "llm_card_news_script": false
  }'
```

응답의 `run_id`로 상태를 확인합니다.

```bash
curl http://127.0.0.1:5050/run/status/RUN_ID
```

## 11. 예약 실행

로컬에서 스케줄러까지 켜려면:

```bash
make web-scheduler
```

배포 환경에서는 `ENABLE_SCHEDULER=1`이면 예약 실행이 켜집니다.

현재 예약 기본값:

```text
매주 금요일 09:00 KST
```

## 12. 자주 하는 실수

- `make web`을 켜지 않고 브라우저 접속부터 함: 먼저 서버를 실행해야 합니다.
- LLM 체크박스를 켜고 실행함: API 비용이 발생할 수 있습니다.
- `make send`를 먼저 실행함: 실제 발송 전에 `make dry-run`으로 결과를 확인합니다.
- `localhost:5000`이 안 열림: 이 프로젝트는 `make web` 기준 `5050`을 씁니다.
- Claude/GPT 웹이 localhost를 호출할 거라고 기대함: 일반 웹 채팅은 로컬 서버에 접근하지 못합니다.
- MCP 클라이언트에 `make web` 주소를 넣으려 함: 이번 개인용 MCP 방식은 `mcp_server.py` 실행 커맨드를 등록합니다.

## 13. 내가 제일 자주 쓰는 루틴

보고서 작성 패키지만 만들기:

```bash
cd /Users/dave/Project/kb-realestate-weekly-automation
make dry-run
open reports/llm_package.md
```

웹에서 확인하면서 만들기:

```bash
cd /Users/dave/Project/kb-realestate-weekly-automation
make web
```

접속:

```text
http://127.0.0.1:5050
```

LLM에서 직접 호출하기:

```bash
cd /Users/dave/Project/kb-realestate-weekly-automation
make mcp-test-run
```

MCP 클라이언트에 `mcp_server.py`를 등록한 뒤 `generate_authoring_package` 또는 `get_latest_package`를 호출합니다.

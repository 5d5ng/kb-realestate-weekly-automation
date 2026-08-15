# KB부동산 MCP 에이전트 구조

## 핵심 원칙

사용자는 `kb-realestate` MCP 하나만 등록한다. 내부에서는 디렉터가 요청을 해석하고 책임별 에이전트에 위임한다.
각 에이전트는 LLM API를 호출하는 별도 AI가 아니라, 독립적으로 테스트 가능한 결정론적 Python 모듈이다.
따라서 에이전트 수가 늘어도 OpenAI·Anthropic API 비용이 자동으로 늘지 않는다.

```text
LLM 클라이언트
      |
      v
kb_realestate_assistant
      |
      v
DirectorAgent
      |
      +-- DataAgent -------- KB 분석 / data_snapshot
      +-- NewsAgent -------- 뉴스 수집 / 최신 뉴스
      +-- TransactionAgent - 캐시 / 실거래
      +-- AuthoringAgent --- 작성 패키지 / 보고서
      +-- QualityAgent ----- 파일·구조 검수
      +-- OpsAgent --------- 상태 / 산출물 목록
      +-- PublishingAgent -- 발송 경계, MCP 기본 경로 비활성
```

## 파일 책임

| 모듈 | 책임 |
|---|---|
| `kb_agents/director.py` | 한국어 요청 분류와 에이전트 위임 |
| `kb_agents/data.py` | KB 분석과 데이터 스냅샷 |
| `kb_agents/news.py` | 뉴스 수집과 최신 뉴스 조회 |
| `kb_agents/transactions.py` | 실거래 캐시·수집·조회 |
| `kb_agents/authoring.py` | 작성 패키지와 보고서 생성·조회 |
| `kb_agents/quality.py` | 필수 프롬프트와 최신 산출물 검수 |
| `kb_agents/publishing.py` | 실제 발송 어댑터 경계 |
| `kb_agents/ops.py` | 에이전트 상태와 산출물 목록 |
| `kb_agents/registry.py` | 에이전트 조립과 MCP 도구 등록 |
| `kb_agents/runtime.py` | MCP용 무발송·무LLM 안전 실행 경계 |

`mcp_server.py`는 에이전트 레지스트리를 stdio MCP에 연결하는 진입점만 담당한다.
`scheduler.py`도 같은 Data → Transaction → News → Authoring → Quality → Publishing 에이전트를 사용한다.

## 공개 도구 호환성

기존 도구는 그대로 유지된다.

- `kb_help`
- `kb_realestate_assistant`
- `generate_authoring_package`
- `generate_weekly_report`
- `get_latest_package`
- `get_latest_weekly_report`
- `get_data_snapshot`
- `list_artifacts`

책임 분리 후 추가된 조회·검수 도구:

- `get_latest_news`
- `get_latest_transactions`
- `check_latest_artifacts`
- `get_agent_status`

## 안전 경계

- MCP 보고서 생성은 항상 `send=False`
- 프로젝트 LLM API는 항상 비활성화
- 기본값은 `skip_transactions=True`
- `PublishingAgent`는 스케줄러가 명시적으로 발송 모드로 실행할 때만 사용
- Instagram 실제 게시는 별도 Publisher MCP의 승인 절차를 사용

## 검증

```bash
make test
make mcp-test
make mcp-test-run
```

`make mcp-test-run`은 네트워크 데이터 수집과 작성 패키지 생성까지 수행하지만 LLM API 호출과 외부 발송은 하지 않는다.

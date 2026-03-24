from __future__ import annotations

import json
import os
import threading
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from flask import Flask, jsonify, render_template_string, request, send_file

from db_backend import get_database_backend_snapshot
from news import get_news_config_snapshot
from reporters.common import get_generation_plan, get_llm_config_snapshot
from scheduler import init_scheduler, run_news_only_pipeline, run_pipeline
from sender import (
    SEND_INSTAGRAM_ENABLED,
    SEND_SMS_ENABLED,
    SEND_TELEGRAM_ENABLED,
    get_delivery_config_snapshot,
    send_telegram,
)
from valuation_web import valuation_bp

app = Flask(__name__)
app.register_blueprint(valuation_bp)
BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / "reports" / "exports"
RUN_STATE_LOCK = threading.Lock()
RUN_STATE: dict[str, dict[str, Any]] = {}
MAX_RUN_HISTORY = 20
RUN_SEQUENCE = 0
WEB_LLM_MODEL_PRESETS = {
    "telegram_report": [
        {"id": "default", "label": "기본값 사용", "provider": None, "model": None, "max_tokens": None},
        {"id": "openai_gpt5mini", "label": "OpenAI GPT-5 mini", "provider": "openai", "model": "gpt-5-mini", "max_tokens": 4000},
        {
            "id": "gemini_flash_lite",
            "label": "Gemini 2.5 Flash-Lite",
            "provider": "gemini",
            "model": "gemini-2.5-flash-lite",
            "max_tokens": 5000,
        },
    ],
    "instagram_caption": [
        {"id": "default", "label": "기본값 사용", "provider": None, "model": None, "max_tokens": None},
        {"id": "gemini_flash_lite", "label": "Gemini 2.5 Flash-Lite", "provider": "gemini", "model": "gemini-2.5-flash-lite", "max_tokens": 700},
        {"id": "openai_gpt5mini", "label": "OpenAI GPT-5 mini", "provider": "openai", "model": "gpt-5-mini", "max_tokens": 900},
    ],
    "card_news_script": [
        {"id": "default", "label": "기본값 사용", "provider": None, "model": None, "max_tokens": None},
        {"id": "openai_gpt5mini", "label": "OpenAI GPT-5 mini", "provider": "openai", "model": "gpt-5-mini", "max_tokens": 1200},
        {"id": "gemini_flash_lite", "label": "Gemini 2.5 Flash-Lite", "provider": "gemini", "model": "gemini-2.5-flash-lite", "max_tokens": 1200},
    ],
}
WEB_LLM_MODEL_PRESET_INDEX = {
    task_name: {item["id"]: item for item in presets}
    for task_name, presets in WEB_LLM_MODEL_PRESETS.items()
}
WEB_LLM_TASKS = [
    {
        "name": "telegram_report",
        "label": "텔레그램 리포트",
        "description": "주간 리포트 문안을 LLM으로 다듬습니다.",
        "model_options": WEB_LLM_MODEL_PRESETS["telegram_report"],
    },
    {
        "name": "instagram_caption",
        "label": "인스타 캡션",
        "description": "인스타 캡션 문안을 LLM으로 생성합니다.",
        "model_options": WEB_LLM_MODEL_PRESETS["instagram_caption"],
    },
    {
        "name": "card_news_script",
        "label": "카드뉴스 스크립트",
        "description": "카드뉴스용 슬라이드 문안을 LLM으로 생성합니다.",
        "model_options": WEB_LLM_MODEL_PRESETS["card_news_script"],
    },
]

INDEX_TEMPLATE = """
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KB부동산 자동화 로컬 실행기</title>
  <style>
    :root {
      --bg: #f5f1e8;
      --card: #fffdf8;
      --ink: #1e1e1e;
      --muted: #6d665c;
      --line: #d7cdbb;
      --accent: #0f766e;
      --accent-soft: #d8f3ef;
      --warn: #7c2d12;
      --warn-soft: #ffedd5;
    }
    body {
      margin: 0;
      font-family: "Pretendard", "Apple SD Gothic Neo", sans-serif;
      background: linear-gradient(180deg, #f8f5ed 0%, #efe5d2 100%);
      color: var(--ink);
    }
    .wrap {
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    .hero {
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: var(--card);
      box-shadow: 0 14px 40px rgba(80, 62, 32, 0.08);
    }
    .hero-actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 18px;
    }
    .hero-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 46px;
      padding: 0 16px;
      border-radius: 12px;
      background: var(--accent);
      color: white;
      text-decoration: none;
      font-size: 14px;
      font-weight: 700;
    }
    .hero-link.secondary {
      background: #334155;
    }
    h1, h2 {
      margin: 0 0 12px;
    }
    p {
      margin: 0;
      line-height: 1.6;
      color: var(--muted);
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px;
      margin-top: 22px;
    }
    .service-strip {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 18px;
      margin-top: 22px;
    }
    .service-link-card {
      display: grid;
      gap: 12px;
      padding: 20px;
      border: 1px solid var(--line);
      border-radius: 20px;
      background: rgba(255, 253, 248, 0.92);
      box-shadow: 0 10px 24px rgba(80, 62, 32, 0.05);
    }
    .service-link-card a {
      width: fit-content;
      text-decoration: none;
    }
    .card {
      padding: 20px;
      border: 1px solid var(--line);
      border-radius: 20px;
      background: var(--card);
      box-shadow: 0 10px 24px rgba(80, 62, 32, 0.06);
    }
    .badge {
      display: inline-block;
      margin-bottom: 10px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 13px;
      font-weight: 700;
    }
    .badge.warn {
      background: var(--warn-soft);
      color: var(--warn);
    }
    label {
      display: block;
      margin-top: 14px;
      margin-bottom: 6px;
      font-size: 14px;
      font-weight: 700;
    }
    input {
      width: 100%;
      box-sizing: border-box;
      padding: 12px 13px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
      font-size: 14px;
    }
    select {
      width: 100%;
      box-sizing: border-box;
      padding: 11px 13px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
      font-size: 14px;
    }
    .channel-list {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .channel-item {
      display: flex;
      align-items: flex-start;
      gap: 10px;
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
    }
    .channel-item input[type="checkbox"] {
      width: auto;
      margin-top: 2px;
      accent-color: var(--accent);
    }
    .channel-copy {
      display: grid;
      gap: 4px;
      flex: 1;
    }
    .channel-title {
      font-size: 14px;
      font-weight: 700;
      color: var(--ink);
    }
    .channel-desc {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.5;
    }
    .llm-model-picker {
      display: grid;
      gap: 6px;
      margin-top: 8px;
    }
    .llm-model-picker label {
      margin: 0;
      font-size: 12px;
      font-weight: 700;
      color: var(--muted);
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 18px;
    }
    button {
      border: 0;
      border-radius: 12px;
      padding: 12px 16px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      background: var(--accent);
      color: white;
    }
    button.secondary {
      background: #374151;
    }
    code {
      font-family: "SFMono-Regular", "Menlo", monospace;
      background: #f5f5f5;
      padding: 2px 6px;
      border-radius: 6px;
    }
    pre {
      margin: 0;
      padding: 18px;
      border-radius: 18px;
      background: #111827;
      color: #f9fafb;
      overflow: auto;
      line-height: 1.5;
      font-size: 13px;
      white-space: pre-wrap;
      word-break: break-word;
      overflow-wrap: anywhere;
    }
    .status-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.8fr);
      gap: 18px;
      margin-top: 22px;
      align-items: start;
    }
    .status-overview {
      grid-column: 1 / -1;
    }
    .status-card {
      display: grid;
      gap: 12px;
    }
    .status-banner {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 16px 18px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: #fff;
    }
    .status-banner.idle {
      background: #f8fafc;
      border-color: #cbd5e1;
    }
    .status-banner.queued,
    .status-banner.running {
      background: #ecfeff;
      border-color: #99f6e4;
    }
    .status-banner.completed {
      background: #dcfce7;
      border-color: #86efac;
    }
    .status-banner.failed {
      background: #fee2e2;
      border-color: #fca5a5;
    }
    .status-banner.skipped {
      background: #ffedd5;
      border-color: #fdba74;
    }
    .status-copy {
      display: grid;
      gap: 4px;
      min-width: 0;
    }
    .status-title {
      font-size: 18px;
      font-weight: 800;
      color: var(--ink);
    }
    .status-subtitle {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.6;
    }
    .status-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 96px;
      min-height: 44px;
      padding: 0 14px;
      border-radius: 999px;
      background: rgba(15, 118, 110, 0.12);
      color: var(--accent);
      font-size: 14px;
      font-weight: 800;
      white-space: nowrap;
    }
    .status-banner.completed .status-badge {
      background: rgba(22, 101, 52, 0.12);
      color: #166534;
    }
    .status-banner.failed .status-badge {
      background: rgba(153, 27, 27, 0.12);
      color: #991b1b;
    }
    .status-banner.skipped .status-badge {
      background: rgba(154, 52, 18, 0.12);
      color: #9a3412;
    }
    .meta-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
    }
    .meta-item {
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
    }
    .meta-label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .meta-value {
      font-size: 15px;
      font-weight: 700;
      word-break: break-word;
    }
    .stage-board {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(132px, 1fr));
      gap: 10px;
      margin-top: 12px;
    }
    .stage-pill {
      display: grid;
      gap: 8px;
      padding: 14px 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--muted);
      font-size: 13px;
      font-weight: 700;
      min-height: 86px;
    }
    .stage-name {
      font-size: 14px;
      font-weight: 800;
      color: var(--ink);
    }
    .stage-meta {
      font-size: 12px;
      color: var(--muted);
    }
    .stage-pill.running {
      border-color: var(--accent);
      background: var(--accent-soft);
      color: var(--accent);
    }
    .stage-pill.completed {
      border-color: #15803d;
      background: #dcfce7;
      color: #166534;
    }
    .stage-pill.failed {
      border-color: #b91c1c;
      background: #fee2e2;
      color: #991b1b;
    }
    .stage-pill.skipped {
      border-color: #92400e;
      background: #ffedd5;
      color: #9a3412;
    }
    .download-row {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }
    .download-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 44px;
      padding: 0 14px;
      border-radius: 12px;
      background: #1d4ed8;
      color: white;
      text-decoration: none;
      font-size: 14px;
      font-weight: 700;
    }
    .hint {
      margin-top: 16px;
      font-size: 13px;
      color: var(--muted);
    }
    .preflight-card {
      margin-top: 22px;
    }
    .json-block {
      min-height: 240px;
    }
    .run-attach {
      display: grid;
      gap: 14px;
    }
    .attach-row {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    .attach-row input {
      flex: 1 1 220px;
      min-width: 0;
    }
    .recent-run-list {
      display: grid;
      gap: 10px;
      max-height: 360px;
      overflow: auto;
      padding-right: 4px;
    }
    .recent-run-empty {
      padding: 14px 16px;
      border: 1px dashed var(--line);
      border-radius: 14px;
      background: rgba(255,255,255,0.7);
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }
    .recent-run-item {
      display: grid;
      gap: 10px;
      padding: 14px 16px;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: #fff;
    }
    .recent-run-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }
    .recent-run-id {
      font-family: "SFMono-Regular", "Menlo", monospace;
      font-size: 12px;
      color: #334155;
      word-break: break-all;
    }
    .recent-run-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 28px;
      padding: 0 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 800;
      background: #e2e8f0;
      color: #334155;
      white-space: nowrap;
    }
    .recent-run-badge.running,
    .recent-run-badge.queued {
      background: #ccfbf1;
      color: #0f766e;
    }
    .recent-run-badge.completed {
      background: #dcfce7;
      color: #166534;
    }
    .recent-run-badge.failed {
      background: #fee2e2;
      color: #991b1b;
    }
    .recent-run-badge.skipped {
      background: #ffedd5;
      color: #9a3412;
    }
    .recent-run-meta {
      display: grid;
      gap: 4px;
      font-size: 13px;
      color: var(--muted);
      line-height: 1.55;
    }
    .recent-run-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .mini-button {
      border: 0;
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
      background: #1d4ed8;
      color: white;
    }
    .mini-button.secondary {
      background: #475569;
    }
    .log-list {
      display: grid;
      gap: 10px;
      max-height: 760px;
      overflow: auto;
      padding-right: 4px;
    }
    .log-entry,
    .log-empty {
      padding: 14px 16px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: #fff;
    }
    .log-empty {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }
    .log-entry.running {
      border-color: #99f6e4;
      background: #f0fdfa;
    }
    .log-entry.completed {
      border-color: #86efac;
      background: #f0fdf4;
    }
    .log-entry.failed {
      border-color: #fca5a5;
      background: #fef2f2;
    }
    .log-entry.skipped {
      border-color: #fdba74;
      background: #fff7ed;
    }
    .log-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
      flex-wrap: wrap;
    }
    .log-stage {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      font-weight: 800;
      color: var(--ink);
    }
    .log-time {
      font-size: 12px;
      color: var(--muted);
      white-space: nowrap;
    }
    .log-message {
      font-size: 14px;
      line-height: 1.65;
      color: var(--ink);
      word-break: break-word;
      overflow-wrap: anywhere;
    }
    .log-extra {
      margin-top: 10px;
      padding: 10px 12px;
      border-radius: 12px;
      background: rgba(17, 24, 39, 0.06);
      color: #334155;
      font-family: "SFMono-Regular", "Menlo", monospace;
      font-size: 12px;
      line-height: 1.55;
      white-space: pre-wrap;
      word-break: break-word;
      overflow-wrap: anywhere;
    }
    .panel-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 10px;
    }
    .panel-copy {
      display: grid;
      gap: 6px;
    }
    .panel-title {
      font-size: 22px;
      font-weight: 800;
      color: var(--ink);
    }
    .panel-text {
      font-size: 14px;
      color: var(--muted);
      line-height: 1.6;
    }
    .llm-list, .prompt-actions {
      display: grid;
      gap: 12px;
      margin-top: 12px;
    }
    .prompt-actions {
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }
    .prompt-button {
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 14px;
      padding: 12px 14px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      text-align: left;
    }
    .prompt-button:hover {
      border-color: var(--primary);
      color: var(--primary);
    }
    .prompt-button.active {
      border-color: var(--primary);
      background: rgba(33, 133, 124, 0.08);
      color: var(--primary);
    }
    .prompt-empty {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }
    .copy-status {
      font-size: 13px;
      color: var(--muted);
      min-height: 20px;
    }
    @media (max-width: 980px) {
      .status-grid {
        grid-template-columns: 1fr;
      }
      .status-overview {
        grid-column: auto;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>KB부동산 자동화 로컬 실행기</h1>
      <p>
        브라우저에서 바로 <code>dry-run</code> 또는 실제 발송 테스트를 실행할 수 있습니다.
        처음에는 dry-run 으로 돌리고, 결과가 괜찮으면 실제 발송을 권장합니다.
      </p>
      <p style="margin-top: 10px;">
        단지 상대가치 분석 서비스는 <code>/valuation</code> 에서 별도 화면으로 사용할 수 있습니다.
      </p>
      <div class="hero-actions">
        <a class="hero-link" href="/">주간 자동화 실행기</a>
        <a class="hero-link secondary" href="/valuation">적정 시세 분석 열기</a>
      </div>
    </section>

    <section class="service-strip">
      <div class="service-link-card">
        <span class="badge">메인 서비스</span>
        <h2>주간 자동화 실행</h2>
        <p>KB 분석, 실거래, 뉴스, 콘텐츠 생성, 발송까지 현재 운영 중인 메인 흐름입니다.</p>
        <a class="hero-link" href="/">이 화면에서 실행</a>
      </div>
      <div class="service-link-card">
        <span class="badge warn">신규 기능</span>
        <h2>단지 적정 시세 분석</h2>
        <p>기준 단지와 비교 단지를 골라 현재 비율, 10년 평균 비율, 상승여력을 따로 확인합니다.</p>
        <a class="hero-link secondary" href="/valuation">분석 화면 열기</a>
      </div>
    </section>

    <section class="card preflight-card">
      <span class="badge">사전 점검</span>
      <h2>실행 전 API / 환경변수 확인</h2>
      <p>현재 Railway 프로세스가 실제로 읽고 있는 뉴스, LLM, 발송 채널 환경설정을 먼저 확인합니다.</p>
      <div class="actions">
        <button id="preflight-button" type="button" class="secondary">사전 점검 실행</button>
        <button id="telegram-test-button" type="button">텔레그램 단독 테스트</button>
      </div>
      <pre id="preflight-result" class="json-block">아직 사전 점검 결과가 없습니다.</pre>
    </section>

    <section class="grid">
      <form class="card" id="dry-run-form">
        <span class="badge">추천</span>
        <h2>Dry Run</h2>
        <p>발송 없이 분석, 실거래, 뉴스, 콘텐츠 생성, 프롬프트 파일 저장까지만 점검합니다.</p>

        <label for="dry-news-days">뉴스 수집 기간</label>
        <input id="dry-news-days" name="news_days" type="number" min="1" value="1">

        <label for="dry-news-max">최대 뉴스 수</label>
        <input id="dry-news-max" name="news_max_articles" type="number" min="1" value="5">

        <label for="dry-transaction-limit">실거래 건수</label>
        <input id="dry-transaction-limit" name="transaction_limit" type="number" min="1" value="2">

        <label>LLM 사용 플랫폼</label>
        <div class="llm-list">
          {% for task in web_llm_tasks %}
          <label class="channel-item">
            <input
              type="checkbox"
              name="llm_{{ task.name }}"
              {% if default_llm_tasks[task.name] %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">{{ task.label }}</span>
              <span class="channel-desc">{{ task.description }}</span>
              <span class="llm-model-picker">
                <label for="dry-llm-model-{{ task.name }}">사용 모델</label>
                <select id="dry-llm-model-{{ task.name }}" name="llm_model_{{ task.name }}">
                  {% for option in task.model_options %}
                  <option
                    value="{{ option.id }}"
                    {% if default_llm_models[task.name] == option.id %}selected{% endif %}
                  >
                    {{ option.label }}
                  </option>
                  {% endfor %}
                </select>
              </span>
            </span>
          </label>
          {% endfor %}
        </div>

        <div class="actions">
          <button type="submit">Dry Run 실행</button>
        </div>
      </form>

      <form class="card" id="send-form">
        <span class="badge warn">주의</span>
        <h2>실제 발송</h2>
        <p>이번 실행에서 사용할 플랫폼을 직접 고를 수 있습니다. 체크한 채널만 발송됩니다.</p>

        <label for="send-news-days">뉴스 수집 기간</label>
        <input id="send-news-days" name="news_days" type="number" min="1" value="1">

        <label for="send-news-max">최대 뉴스 수</label>
        <input id="send-news-max" name="news_max_articles" type="number" min="1" value="5">

        <label for="send-transaction-limit">실거래 건수</label>
        <input id="send-transaction-limit" name="transaction_limit" type="number" min="1" value="2">

        <label>LLM 사용 플랫폼</label>
        <div class="llm-list">
          {% for task in web_llm_tasks %}
          <label class="channel-item">
            <input
              type="checkbox"
              name="llm_{{ task.name }}"
              {% if default_llm_tasks[task.name] %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">{{ task.label }}</span>
              <span class="channel-desc">{{ task.description }}</span>
              <span class="llm-model-picker">
                <label for="send-llm-model-{{ task.name }}">사용 모델</label>
                <select id="send-llm-model-{{ task.name }}" name="llm_model_{{ task.name }}">
                  {% for option in task.model_options %}
                  <option
                    value="{{ option.id }}"
                    {% if default_llm_models[task.name] == option.id %}selected{% endif %}
                  >
                    {{ option.label }}
                  </option>
                  {% endfor %}
                </select>
              </span>
            </span>
          </label>
          {% endfor %}
        </div>

        <label>발송 플랫폼</label>
        <div class="channel-list">
          <label class="channel-item">
            <input
              type="checkbox"
              name="send_telegram"
              {% if default_channels.telegram %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">텔레그램</span>
              <span class="channel-desc">운영 기본 채널입니다. 텍스트 리포트를 바로 전송합니다.</span>
            </span>
          </label>

          <label class="channel-item">
            <input
              type="checkbox"
              name="send_sms"
              {% if default_channels.sms %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">SMS / 알림 메시지</span>
              <span class="channel-desc">솔라피를 통해 발송합니다. 비용이 발생할 수 있습니다.</span>
            </span>
          </label>

          <label class="channel-item">
            <input
              type="checkbox"
              name="send_instagram"
              {% if default_channels.instagram %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">인스타그램</span>
              <span class="channel-desc">현재 업로드 기능은 보류 상태입니다. 켜도 스킵될 수 있습니다.</span>
            </span>
          </label>
        </div>

        <div class="actions">
          <button type="submit" class="secondary">실제 발송 실행</button>
        </div>
      </form>

      <form class="card" id="news-only-form">
        <span class="badge">빠른 발송</span>
        <h2>뉴스만 발송</h2>
        <p>실거래와 분석을 건너뛰고 뉴스만 수집해서 바로 발송합니다. 실거래 단계가 오래 걸릴 때 빠르게 쓰기 좋습니다.</p>

        <label for="news-only-days">뉴스 수집 기간</label>
        <input id="news-only-days" name="news_days" type="number" min="1" value="1">

        <label for="news-only-max">최대 뉴스 수</label>
        <input id="news-only-max" name="news_max_articles" type="number" min="1" value="5">

        <label>LLM 사용</label>
        <div class="llm-list">
          <label class="channel-item">
            <input
              type="checkbox"
              name="llm_telegram_report"
              {% if default_llm_tasks['telegram_report'] %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">텔레그램 리포트</span>
              <span class="channel-desc">뉴스 전용 텔레그램 브리핑 문안을 LLM으로 다듬습니다.</span>
              <span class="llm-model-picker">
                <label for="news-only-llm-model-telegram-report">사용 모델</label>
                <select id="news-only-llm-model-telegram-report" name="llm_model_telegram_report">
                  {% for option in telegram_model_options %}
                  <option
                    value="{{ option.id }}"
                    {% if default_llm_models['telegram_report'] == option.id %}selected{% endif %}
                  >
                    {{ option.label }}
                  </option>
                  {% endfor %}
                </select>
              </span>
            </span>
          </label>
        </div>

        <label>발송 플랫폼</label>
        <div class="channel-list">
          <label class="channel-item">
            <input
              type="checkbox"
              name="send_telegram"
              {% if default_channels.telegram %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">텔레그램</span>
              <span class="channel-desc">뉴스 브리핑을 텔레그램으로 바로 전송합니다.</span>
            </span>
          </label>

          <label class="channel-item">
            <input
              type="checkbox"
              name="send_sms"
              {% if default_channels.sms %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">SMS / 알림 메시지</span>
              <span class="channel-desc">뉴스 브리핑 문안을 솔라피로 발송합니다.</span>
            </span>
          </label>
        </div>

        <div class="actions">
          <button type="submit" class="secondary">뉴스만 발송 실행</button>
        </div>
      </form>
    </section>

    <p class="hint">
      프롬프트 파일은 <code>reports/prompts/</code> 아래에 저장됩니다.
      수동 API 호출은 <code>POST /run</code> 으로도 가능합니다.
    </p>

    <section class="status-grid">
      <div class="card status-card status-overview">
        <div class="panel-head">
          <div class="panel-copy">
            <div class="panel-title">실행 상태</div>
            <div class="panel-text">현재 실행 단계, 전체 진행 상황, 생성 파일 다운로드를 한눈에 확인합니다.</div>
          </div>
        </div>
        <div id="status-banner" class="status-banner idle">
          <div class="status-copy">
            <div class="status-title" id="status-title">대기 중</div>
            <div class="status-subtitle" id="status-subtitle">아직 실행된 작업이 없습니다. Dry Run 또는 실제 발송을 시작해 주세요.</div>
          </div>
          <div class="status-badge" id="status-badge">대기</div>
        </div>
        <div class="meta-grid">
          <div class="meta-item">
            <span class="meta-label">상태</span>
            <span class="meta-value" id="run-status">대기</span>
          </div>
          <div class="meta-item">
            <span class="meta-label">현재 단계</span>
            <span class="meta-value" id="run-stage">-</span>
          </div>
          <div class="meta-item">
            <span class="meta-label">Run ID</span>
            <span class="meta-value" id="run-id">-</span>
          </div>
          <div class="meta-item">
            <span class="meta-label">시작 시각</span>
            <span class="meta-value" id="run-started-at">-</span>
          </div>
        </div>
        <div class="stage-board" id="stage-board"></div>
        <div class="download-row">
          <a id="download-artifacts" class="download-link" href="#" hidden>생성 파일 ZIP 다운로드</a>
        </div>
      </div>
      <div class="card">
        <div class="panel-head">
          <div class="panel-copy">
            <div class="panel-title">진행 로그</div>
            <div class="panel-text">가장 최근 로그까지 줄바꿈된 카드 형태로 표시합니다.</div>
          </div>
        </div>
        <div id="progress" class="log-list">
          <div class="log-empty">아직 실행 로그가 없습니다.</div>
        </div>
      </div>
      <div class="card">
        <div class="panel-head">
          <div class="panel-copy">
            <div class="panel-title">최종 결과</div>
            <div class="panel-text">실행이 끝나면 최종 응답 JSON을 보기 좋게 줄바꿈해서 보여줍니다.</div>
          </div>
        </div>
        <pre id="result" class="json-block">아직 실행 결과가 없습니다.</pre>
      </div>
      <div class="card">
        <div class="panel-head">
          <div class="panel-copy">
            <div class="panel-title">LLM 프롬프트</div>
            <div class="panel-text">실행 중 생성된 프롬프트를 바로 열고 복사할 수 있습니다.</div>
          </div>
        </div>
        <div id="prompt-actions" class="prompt-actions">
          <div class="prompt-empty">아직 확인할 프롬프트가 없습니다.</div>
        </div>
        <div class="actions">
          <button id="copy-current-prompt" type="button" class="secondary">현재 프롬프트 복사</button>
        </div>
        <div id="prompt-copy-status" class="copy-status"></div>
        <pre id="prompt-viewer" class="json-block">아직 실행된 프롬프트가 없습니다.</pre>
      </div>
      <div class="card">
        <div class="panel-head">
          <div class="panel-copy">
            <div class="panel-title">기존 실행 다시 보기</div>
            <div class="panel-text">이미 돌고 있는 수동 실행에 다시 붙거나, 최근 실행 기록을 다시 열어 상태를 확인합니다.</div>
          </div>
        </div>
        <div class="run-attach">
          <div class="attach-row">
            <input id="attach-run-id" type="text" placeholder="run_id를 입력하세요">
            <button id="attach-run-button" type="button" class="secondary">run_id로 다시 보기</button>
            <button id="refresh-runs-button" type="button" class="secondary">최근 실행 새로고침</button>
          </div>
          <div id="recent-run-list" class="recent-run-list">
            <div class="recent-run-empty">아직 기록된 실행이 없습니다.</div>
          </div>
        </div>
      </div>
    </section>
  </div>

  <script>
    let activeRunId = null;
    let activePoller = null;
    const ACTIVE_RUN_STORAGE_KEY = "kb_active_run_id";
    const STAGES = ["queued", "lock", "analysis", "cache", "transactions", "news", "contents", "send", "done"];
    const PROMPT_LABELS = {
      telegram_report: "텔레그램 리포트",
      instagram_caption: "인스타 캡션",
      card_news_script: "카드뉴스 스크립트",
      naver_blog_post: "네이버 블로그 초안",
    };
    const STAGE_LABELS = {
      queued: "대기",
      lock: "락",
      analysis: "분석",
      cache: "캐시 갱신",
      transactions: "실거래",
      news: "뉴스",
      contents: "콘텐츠",
      send: "발송",
      done: "완료",
    };
    let currentPromptTask = null;
    let currentPromptContent = "";
    const STATUS_COPY = {
      idle: {
        title: "대기 중",
        subtitle: "아직 실행된 작업이 없습니다. Dry Run 또는 실제 발송을 시작해 주세요.",
        badge: "대기",
      },
      queued: {
        title: "실행 대기",
        subtitle: "요청은 등록됐고, 백그라운드 작업을 시작할 준비를 하고 있습니다.",
        badge: "대기",
      },
      running: {
        title: "실행 중",
        subtitle: "현재 선택한 단계들을 순서대로 처리하고 있습니다.",
        badge: "진행 중",
      },
      completed: {
        title: "실행 완료",
        subtitle: "전체 작업이 끝났습니다. 결과와 생성 파일을 바로 확인할 수 있습니다.",
        badge: "완료",
      },
      failed: {
        title: "실행 실패",
        subtitle: "중간 단계에서 오류가 발생했습니다. 아래 로그와 결과를 먼저 확인해 주세요.",
        badge: "실패",
      },
      skipped: {
        title: "실행 스킵",
        subtitle: "우선순위나 락 조건 때문에 이번 실행은 건너뛰었습니다.",
        badge: "스킵",
      },
    };

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function updateRunMeta(payload) {
      const status = payload.status || "idle";
      const currentStage = payload.current_stage || "-";
      document.getElementById("run-status").textContent = status === "idle" ? "대기" : status;
      document.getElementById("run-stage").textContent = currentStage === "-" ? "-" : (STAGE_LABELS[currentStage] || currentStage);
      document.getElementById("run-id").textContent = payload.run_id || "-";
      document.getElementById("run-started-at").textContent = payload.started_at || "-";

      const banner = document.getElementById("status-banner");
      const title = document.getElementById("status-title");
      const subtitle = document.getElementById("status-subtitle");
      const badge = document.getElementById("status-badge");
      const copy = STATUS_COPY[status] || STATUS_COPY.idle;
      banner.className = `status-banner ${status}`;
      title.textContent = copy.title;
      subtitle.textContent = currentStage && currentStage !== "-" && STAGE_LABELS[currentStage]
        ? `${copy.subtitle} 현재 단계: ${STAGE_LABELS[currentStage]}`
        : copy.subtitle;
      badge.textContent = copy.badge;

      const downloadLink = document.getElementById("download-artifacts");
      if (payload.artifact_download_url && (payload.status === "completed" || payload.status === "failed" || payload.status === "skipped")) {
        downloadLink.href = payload.artifact_download_url;
        downloadLink.hidden = false;
      } else {
        downloadLink.hidden = true;
        downloadLink.removeAttribute("href");
      }
    }

    function renderStageBoard(payload) {
      const board = document.getElementById("stage-board");
      const logs = payload.logs || [];
      const stageStatuses = {};

      for (const log of logs) {
        stageStatuses[log.stage] = log.status || "running";
      }

      if (payload.status === "failed" && payload.current_stage) {
        stageStatuses[payload.current_stage] = "failed";
      }
      if (payload.status === "skipped" && payload.current_stage) {
        stageStatuses[payload.current_stage] = "skipped";
      }
      if (payload.status === "completed") {
        stageStatuses["done"] = "completed";
      }

      board.innerHTML = STAGES.map((stage) => {
        const status = stageStatuses[stage] || "";
        const metaMap = {
          completed: "완료됨",
          running: "진행 중",
          failed: "오류 발생",
          skipped: "건너뜀",
        };
        const meta = metaMap[status] || "대기";
        return `
          <div class="stage-pill ${status}">
            <div class="stage-name">${STAGE_LABELS[stage]}</div>
            <div class="stage-meta">${meta}</div>
          </div>
        `;
      }).join("");
    }

    function buildPayload(form, send, runMode = "full") {
      const formData = new FormData(form);
      const data = Object.fromEntries(formData.entries());
      data.send = send;
      data.run_mode = runMode;

      if (send) {
        data.send_telegram = form.querySelector('input[name="send_telegram"]')?.checked || false;
        data.send_sms = form.querySelector('input[name="send_sms"]')?.checked || false;
        data.send_instagram = form.querySelector('input[name="send_instagram"]')?.checked || false;
      }

      data.llm_telegram_report = form.querySelector('input[name="llm_telegram_report"]')?.checked || false;
      data.llm_instagram_caption = form.querySelector('input[name="llm_instagram_caption"]')?.checked || false;
      data.llm_card_news_script = form.querySelector('input[name="llm_card_news_script"]')?.checked || false;
      data.llm_model_telegram_report = form.querySelector('select[name="llm_model_telegram_report"]')?.value || "default";
      data.llm_model_instagram_caption = form.querySelector('select[name="llm_model_instagram_caption"]')?.value || "default";
      data.llm_model_card_news_script = form.querySelector('select[name="llm_model_card_news_script"]')?.value || "default";

      return data;
    }

    function syncLlmModelControls() {
      document.querySelectorAll('input[name^="llm_"]').forEach((checkbox) => {
        const taskName = checkbox.name.replace("llm_", "");
        const form = checkbox.closest("form");
        const select = form?.querySelector(`select[name="llm_model_${taskName}"]`);
        if (!select) return;
        select.disabled = !checkbox.checked;
      });
    }

    function resetPromptPanel() {
      currentPromptTask = null;
      currentPromptContent = "";
      document.getElementById("prompt-actions").innerHTML = '<div class="prompt-empty">아직 확인할 프롬프트가 없습니다.</div>';
      document.getElementById("prompt-viewer").textContent = "아직 실행된 프롬프트가 없습니다.";
      document.getElementById("prompt-copy-status").textContent = "";
    }

    async function loadPrompt(runId, taskName, { copy = false } = {}) {
      const viewerEl = document.getElementById("prompt-viewer");
      const statusEl = document.getElementById("prompt-copy-status");
      viewerEl.textContent = `${PROMPT_LABELS[taskName] || taskName} 프롬프트를 불러오는 중입니다.`;
      statusEl.textContent = "";
      try {
        const response = await fetch(`/run/prompt/${runId}/${taskName}`);
        const payload = await response.json();
        if (!response.ok || !payload.success) {
          viewerEl.textContent = JSON.stringify(payload, null, 2);
          return;
        }
        currentPromptTask = taskName;
        currentPromptContent = payload.content || "";
        viewerEl.textContent = currentPromptContent || "프롬프트 내용이 비어 있습니다.";
        document.querySelectorAll(".prompt-button").forEach((button) => {
          button.classList.toggle("active", button.getAttribute("data-task-name") === taskName);
        });
        if (copy && currentPromptContent) {
          await navigator.clipboard.writeText(currentPromptContent);
          statusEl.textContent = `${PROMPT_LABELS[taskName] || taskName} 프롬프트를 복사했습니다.`;
        }
      } catch (error) {
        viewerEl.textContent = JSON.stringify({ success: false, error: String(error) }, null, 2);
      }
    }

    function renderPromptPanel(payload) {
      const actionsEl = document.getElementById("prompt-actions");
      const promptFiles = payload?.result?.prompt_files || {};
      const runId = payload?.run_id;
      const entries = Object.entries(promptFiles);
      if (!runId || !entries.length) {
        resetPromptPanel();
        return;
      }

      actionsEl.innerHTML = entries.map(([taskName, _path]) => `
        <button type="button" class="prompt-button" data-task-name="${taskName}">
          ${PROMPT_LABELS[taskName] || taskName}
        </button>
      `).join("");

      actionsEl.querySelectorAll(".prompt-button").forEach((button) => {
        button.addEventListener("click", () => {
          const taskName = button.getAttribute("data-task-name");
          loadPrompt(runId, taskName || "", { copy: true });
        });
      });

      const firstTaskName = entries[0][0];
      if (currentPromptTask !== firstTaskName) {
        loadPrompt(runId, firstTaskName);
      }
    }

    function renderProgress(payload) {
      const progressEl = document.getElementById("progress");
      const logs = payload.logs || [];
      if (!logs.length) {
        progressEl.innerHTML = '<div class="log-empty">아직 실행 로그가 없습니다.</div>';
        return;
      }

      progressEl.innerHTML = logs.map((log) => {
        const status = log.status || "running";
        const extra = log.extra
          ? `<div class="log-extra">${escapeHtml(JSON.stringify(log.extra, null, 2))}</div>`
          : "";
        return `
          <div class="log-entry ${status}">
            <div class="log-top">
              <div class="log-stage">${escapeHtml(STAGE_LABELS[log.stage] || log.stage)}</div>
              <div class="log-time">${escapeHtml(log.time || "-")}</div>
            </div>
            <div class="log-message">${escapeHtml(log.message || "")}</div>
            ${extra}
          </div>
        `;
      }).join("");
      progressEl.scrollTop = progressEl.scrollHeight;
    }

    function renderResult(payload) {
      const resultEl = document.getElementById("result");
      if (payload.result) {
        resultEl.textContent = JSON.stringify(payload.result, null, 2);
        renderPromptPanel(payload);
        return;
      }
      resetPromptPanel();
      if (payload.error) {
        resultEl.textContent = JSON.stringify(payload.error, null, 2);
        return;
      }
      resultEl.textContent = JSON.stringify(payload, null, 2);
    }

    function attachToRun(runId, { announce = true } = {}) {
      if (!runId) return;
      if (activePoller) {
        clearInterval(activePoller);
        activePoller = null;
      }
      activeRunId = runId;
      localStorage.setItem(ACTIVE_RUN_STORAGE_KEY, runId);

      if (announce) {
        resetPromptPanel();
        document.getElementById("result").textContent = JSON.stringify(
          {
            run_id: runId,
            detail: "기존 실행 상태를 다시 불러오는 중입니다.",
          },
          null,
          2
        );
      }

      pollRun(runId);
      activePoller = setInterval(() => {
        if (!activeRunId) {
          clearInterval(activePoller);
          activePoller = null;
          return;
        }
        pollRun(activeRunId);
      }, 1500);
    }

    function renderRecentRuns(payload) {
      const listEl = document.getElementById("recent-run-list");
      const runs = payload.runs || [];
      if (!runs.length) {
        listEl.innerHTML = '<div class="recent-run-empty">아직 기록된 실행이 없습니다.</div>';
        return;
      }

      listEl.innerHTML = runs.map((run) => {
        const status = escapeHtml(run.status || "unknown");
        const trigger = escapeHtml(run.trigger || "-");
        const stage = escapeHtml(STAGE_LABELS[run.current_stage] || run.current_stage || "-");
        const startedAt = escapeHtml(run.started_at || "-");
        const updatedAt = escapeHtml(run.updated_at || "-");
        const runId = escapeHtml(run.run_id || "-");
        const duration = run.duration_sec == null ? "-" : `${run.duration_sec}s`;
        return `
          <div class="recent-run-item">
            <div class="recent-run-top">
              <div class="recent-run-id">${runId}</div>
              <div class="recent-run-badge ${status}">${status}</div>
            </div>
            <div class="recent-run-meta">
              <div>trigger: ${trigger}</div>
              <div>현재 단계: ${stage}</div>
              <div>시작: ${startedAt}</div>
              <div>업데이트: ${updatedAt}</div>
              <div>소요시간: ${escapeHtml(duration)}</div>
            </div>
            <div class="recent-run-actions">
              <button type="button" class="mini-button" data-run-id="${runId}">이 실행 다시 보기</button>
            </div>
          </div>
        `;
      }).join("");

      listEl.querySelectorAll("[data-run-id]").forEach((button) => {
        button.addEventListener("click", () => {
          const runId = button.getAttribute("data-run-id");
          document.getElementById("attach-run-id").value = runId || "";
          attachToRun(runId || "");
        });
      });
    }

    async function loadRecentRuns() {
      const listEl = document.getElementById("recent-run-list");
      listEl.innerHTML = '<div class="recent-run-empty">최근 실행 목록을 불러오는 중입니다.</div>';
      try {
        const response = await fetch("/run/recent");
        const payload = await response.json();
        renderRecentRuns(payload);
      } catch (error) {
        listEl.innerHTML = `<div class="recent-run-empty">최근 실행 목록 조회에 실패했습니다: ${escapeHtml(String(error))}</div>`;
      }
    }

    async function pollRun(runId) {
      const progressEl = document.getElementById("progress");
      const resultEl = document.getElementById("result");

      try {
        const response = await fetch(`/run/status/${runId}`);
        const payload = await response.json();
        updateRunMeta(payload);
        renderStageBoard(payload);
        renderProgress(payload);

        if (payload.status === "completed" || payload.status === "failed" || payload.status === "skipped") {
          renderResult(payload);
          if (activePoller) {
            clearInterval(activePoller);
            activePoller = null;
          }
          activeRunId = null;
          localStorage.removeItem(ACTIVE_RUN_STORAGE_KEY);
          loadRecentRuns();
          return;
        }

        resultEl.textContent = JSON.stringify(
          {
            run_id: payload.run_id,
            status: payload.status,
            started_at: payload.started_at,
            current_stage: payload.current_stage,
          },
          null,
          2
        );
      } catch (error) {
        progressEl.innerHTML += `<div class="log-entry failed"><div class="log-top"><div class="log-stage">오류</div><div class="log-time">-</div></div><div class="log-message">상태 조회 실패: ${escapeHtml(String(error))}</div></div>`;
        if (activePoller) {
          clearInterval(activePoller);
          activePoller = null;
        }
        activeRunId = null;
      }
    }

    async function submitForm(form, send, runMode = "full") {
      const progressEl = document.getElementById("progress");
      const resultEl = document.getElementById("result");
      const data = buildPayload(form, send, runMode);

      if (activePoller) {
        clearInterval(activePoller);
        activePoller = null;
      }
      activeRunId = null;
      updateRunMeta({ status: "queued", current_stage: "queued", run_id: "-", started_at: "-" });
      renderStageBoard({ logs: [{ stage: "queued", status: "running" }], status: "queued" });
      progressEl.innerHTML = '<div class="log-entry running"><div class="log-top"><div class="log-stage">대기</div><div class="log-time">-</div></div><div class="log-message">실행 요청을 전송했습니다.</div></div>';
      resultEl.textContent = "백그라운드 실행을 시작하는 중입니다.";
      resetPromptPanel();

      try {
        const response = await fetch("/run/start", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(data),
        });
        const payload = await response.json();
        if (!response.ok || !payload.run_id) {
          resultEl.textContent = JSON.stringify(payload, null, 2);
          return;
        }

        activeRunId = payload.run_id;
        localStorage.setItem(ACTIVE_RUN_STORAGE_KEY, payload.run_id);
        progressEl.innerHTML = `
          <div class="log-entry running">
            <div class="log-top">
              <div class="log-stage">대기</div>
              <div class="log-time">${escapeHtml(payload.started_at || "-")}</div>
            </div>
            <div class="log-message">실행 요청이 등록되었습니다. run_id=${escapeHtml(payload.run_id || "-")}</div>
          </div>
        `;
        updateRunMeta({
          status: payload.status,
          current_stage: "queued",
          run_id: payload.run_id,
          started_at: payload.started_at,
        });
        renderStageBoard({ logs: [{ stage: "queued", status: "running" }], status: "queued" });
        resultEl.textContent = JSON.stringify(
          {
            run_id: payload.run_id,
            status: payload.status,
            detail: payload.detail,
            run_mode: runMode,
          },
          null,
          2
        );

        loadRecentRuns();
        attachToRun(payload.run_id, { announce: false });
      } catch (error) {
        resultEl.textContent = JSON.stringify(
          { success: false, error: String(error) },
          null,
          2
        );
      }
    }

    async function runPreflight() {
      const preflightEl = document.getElementById("preflight-result");
      preflightEl.textContent = "현재 프로세스 환경설정을 확인하는 중입니다.";
      try {
        const response = await fetch("/config/status");
        const payload = await response.json();
        preflightEl.textContent = JSON.stringify(payload, null, 2);
      } catch (error) {
        preflightEl.textContent = JSON.stringify({ success: false, error: String(error) }, null, 2);
      }
    }

    async function runTelegramTest() {
      const preflightEl = document.getElementById("preflight-result");
      const ok = window.confirm("텔레그램으로 단독 테스트 메시지를 발송할까요?");
      if (!ok) return;

      preflightEl.textContent = "텔레그램 단독 테스트를 실행하는 중입니다.";
      try {
        const response = await fetch("/test/telegram", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        });
        const payload = await response.json();
        preflightEl.textContent = JSON.stringify(payload, null, 2);
      } catch (error) {
        preflightEl.textContent = JSON.stringify({ success: false, error: String(error) }, null, 2);
      }
    }

    document.getElementById("dry-run-form").addEventListener("submit", function (event) {
      event.preventDefault();
      submitForm(event.currentTarget, false, "full");
    });

    document.getElementById("send-form").addEventListener("submit", function (event) {
      event.preventDefault();
      const ok = window.confirm("체크한 플랫폼으로 실제 발송을 진행할까요?");
      if (!ok) return;
      submitForm(event.currentTarget, true, "full");
    });

    document.getElementById("news-only-form").addEventListener("submit", function (event) {
      event.preventDefault();
      const ok = window.confirm("뉴스만 수집해서 선택한 플랫폼으로 발송할까요?");
      if (!ok) return;
      submitForm(event.currentTarget, true, "news_only");
    });
    document.getElementById("attach-run-button").addEventListener("click", function () {
      const runId = document.getElementById("attach-run-id").value.trim();
      if (!runId) {
        window.alert("run_id를 입력해 주세요.");
        return;
      }
      attachToRun(runId);
    });
    document.getElementById("refresh-runs-button").addEventListener("click", loadRecentRuns);
    document.getElementById("preflight-button").addEventListener("click", runPreflight);
    document.getElementById("telegram-test-button").addEventListener("click", runTelegramTest);
    document.querySelectorAll('input[name^="llm_"]').forEach((checkbox) => {
      checkbox.addEventListener("change", syncLlmModelControls);
    });
    document.getElementById("copy-current-prompt").addEventListener("click", async function () {
      const statusEl = document.getElementById("prompt-copy-status");
      if (!currentPromptContent) {
        statusEl.textContent = "복사할 프롬프트가 아직 없습니다.";
        return;
      }
      await navigator.clipboard.writeText(currentPromptContent);
      statusEl.textContent = `${PROMPT_LABELS[currentPromptTask] || currentPromptTask || "현재"} 프롬프트를 복사했습니다.`;
    });

    updateRunMeta({ status: "대기", current_stage: "-", run_id: "-", started_at: "-" });
    renderStageBoard({});
    resetPromptPanel();
    syncLlmModelControls();
    loadRecentRuns();

    const savedRunId = localStorage.getItem(ACTIVE_RUN_STORAGE_KEY);
    if (savedRunId) {
      document.getElementById("attach-run-id").value = savedRunId;
      attachToRun(savedRunId, { announce: false });
    }
  </script>
</body>
</html>
"""


def _parse_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "y", "yes", "on"}


def _parse_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_llm_override(payload: dict[str, Any], task_name: str, *, default_enabled: bool = True) -> dict[str, Any] | bool:
    enabled = _parse_bool(payload.get(f"llm_{task_name}"), default=default_enabled)
    if not enabled:
        return False

    model_choice = str(payload.get(f"llm_model_{task_name}") or "default").strip()
    preset = WEB_LLM_MODEL_PRESET_INDEX.get(task_name, {}).get(model_choice)
    if not preset or model_choice == "default":
        return True

    return {
        "enabled": True,
        "provider": preset.get("provider"),
        "model": preset.get("model"),
        "max_tokens": preset.get("max_tokens"),
        "allow_backup": False,
    }


def _parse_run_payload(payload: dict[str, Any]) -> dict[str, Any]:
    run_mode = str(payload.get("run_mode") or "full").strip().lower()
    if run_mode not in {"full", "news_only"}:
        run_mode = "full"
    send = _parse_bool(payload.get("send"), default=False)
    news_days = _parse_int(payload.get("news_days"), default=1)
    news_max_articles = _parse_int(payload.get("news_max_articles"), default=5)
    transaction_limit = _parse_int(payload.get("transaction_limit"), default=2)
    channel_overrides = {
        "telegram": _parse_bool(payload.get("send_telegram"), default=False),
        "sms": _parse_bool(payload.get("send_sms"), default=False),
        "instagram": _parse_bool(payload.get("send_instagram"), default=False) if run_mode == "full" else False,
    }
    llm_overrides = {
        "telegram_report": _parse_llm_override(payload, "telegram_report", default_enabled=True),
        "instagram_caption": _parse_llm_override(payload, "instagram_caption", default_enabled=True),
        "card_news_script": _parse_llm_override(payload, "card_news_script", default_enabled=True),
    }
    return {
        "run_mode": run_mode,
        "send": send,
        "news_days": news_days,
        "news_max_articles": news_max_articles,
        "transaction_limit": transaction_limit,
        "channel_overrides": channel_overrides if send else None,
        "llm_overrides": llm_overrides,
    }


def _trim_run_history() -> None:
    if len(RUN_STATE) <= MAX_RUN_HISTORY:
        return
    sorted_items = sorted(RUN_STATE.items(), key=lambda item: item[1].get("created_order", 0))
    for run_id, _state in sorted_items[:-MAX_RUN_HISTORY]:
        RUN_STATE.pop(run_id, None)


def _to_relative_name(path: Path) -> str:
    try:
        return str(path.relative_to(BASE_DIR))
    except ValueError:
        return path.name


def _write_json_to_zip(zip_handle: zipfile.ZipFile, arcname: str, payload: dict[str, Any]) -> None:
    zip_handle.writestr(
        arcname,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )


def _create_artifact_bundle(run_id: str, state: dict[str, Any], result: dict[str, Any]) -> str | None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    bundle_path = EXPORT_DIR / f"{run_id}_artifacts.zip"
    artifact_files = result.get("artifact_files", []) or []
    existing_paths = [Path(path) for path in artifact_files if Path(path).exists()]
    written_arcnames: set[str] = set()

    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        _write_json_to_zip(
            zf,
            "run/result.json",
            result,
        )
        _write_json_to_zip(
            zf,
            "run/logs.json",
            {"logs": state.get("logs", [])},
        )

        for path in existing_paths:
            if path.is_dir():
                for file_path in sorted(child for child in path.rglob("*") if child.is_file()):
                    arcname = _to_relative_name(file_path)
                    if arcname in written_arcnames:
                        continue
                    written_arcnames.add(arcname)
                    zf.write(file_path, arcname=arcname)
            elif path.is_file():
                arcname = _to_relative_name(path)
                if arcname in written_arcnames:
                    continue
                written_arcnames.add(arcname)
                zf.write(path, arcname=arcname)

    return str(bundle_path)


def _append_run_log(run_id: str, event: dict[str, Any]) -> None:
    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return
        state["logs"].append(event)
        state["current_stage"] = event.get("stage", state.get("current_stage"))
        state["updated_at"] = event.get("time")


def _execute_run_async(run_id: str, options: dict[str, Any]) -> None:
    def progress_callback(event: dict[str, Any]) -> None:
        _append_run_log(run_id, event)

    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return
        state["status"] = "running"

    if options.get("run_mode") == "news_only":
        result = run_news_only_pipeline(
            send=options["send"],
            trigger="manual",
            news_days=options["news_days"],
            news_max_articles=options["news_max_articles"],
            channel_overrides=options["channel_overrides"],
            llm_overrides=options.get("llm_overrides"),
            progress_callback=progress_callback,
        )
    else:
        result = run_pipeline(
            send=options["send"],
            trigger="manual",
            news_days=options["news_days"],
            news_max_articles=options["news_max_articles"],
            transaction_limit=options["transaction_limit"],
            channel_overrides=options["channel_overrides"],
            llm_overrides=options.get("llm_overrides"),
            progress_callback=progress_callback,
        )

    final_status = "completed" if result.get("success") and not result.get("skipped") else "failed"
    if result.get("skipped"):
        final_status = "skipped"

    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return
        artifact_bundle_path: str | None = None
        artifact_bundle_error: str | None = None
        try:
            artifact_bundle_path = _create_artifact_bundle(run_id, state, result)
        except Exception as exc:
            artifact_bundle_error = str(exc)
        state["status"] = final_status
        state["result"] = result
        state["updated_at"] = result.get("completed_at")
        state["artifact_bundle_path"] = artifact_bundle_path
        state["artifact_download_url"] = f"/run/artifacts/{run_id}"
        state["artifact_bundle_error"] = artifact_bundle_error


def _start_background_run(options: dict[str, Any]) -> dict[str, Any]:
    global RUN_SEQUENCE
    run_id = uuid4().hex
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with RUN_STATE_LOCK:
        RUN_SEQUENCE += 1
        RUN_STATE[run_id] = {
            "run_id": run_id,
            "status": "queued",
            "started_at": started_at,
            "updated_at": started_at,
            "created_order": RUN_SEQUENCE,
            "current_stage": "queued",
            "options": options,
            "logs": [
                {
                    "time": started_at,
                    "stage": "queued",
                    "status": "queued",
                    "message": "실행 요청이 등록되었습니다.",
                    "extra": {
                        "send": options["send"],
                        "run_mode": options.get("run_mode", "full"),
                        "channel_overrides": options.get("channel_overrides") or {},
                        "llm_overrides": options.get("llm_overrides") or {},
                    },
                }
            ],
        }
        _trim_run_history()

    thread = threading.Thread(target=_execute_run_async, args=(run_id, options), daemon=True)
    thread.start()
    return {
        "success": True,
        "run_id": run_id,
        "status": "queued",
        "started_at": started_at,
        "detail": "백그라운드 실행을 시작했습니다.",
    }


@app.route("/")
def index():
    return render_template_string(
        INDEX_TEMPLATE,
        default_channels={
            "telegram": SEND_TELEGRAM_ENABLED,
            "sms": SEND_SMS_ENABLED,
            "instagram": SEND_INSTAGRAM_ENABLED,
        },
        web_llm_tasks=WEB_LLM_TASKS,
        default_llm_tasks={
            task["name"]: (get_generation_plan().get(task["name"], {}).get("provider") != "none")
            and int(get_generation_plan().get(task["name"], {}).get("max_tokens", 0)) > 0
            for task in WEB_LLM_TASKS
        },
        default_llm_models={task["name"]: "default" for task in WEB_LLM_TASKS},
        telegram_model_options=WEB_LLM_MODEL_PRESETS["telegram_report"],
    )


@app.route("/run", methods=["POST"])
def run_manual():
    """수동 실행 엔드포인트"""
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    options = _parse_run_payload(payload)

    if options.get("run_mode") == "news_only":
        result = run_news_only_pipeline(
            send=options["send"],
            trigger="manual",
            news_days=options["news_days"],
            news_max_articles=options["news_max_articles"],
            channel_overrides=options["channel_overrides"],
            llm_overrides=options.get("llm_overrides"),
        )
    else:
        result = run_pipeline(
            send=options["send"],
            trigger="manual",
            news_days=options["news_days"],
            news_max_articles=options["news_max_articles"],
            transaction_limit=options["transaction_limit"],
            channel_overrides=options["channel_overrides"],
            llm_overrides=options.get("llm_overrides"),
        )
    status_code = 200 if result.get("success") else 500
    return jsonify(result), status_code


@app.route("/run/start", methods=["POST"])
def run_manual_start():
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    options = _parse_run_payload(payload)
    return jsonify(_start_background_run(options)), 202


@app.route("/run/status/<run_id>", methods=["GET"])
def run_status(run_id: str):
    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return jsonify({"success": False, "error": "run_id 를 찾을 수 없습니다."}), 404
        return jsonify(state)


@app.route("/run/recent", methods=["GET"])
def run_recent():
    limit = _parse_int(request.args.get("limit"), default=10)
    limit = max(1, min(limit, MAX_RUN_HISTORY))

    with RUN_STATE_LOCK:
        sorted_items = sorted(
            RUN_STATE.items(),
            key=lambda item: item[1].get("created_order", 0),
            reverse=True,
        )
        runs = []
        for run_id, state in sorted_items[:limit]:
            result = state.get("result") or {}
            runs.append(
                {
                    "run_id": run_id,
                    "status": state.get("status", "unknown"),
                    "current_stage": state.get("current_stage", "-"),
                    "started_at": state.get("started_at", "-"),
                    "updated_at": state.get("updated_at", "-"),
                    "trigger": (result.get("trigger") or state.get("options", {}).get("trigger") or "manual"),
                    "duration_sec": result.get("duration_sec"),
                }
            )

    return jsonify({"success": True, "runs": runs})


@app.route("/run/prompt/<run_id>/<task_name>", methods=["GET"])
def run_prompt(run_id: str, task_name: str):
    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return jsonify({"success": False, "error": "run_id 를 찾을 수 없습니다."}), 404
        result = state.get("result") or {}
        prompt_files = result.get("prompt_files") or {}
        prompt_path = prompt_files.get(task_name)

    if not prompt_path:
        return jsonify({"success": False, "error": "해당 실행에서 생성된 프롬프트를 찾을 수 없습니다."}), 404

    path = Path(prompt_path)
    if not path.exists():
        return jsonify({"success": False, "error": "프롬프트 파일을 찾을 수 없습니다."}), 404

    return jsonify(
        {
            "success": True,
            "run_id": run_id,
            "task_name": task_name,
            "path": str(path),
            "content": path.read_text(encoding="utf-8"),
        }
    )


@app.route("/config/status", methods=["GET"])
def config_status():
    return jsonify(
        {
            "success": True,
            "database": get_database_backend_snapshot(),
            "news": get_news_config_snapshot(),
            "llm": get_llm_config_snapshot(),
            "generation_plan": get_generation_plan(),
            "delivery": get_delivery_config_snapshot(),
        }
    )


@app.route("/test/telegram", methods=["POST"])
def test_telegram():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = (
        "[KB자동화 텔레그램 단독 테스트]\n"
        f"실행 시각: {timestamp}\n"
        "이 메시지가 도착하면 텔레그램 봇/채팅 설정은 정상입니다."
    )
    result = send_telegram(message, enabled=True)
    status_code = 200 if result.get("success") else 500
    return jsonify(result), status_code


@app.route("/run/artifacts/<run_id>", methods=["GET"])
def download_artifacts(run_id: str):
    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return jsonify({"success": False, "error": "run_id 를 찾을 수 없습니다."}), 404
        bundle_path = state.get("artifact_bundle_path")

    if not bundle_path:
        return jsonify({"success": False, "error": "아직 다운로드 가능한 아티팩트가 없습니다."}), 404

    path = Path(bundle_path)
    if not path.exists():
        return jsonify({"success": False, "error": "아티팩트 파일을 찾을 수 없습니다."}), 404

    return send_file(path, as_attachment=True, download_name=path.name)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


def _should_enable_scheduler() -> bool:
    return os.getenv("ENABLE_SCHEDULER", "1").strip().lower() not in {"0", "false", "no", "off"}


if __name__ == "__main__":
    if _should_enable_scheduler():
        init_scheduler()
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)

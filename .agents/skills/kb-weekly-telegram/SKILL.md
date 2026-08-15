---
name: kb-weekly-telegram
description: "Run the complete KB real-estate weekly workflow in the Codex desktop app: refresh weekly-series, transaction, and news data; author a Naver Blog draft without an LLM API; create and verify an editable Canva card-news project; and publish the result to Telegram with receipt-based success and duplicate protection. Use for the Friday KB automation or an explicit rerun of that same workflow."
---

# KB Weekly Telegram

Run this workflow from `/Users/dave/Project/kb-realestate-weekly-automation`. Treat the job as successful only after Telegram returns real message IDs. Keep Naver Blog and Instagram unpublished, and keep Canva as an editable project only.

## Operating boundary

- Publish to Telegram. The user has authorized this recurring delivery.
- Do not publish to Naver Blog or Instagram.
- Do not send SMS or Kakao unless a later user request explicitly enables that channel.
- Do not call an LLM API. Use the active Codex desktop model to write the blog draft and card-news copy.
- Preserve local source files and prior receipts. Never overwrite a dated output with different content.
- Do not report success from tests, artifact creation, or a Canva project alone.

## Workflow

1. Run the full no-send data stage:

   ```bash
   .venv/bin/python scripts/run_local_pipeline_test.py \
     --news-days 7 \
     --news-max-articles 12 \
     --transaction-limit 5 \
     --refresh-cache \
     --output reports/runtime/desktop_weekly_pipeline.json \
     --json
   ```

2. Stop if the result is unsuccessful, either Seoul sale/rent section does not contain all 25 districts, any capital/non-capital top section contains more than five regions, or the transaction summary is missing. A top section may contain fewer than five regions when fewer than five regions rose in the latest week. A cache refresh may be `degraded` only when the receipt explicitly says existing cache was used; disclose that state in the final receipt.

3. Read `reports/data_snapshot.json` and `reports/prompts/naver_blog_post_prompt.txt`. Write a fact-only Korean Naver draft with the active desktop model to `outputs/<latest_date>_naver_blog_draft_codex_desktop.md`. Include the six rise-focused sections (Seoul all districts plus capital/non-capital top five, each split into sale and rent), consecutive-rise weeks, representative transactions, dated news links, source dates, and the investment disclaimer. Do not expose weekly change rates in the content. Do not publish it.

4. Run `.venv/bin/python scripts/build_canva_weekly_cardnews.py`. Import the returned HTML through Canva `import-design-from-url` using `design_file`, `intended_design_type=instagram_post`, and the name `<latest_date> KB부동산 주간시계열·실거래 카드뉴스 | 업로드 전`.

5. Verify the new Canva design with `get-design` and `get-design-content`. Require exactly 16 pages, the same `latest_date`, all six section titles, all 25 Seoul districts in both sale and rent, transaction content, news, and the disclaimer. Do not export or publicly upload it.

6. Finalize Telegram with the verified Canva identifiers and URLs:

   ```bash
   .venv/bin/python scripts/finalize_desktop_weekly.py \
     --canva-design-id '<design_id>' \
     --canva-title '<title>' \
     --canva-edit-url '<edit_url>' \
     --canva-view-url '<view_url>' \
     --canva-page-count 16 \
     --blog-path 'outputs/<latest_date>_naver_blog_draft_codex_desktop.md' \
     --cardnews-path 'outputs/<latest_date>_kb_weekly_cardnews_codex_desktop_canva_import.html'
   ```

7. Read `reports/runtime/last_desktop_weekly_run.json`. Success requires:

   - `success=true`
   - `api_call_count=0`
   - `telegram.success=true` with non-empty `message_ids`
   - both Telegram document attachments successful
   - `canva.page_count=16`
   - `instagram_uploaded=false` and `naver_published=false`

If the same artifact digest was already delivered, accept `duplicate_prevented=true` only when the prior successful Telegram message IDs are preserved in the receipt.

## Failure handling

- Retry a transient data or Canva connector error once.
- Never convert a Telegram error, empty message ID, missing Canva page, or stale artifact into success.
- Leave a failed receipt in `reports/runtime/history/` and return a concise failure reason. The macOS fallback job will perform a deterministic, no-LLM-API Telegram delivery at 11:00 when the desktop receipt is missing.

## Final response

Return one compact Korean summary containing the KB date, data counts, Canva edit URL, Telegram message IDs, receipt path, blog path, and card-news HTML path. Distinguish `degraded` data from a fully fresh run.

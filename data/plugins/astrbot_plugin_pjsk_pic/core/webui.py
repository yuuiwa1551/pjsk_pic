from __future__ import annotations

import json
import socket
from pathlib import Path

from aiohttp import web
from astrbot.api import logger

from .crawl_tag_rules import parse_crawl_rule_text, parse_tag_csv
from .db import ImageIndexDB

HTML_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <title>PJSK 图片库管理</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 0; background: #f5f6f8; color: #222; }
    header { padding: 16px 20px; background: #3c65f5; color: white; }
    main { padding: 16px; display: grid; grid-template-columns: 1.1fr 0.9fr; gap: 16px; }
    section { background: white; border-radius: 10px; padding: 16px; box-shadow: 0 2px 8px rgba(0,0,0,.06); }
    h2 { margin-top: 0; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }
    input, select, button { padding: 8px; border: 1px solid #d5d8df; border-radius: 8px; }
    button { cursor: pointer; background: #3c65f5; color: white; border: none; }
    button.secondary { background: #8892a6; }
    .stats { display: grid; grid-template-columns: repeat(5, minmax(90px,1fr)); gap: 10px; }
    .stat { background: #eef2ff; border-radius: 8px; padding: 10px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 12px; }
    .card { border: 1px solid #eceef2; border-radius: 10px; overflow: hidden; background: #fff; }
    .card img { width: 100%; height: 180px; object-fit: cover; background: #ddd; }
    .card .body { padding: 10px; font-size: 13px; }
    .list { display: grid; gap: 10px; }
    .item { border: 1px solid #eceef2; border-radius: 8px; padding: 10px; }
    .muted { color: #666; font-size: 12px; }
    .pill { display: inline-block; background: #eef2ff; color: #2f52d6; border-radius: 999px; padding: 2px 8px; margin: 2px 4px 2px 0; }
    .notice { margin-top: 8px; font-size: 12px; color: #dce4ff; }
  </style>
</head>
<body>
<header>
  <h1 style="margin:0;">PJSK 图片库管理台</h1>
  <div class="muted" style="color:#dce4ff;">独立 WebUI：支持图库检索、tag/别名管理、审核任务、采集任务与平台来源信息查看</div>
  <div class="notice" id="notice"></div>
</header>
<main>
  <div>
    <section>
      <h2>概览</h2>
      <div class="stats" id="stats"></div>
    </section>
    <section>
      <h2>图片检索</h2>
      <div class="row">
        <input id="keyword" placeholder="关键词 / tag / alias" style="flex:1;" />
        <input id="tag" placeholder="精确 tag" />
        <select id="status"><option value="">全部状态</option><option>approved</option><option>manual_approved</option><option>pending</option><option>uncertain</option><option>rejected</option><option>manual_rejected</option></select>
        <select id="platform"><option value="">全部平台</option><option>pixiv</option><option>x</option><option>xiaohongshu</option><option>generic</option><option>submission</option></select>
        <button onclick="loadImages()">搜索</button>
      </div>
      <div class="grid" id="images"></div>
    </section>
  </div>
  <div>
    <section>
      <h2>采集任务</h2>
      <div class="row">
        <select id="jobPlatform"><option>pixiv</option><option>x</option><option>xiaohongshu</option><option>generic</option></select>
        <input id="jobUrl" placeholder="帖子链接或图片直链" style="flex:1;" />
      </div>
      <div class="row">
        <input id="jobTags" placeholder="可选 tags_csv，例如 初音未来,miku" style="flex:1;" />
        <button onclick="createJob()">新建任务</button>
      </div>
      <div class="row">
        <input id="jobIncludeTags" placeholder="可选 include tags，例如 初音未来,天马司" style="flex:1;" />
        <input id="jobExcludeTags" placeholder="可选 exclude tags，例如 R-18,梦向" style="flex:1;" />
      </div>
      <div class="list" id="jobs"></div>
    </section>
    <section>
      <h2>审核任务</h2>
      <div class="row">
        <select id="reviewStatus"><option value="">全部</option><option>pending</option><option>uncertain</option><option>rejected</option><option>approved</option><option>manual_approved</option><option>manual_rejected</option></select>
        <button onclick="loadReviews()">刷新审核</button>
      </div>
      <div class="list" id="reviews"></div>
    </section>
    <section>
      <h2>tag 管理</h2>
      <div class="row">
        <input id="tagSearch" placeholder="搜索 tag" style="flex:1;" />
        <button onclick="loadTags()">搜索</button>
      </div>
      <div class="row">
        <input id="aliasTag" placeholder="tag" />
        <input id="aliasValue" placeholder="alias" />
        <button onclick="addAlias()">添加别名</button>
        <button class="secondary" onclick="removeAlias()">删除别名</button>
      </div>
      <div class="row">
        <input id="charTag" placeholder="tag" />
        <select id="charValue"><option value="true">设为角色</option><option value="false">设为普通</option></select>
        <button onclick="setCharacter()">提交</button>
      </div>
      <div class="list" id="tags"></div>
    </section>
  </div>
</main>
<script>
const params = new URLSearchParams(location.search);
const token = params.get('token') || '';
document.getElementById('notice').textContent = token ? '当前已附带访问令牌。' : '当前未附带访问令牌。';

function api(path) {
  const url = new URL(path, location.origin);
  if (token) url.searchParams.set('token', token);
  return url.toString();
}

async function fetchJson(path, options = {}) {
  const headers = {'Content-Type': 'application/json', ...(options.headers || {})};
  if (token) headers['X-PJSK-Token'] = token;
  const resp = await fetch(api(path), {...options, headers});
  if (!resp.ok) throw new Error(await resp.text());
  return await resp.json();
}

function renderStats(stats) {
  const items = [['图片', stats.images], ['tag', stats.tags], ['alias', stats.aliases], ['采集任务', stats.crawl_jobs], ['待审核', stats.pending_reviews]];
  document.getElementById('stats').innerHTML = items.map(([k,v]) => `<div class="stat"><div class="muted">${k}</div><div style="font-size:22px;font-weight:bold;">${v}</div></div>`).join('');
}

async function loadSummary() { renderStats(await fetchJson('/api/summary')); }

async function loadImages() {
  const q = new URLSearchParams({
    keyword: document.getElementById('keyword').value,
    tag: document.getElementById('tag').value,
    review_status: document.getElementById('status').value,
    platform: document.getElementById('platform').value,
    limit: '30'
  });
  const data = await fetchJson(`/api/images?${q.toString()}`);
  document.getElementById('images').innerHTML = (data.items || []).map(item => `
    <div class="card">
      <img src="${api(`/api/image-file?image_id=${item.id}`)}" loading="lazy" />
      <div class="body">
        <div><strong>#${item.id}</strong> ${item.file_name}</div>
        <div class="muted">${item.width}x${item.height} · ${item.format} · ${item.platform || 'local'}</div>
        <div class="muted">phash: ${item.phash || '-'}</div>
        <div class="muted">来源: ${item.post_url || '-'}</div>
        <div class="muted">疑似重复: ${(item.similar_image_ids || []).join(', ') || '无'}</div>
        <div>${(item.tags || []).map(t => `<span class="pill">${t.name}(${t.review_status})</span>`).join('')}</div>
      </div>
    </div>
  `).join('') || '<div class="muted">暂无结果</div>';
}

async function loadJobs() {
  const data = await fetchJson('/api/jobs');
  document.getElementById('jobs').innerHTML = (data.items || []).map(item => `
    <div class="item">
      <div><strong>#${item.id}</strong> [${item.status}] ${item.platform} · 第${item.attempt_count || 0}次</div>
      <div class="muted">${item.source_url}</div>
      <div>标签：${item.tags_text || '自动提取'}</div>
      <div>包含采集：${item.include_tags_text || '-'}</div>
      <div>排除采集：${item.exclude_tags_text || '-'}</div>
      <div>结果：${item.result_summary || item.error_log || '-'}</div>
      <div class="row"><button onclick="retryJob(${item.id})">重试</button></div>
    </div>
  `).join('') || '<div class="muted">暂无任务</div>';
}

async function createJob() {
  await fetchJson('/api/jobs', {method: 'POST', body: JSON.stringify({
    platform: document.getElementById('jobPlatform').value,
    source_url: document.getElementById('jobUrl').value,
    tags: document.getElementById('jobTags').value,
    include_tags: document.getElementById('jobIncludeTags').value,
    exclude_tags: document.getElementById('jobExcludeTags').value,
  })});
  await loadJobs(); await loadSummary();
}

async function retryJob(jobId) {
  await fetchJson('/api/jobs/retry', {method: 'POST', body: JSON.stringify({job_id: jobId})});
  await loadJobs();
}

async function loadReviews() {
  const q = new URLSearchParams({status: document.getElementById('reviewStatus').value, limit: '20'});
  const data = await fetchJson(`/api/reviews?${q.toString()}`);
  document.getElementById('reviews').innerHTML = (data.items || []).map(item => `
    <div class="item">
      <div><strong>#${item.id}</strong> [${item.status}] ${item.tag_name}</div>
      <div class="muted">image=${item.image_id} · source=${item.source_type || '-'}</div>
      <div>${item.reason || '-'}</div>
      <div class="row">
        <button onclick="reviewDecision(${item.id}, true)">通过</button>
        <button class="secondary" onclick="reviewDecision(${item.id}, false)">拒绝</button>
      </div>
    </div>
  `).join('') || '<div class="muted">暂无审核任务</div>';
}

async function reviewDecision(reviewId, approved) {
  await fetchJson('/api/reviews/decision', {method: 'POST', body: JSON.stringify({review_id: reviewId, approved})});
  await loadReviews(); await loadImages(); await loadSummary();
}

async function loadTags() {
  const q = new URLSearchParams({keyword: document.getElementById('tagSearch').value, limit: '50'});
  const data = await fetchJson(`/api/tags?${q.toString()}`);
  document.getElementById('tags').innerHTML = (data.items || []).map(item => `
    <div class="item">
      <div><strong>${item.name}</strong> ${item.is_character ? '<span class="pill">角色</span>' : ''}</div>
      <div class="muted">图片数：${item.image_count}</div>
      <div>别名：${(item.aliases || []).join('、') || '无'}</div>
    </div>
  `).join('') || '<div class="muted">暂无 tag</div>';
}

async function addAlias() {
  await fetchJson('/api/tag/alias', {method: 'POST', body: JSON.stringify({tag_name: document.getElementById('aliasTag').value, alias: document.getElementById('aliasValue').value})});
  await loadTags();
}

async function removeAlias() {
  await fetchJson('/api/tag/alias', {method: 'DELETE', body: JSON.stringify({tag_name: document.getElementById('aliasTag').value, alias: document.getElementById('aliasValue').value})});
  await loadTags();
}

async function setCharacter() {
  await fetchJson('/api/tag/character', {method: 'POST', body: JSON.stringify({tag_name: document.getElementById('charTag').value, is_character: document.getElementById('charValue').value === 'true'})});
  await loadTags();
}

Promise.all([loadSummary(), loadImages(), loadJobs(), loadReviews(), loadTags()]).catch(err => { console.error(err); alert(err.message || err); });
</script>
</body>
</html>
"""


class GalleryWebUI:
    def __init__(self, db: ImageIndexDB, crawl_service) -> None:
        self.db = db
        self.crawl_service = crawl_service
        self.host = "0.0.0.0"
        self.port = 9099
        self.access_token = ""
        self._runner: web.AppRunner | None = None
        self._site: web.BaseSite | None = None
        self._actual_port: int | None = None

    @property
    def is_running(self) -> bool:
        return self._runner is not None

    async def start(self, *, host: str, port: int, access_token: str = "") -> None:
        self.host = str(host or "0.0.0.0").strip() or "0.0.0.0"
        self.port = max(1, int(port or 9099))
        self.access_token = str(access_token or "").strip()

        if self.is_running:
            return

        app = web.Application()
        app.add_routes(
            [
                web.get("/", self.ui_page),
                web.get("/api/summary", self.api_summary),
                web.get("/api/images", self.api_images),
                web.get("/api/image", self.api_image_detail),
                web.get("/api/image-file", self.api_image_file),
                web.get("/api/tags", self.api_tags),
                web.get("/api/jobs", self.api_jobs),
                web.post("/api/jobs", self.api_jobs),
                web.post("/api/jobs/retry", self.api_jobs_retry),
                web.get("/api/reviews", self.api_reviews),
                web.post("/api/reviews/decision", self.api_review_decision),
                web.post("/api/tag/alias", self.api_tag_alias),
                web.delete("/api/tag/alias", self.api_tag_alias),
                web.post("/api/tag/character", self.api_tag_character),
            ]
        )

        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host=self.host, port=self.port)
        await site.start()

        self._runner = runner
        self._site = site
        sockets = getattr(getattr(site, "_server", None), "sockets", None) or []
        if sockets:
            self._actual_port = int(sockets[0].getsockname()[1])
        else:
            self._actual_port = self.port

        for url in self.get_access_urls():
            logger.info(f"[PJSKPic] 独立 WebUI 已启动: {url}")
        if self.host in {"0.0.0.0", "::"} and not self.access_token:
            logger.warning("[PJSKPic] 独立 WebUI 当前对局域网开放且未配置 webui_access_token，请注意访问安全。")

    async def stop(self) -> None:
        if self._runner is None:
            return
        try:
            await self._runner.cleanup()
        finally:
            self._runner = None
            self._site = None
            self._actual_port = None

    def get_access_urls(self) -> list[str]:
        port = self._actual_port or self.port
        token_suffix = f"?token={self.access_token}" if self.access_token else ""

        if self.host in {"0.0.0.0", "::"}:
            urls = [f"http://127.0.0.1:{port}/{token_suffix}"]
            lan_ip = self._detect_lan_ip()
            if lan_ip:
                urls.insert(0, f"http://{lan_ip}:{port}/{token_suffix}")
            return urls
        return [f"http://{self.host}:{port}/{token_suffix}"]

    @staticmethod
    def _detect_lan_ip() -> str:
        sock: socket.socket | None = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect(("8.8.8.8", 80))
            return str(sock.getsockname()[0])
        except OSError:
            return ""
        finally:
            if sock:
                sock.close()

    def _check_access(self, request: web.Request) -> web.Response | None:
        if not self.access_token:
            return None
        token = (
            request.query.get("token", "")
            or request.headers.get("X-PJSK-Token", "")
            or self._bearer_token(request.headers.get("Authorization", ""))
        )
        if token == self.access_token:
            return None
        return self._json_response({"ok": False, "message": "forbidden"}, status=403)

    @staticmethod
    def _bearer_token(value: str) -> str:
        text = str(value or "").strip()
        if text.lower().startswith("bearer "):
            return text[7:].strip()
        return ""

    def _json_response(self, payload: dict, *, status: int = 200) -> web.Response:
        return web.Response(
            text=json.dumps(payload, ensure_ascii=False),
            status=status,
            content_type="application/json",
            charset="utf-8",
        )

    async def _json_body(self, request: web.Request) -> dict:
        try:
            data = await request.json()
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    async def ui_page(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        return web.Response(text=HTML_PAGE, content_type="text/html", charset="utf-8")

    async def api_summary(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        return self._json_response(self.db.get_stats())

    async def api_images(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        args = request.query
        rows = self.db.search_images(
            keyword=args.get("keyword", ""),
            review_status=args.get("review_status", ""),
            tag_name=args.get("tag", ""),
            platform=args.get("platform", ""),
            limit=min(max(int(args.get("limit", 30) or 30), 1), 100),
            offset=max(int(args.get("offset", 0) or 0), 0),
        )
        items: list[dict] = []
        for row in rows:
            detail = self.db.get_image_detail(int(row["id"])) or {}
            sources = detail.get("sources", [])
            source0 = sources[0] if sources else {}
            items.append(
                {
                    "id": int(row["id"]),
                    "file_name": str(row["file_name"]),
                    "width": int(row["width"] or 0),
                    "height": int(row["height"] or 0),
                    "format": str(row["format"] or ""),
                    "updated_at": str(row["updated_at"] or ""),
                    "phash": str(row["phash"] or ""),
                    "tags": detail.get("tags", []),
                    "sources": sources,
                    "platform": source0.get("platform", ""),
                    "post_url": source0.get("post_url", ""),
                    "similar_image_ids": source0.get("extra", {}).get("similar_image_ids", []),
                }
            )
        return self._json_response({"items": items})

    async def api_image_detail(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        image_id = int(request.query.get("image_id", 0) or 0)
        detail = self.db.get_image_detail(image_id)
        if not detail:
            return self._json_response({"error": "image_not_found"}, status=404)
        return self._json_response(detail)

    async def api_image_file(self, request: web.Request) -> web.StreamResponse:
        denied = self._check_access(request)
        if denied:
            return denied
        image_id = int(request.query.get("image_id", 0) or 0)
        detail = self.db.get_image_detail(image_id)
        if not detail:
            return self._json_response({"error": "image_not_found"}, status=404)
        resolved_path = self.db.get_image_file_path(image_id)
        path = Path(resolved_path) if resolved_path else Path(str(detail["image"]["file_path"]))
        if not path.exists():
            return self._json_response({"error": "file_not_found"}, status=404)
        return web.FileResponse(path)

    async def api_tags(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        args = request.query
        rows = self.db.list_tags(
            keyword=args.get("keyword", ""),
            limit=min(max(int(args.get("limit", 50) or 50), 1), 200),
        )
        items = [
            {
                "id": int(row["id"]),
                "name": str(row["name"]),
                "is_character": bool(row["is_character"]),
                "image_count": int(row["image_count"] or 0),
                "aliases": self.db.list_aliases(str(row["name"])),
            }
            for row in rows
        ]
        return self._json_response({"items": items})

    async def api_jobs(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        if request.method == "GET":
            rows = self.db.list_crawl_jobs(limit=50)
            return self._json_response({"items": [dict(row) for row in rows]})

        data = await self._json_body(request)
        parsed_rules = parse_crawl_rule_text(str(data.get("tags", "")))
        try:
            job_id = await self.crawl_service.submit_job(
                str(data.get("platform", "")).strip(),
                str(data.get("source_url", "")).strip(),
                parsed_rules.manual_tags,
                include_tags=parse_tag_csv([*parsed_rules.include_tags, *parse_tag_csv(str(data.get("include_tags", "")))]),
                exclude_tags=parse_tag_csv([*parsed_rules.exclude_tags, *parse_tag_csv(str(data.get("exclude_tags", "")))]),
            )
        except Exception as exc:
            return self._json_response({"ok": False, "message": str(exc)}, status=400)
        return self._json_response({"ok": True, "job_id": job_id})

    async def api_jobs_retry(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        data = await self._json_body(request)
        ok, message = await self.crawl_service.retry_job(int(data.get("job_id", 0) or 0))
        return self._json_response({"ok": ok, "message": message}, status=(200 if ok else 400))

    async def api_reviews(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        args = request.query
        rows = self.db.list_review_tasks(
            status=args.get("status", "") or None,
            limit=min(max(int(args.get("limit", 20) or 20), 1), 100),
        )
        return self._json_response({"items": [dict(row) for row in rows]})

    async def api_review_decision(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        data = await self._json_body(request)
        ok, message = self.db.apply_manual_review(
            int(data.get("review_id", 0) or 0),
            approved=bool(data.get("approved", False)),
        )
        return self._json_response({"ok": ok, "message": message}, status=(200 if ok else 400))

    async def api_tag_alias(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        data = await self._json_body(request)
        tag_name = str(data.get("tag_name", "")).strip()
        alias = str(data.get("alias", "")).strip()
        if request.method == "POST":
            ok, message = self.db.add_alias(tag_name, alias)
        else:
            ok, message = self.db.remove_alias(tag_name, alias)
        return self._json_response({"ok": ok, "message": message}, status=(200 if ok else 400))

    async def api_tag_character(self, request: web.Request) -> web.Response:
        denied = self._check_access(request)
        if denied:
            return denied
        data = await self._json_body(request)
        ok, message = self.db.set_tag_character(
            str(data.get("tag_name", "")).strip(),
            bool(data.get("is_character", False)),
        )
        return self._json_response({"ok": ok, "message": message}, status=(200 if ok else 400))

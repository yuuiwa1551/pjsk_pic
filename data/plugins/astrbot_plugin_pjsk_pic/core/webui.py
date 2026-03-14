from __future__ import annotations

from pathlib import Path

from quart import Response, jsonify, request, send_file

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
  </style>
</head>
<body>
<header>
  <h1 style="margin:0;">PJSK 图片库管理台</h1>
  <div class="muted" style="color:#dce4ff;">支持图库检索、tag/别名管理、审核任务与采集任务查看</div>
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
        <select id="platform"><option value="">全部平台</option><option>pixiv</option><option>x</option><option>xiaohongshu</option><option>lofter</option><option>generic</option></select>
        <button onclick="loadImages()">搜索</button>
      </div>
      <div class="grid" id="images"></div>
    </section>
  </div>
  <div>
    <section>
      <h2>采集任务</h2>
      <div class="row">
        <select id="jobPlatform"><option>pixiv</option><option>x</option><option>xiaohongshu</option><option>lofter</option><option>generic</option></select>
        <input id="jobUrl" placeholder="帖子链接或图片直链" style="flex:1;" />
      </div>
      <div class="row">
        <input id="jobTags" placeholder="可选 tags_csv，例如 初音未来,miku" style="flex:1;" />
        <button onclick="createJob()">新建任务</button>
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
const base = location.pathname.replace(/\\/ui$/, '');
const api = (path) => `${base}${path}`;
async function fetchJson(path, options = {}) {
  const resp = await fetch(api(path), {headers: {'Content-Type': 'application/json'}, ...options});
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
        <div class="muted">${item.width}x${item.height} · ${item.format}</div>
        <div>${(item.tags || []).map(t => `<span class="pill">${t.name}(${t.review_status})</span>`).join('')}</div>
      </div>
    </div>
  `).join('') || '<div class="muted">暂无结果</div>';
}
async function loadJobs() {
  const data = await fetchJson('/api/jobs');
  document.getElementById('jobs').innerHTML = (data.items || []).map(item => `
    <div class="item">
      <div><strong>#${item.id}</strong> [${item.status}] ${item.platform}</div>
      <div class="muted">${item.source_url}</div>
      <div>标签：${item.tags_text || '自动提取'}</div>
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
      <div class="muted">image=${item.image_id}</div>
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
        self.route_prefix = "/pjsk_pic"

    def register(self, context) -> None:
        context.register_web_api(f"{self.route_prefix}/ui", self.ui_page, ["GET"], "PJSK 图片库 WebUI")
        context.register_web_api(f"{self.route_prefix}/api/summary", self.api_summary, ["GET"], "PJSK 图片库统计")
        context.register_web_api(f"{self.route_prefix}/api/images", self.api_images, ["GET"], "PJSK 图片检索")
        context.register_web_api(f"{self.route_prefix}/api/image", self.api_image_detail, ["GET"], "PJSK 图片详情")
        context.register_web_api(f"{self.route_prefix}/api/image-file", self.api_image_file, ["GET"], "PJSK 图片文件")
        context.register_web_api(f"{self.route_prefix}/api/tags", self.api_tags, ["GET"], "PJSK tag 列表")
        context.register_web_api(f"{self.route_prefix}/api/jobs", self.api_jobs, ["GET", "POST"], "PJSK 采集任务")
        context.register_web_api(f"{self.route_prefix}/api/jobs/retry", self.api_jobs_retry, ["POST"], "PJSK 重试采集任务")
        context.register_web_api(f"{self.route_prefix}/api/reviews", self.api_reviews, ["GET"], "PJSK 审核列表")
        context.register_web_api(f"{self.route_prefix}/api/reviews/decision", self.api_review_decision, ["POST"], "PJSK 审核处理")
        context.register_web_api(f"{self.route_prefix}/api/tag/alias", self.api_tag_alias, ["POST", "DELETE"], "PJSK 别名管理")
        context.register_web_api(f"{self.route_prefix}/api/tag/character", self.api_tag_character, ["POST"], "PJSK 角色标记")

    async def ui_page(self):
        return Response(HTML_PAGE, mimetype="text/html; charset=utf-8")

    async def api_summary(self):
        return jsonify(self.db.get_stats())

    async def api_images(self):
        args = request.args
        rows = self.db.search_images(
            keyword=args.get("keyword", ""),
            review_status=args.get("review_status", ""),
            tag_name=args.get("tag", ""),
            platform=args.get("platform", ""),
            limit=min(max(int(args.get("limit", 30) or 30), 1), 100),
            offset=max(int(args.get("offset", 0) or 0), 0),
        )
        items = []
        for row in rows:
            detail = self.db.get_image_detail(int(row["id"])) or {}
            items.append(
                {
                    "id": int(row["id"]),
                    "file_name": str(row["file_name"]),
                    "width": int(row["width"] or 0),
                    "height": int(row["height"] or 0),
                    "format": str(row["format"] or ""),
                    "updated_at": str(row["updated_at"] or ""),
                    "tags": detail.get("tags", []),
                    "sources": detail.get("sources", []),
                }
            )
        return jsonify({"items": items})

    async def api_image_detail(self):
        image_id = int(request.args.get("image_id", 0) or 0)
        detail = self.db.get_image_detail(image_id)
        if not detail:
            return jsonify({"error": "image_not_found"}), 404
        return jsonify(detail)

    async def api_image_file(self):
        image_id = int(request.args.get("image_id", 0) or 0)
        detail = self.db.get_image_detail(image_id)
        if not detail:
            return jsonify({"error": "image_not_found"}), 404
        path = Path(str(detail["image"]["file_path"]))
        if not path.exists():
            return jsonify({"error": "file_not_found"}), 404
        return await send_file(path)

    async def api_tags(self):
        args = request.args
        rows = self.db.list_tags(keyword=args.get("keyword", ""), limit=min(max(int(args.get("limit", 50) or 50), 1), 200))
        items = []
        for row in rows:
            items.append(
                {
                    "id": int(row["id"]),
                    "name": str(row["name"]),
                    "is_character": bool(row["is_character"]),
                    "image_count": int(row["image_count"] or 0),
                    "aliases": self.db.list_aliases(str(row["name"])),
                }
            )
        return jsonify({"items": items})

    async def api_jobs(self):
        if request.method == "GET":
            rows = self.db.list_crawl_jobs(limit=50)
            return jsonify({"items": [dict(row) for row in rows]})
        data = await self._json_body()
        try:
            job_id = await self.crawl_service.submit_job(
                str(data.get("platform", "")).strip(),
                str(data.get("source_url", "")).strip(),
                self._parse_tags_text(str(data.get("tags", ""))),
            )
        except Exception as exc:
            return jsonify({"ok": False, "message": str(exc)}), 400
        return jsonify({"ok": True, "job_id": job_id})

    async def api_jobs_retry(self):
        data = await self._json_body()
        ok, message = await self.crawl_service.retry_job(int(data.get("job_id", 0) or 0))
        return jsonify({"ok": ok, "message": message}), (200 if ok else 400)

    async def api_reviews(self):
        args = request.args
        rows = self.db.list_review_tasks(
            status=args.get("status", "") or None,
            limit=min(max(int(args.get("limit", 20) or 20), 1), 100),
        )
        return jsonify({"items": [dict(row) for row in rows]})

    async def api_review_decision(self):
        data = await self._json_body()
        ok, message = self.db.apply_manual_review(int(data.get("review_id", 0) or 0), approved=bool(data.get("approved", False)))
        return jsonify({"ok": ok, "message": message}), (200 if ok else 400)

    async def api_tag_alias(self):
        data = await self._json_body()
        tag_name = str(data.get("tag_name", "")).strip()
        alias = str(data.get("alias", "")).strip()
        if request.method == "POST":
            ok, message = self.db.add_alias(tag_name, alias)
        else:
            ok, message = self.db.remove_alias(tag_name, alias)
        return jsonify({"ok": ok, "message": message}), (200 if ok else 400)

    async def api_tag_character(self):
        data = await self._json_body()
        ok, message = self.db.set_tag_character(str(data.get("tag_name", "")).strip(), bool(data.get("is_character", False)))
        return jsonify({"ok": ok, "message": message}), (200 if ok else 400)

    @staticmethod
    async def _json_body() -> dict:
        data = await request.get_json(silent=True)
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _parse_tags_text(tags_text: str) -> list[str]:
        if not tags_text:
            return []
        raw = tags_text.replace("，", ",").split(",")
        return [item.strip() for item in raw if item.strip()]

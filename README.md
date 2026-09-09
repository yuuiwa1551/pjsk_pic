# astrbot_plugin_pjsk_pic

PJSK 图片图库插件，支持本地图库发图、用户投稿、多平台采集、Pixiv / 小红书按 tag 增量抓图、LLM 图片质量与候选 tag 审核、人工审核及独立 WebUI 管理。

## 安装方式

- 插件仓库：`https://github.com/yuuiwa1551/astrbot_plugin_pjsk_pic`
- 作为独立插件仓使用时，**仓库根目录就是插件目录**
- 放入 AstrBot 运行仓后，通常路径为：`data/plugins/astrbot_plugin_pjsk_pic`
- 依赖声明位于：`requirements.txt`

## 1. 插件简介

这是 `astrbot_plugin_pjsk_pic` 的插件源码目录。

常见使用方式有两种：

- 直接作为独立插件仓安装
- 作为 AstrBot 运行仓中的一个插件目录使用

插件运行数据默认位于：

- `data/plugin_data/astrbot_plugin_pjsk_pic/`

这个插件主要解决以下几类需求：

- 从本地图库按 tag / alias 随机发图
- 通过自然语言触发或 LLM Tool 触发发图
- 接收用户投稿，并接入通知 / 审核流程
- 从 Pixiv、X / Twitter、小红书、lofter / generic 来源采集图片
- 对 tag 做别名、主 tag、角色标记、清理与审核治理
- 通过独立 WebUI 查看图片、来源、审核任务与采集任务

当前重点支持的平台 / 来源：

- 本地图库
- Pixiv
- X / Twitter
- 小红书
- lofter / generic 兼容采集

## 2. 当前功能

### 发图能力

- 自然语言规则触发
- LLM Tool 发图
- tag / alias 匹配
- 模糊匹配可配置
- 会话级简单去重
- 同 sha256 多路径图片自动回退到可用文件

### 投稿能力

- 用户命令投稿
- 支持管理员通知
- 支持开启 / 关闭投稿审核
- 投稿时可自动补 tag / alias
- 陌生投稿 tag 进入待处理提案，不会自动创建主 tag 或提前导入图片

### 图库管理 / tag 治理

- SQLite 索引
- 本地图库扫描
- tag / alias 管理
- 主 tag 类型（角色 / CP / 主题 / 其他）与状态（启用 / 待确认 / 归档）
- QQ tag 规范报告与提案审核
- tag 合并
- 主 tag 切换（canonical tag）
- 角色 tag 标记
- 仅针对无有效图片和治理依赖项的安全 tag 清理预览 / 执行
- 图片删除 / 恢复

### 采集能力

- 采集任务队列
- 图片下载入库
- Pixiv / X / 小红书 / lofter / generic 适配
- include / exclude tag 过滤
- Pixiv 采集过滤可识别主 tag、alias、平台词和内置常用查询词
- 主 tag 收口（仅保留 canonical tag）
- pHash 重复图识别
- 采集失败重试

### Pixiv 自动采集

- 按 tag 自动周期性搜索 Pixiv
- 支持 Pixiv 平台专用 query / match 词映射
- 默认只同步“启用 + 角色”的主 tag；关闭 `pixiv_auto_crawl_character_only` 时仍只同步启用状态的主 tag
- 支持查询后缀（默认常见用法是给 tag 追加 `user`）
- 支持每轮任务上限、每 tag 结果上限、页数上限
- Pixiv 搜索、作品详情、历史回填与 WebUI 预览共享连接和 access token，OAuth 连续失败时会短暂熔断
- 每个订阅的每个查询词独立保存增量游标，单个查询词失败或遇到旧游标不会截断其他查询词
- 搜索命中先持久化到发现队列，再按单轮任务上限逐步入队；插件重载或额度不足不会丢失已发现作品
- 同一 Pixiv 作品的自动采集任务会复用并合并主 tag，避免跨订阅重复下载和审核
- 当前自动采集是“最新增量采集”，不是一次性把某个 Pixiv tag 的历史结果全量扒完
- 支持手动 Pixiv 历史回填任务：指定 tag、页数、扫描上限、入队上限后逐页补旧图
- 自动采集只使用显式 Pixiv 平台词或内置官方 Pixiv 词映射，不再默认用普通 alias 搜图，避免短英文 alias 误命中其他作品

### 小红书自动采集

- `xhs_provider_kind`
  - 默认 `xiaohongshu_mcp`；显式选择 `xiaohongshu_cli` 并配置分页 sidecar 地址后支持历史回填
- 使用隔离部署、固定版本的 `xiaohongshu-mcp` REST 提供者，不把 Cookie 放进插件进程
- 按显式 `xiaohongshu` 平台词低频搜索“最新 + 图文 + 一周内”公开笔记
- `query / both` 负责发现，详情标题、正文和话题必须命中 `match / both` 才能归入主 tag
- 普通 alias、主 tag 名和小红书原始话题都不会隐式开启自动搜图或创建 canonical tag
- 详情中的全部图片按原顺序进入现有下载、SHA-256、pHash 和审核流程，不套用通用 6 图上限
- 同一笔记跨 query / 主 tag 命中时合并发现记录和任务，登录态、风控和上游契约异常会持久化暂停
- 默认关闭，首次启用受种子上限、单轮查询/详情/任务预算和最久未检查优先调度约束
- 二期只做首批最新增量；分页和历史回填延后到提供者稳定运行后

### 审核能力

- 新版 LLM 审图一张图片只调用一次，同时给出技术质量、美观程度、图库适用性和候选角色判断
- 候选角色只来自已存在的启用角色主 tag，模型只能返回候选 `tag_id`，不会自动创建或合并 tag / alias
- 支持 `shadow`、`assist`、`auto_approve` 三种模式；模型异常、非法 JSON、候选越界和低置信结果保留人工审核
- `auto_approve` 只有在质量、图库适用性和全部选中角色置信度同时达标时自动通过，并支持多角色 tag 与随机人工抽检
- 审核模型使用独立持久队列、单并发、单轮与每日预算，不阻塞 Pixiv / 小红书下载任务，插件重启后可恢复并保留有限重试历史
- 人工审核命令
- 审核列表 / 查看 / 通过 / 拒绝
- 按图片聚合的来源人工审核
- Pixiv 图片批量人工审核
- 人工审核结果可沉淀 Pixiv tag 映射
- 所有群友均可通过显式 `.pp` 指令随机领取 Pixiv 或小红书待审图
- 群友审核会话按群聊/私聊来源和 QQ 用户隔离，支持指定最终 tag、整图拒绝、跳过和自动下一张

### WebUI

- 独立 WebUI 服务
- 图片搜索与预览
- 来源信息查看
- tag 管理
- 审核任务查看与处理
- 采集任务查看、新建、重试
- Pixiv 审批页
- Pixiv 大图预览审核
- Pixiv 审批页支持按角色、alias、Pixiv tag 搜索相关待审图
- Pixiv 原始 tag / 翻译 tag 点选确认
- Pixiv 审批页可直接新增主 tag，并自动加入当前图片选择
- Pixiv 审批通过时只归入一个规范主 tag，已选同义词会沉淀为 alias / Pixiv 搜索词
- Pixiv 审批默认仅显示 `pending / uncertain` 待处理队列，可手动切到 `rejected`
- Pixiv 审批页支持拒绝整张图片，被拒绝 Pixiv 来源会在后续自动来源搜图中跳过
- Pixiv 平台词管理页
- Pixiv 历史建议词 / 未解决词查看与一键采纳
- Pixiv 批量审核预览 / 批量确认映射
- 历史 tag 归并助手

## 3. 命令与触发方式

### 自然语言触发

支持类似：

- `看看初音未来`
- `来张miku`
- `发一张宁宁`
- `来点瑞希`
- `看看id123`
- `看看 ID 123`

### 投稿与快捷命令

- `.投稿 <tag>` 或 `.tg <tag>`
  - 发送命令后附图，进入投稿流程
  - 可写成 `.投稿 <tag> 别名 <alias1,alias2>` 或 `.tg <tag> alias <alias1,alias2>`，投稿时顺手补充别名
  - 也可以先回复一条带图消息，再发送投稿命令
  - 如果 tag 或 alias 尚不存在，只会登记 tag 提案并返回编号，图片不会入库；管理员确认词条后需要重新投稿
- `.alias <tag或alias> [alias1,alias2]`
- `.unalias <tag或alias> <alias1,alias2>`

### 图库管理命令

图库管理命令组同时支持：

- `.pjsk图库 ...`
- `.pp ...`
- `.子命令 ...`（省略 `pp`，例如 `.tag合并 <目标tag> <来源tag>`）

下列中文主子命令均可省略 `pp`，原 `.pp` / `.pjsk图库` 写法继续保留。为避免抢占现有顶层命令，`help`、`菜单`、`命令`、`抽审` 等 alias 仍需写在 `.pp` / `.pjsk图库` 后；例如 `.帮助` 可用，`.pp help` 也可用，但 `.help` 仍由原有帮助插件处理。

常用命令包括：

- `.pp 帮助 [投稿|tag|审核|采集]`
- `.pp 重扫`
- `.pp 统计`
- `.pp 查看 <tag>`
- `.pp 看图 <image_id>`
- `.pp 别名添加 <tag> <alias>`
- `.pp 别名删除 <tag> <alias>`
- `.pp 别名查看 <tag>`
- `.pp tag列表`
- `.pp tag规范报告`
- `.pp tag提案 [数量]`
- `.pp tag提案通过 <id> <角色|CP|主题|其他>`
- `.pp tag提案归并 <id> <现有tag>`
- `.pp tag提案拒绝 <id> [原因]`
- `.pp tag类型 <tag> <角色|CP|主题|其他>`
- `.pp tag状态 <tag> <启用|待确认|归档>`
- `.pp 平台词添加 <pixiv|小红书|x> <tag> <query|match|both> <term>`
- `.pp 平台词列表 <pixiv|小红书|x> [tag]`
- `.pp 平台词删除 <term_id>`
- `.pp tag合并 <目标tag> <来源tag1,来源tag2>`
- `.pp 主tag切换 <old_tag> <new_tag>`
- `.pp 角色标记 <tag> <true|false>`
- `.pp tag清理预览`
- `.pp tag清理执行 确认`（只删除当前安全候选，受保护 tag 保留）
- `.pp 采集添加 <platform> <url> [tags_csv]`
- `.pp 采集列表`
- `.pp 采集诊断`
- `.pp 失败列表 [platform]`
- `.pp 失败重试 <job_id|全部>`
- `.pp 采集重试 <job_id>`
- `.pp 自动采集状态`
- `.pp 自动采集列表`
- `.pp 自动采集执行`
- `.pp 小红书采集状态`
- `.pp 小红书采集列表`
- `.pp 小红书采集执行 [tag]`
- `.pp 小红书采集暂停 [原因]`
- `.pp 小红书采集恢复`
- `.pp 历史回填添加 <tag> [页数上限] [扫描上限] [入队上限]`
- `.pp 历史回填列表`
- `.pp 审核列表 [status]`
- `.pp 审核查看 <review_id>`
- `.pp 审核通过 <review_id>`
- `.pp 审核拒绝 <review_id>`
- `.pp 审图帮助`
- `.pp 随机审核 [Pixiv|小红书] [候选tag]`（默认 Pixiv；别名 `.pp 抽审`，所有群友可用）
- `.pp 审图通过 <最终tag>`
- `.pp 审图拒绝 [原因]`
- `.pp 审图跳过`
- `.pp 审图当前`
- `.pp 审图结束`
- `.pp 投稿审核状态`
- `.pp 投稿审核开启`
- `.pp 投稿审核关闭`
- `.pp 删图 <image_id>`
- `.pp 恢复图 <image_id>`
- `.pp 重复忽略 <id1> <id2> [原因]`
- `.pp 重复恢复 <id1> <id2>`
- `.pp 重复忽略列表 [image_id]`
- `.pp 面板地址`

数据库图片 ID 可使用 `.pp 看图 <image_id>`，也可自然语言发送 `看看id<image_id>` 查看。纯数字写法如 `看看1` 不再代表最近展示列表序号。

## 4. 配置说明

主要配置项如下：

### 基础图库

- `library_root`
  - 本地图库根目录；留空时默认使用插件数据目录下的 `library/`
- `scan_on_startup`
  - AstrBot 启动后是否自动扫描本地图库
- `allow_fuzzy_match`
  - 是否允许 tag / alias 模糊匹配
- `recent_dedupe_count`
  - 每个会话最近去重的图片数量
- `image_id_lookup_enabled`
  - 是否启用 `看看id<image_id>` 图片 ID 查看入口
- `image_id_lookup_admin_only`
  - `看看id<image_id>` 是否仅管理员可用，默认开启
- `enable_llm_tool`
  - 是否开启 LLM 发图工具

### WebUI

- `webui_enabled`
  - 是否启用独立 WebUI
- `webui_host`
  - WebUI 监听地址，默认 `0.0.0.0`
- `webui_port`
  - WebUI 监听端口，默认 `9099`
- `webui_access_token`
  - 可选访问令牌

### 投稿 / 审核

- `qq_review_enabled`
  - 是否向所有群友开放 `.pp 随机审核` 和审图指令，默认开启
- `qq_review_claim_ttl_seconds`
  - 单张待审图的领取有效期，默认 `600` 秒；过期后可由其他群友重新领取
- `qq_review_recent_count`
  - 每位群友最近抽到/跳过图片的去重窗口，默认 `30`
- `qq_review_source_term_limit`
  - QQ 审核消息最多展示的 Pixiv 来源词数量，默认 `12`
- `qq_review_auto_next`
  - 通过、拒绝或跳过后是否自动发送下一张，默认开启
- `submission_notify_enabled`
  - 是否启用投稿通知
- `submission_notify_use_astr_admins`
  - 是否将 AstrBot 管理员作为默认通知目标
- `submission_notify_targets`
  - 额外投稿通知目标列表
- `submission_review_enabled`
  - 是否开启投稿审核
- `enable_auto_review`
  - 旧版逐 tag 自动审核兼容开关；新版 LLM 审图启用时不会执行旧版逐 tag 模型调用
- `review_provider_id`
  - 自动审核使用的 provider id
- `review_confidence_threshold`
  - 自动审核置信度阈值
- `approve_non_character_tags`
  - 非角色 tag 是否自动放行
- `guess_character_tags`
  - 是否自动猜测角色 tag
- `llm_image_review_enabled`
  - 是否启用新版 LLM 图片审核工作线程；默认关闭
- `llm_image_review_mode`
  - `shadow` 只记录、`assist` 在 QQ 审图卡展示建议、`auto_approve` 允许高置信度自动通过
- `llm_image_review_provider_id`
  - 使用的多模态 provider id；留空时回退到旧版 `review_provider_id`
- `llm_image_review_auto_queue_new`
  - 是否自动排队新产生的待审图；不会默认扫完历史积压
- `llm_image_review_max_per_cycle` / `llm_image_review_daily_limit`
  - 每轮与每日模型调用硬预算，审核固定单并发
- `llm_image_review_startup_delay_seconds`
  - AstrBot 启动后等待 provider 管理器加载完成再处理队列，默认 `30` 秒
- `llm_image_review_max_candidates`
  - 单张图片最多提供给模型的现有角色候选数，默认 `8`
- `llm_image_review_preview_max_side` / `llm_image_review_min_side`
  - 模型预览最长边与本地硬检查最短边；原图不修改，预览去除元数据
- `llm_image_review_quality_threshold` / `llm_image_review_technical_threshold`
  - 自动通过要求的整体与技术质量分数
- `llm_image_review_aesthetic_threshold`
  - 自动通过要求的美观程度最低分
- `llm_image_review_gallery_fit_threshold` / `llm_image_review_identity_threshold`
  - 自动通过要求的图库适用性与每个选中角色置信度
- `llm_image_review_spot_check_rate`
  - 达标图片仍保留人工抽检的比例
- `.pp LLM审图状态`
  - 查看模式、provider、工作线程、队列和阈值
- `.pp LLM审图执行 [数量] [Pixiv|小红书|投稿]`
  - 管理员受限排队并执行一批待审图片
- `.pp LLM审图重试 [数量]`
  - 重新排队失败的模型审核任务

完整的输出契约、安全边界和放量方法见 [LLM 图片审核说明](docs/llm-image-review.md)。

### 采集

- `crawler_max_candidates`
  - 每个采集任务最多处理的候选图片数
- `crawler_max_image_bytes`
  - 单张远程图片下载上限，默认 `26214400`（25 MiB），同时校验声明长度和实际读取长度
- `crawl_worker_count`
  - 采集任务 worker 数量，默认 `2`；手工任务优先于最新增量，历史回填最后执行
- `crawl_image_download_concurrency`
  - 同一作品内图片并发下载数，默认 `3`
- `crawl_backfill_queue_high_watermark`
  - 历史回填继续投喂任务时允许的采集队列上限，默认 `20`
- `crawl_discovery_dispatch_interval_seconds`
  - 有待分发发现或 Pixiv 分页 checkpoint 时的快速调度间隔，默认 `30` 秒
- `platform_request_timeout`
  - 平台请求超时秒数
- `platform_retry_times`
  - 平台采集失败后的自动重试次数
- `max_tags_per_image`
  - 每张图最多写入的 tag 数量
- `tag_blacklist`
  - 额外 tag 黑名单
- `crawl_include_tags`
  - 强制包含的 tag 列表
- `crawl_exclude_tags`
  - 强制排除的 tag 列表，默认常见会排除 `R-18,AI`
- `crawl_keep_primary_tags_only`
  - 是否将采集 tag 收口到主 tag / canonical tag
- `.pp 采集诊断`
  - 查看采集 worker、Pixiv 自动采集、refresh token 配置、任务状态计数、最近失败原因和历史回填队列状态
- `.pp 失败列表 [platform]`
  - 查看最近失败采集任务，可按平台筛选
- `.pp 失败重试 <job_id|全部>`
  - 重新入队指定失败任务，或批量重试最近失败任务
- `enable_phash_dedupe`
  - 是否启用 pHash 去重
- `phash_max_distance`
  - pHash 判重最大距离

### Pixiv 自动采集

- `pixiv_refresh_token`
  - Pixiv App API 所需 refresh token
- `pixiv_auto_crawl_enabled`
  - 是否开启 Pixiv 自动采集
- `pixiv_auto_crawl_character_only`
  - 是否仅订阅角色 tag
- `pixiv_auto_crawl_interval_minutes`
  - 自动采集周期（分钟）
- `pixiv_auto_crawl_query_suffix`
  - 搜索后缀，默认可用于追加 `user`
- `pixiv_auto_crawl_max_results_per_tag`
  - 每个 tag 单轮最多拉取多少搜索结果；这是增量采集上限，不是全量回填
- `pixiv_auto_crawl_max_pages_per_tag`
  - 每个 tag 单轮最多拉取多少页
- `pixiv_auto_crawl_max_new_jobs_per_cycle`
  - 每轮全局最多新增多少自动采集任务
- `pixiv_backfill_default_pages`
  - Pixiv 历史回填任务默认页数上限
- `pixiv_backfill_default_max_results`
  - Pixiv 历史回填任务默认扫描结果上限
- `pixiv_backfill_default_max_new_jobs`
  - Pixiv 历史回填任务默认新增采集任务上限

自动采集按周期从 Pixiv 最新搜索结果里增量补任务。每个订阅下的每个查询词分别记录上次看到的作品；命中结果会先写入持久化发现队列，再受单轮全局额度控制创建采集任务，因此达到额度或插件重载时，未提交的结果会留到下一轮继续处理。已入库、已拒绝、未命中目标 tag / match 词的作品会被跳过；同一 Pixiv 作品被多个 tag 命中时会复用任务并合并 tag。需要把某个 tag 的全部历史作品补齐时，应单独做“历史回填”任务，而不是调大日常自动采集配置。

### 小红书自动采集

- `xhs_auto_crawl_enabled`
  - 是否开启小红书自动采集，默认关闭
- `xhs_provider_base_url`
  - 隔离提供者 REST 地址，推荐 `http://pjsk-xhs-provider:18060`
- `xhs_provider_access_token`
  - 提供者鉴权 token；属于秘密，不应写入日志或仓库
- `xhs_provider_timeout_seconds`
  - 健康、搜索、详情请求超时
- `xhs_provider_min_interval_seconds`
  - 相邻请求最小间隔；插件会串行访问提供者
- `xhs_auto_crawl_interval_minutes`
  - 自动采集周期，运行时最低 15 分钟
- `xhs_auto_crawl_max_subscriptions_per_cycle`
  - 单轮最多检查的主 tag 数，按最久未检查优先
- `xhs_auto_crawl_max_queries_per_cycle`
  - 单轮最多搜索请求数
- `xhs_auto_crawl_max_details_per_cycle`
  - 单轮最多详情请求数
- `xhs_auto_crawl_max_new_jobs_per_cycle`
  - 单轮最多新增任务数
- `xhs_auto_crawl_seed_max_notes`
  - 新订阅首次运行时每个 query 词最多接纳的笔记数
- `xhs_max_images_per_note`
  - 单篇异常响应安全上限，默认 60；超过时失败并暂停检查，不会截断
- `xhs_auto_crawl_notify_*`
  - 登录失效、验证码、风控或契约异常时的管理员状态通知

启用前，先用 QQ 管理命令显式建立平台词。例如：

```text
.pp 平台词添加 小红书 初音未来 both 初音未来
.pp 小红书采集执行 初音未来
```

只有同时具备 `query / both` 搜索词和 `match / both` 详情匹配词的启用主 tag 才会生成订阅。自动运行使用持久化水位；首次仅接纳少量最新笔记，后续逐轮增量处理。登录失效、验证码、`300012` 风控、未知图片域名或上游字段变化会暂停，而不是被当成“零结果成功”或连续重试。

提供者的 `/health` 与登录状态都成功时，浏览器搜索仍可能卡住；上线和故障恢复必须再通过一次低频搜索。连续搜索超时应先保持自动采集关闭，重启原登录容器并确认登录态和搜索恢复。若登录态无法随数据卷迁移，可在“专用 Docker 网络 + 宿主机仅回环绑定”的前提下保留原容器作为未鉴权过渡部署，具体边界见下方契约文档。

提供者部署、REST 字段、错误分类、图片白名单与 2026-08-30 隔离 POC 结果见 [小红书采集提供者契约](docs/xiaohongshu-provider.md)。

### 小红书历史回填

小红书分页回填命令（管理员，支持省略 `pp`）：

```text
.pp 小红书回填添加 初音未来 2 40 3
.pp 小红书回填列表
.pp 小红书回填重试 <任务ID>
.pp 小红书饱和列表
```

参数依次为页数、扫描笔记数、新建图片采集任务数上限。任务保存当前页与页内位置，失败重试和进程重启从断点继续。`limited` 表示达到预算，`completed` 表示所有查询词已无下一页；回填完成是发现/分发结束，图片下载状态仍在采集列表中查看。

`xhs_backfill_page_size` 默认 20，`xhs_backfill_page_interval_seconds` 默认 30 秒；回填服从小红书暂停状态，增量周期运行时让路，图片任务优先级为 50。饱和提示保留至对应查询词历史补扫结束，不会因下轮首页找到水位而消失。部署说明见 [分页 provider](provider/xhs_cli_sidecar/README.md)。

### Pixiv 历史回填

历史回填是手动启动的独立任务，用来补某个 tag 的旧 Pixiv 搜索结果。它会先把输入词解析到主 tag，再优先使用该 tag 的 Pixiv `query` / `both` 平台词或内置官方 Pixiv 词映射逐页搜索；命中后创建普通 Pixiv 采集任务，下载、入库和后续审批仍由现有采集队列处理。采集队列会按候选图逐张导入并写入审核任务，WebUI 的采集页会低频自动刷新，Pixiv 审批页保留手动刷新以避免大量待审图时反复重查。

每个历史回填任务都有明确边界：

- `页数上限`：最多翻多少页 Pixiv 搜索结果
- `扫描上限`：最多检查多少个搜索结果
- `入队上限`：最多新增多少个采集任务

任务会跳过已入库、已拒绝、已存在采集任务、未命中目标 tag / match 词和重复搜索结果。WebUI 的“采集任务”页可查看回填任务进度，也可对失败任务重新入队。

### 平台接入

- `x_cookie_string`
  - X / Twitter 登录 Cookie
- `x_account_pool_enabled`
  - 是否启用 X 账号池模式
- `xiaohongshu_cookie_string`
  - 旧版小红书网页采集配置，结构化提供者模式已停用该项

完整配置见：

- `_conf_schema.json`

## 5. WebUI 说明

插件启动后会拉起 **独立 WebUI 服务**。

从 `v0.14.0` 起，WebUI 前端使用 `Vue 3 + Vite + TypeScript` 维护；插件仓库已包含构建产物，正常安装与运行不需要 Node/npm。

默认配置：

- 监听地址：`0.0.0.0`
- 监听端口：`9099`

若仅希望本机访问，可设置：

- `webui_host=127.0.0.1`

若配置了 `webui_access_token`，可通过以下任一方式访问：

- 浏览器访问根页面后，在登录页输入访问令牌建立站点会话（HttpOnly Cookie）
- 请求头：`X-PJSK-Token: 你的令牌`
- 请求头：`Authorization: Bearer 你的令牌`

> 从 `v0.12.2` 起，不再支持通过 `?token=` URL 查询参数传递 WebUI 访问令牌，以避免令牌进入浏览器历史、日志或 Referrer。

管理员可通过命令查看当前访问地址：

- `.pp 面板地址`

当前 WebUI 已支持：

- 按图片聚合查看 Pixiv 待审图
- 在 Pixiv 审批页先按角色 / alias / Pixiv tag 筛出相关待审图，再进行单张或批量审核
- 搜索命中主 tag 后会展开该 tag 的 alias、Pixiv 平台词和历史来源中的同义角色候选
- 在卡片页直接勾选主 tag 与 Pixiv 来源词后提交人工审核
- 点击图片进入大图预览，并在预览页继续完成同一套审核操作
- 在 Pixiv 审批页直接新增主 tag，新增后会自动加入当前图片选择
- 审核通过时只把图片归入一个规范主 tag，并把已选 Pixiv 来源 tag / 同义候选词沉淀为 alias / Pixiv 搜索词
- 如果同义候选词已经是独立 tag，会归并到规范主 tag，避免同一角色多 tag 分裂
- 后续自动来源搜图会使用规范主 tag 下的 Pixiv 平台词或内置官方 Pixiv 词映射进行搜索和匹配；普通 alias 仍可用于聊天触发和人工搜索，但不再默认参与自动 Pixiv 搜图
- 直接拒绝整张 Pixiv 图片，并记录该 Pixiv 来源为后续自动搜图跳过项
- 勾选多张 Pixiv 图片后批量预览 / 批量确认人工审核
- 单张或批量审核成功后，图片会立即从当前审批队列移除，并以非阻塞提示反馈结果
- 查看候选主 tag 的 alias、已有 Pixiv 平台词与历史建议词
- 按主 tag / Pixiv 词查看已有平台映射
- 手动新增、删除、修改 `query / match / both` 平台词
- 查看历史建议词、未解决词，并直接采纳为 Pixiv 平台词
- 批量确认多个 Pixiv 来源词映射到指定主 tag
- 查看历史 tag 归并候选、影响预览，并在 WebUI 内直接执行归并

## 6. 数据位置

插件运行数据目录：

- `data/plugin_data/astrbot_plugin_pjsk_pic/`

常见内容包括：

- `image_index.db`
  - SQLite 图片索引数据库
- `library/`
  - 本地图库目录
- `images/`
  - 导入 / 采集图片目录
- `trash/`
  - 删除后暂存目录

数据库会同时维护：

- 逻辑图片记录
- 物理文件位置记录
- tag / alias / 主 tag 关系
- 审核任务
- 采集任务与自动采集订阅
- 人工确认过的非重复图片对

## 7. TODO / 后续计划

后续优先事项：

1. 在受控分页回填基础上评估小红书平台词建议和按需摘要
2. 把平台词治理继续推广到 X，并为历史 tag 归并补更多候选来源
3. 继续清理历史遗留乱码文案，统一插件内中文文案质量
4. 为采集适配器补更多上游字段变化和失败场景回归
5. 为群友审图补充贡献统计、持久化领取、操作日志与最近一次操作撤销
6. 补充更完整的使用说明与升级迁移说明

## 8. 开源协议

本项目采用 `AGPL-3.0-or-later` 许可。

许可仅覆盖本插件的源代码与文档，不授予 Project Sekai 相关素材、Pixiv 作品、用户投稿图片或其他第三方内容的使用权。

详见仓库根目录的 `LICENSE` 文件。

## 9. 当前版本

- 当前插件版本：`0.23.3`

## 10. 更新记录

### v0.23.3

- 修复扫描历史 URL/base64 图片时漏传 AstrBot Image 必需的 file 参数，导致整轮收图钩子中断的问题
- 历史图片统一通过 Image(file=location) 交给框架解析，不改变收录标准，不额外调用模型
- 回归使用与真实框架一致的构造签名，并覆盖历史图片与当前上下文原图同时存在的情况；实际群聊入库单独验收

### v0.23.2

- 修复模型先发送正文再调用收图工具时，请求状态被提前清空导致“当前请求未启用主聊天收图”的问题
- 以 AstrBot 整轮 Agent 完成回调为准，保留中间回复期间的状态，最终统一汇总并清理
- 补充正文/工具/最终回复生命周期回归，并保留 Luna 的收图专项测试

### v0.23.1

- 收图引用与实际图片逐一标注，补齐官方群上下文和 Group Chat Plus 的图片标记及原图/本地文件映射
- 角色候选复用中日文名和已有 alias，修正杏豆、遥实的 CP 类型，保留原角色 ID
- 收录回执只报告实际接受的标签，重复工具调用保留首次成功汇总
- 接入方法见 [聊天收图接入](integrations/README.md)

### v0.22.0

- 回复 QQ 合并转发后发送 `.tg <tag>` / `.投稿 <tag>`，展开节点和嵌套转发中的全部图片统一投稿
- 使用原始 OneBot 消息保留转发信息，批量复用下载与去重，记录节点来源，统一报告结果及展开失败项

### v0.21.0

- 增加固定版 xiaohongshu-cli 分页 sidecar，传递排序、图文类型与发布时间过滤，搜索和详情沿用 REST 接口
- 小红书历史回填持久化当前页、页内位置和已处理笔记，任务创建与计数/断点在同一事务提交
- 回填服从暂停状态和增量优先调度，增加添加、列表、失败重试及饱和列表命令
- 区分达到预算与历史扫描结束，饱和记录仅在对应历史补扫结束后清除

### v0.20.0

- Pixiv 自动发现和历史回填在搜索结果阶段直接应用 include/exclude，已排除作品不再创建空任务或重复请求详情
- Pixiv 查询词增加分页 checkpoint；旧水位不在当前结果页时分轮续扫，找到旧水位后才推进高水位
- Pixiv 与小红书优先分发已有 discovery；小红书积压改为 30 秒快速分发，不再等待下一个 180 分钟搜索周期
- 小红书发现阶段的详情快照直接交给任务，避免同一笔记第二次调用详情；Pixiv 搜索结果完整覆盖全部原图页时同样复用
- 每个作品只规范化一次 tag；同图的 source、image_tags、review_tasks 合并为一个数据库事务，并取消逐图片进度写库
- 新增来源 URL 和任务 URL 复合索引；Pixiv 水位 checkpoint、任务 origin/priority 均为无损迁移字段
- 采集队列改为 PriorityQueue，默认两个 worker；手工、最新增量、历史回填依次为高、中、低优先级，回填按低水位投喂
- 图片下载复用 HTTP 连接池，同作品默认并发下载 3 张；SHA 命中时跳过解码和 pHash，全量活动 pHash 参与相似图比较
- 删除通用采集外层重复重试，网络重试由具体 Pixiv API 客户端负责

### v0.19.2

- LLM 审核工作线程增加默认 30 秒启动延迟，避免插件初始化早于 AstrBot provider 管理器导致首次调用误报 provider 不存在
- 可重试 provider 故障会立即结束本轮，下一轮到期后再试，不会在同一轮重新领取并耗尽尝试次数

### v0.19.1

- 真实 shadow 校准后将提示词升级为 v2，完整列出 parser 允许的质量 flags，并明确“无候选匹配”应返回空角色数组，减少模型自创枚举导致的无效结果
- LLM provider 的有限重试现在保留最近错误历史；任务最终成功后仍能审计中间失败，而当前错误状态会正常清空

### v0.19.0

- 新增独立持久化 LLM 图片审核队列，一张图片只调用一次模型，同时评价技术质量、美观程度、图库适用性和候选角色
- 候选只接受已存在的启用角色主 tag，严格校验结构化 JSON、候选 ID、分数、flags 与最多 3 个角色，不使用宽松文本兜底
- 新增 `shadow`、`assist`、`auto_approve` 模式；影子和辅助模式不修改审核状态，高置信自动模式原子更新多个候选 tag
- 人工审核状态优先，模型结果过期时不会覆盖；低分辨率、模型异常、非法结果、低置信或随机抽检均保留人工待审
- 新图片可自动排队，历史积压只能由管理员受限执行；增加单轮、每日、超时、重试、预览尺寸和多维阈值配置
- QQ 审图卡在辅助/自动模式展示模型建议，新增 LLM 审图状态、执行和重试管理员命令
- 审图预览限制最长边并去除元数据，不向模型发送来源 URL、Cookie、token、`xsec_token` 或投稿人信息

### v0.18.0

- 新增基于固定版 `xiaohongshu-mcp` REST 的小红书最新图文增量采集，登录态由隔离提供者持有
- 小红书自动订阅只接受显式 `query / match / both` 平台词，详情不匹配时不会归入主 tag，原始话题不会自动建 tag
- 一篇笔记按结构化详情导入全部图片，不受通用 6 图上限影响；未知 CDN、错误 MIME、超出安全上限会失败
- 远程图片增加默认 25 MiB 下载硬上限，声明长度和实际读取长度都会检查
- 新增首次种子、单轮订阅/查询/详情/任务预算、独立查询水位、跨 tag 发现合并和持久化风控暂停
- QQ 管理命令可新增、查看、删除任意平台词，并可查询、执行、暂停、恢复小红书采集
- 群友随机审图支持选择 Pixiv 或小红书来源，审批和整图拒绝沿用对应平台来源
- 增加提供者契约、隔离 POC 记录和小红书专项回归测试

### v0.17.0

- 主 tag 增加 `character`、`pairing`、`theme`、`other` 类型和 `active`、`pending`、`archived` 状态；旧库按原 `is_character` 无损回填
- 陌生投稿 tag 改为可累计的待处理提案，不再自动创建角色主 tag 或在审核关闭时直接导入图片
- 新增 QQ tag 规范报告、提案通过 / 归并 / 拒绝、tag 类型和状态管理命令，均保留 `.pp` 与省略 `pp` 的入口
- 普通 tag 清理改为安全候选清理：有已通过图片、开放审核、alias、平台词、订阅、提案或身份候选依赖时一律保留
- 默认角色模式下，自动采集和同角色候选只处理启用角色主 tag；身份候选不再使用图片历史共现词，角色名重合阈值同步收紧
- 增加旧 schema 迁移、提案生命周期、投稿准入、安全清理、自动采集过滤和身份候选回归测试

### v0.16.0

- 新增进程内共享 `PixivAppClient`，复用 HTTPS Session 与有效 access token，并支持按 `platform_retry_times` 退避重试、`Retry-After`、401 单次刷新和 OAuth 完整失败后的短熔断
- 自动采集改为“订阅 + 查询词”独立游标，旧版父订阅游标仅迁移到主查询词，避免多查询词之间互相截断
- 新增持久化发现队列，搜索命中先落库再推进游标，单轮任务额度不足和插件重载后均可继续提交
- 自动采集与 Pixiv 历史回填使用幂等任务入口，同一规范化作品 URL 复用任务并合并 tag
- 数据库连接在每次事务提交或回滚后显式关闭，避免长期运行累积连接和 Windows 数据库文件锁
- 增加 OAuth 缓存与熔断、多查询词游标、发现队列续跑、旧游标迁移和任务幂等回归测试

### v0.15.2

- 为全部 45 个 `.pp` 中文主子命令增加省略 `pp` 的顶层等价入口，例如 `.tag合并`、`.随机审核` 和 `.面板地址`
- 保留 `.pp` / `.pjsk图库` 原入口、原参数解析、管理员权限和业务逻辑
- 顶层入口继续受 AstrBot 的 `.` / `。` 唤醒前缀约束，不响应无前缀普通聊天文本
- 子命令 alias 不提升到顶层，避免 `.help` 等名称与现有命令冲突
- 增加命令兼容层回归测试，覆盖 group 路径保留、alias 隔离和幂等注册

### v0.15.1

- QQ 审图卡片、帮助、用法错误、审核通知和重复图提示中的命令示例统一使用 `.` 前缀
- README 与配置说明同步使用 `.pp`、`.tg`、`.alias` 等实际部署前缀
- 增加用户可见命令提示回归测试，避免重新出现斜杠命令示例

### v0.15.0

- 新增面向所有群友的 `.pp 随机审核 [候选tag]`、`.pp 抽审` 图片级 Pixiv 随机审核入口
- 新增 `.pp 审图通过 <最终tag>`、`.pp 审图拒绝 [原因]`、`.pp 审图跳过`、`.pp 审图当前` 和 `.pp 审图结束`
- 群友审核按消息来源与 QQ 用户隔离短期会话，同一图片不会同时分配给多个群友，领取过期后自动释放
- 通过时复用整图审核事务，批准指定现有主 tag 并拒绝其他候选；整图拒绝会记录 Pixiv 拒绝来源
- 增加提交前开放状态复核，避免 QQ 与 WebUI 并发处理时覆盖已经完成的审核
- 增加群友审图开关、领取 TTL、最近去重、来源词展示数量和自动下一张配置

### v0.14.14

- WebUI 图片文件接口改为 path-only 响应，避免缩略图加载时为每张图构建完整详情
- WebUI 图片文件响应增加浏览器缓存头，降低重复打开页面时的文件读取压力
- 图库列表接口跳过批量文件状态同步，完整校验保留在详情和文件读取路径
- 前端图库与 Pixiv 审批列表增加过期响应保护，避免快速翻页或搜索时旧结果覆盖新结果

### v0.14.13

- 新增 `.pp 帮助`，支持投稿、tag、审核、采集等分组帮助，便于 QQ 内快速查看常用命令
- README 修正 `.pp tag合并` 参数方向，统一为目标 tag 在前、来源 tag 在后
- README 补充投稿时顺手添加 alias 的写法

### v0.14.12

- Pixiv 审核页补充查询索引，并将列表改为轻量加载、预览再加载来源词，降低大量待审图片下的加载压力
- Pixiv 自动采集不再默认使用普通 alias 搜索，改为显式 Pixiv 平台词和内置官方 Pixiv 词映射
- 自动采集与历史回填取消 substring 归属判断，避免 `rin` 命中 `Durin` 等误抓
- 新增 `.pp 重复忽略 <id1> <id2> [原因]`，人工确认两张图不是重复后可过滤后续疑似重复提示
- 新增 `.pp 重复恢复` 与 `.pp 重复忽略列表` 便于恢复和查看重复忽略记录

### v0.14.11

- 移除 `看看<序号>` / `.pp 看看 <序号>` 最近展示列表序号自查功能
- 发图结果和审核列表不再写入或提示 `自查：看看N`
- `看看id<image_id>` 图片 ID 查看入口改用独立配置项 `image_id_lookup_enabled` / `image_id_lookup_admin_only`

### v0.14.10

- 新增 `看看id<image_id>` / `看看 ID <image_id>` 自然语言图片 ID 查看入口
- `看看id123` 会优先按数据库图片 ID 查图，不再被当成 `id123` tag 静默吞掉

### v0.14.9

- 补充 `AGPL-3.0-or-later` 开源许可声明与 `LICENSE` 文件
- README 明确许可仅覆盖插件代码与文档，不覆盖第三方素材、Pixiv 作品或用户投稿图片

### v0.14.8

- 修复自然语言发图如 `看看初音未来` 可能先被群聊增强插件触发 LLM 回复、再由图库发图的问题
- 普通发图结果不再无条件展示 `自查：看看1`，仅在当前发送者有自查权限时提示

### v0.14.7

- 新增 `.pp 采集诊断`，集中查看采集 worker、Pixiv 自动采集、任务状态、最近失败和历史回填队列
- 新增 `.pp 失败列表 [platform]` 与 `.pp 失败重试 <job_id|全部>`，便于直接处理失败采集任务
- 新增 `看看<序号>` / `.pp 看看 <序号>` 管理员自查入口，可按当前会话最近展示图片序号查看图片详情

### v0.14.6

- Pixiv 采集 include / exclude 过滤支持展开主 tag、alias、平台词和内置常用 Pixiv 查询词
- Pixiv 历史回填创建采集任务时会把实际 Pixiv 查询词纳入 include 规则，减少中文主 tag 与 Pixiv 日文 tag 不一致导致的误跳过
- 采集任务页和 Pixiv 审批页新增静默自动刷新，新入审图片可更快进入审批列表

### v0.14.5

- 新增 Pixiv 历史回填任务，支持按 tag 逐页扫描旧搜索结果并创建普通采集任务
- 采集页新增历史回填面板，可设置页数、扫描结果和新增任务上限，并查看回填任务统计
- 新增 `.pp 历史回填添加` 和 `.pp 历史回填列表` 管理命令
- 后端新增历史回填任务表与后台 worker，避免污染日常自动采集的 `last_seen` 增量状态

### v0.14.4

- tag 身份候选的角色名重合规则改为约 40% CJK 字符重合即可进入待确认候选
- tag 归并页的历史候选默认收起，可按需显示并可一键取消显示
- 配置与 README 明确 Pixiv 自动采集是最新增量采集，不是全量历史回填

### v0.14.3

- tag 归并页新增“待手动确认”候选池，可扫描疑似同一角色 tag 并保留 Pixiv 证据和 LLM 复核结论
- 新增 tag 身份候选接口，候选只用于人工确认，不会自动执行归并
- 采集页 Pixiv 空 URL 改为按输入 tag 搜索预览，勾选作品后再批量创建真实采集任务
- Pixiv 平台词类型统一为 `query` / `match` / `both`：`query` 用于搜图，`match` 用于归属判断

### v0.14.2

- WebUI 状态枚举改为中文展示，图片检索默认显示已通过图片
- 图片检索和 Pixiv 审批增加页码、跳页和每页数量切换
- 移除重复的“审核任务”前端页面，Pixiv 图片审核统一在 Pixiv 审批页完成
- Pixiv 审批卡片和预览弹窗改为完整图显示，tag 多时在局部区域内滚动

### v0.14.1

- WebUI 访问入口改为稳定地址，不再需要 `?v=版本号` 查询参数
- 前端启动时会自动清理历史缓存参数 `?v=...`，旧链接仍可打开并跳转到稳定地址
- 侧边栏不再硬编码 WebUI 版本号，避免日常入口和版本发布节奏绑定

### v0.14.0

- 独立 WebUI 从 `core/webui.py` 内嵌 HTML/CSS/vanilla JS 重构为 Vue 3 + Vite + TypeScript 单页应用
- WebUI 采用工作台式布局，统一导航、筛选区、列表、预览弹窗、toast、loading 和状态反馈
- 保留现有 aiohttp `/api/*` 协议，插件普通使用者仍只需要 Python 运行依赖
- 新增 `webui/` 前端源码工程，构建产物随插件发布到 `core/webui_static/`

### v0.13.1

- Pixiv 审批页新增角色 / alias / Pixiv tag 搜索，输入 `mzk` 等 alias 可筛出对应角色相关待审图
- 搜索命中主 tag 后会展开 alias、Pixiv 平台词和历史来源中的同义角色候选，便于集中处理同一角色
- Pixiv 审批页交互改为先筛选、再选图、再批量审核，批量审核区未选图时默认收起
- `/api/pixiv-review-images` 新增 `keyword` 参数，并返回 `search_context` 解释命中来源

### v0.13.0

- Pixiv 审批页候选 tag 改为“归入主 tag”单选，避免同一图片被打上多个同义主 tag
- Pixiv 来源 tag 改为 alias / Pixiv 搜索词多选，审核通过时沉淀到规范主 tag
- 审核提交兼容历史多主 tag 输入，第一个作为规范主 tag，其余作为同义词归并或沉淀
- 同义词已经是独立 tag 时会归并到规范主 tag，并迁移图片关系、审核任务、alias、平台词和自动搜图订阅

### v0.12.9

- Pixiv 审批页单张“拒绝图片”改为直接执行，不再弹出浏览器确认框

### v0.12.8

- 修复 Pixiv 大图预览里新增主 tag 时读到背后卡片空输入框的问题
- 新增主 tag 表单现在按卡片区和预览区分别定位，避免同图多处渲染时 id 冲突

### v0.12.7

- Pixiv 审批页候选主 tag 区新增手动添加入口，新增或复用主 tag 后会自动选中
- Pixiv 审批通过时继续沿用已选来源 tag 沉淀平台词映射，便于后续来源搜图使用 Pixiv 原始词
- Pixiv 审批页新增“拒绝图片”动作，可将整张 Pixiv 图标记为不收并立即移出当前队列
- 新增 Pixiv 来源拒绝记录，自动来源搜图和手动采集会跳过已拒绝来源

### v0.12.6

- Pixiv 审批页默认只显示 `pending / uncertain` 待处理队列，`rejected` 需要在筛选器中显式查看
- Pixiv 审批列表接口按当前筛选构造卡片内的“当前审核项”，避免历史 rejected 任务混入默认队列造成误判
- 单张 / 批量审核成功后，图片会立即从当前审批队列移除，并改用非阻塞提示展示结果
- 刷新审批列表时会清理已不存在图片的勾选状态，避免批量选择计数残留

### v0.12.5

- Pixiv 采集将 `x_restrict` / `illust_ai_type` 等限制级与 AI 元数据补成过滤 tag，避免 `R-18`、`AI生成` 未出现在普通 tag 列表时漏过
- 默认采集排除项扩展为 `R-18,R-18G,AI,AI生成,AI-generated`
- WebUI 改为功能导航页，初次打开只加载当前页数据，图库、审核、采集、tag、Pixiv 审批、平台词和 tag 归并分开查看

### v0.12.4

- 修复图片检索卡片未接入大图预览入口的问题，缩略图和“预览”按钮现在会打开通用大图预览
- 图片检索卡片与通用大图预览补齐待审批任务的“通过 / 拒绝”操作

### v0.12.3

- 修复 WebUI 批量输入解析正则在运行时被换行转义破坏，导致管理台脚本无法执行的问题

### v0.12.2

- 修复图片级人工审核在同一 `(image_id, tag_id)` 存在多来源记录时，`reject_unselected` 可能误伤非当前来源 tag 的问题
- 修复手动审核的事务边界，将任务读取、任务更新与图片 tag 状态更新收口到单次数据库事务中
- 调整独立 WebUI 访问控制：移除 `?token=` URL 访问方案，改为登录页 + HttpOnly Cookie 会话，同时保留 `X-PJSK-Token` / `Authorization: Bearer`
- 修复 `datetime.utcnow()` 兼容性告警，统一改为带时区 UTC 时间
- 修复插件配置页 `_conf_schema.json` 中文说明乱码
- 移除仓库内仅面向插件目录整理的说明，收口为通用仓库描述

### v0.12.1

- 修正 `metadata.yaml` 的作者与仓库信息
- 新增 `requirements.txt` 与 `logo.png`
- README 改为兼容独立插件仓 / AstrBot 运行仓两种阅读场景
- 插件仓将同步整理为“仓库根目录即插件根目录”的结构

### v0.12.0

- Pixiv 审批页新增图片多选、批量审核预览、批量确认审核能力
- Pixiv 平台词管理页新增批量确认映射预览 / 提交能力，可对未解决词批量挂到指定主 tag
- 新增历史 tag 归并助手，支持候选归并对、影响预览与直接执行归并
- tag 归并流程补齐 `image_tags / review_tasks / tag_aliases / platform_tag_terms / crawl_subscriptions` 同步迁移

### v0.11.1

- 新增 Pixiv 平台词独立管理页，支持查看 / 搜索 / 新增 / 删除 / 修改 Pixiv query / match / both 映射
- 支持按主 tag 查看历史建议词，并可从建议词直接采纳到当前主 tag
- 支持查看未解决的 Pixiv 来源词，并从候选主 tag 直接一键沉淀平台词
- WebUI 与平台词 API 已联动 Pixiv 审批页，人工审核后可即时刷新映射治理结果

### v0.11.0

- 新增 Pixiv Web 审批页，支持按图片聚合查看待审图
- 支持在卡片页 / 预览页直接点选主 tag 与 Pixiv 来源词后提交人工审核
- 新增 Pixiv 平台 tag 映射层，并允许从人工审核与历史 Pixiv tag 中沉淀 query / match 词
- Pixiv 自动采集改为优先使用平台映射词查询与匹配

### v0.10.0

- 主 tag / canonical tag 治理完善
- 新增 tag 合并、主 tag 切换、tag 清理相关能力
- 强化抓图后的 tag 收口与图库整理

### v0.9.8

- 优化去重逻辑
- 调整免 token 访问场景下的 WebUI 体验

### v0.9.7

- 开放 WebUI 使用
- 补充审核图片预览能力

### v0.9.5

- 修复 Docker 代理环境下 Pixiv 自动采集问题

### v0.9.0

- 新增 Pixiv 自动按 tag 采集
- 整理自动采集相关仓库结构

### v0.5.7

- 修复 pjsk WebUI 端口冲突问题

### v0.5.6

- 增强投稿审核命令

### v0.5.5

- 新增投稿管理员通知

### v0.5.4

- 修复 `.tg` 投稿触发

### v0.5.3

- 优化投稿自动建 tag 与别名

### v0.5.2

- 调整未命中时的静默行为
- 完成一轮回归收口

### v0.5.1

- 新增用户投稿能力

### v0.5.0

- 独立 WebUI 改造
- 新增面板地址命令
- 使 WebUI 与 AstrBot Dashboard 解耦
- v0.23.0：主聊天可在指定群中顺手收录当前请求实际包含的 PJSK 图片，支持多角色及已有 CP/团体 tag；默认群白名单为 `822728596`、`1067608954`。收图只复用已有主聊天 LLM 请求，不单独触发模型，成功后追加一次汇总。

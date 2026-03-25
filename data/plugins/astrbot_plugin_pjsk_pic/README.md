# astrbot_plugin_pjsk_pic

PJSK 图片图库插件。

## 1. 插件简介

这个插件用于给 AstrBot 提供“图片图库”能力，解决以下问题：

- 用户通过自然语言或 LLM Tool 请求发图
- 从本地图库按 tag / alias 随机发图
- 用户通过投稿命令附图入库
- 从多平台采集图片并入库
- 对角色 tag 做自动审核 / 人工审核
- 在独立 WebUI 中管理图库、来源、tag、审核任务和采集任务

当前支持的平台重点为：

- 本地图库
- Pixiv
- X / Twitter
- 小红书
- generic / lofter 兼容采集

## 2. 当前功能

### 发图能力

- 自然语言规则触发
- LLM Tool 发图
- tag / alias 匹配
- 会话级简单去重
- 同 sha256 图片多路径下的可用路径自动回退

### 图库管理

- SQLite 索引
- 本地图库扫描
- tag / alias 管理
- 角色 tag 标记
- 图片来源记录
- 感知哈希重复图识别

### 采集能力

- 采集任务队列
- 图片下载入库
- 用户投稿入库
- Pixiv / X / 小红书专用适配增强
- tag 清洗与黑名单
- 候选图部分失败容错

### 审核能力

- 角色 tag 自动审核接入
- 人工审核命令

### WebUI

- 图片搜索与预览
- 来源信息查看
- tag 管理
- 审核任务查看与处理
- 采集任务查看、新建、重试

## 3. 命令 / 触发方式

### 自然语言触发

支持类似：

- `看看初音未来`
- `来张miku`
- `发一张宁宁`

### 用户投稿

支持类似：

- `投稿 初音未来` + 附图
- `tg miku` + 附图
- `投稿 晓山瑞希 别名:瑞希,mzk,mizuki,糖` + 附图
- `tg 晓山瑞希 alias 瑞希,mzk,mizuki,糖` + 附图

当前 MVP 规则：

- 需要在同一条消息里附带图片
- 当前仅支持单图投稿
- 也支持 AstrBot 原生命令前缀：`/投稿`、`/tg`
- 支持“文字在前图片在后”与“图片在前文字在后”
- 投稿主 tag 不存在时，会自动创建并按角色 tag 处理
- 可在投稿时通过 `别名:` / `alias` 一次性补充多个别名，多个别名建议用逗号分隔
- 投稿图片会复用现有导入、去重、审核、入库流程
- 投稿成功后的“已收录 / 等待审核”回执会直接发在当前对话里
- 同时可额外把“收到新投稿 #review_id”通知发给 AstrBot 管理员或指定白名单

### 管理命令

- `/pjsk图库 重扫`
- `/pjsk图库 统计`
- `/pjsk图库 查看 <tag>`
- `/pjsk图库 别名添加 <tag> <alias>`
- `/pjsk图库 别名删除 <tag> <alias>`
- `/pjsk图库 别名查看 <tag>`
- `/pjsk图库 角色标记 <tag> <true|false>`
- `/pjsk图库 采集添加 <platform> <url> [tags_csv]`
- `/pjsk图库 采集列表`
- `/pjsk图库 采集重试 <job_id>`
- `/pjsk图库 审核列表 [status]`
- `/pjsk图库 审核查看 [review_id]`
- `/pjsk图库 审核通过 <review_id>`
- `/pjsk图库 审核拒绝 <review_id>`
- `/pjsk图库 投稿审核状态`
- `/pjsk图库 投稿审核开启`
- `/pjsk图库 投稿审核关闭`
- `/pjsk图库 面板地址`

## 4. 配置说明

主要配置项：

- `library_root`
  - 本地图库根目录；留空时默认使用插件数据目录下的 `library/`
- `scan_on_startup`
  - AstrBot 启动后是否自动扫描本地图库
- `allow_fuzzy_match`
  - 是否允许 tag / alias 模糊匹配
- `recent_dedupe_count`
  - 每个会话最近去重的图片数量
- `enable_llm_tool`
  - 是否开启 LLM 发图工具
- `webui_enabled`
  - 是否启用独立 WebUI
- `webui_host`
  - 独立 WebUI 监听地址；默认 `0.0.0.0`，可改成 `127.0.0.1`
- `webui_port`
  - 独立 WebUI 监听端口
- `webui_access_token`
  - 独立 WebUI 可选访问令牌
- `submission_notify_enabled`
  - 是否启用投稿后的管理员/白名单通知
- `submission_notify_use_astr_admins`
  - 是否默认通知 AstrBot 全局 `admins_id`
- `submission_notify_targets`
  - 额外通知目标；支持逗号/分号/换行分隔的用户 ID，或直接填写 unified_msg_origin
- `submission_review_enabled`
  - 是否启用投稿审核；关闭后新投稿默认直接入库并参与发图
- `crawler_max_candidates`
  - 每个采集任务最多解析 / 下载的候选图数量
- `platform_request_timeout`
  - 平台请求超时秒数
- `platform_retry_times`
  - 平台采集失败后自动重试次数
- `max_tags_per_image`
  - 每张图最多写入的 tag 数量
- `tag_blacklist`
  - 额外 tag 黑名单
- `enable_phash_dedupe`
  - 是否启用感知哈希重复识别
- `enable_auto_review`
  - 是否启用多模态自动审核
- `review_provider_id`
  - 自动审核使用的 provider id

完整配置见：

- `_conf_schema.json`

## 5. WebUI 说明

插件启动后会拉起 **独立 WebUI 服务**。

默认配置：

- 监听地址：`0.0.0.0`
- 监听端口：`9099`

你也可以改为仅本机访问：

- `webui_host=127.0.0.1`

若配置了 `webui_access_token`，可通过：

- `?token=你的令牌`
- 请求头 `X-PJSK-Token: 你的令牌`

进行访问。

管理员可通过命令查看当前访问地址：

- `/pjsk图库 面板地址`

## 6. 数据说明

插件运行数据目录：

- `data/plugin_data/astrbot_plugin_pjsk_pic/`

主要内容：

- `image_index.db`
  - SQLite 数据库
- `library/`
  - 本地图库根目录
- `images/imported/`
  - 采集 / 投稿导入图片目录

数据库中当前会同时维护：

- 逻辑图片记录
- 物理文件位置记录

这样可以避免同一张图在“本地图库 / 导入目录”双来源下互相覆盖可用路径。

## 7. TODO / 后续计划

后续优先事项：

1. 优化 WebUI 检索接口，减少 N+1 查询
2. 将角色 tag 判断改成更保守的策略
3. 新增用户投稿历史与来源筛选
4. 支持多图投稿与频率限制
5. 清理未真正落地的预留配置项
6. 为采集适配器补测试样本
7. 给投稿通知增加更细的模板与分平台目标管理
8. 给审核命令补更多批量处理能力

## 8. 当前版本

- 当前插件版本：`0.5.6`

## 9. 更新记录

### v0.5.6

投稿审核命令增强版：

- 新增 `/pjsk图库 审核查看 [review_id]`，可直接查看当前待处理审核图片
- `/pjsk图库 审核列表` 默认改为查看待处理审核任务
- 新增 `/pjsk图库 投稿审核状态`、`/pjsk图库 投稿审核开启`、`/pjsk图库 投稿审核关闭`
- 关闭投稿审核后，新投稿默认直接入库并参与发图，不再创建审核任务

### v0.5.7

独立 WebUI 端口冲突修复版：

- 将 `pjsk_pic` 独立 WebUI 默认端口从 `6199` 调整为 `9099`
- 避免与 `aiocqhttp` 反向 WS 默认端口冲突导致 AstrBot 启动时报 `Address already in use`

### v0.5.5

投稿通知版：

- 保持投稿成功后的回执继续直接发在当前会话里
- 新增“收到新投稿 #编号”主动通知，可默认发给 AstrBot 全局管理员
- 新增 `submission_notify_enabled`、`submission_notify_use_astr_admins`、`submission_notify_targets` 配置项
- 支持把额外白名单目标写成用户 ID 或 unified_msg_origin

### v0.5.4

投稿指令补丁版：

- 补齐 AstrBot 原生命令 `/投稿` / `/tg` 的投稿入口，避免只靠正则匹配导致的漏触发
- 保留无前缀 `投稿 ...` / `tg ...` 的文本投稿方式
- 命令模式下缺少 tag 时会给出明确提示

### v0.5.3

投稿增强版：

- 投稿命中不存在的主 tag 时，会自动创建主 tag 并按角色 tag 处理
- 支持在投稿时通过 `别名:` / `alias` 一次性补充多个别名
- 投稿补别名时会自动跳过重复项，并拦截与现有 tag / alias 的冲突
- 补充投稿自动建 tag、投稿补别名、别名冲突拦截的 smoke test 验证

### v0.5.2

交互收口版：

- “看看xx / 来张xx / 发一张xx” 在未命中 tag 时改为静默不回复
- 自然语言发图命中本插件规则后会直接消费事件，避免继续触发其它回复链路
- 补充独立 WebUI、投稿、人工审核回写的 smoke test 验证

### v0.5.1

用户投稿版：

- 新增 `投稿 <角色tag>` / `tg <角色tag>` + 附图 的用户投稿能力
- 投稿图片复用现有导入、去重、审核、入库流程
- 投稿来源记录为 `submission`
- 修复人工审核命令对 `submission` 来源的状态回写

### v0.5.0

独立 WebUI 版：

- 将原 Dashboard 内嵌 WebUI 改为独立 WebUI 服务
- 支持 `0.0.0.0` / `127.0.0.1` 切换监听地址，默认局域网可访问
- 新增 `webui_enabled`、`webui_host`、`webui_port`、`webui_access_token` 配置项
- 新增管理员命令 `/pjsk图库 面板地址`
- 移除对 `quart` Dashboard 内嵌接口的依赖，降低对 AstrBot 本地稳定性的影响

### v0.4.1

稳定性整改版：

- 新增图片物理文件位置跟踪，修复同 sha256 多路径覆盖风险
- 支持当前路径失效时自动回退到其他可用路径
- 修复采集任务中单候选失败拖垮整任务的问题
- 增加本地图库扫描失败日志
- 移除 `imghdr` 依赖，兼容 Python 3.14

### v0.4.0

平台采集增强版：

- 增强 Pixiv 作品页图片提取
- 增强 X 推文图片提取
- 增强小红书分享链接 / 笔记图片提取
- 新增 tag 黑名单与 tag 清洗
- 新增轻量感知哈希重复图识别
- 增强采集任务重试与平台请求配置
- WebUI 增加来源、phash、疑似重复信息展示

### v0.3.0

WebUI 管理版：

- 新增 Dashboard 内嵌 WebUI
- 新增图片检索与预览
- 新增 tag / alias / 角色标记管理
- 新增审核任务与采集任务页面

### v0.2.0

采集与审核基础版：

- 新增采集任务队列
- 新增下载入库与来源记录
- 新增角色 tag 自动审核接入点
- 新增人工审核命令

### v0.1.0

初版 MVP：

- 新建插件骨架
- 本地图库扫描入库
- SQLite 索引
- tag / alias 随机发图
- 自然语言规则触发
- LLM Tool 发图
- 基础管理命令

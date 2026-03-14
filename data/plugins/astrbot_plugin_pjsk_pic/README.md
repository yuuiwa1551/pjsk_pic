# astrbot_plugin_pjsk_pic

PJSK 图片图库插件。

## 插件作用

这个插件用于给 AstrBot 提供“图片图库”能力，主要包括：

1. 用户自然语言请求发图
   - 例如：看看初音未来、来张miku、发一张宁宁
2. 从本地图库按 tag / alias 随机发图
3. 多平台采集图片并入库
   - 当前重点支持：Pixiv、X、小红书
4. 自动审核角色 tag
5. 提供 Dashboard 内 WebUI 管理图库、tag、审核任务和采集任务

## 当前版本

- 当前插件版本：`0.4.0`

## 功能概览

### 发图能力

- 自然语言规则触发
- LLM Tool 发图
- tag / alias 匹配
- 会话级简单去重

### 图库管理

- SQLite 索引
- 本地图库扫描
- tag / alias 管理
- 角色 tag 标记

### 采集能力

- 采集任务队列
- 图片下载入库
- 来源记录
- Pixiv / X / 小红书专用适配增强
- tag 清洗与黑名单
- 感知哈希重复图识别

### 审核能力

- 角色 tag 自动审核接入
- 人工审核命令

### WebUI

- 图片搜索与预览
- 来源信息查看
- tag 管理
- 审核任务查看与处理
- 采集任务查看、新建、重试

## 命令

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
- `/pjsk图库 审核通过 <review_id>`
- `/pjsk图库 审核拒绝 <review_id>`

## WebUI

插件启动后会注册 WebUI：

- `/api/plug/pjsk_pic/ui`

可在 AstrBot Dashboard 登录后访问。

## 版本更新记录

### v0.1.0

初版 MVP：

- 新建插件骨架
- 本地图库扫描入库
- SQLite 索引
- tag / alias 随机发图
- 自然语言规则触发
- LLM Tool 发图
- 基础管理命令

### v0.2.0

采集与审核基础版：

- 新增采集任务队列
- 新增下载入库与来源记录
- 新增角色 tag 自动审核接入点
- 新增人工审核命令

### v0.3.0

WebUI 管理版：

- 新增 Dashboard 内 WebUI
- 新增图片检索与预览
- 新增 tag / alias / 角色标记管理
- 新增审核任务与采集任务页面

### v0.4.0

平台采集增强版：

- 增强 Pixiv 作品页图片提取
- 增强 X 推文图片提取
- 增强小红书分享链接 / 笔记图片提取
- 新增 tag 黑名单与 tag 清洗
- 新增轻量感知哈希重复图识别
- 增强采集任务重试与平台请求配置
- WebUI 增加来源、phash、疑似重复信息展示

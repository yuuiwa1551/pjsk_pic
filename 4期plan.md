# 第4期计划：Pixiv / X / 小红书平台采集增强

## 完成情况

- [x] 已完成并落地

## 目标
把当前“通用公开页 / 直链采集”增强成更像正式可用的多平台采集能力，优先支持：

1. Pixiv
2. X
3. 小红书

本期明确先不做：

- lofter 专用适配

## 本期原则

1. 优先“借鉴成熟开源方案的思路”，不直接照搬 GPL 代码
2. 优先做“能稳定拿到图片与基础元数据”的链路
3. 优先做“导入质量提升”，避免错误 tag 和重复图污染图库
4. 平台能力按“可维护”优先，不追求一步到位做成重度反爬系统

## 开源参考方向

### Pixiv
- 参考方向：pixivpy
- 借鉴点：
  - refresh token 登录思路
  - 插画详情获取
  - 多图作品解析
  - 标签、作者、作品元数据组织

### X
- 参考方向：twscrape
- 借鉴点：
  - 登录态管理
  - tweet 详情 / user media / 搜索结果组织
  - 限流与请求失败重试策略

### 小红书
- 参考方向：xiaohongshu-importer、xiaohongshu-mcp
- 借鉴点：
  - 分享链接标准化
  - 帖子图片、标题、tag 提取
  - 登录态 / 页面驱动思路

## 本期范围

### 1. 平台专用适配器

新增三个平台专用 adapter：

1. pixiv_adapter
2. x_adapter
3. xiaohongshu_adapter

目标：

- 不再只依赖通用 meta 抓图
- 能按平台页面结构提取多图
- 能补充平台特有元数据

### 2. Pixiv 增强

目标能力：

1. 支持作品详情页解析
2. 支持单图 / 多图作品
3. 提取：
   - 作品 id
   - 标题
   - 作者
   - 原始 tags
   - 原图 / 多页图 URL
4. 支持 pixiv 图片 referer 处理
5. 为后续接 refresh token 登录预留配置位

本期实现建议：

- 先做“作品链接 -> 多图解析 -> 下载”
- 登录态增强放本期后半或下期

### 3. X 增强

目标能力：

1. 支持 tweet 链接解析
2. 提取推文中的多张图片
3. 提取：
   - tweet id
   - 作者
   - 文本
   - hashtag
   - 图片 URL
4. 支持 x / twitter 域名兼容
5. 为 cookie / 账号池 / 限流预留配置位

本期实现建议：

- 先做“公开 tweet 链接 / 已可访问内容”的图片提取
- 登录态和更强抓取能力先预留接口

### 4. 小红书增强

目标能力：

1. 支持分享链接标准化
2. 支持笔记详情页图片提取
3. 提取：
   - note id
   - 作者
   - 标题
   - 原始 tag / 话题
   - 多张图片 URL
4. 区分封面图和正文图
5. 为 cookie / 页面驱动预留配置位

本期实现建议：

- 先做“分享链接 -> 标准化 -> 笔记图片提取”
- 登录态和更强反爬适配后置

### 5. 采集质量增强

1. tag 清洗
   - 去掉明显无意义 tag
   - 过滤平台公共 tag
   - 拆分 hashtag
   - 支持 tag 黑名单

2. 多图帖子处理
   - 每张图单独入库
   - 同一帖子来源关联到多张图片
   - 公共 tag 与候选角色 tag 分开存

3. 去重增强
   - 保留 sha256 去重
   - 新增 phash 预留 / 初步实现
   - 支持识别重复导入图

### 6. 任务与配置增强

新增配置建议：

1. pixiv_refresh_token
2. x_cookie_string
3. x_account_pool_enabled
4. xiaohongshu_cookie_string
5. platform_request_timeout
6. platform_retry_times
7. tag_blacklist
8. enable_phash_dedupe

任务增强：

1. 失败重试次数
2. 错误分类
3. 平台级限流
4. 采集日志更详细

## 代码结构建议

建议新增：

1. `core/adapters/pixiv_adapter.py`
2. `core/adapters/x_adapter.py`
3. `core/adapters/xiaohongshu_adapter.py`
4. `core/tag_cleaner.py`
5. `core/phash.py`

现有模块扩展：

1. `crawl_service.py`
   - 平台分发
   - 重试与限流
   - 多图帖子处理增强

2. `importer.py`
   - phash 计算
   - 平台来源信息扩展

3. `db.py`
   - 增加平台特有 source 字段或 extra_json 规范
   - 增加 phash 相关查询

## 开发顺序

### Step 1
- 抽离 adapters 子目录
- 接好 pixiv / x / 小红书三个专用 adapter 骨架

### Step 2
- 先完成 pixiv 作品页解析与多图下载

### Step 3
- 完成 X tweet 多图解析

### Step 4
- 完成小红书分享链接标准化与笔记图提取

### Step 5
- 做 tag 清洗、黑名单、公共 tag 过滤

### Step 6
- 做 phash 去重与重复检测

### Step 7
- 给 WebUI 增加平台来源信息展示与错误日志展示

## 验收标准

1. Pixiv 作品链接能正确抓到单图 / 多图
2. X 推文链接能正确抓到多张图片
3. 小红书分享链接能正确解析并抓到图片
4. 入库时能记录平台来源元数据
5. tag 黑名单和基础 tag 清洗生效
6. 出现重复图时能在入库阶段识别
7. 失败任务可明确看到失败原因

## 本期产出

1. 第4期平台专用采集增强代码
2. 新增配置项
3. 平台采集测试样例
4. plan.md 更新第4期完成情况
